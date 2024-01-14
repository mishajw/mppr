from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Generic, TypeVar

import pandas as pd
import tqdm

from mppr.io.base import IoMethod
from mppr.io.creator import ToType, create_io_method

T = TypeVar("T")
NewT = TypeVar("NewT")
JoinT = TypeVar("JoinT")


def init(
    stage_name: str,
    base_dir: Path,
    init_fn: Callable[[], dict[str, T]],
    to: ToType[T],
) -> "Mappable[T]":
    """
    Creates a mappable object from an init function.

    The function is only called if the stage does not exist.

    Args:
        stage_name: The name of the stage.
        base_dir: Where the stages are saved out to.
        init_fn: The function to call to initialize the stage.
        to: The class of the values in the stage. Used for deserialization.
    """

    base_dir.mkdir(parents=True, exist_ok=True)
    io_method = create_io_method(base_dir, stage_name, to)
    read_values = io_method.read()
    if read_values is not None:
        return Mappable(values=read_values, base_dir=base_dir)

    values = init_fn()
    with io_method.create_writer() as writer:
        for key, value in values.items():
            writer.write(key, value)
    return Mappable(values, base_dir)


def load(stage_name: str, base_dir: Path, to: ToType[T]) -> "Mappable[T]":
    """
    Loads a previously created stage.

    Args:
        stage_name: The name of the stage.
        base_dir: Where the stages are saved out to.
        to: The class of the values in the stage. Used for deserialization.
    """

    io_method = create_io_method(base_dir, stage_name, to)
    values = io_method.read()
    assert values is not None, f"Stage {stage_name} does not exist"
    return Mappable(values, base_dir)


@dataclass
class Mappable(Generic[T]):
    values: dict[str, T]
    base_dir: Path

    def map(
        self,
        stage_name: str,
        fn: Callable[[str, T], NewT],
        to: ToType[NewT],
    ) -> "Mappable[NewT]":
        """
        Maps a function over the values in the stage.

        Resumable: If the stage already exists, the function is only called on
        the values that have not been processed yet.

        Args:
            stage_name: The name of the stage.
            fn: The function to call on each value.
            to: The class of the values in the stage. Used for deserialization.
        """

        self.base_dir.mkdir(parents=True, exist_ok=True)
        io_method = create_io_method(self.base_dir, stage_name, to)
        mapped_values: dict[str, NewT] = self._load_for_map(io_method)

        with io_method.create_writer() as writer:
            with tqdm.tqdm(
                total=len(self.values),
                initial=len(mapped_values),
                desc=stage_name,
            ) as pbar:
                for key, value in self.values.items():
                    if key not in mapped_values:
                        mapped_value = fn(key, value)
                        mapped_values[key] = mapped_value
                        writer.write(key, mapped_value)
                        pbar.update(1)

        return Mappable(mapped_values, self.base_dir)

    async def amap(
        self,
        stage_name: str,
        fn: Callable[[str, T], Awaitable[NewT]],
        to: ToType[NewT],
    ) -> "Mappable[NewT]":
        """
        Asynchronous version of map.
        """

        self.base_dir.mkdir(parents=True, exist_ok=True)
        io_method = create_io_method(self.base_dir, stage_name, to)
        mapped_values: dict[str, NewT] = self._load_for_map(io_method)

        with io_method.create_writer() as writer:
            with tqdm.tqdm(
                desc=stage_name,
                total=len(self.values) - len(mapped_values),
                initial=len(mapped_values),
            ) as pbar:
                for key, value in self.values.items():
                    if key not in mapped_values:
                        mapped_value = await fn(key, value)
                        mapped_values[key] = mapped_value
                        writer.write(key, mapped_value)
                        pbar.update(len(mapped_values))

        return Mappable(mapped_values, self.base_dir)

    def _load_for_map(
        self,
        io_method: IoMethod[NewT],
    ) -> dict[str, NewT]:
        read_values = io_method.read()
        if read_values is None:
            return {}
        return {key: value for key, value in read_values.items() if key in self.values}

    def join(
        self,
        other: "Mappable[JoinT]",
        fn: Callable[[str, T, JoinT], NewT],
    ) -> "Mappable[NewT]":
        """
        Joins two mappable objects together.

        N.B.: This operation is *not* cached.
        """
        return Mappable(
            {
                key: fn(key, self.values[key], other.values[key])
                for key in self.values.keys()
                if key in other.values
            },
            self.base_dir,
        )

    def flat_map(
        self,
        fn: Callable[[str, T], dict[str, NewT]],
    ) -> "Mappable[NewT]":
        """
        Maps each row into multiple rows.

        N.B.: This operation is *not* cached.
        """
        return Mappable(
            {
                new_key: new_value
                for key, value in self.values.items()
                for new_key, new_value in fn(key, value).items()
            },
            self.base_dir,
        )

    def get(self) -> list[T]:
        """
        Gets the values in the map.
        """
        return list(self.values.values())

    def limit(self, n: int) -> "Mappable[T]":
        """
        Limits the number of values in the map.
        """
        return Mappable(dict(list(self.values.items())[:n]), self.base_dir)

    def to_dataframe(
        self,
        fn: Callable[[T], dict[str, Any]],
    ) -> pd.DataFrame:
        """
        Creates a dataframe out of the mappable.
        """
        return pd.DataFrame(
            [fn(value) for value in self.values.values()],
            index=list(self.values.keys()),
        )
