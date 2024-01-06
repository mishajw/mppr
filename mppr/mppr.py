from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable, Generic, TypeVar

import tqdm

from mppr.io.creator import create_io_method

T = TypeVar("T")
NewT = TypeVar("NewT")


def init(
    stage_name: str,
    base_dir: Path,
    init_fn: Callable[[], dict[str, T]],
    to: type[T],
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


def load(stage_name: str, base_dir: Path, to: type[T]) -> "Mappable[T]":
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
        to: type[NewT],
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
        mapped_values: dict[str, NewT] = {}

        read_values = io_method.read()
        if read_values is not None:
            mapped_values.update(read_values)

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
        to: type[NewT],
    ) -> "Mappable[NewT]":
        """
        Asyncronous version of map.
        """

        self.base_dir.mkdir(parents=True, exist_ok=True)
        io_method = create_io_method(self.base_dir, stage_name, to)
        mapped_values: dict[str, NewT] = {}

        read_values = io_method.read()
        if read_values is not None:
            mapped_values.update(read_values)

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

    def get(self) -> list[T]:
        """
        Gets the values in the map.
        """
        return list(self.values.values())
