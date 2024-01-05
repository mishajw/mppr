from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable, Generic, TypeVar

import jsonlines
import tqdm
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)
NewT = TypeVar("NewT", bound=BaseModel)


def init(
    stage_name: str,
    base_dir: Path,
    init_fn: Callable[[], dict[str, T]],
    clazz: type[T],
) -> "Mappable[T]":
    """
    Creates a mappable object from an init function.

    The function is only called if the stage does not exist.

    Args:
        stage_name: The name of the stage.
        base_dir: Where the stages are saved out to.
        init_fn: The function to call to initialize the stage.
        clazz: The class of the values in the stage. Used for deserialization.
    """

    stage_path = base_dir / f"{stage_name}.jsonl"
    base_dir.mkdir(parents=True, exist_ok=True)
    values: dict[str, T] = {}

    if stage_path.is_file():
        return load(stage_name, base_dir, clazz)

    values = init_fn()
    with jsonlines.open(stage_path, "w") as f:
        for key, value in values.items():
            f.write({"key": key, "value": value.model_dump()})
    return Mappable(values, base_dir)


def load(stage_name: str, base_dir: Path, clazz: type[T]) -> "Mappable[T]":
    """
    Loads a previously created stage.

    Args:
        stage_name: The name of the stage.
        base_dir: Where the stages are saved out to.
        clazz: The class of the values in the stage. Used for deserialization.
    """

    stage_path = base_dir / f"{stage_name}.jsonl"
    values: dict[str, T] = {}
    assert stage_path.is_file(), f"Stage {stage_name} not found"
    with jsonlines.open(stage_path, "r") as f:
        for line in f:
            assert line.keys() == {"key", "value"}
            values[line["key"]] = clazz(**line["value"])
    return Mappable(values, base_dir)


@dataclass
class Mappable(Generic[T]):
    values: dict[str, T]
    base_dir: Path

    def map(
        self,
        stage_name: str,
        fn: Callable[[str, T], NewT],
        clazz: type[NewT],
    ) -> "Mappable[NewT]":
        """
        Maps a function over the values in the stage.

        Resumable: If the stage already exists, the function is only called on
        the values that have not been processed yet.

        Args:
            stage_name: The name of the stage.
            fn: The function to call on each value.
            clazz: The class of the values in the stage. Used for deserialization.
        """

        mapped_values: dict[str, NewT] = {}
        self.base_dir.mkdir(parents=True, exist_ok=True)

        stage_path = self.base_dir / f"{stage_name}.jsonl"
        if stage_path.is_file():
            mapped_values = load(stage_name, self.base_dir, clazz).values

        with jsonlines.open(stage_path, "a") as f:
            with tqdm.tqdm(
                total=len(self.values),
                initial=len(mapped_values),
                desc=stage_name,
            ) as pbar:
                for key, value in self.values.items():
                    if key not in mapped_values:
                        mapped_value = fn(key, value)
                        mapped_values[key] = mapped_value
                        f.write({"key": key, "value": mapped_value.model_dump()})
                        pbar.update(1)

        return Mappable(mapped_values, self.base_dir)

    async def amap(
        self,
        stage_name: str,
        fn: Callable[[str, T], Awaitable[NewT]],
        clazz: type[NewT],
    ) -> "Mappable[NewT]":
        """
        Asyncronous version of map.
        """

        mapped_values: dict[str, NewT] = {}
        self.base_dir.mkdir(parents=True, exist_ok=True)

        stage_path = self.base_dir / f"{stage_name}.jsonl"
        if stage_path.is_file():
            mapped_values = load(stage_name, self.base_dir, clazz).values

        with jsonlines.open(stage_path, "w") as f:
            with tqdm.tqdm(
                desc=stage_name,
                total=len(self.values) - len(mapped_values),
                initial=len(mapped_values),
            ) as pbar:
                for key, value in self.values.items():
                    if key not in mapped_values:
                        mapped_value = await fn(key, value)
                        mapped_values[key] = mapped_value
                        f.write({"key": key, "value": mapped_value.model_dump()})
                        pbar.update(len(mapped_values))

        return Mappable(mapped_values, self.base_dir)

    def get(self) -> list[T]:
        """
        Gets the values in the map.
        """
        return list(self.values.values())
