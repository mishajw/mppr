from pathlib import Path
from typing import Awaitable, Callable, Generic, TypeVar
from dataclasses import dataclass
import jsonlines
from pydantic import BaseModel


T = TypeVar("T", bound=BaseModel)
NewT = TypeVar("NewT", bound=BaseModel)


def init(
    stage_name: str, base_dir: Path, init_fn: Callable[[], dict[str, T]], clazz: type[T]
) -> "Mappable[T]":
    stage_path = base_dir / f"{stage_name}.jsonl"
    values: dict[str, T] = {}

    if stage_path.is_file():
        return load(stage_name, base_dir, clazz)

    values = init_fn()
    with jsonlines.open(stage_path, "w") as f:
        for key, value in values.items():
            f.write({"key": key, "value": value.model_dump()})
    return Mappable(values, base_dir)


def load(stage_name: str, base_dir: Path, clazz: type[T]) -> "Mappable[T]":
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
        mapped_values: dict[str, NewT] = {}
        self.base_dir.mkdir(parents=True, exist_ok=True)

        stage_path = self.base_dir / f"{stage_name}.jsonl"
        if stage_path.is_file():
            mapped_values = load(stage_name, self.base_dir, clazz).values

        with jsonlines.open(stage_path, "w") as f:
            for key, value in self.values.items():
                if key not in mapped_values:
                    mapped_value = fn(key, value)
                    mapped_values[key] = mapped_value
                    f.write({"key": key, "value": mapped_value.model_dump()})

        return Mappable(mapped_values, self.base_dir)

    async def amap(
        self,
        stage_name: str,
        fn: Callable[[str, T], Awaitable[NewT]],
        clazz: type[NewT],
    ) -> "Mappable[NewT]":
        mapped_values: dict[str, NewT] = {}
        self.base_dir.mkdir(parents=True, exist_ok=True)

        stage_path = self.base_dir / f"{stage_name}.jsonl"
        if stage_path.is_file():
            mapped_values = load(stage_name, self.base_dir, clazz).values

        with jsonlines.open(stage_path, "w") as f:
            for key, value in self.values.items():
                if key not in mapped_values:
                    mapped_value = await fn(key, value)
                    mapped_values[key] = mapped_value
                    f.write({"key": key, "value": mapped_value.model_dump()})

        return Mappable(mapped_values, self.base_dir)

    def get(self) -> list[T]:
        return list(self.values.values())
