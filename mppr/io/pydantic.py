from pathlib import Path

import jsonlines
from attr import dataclass
from pydantic import BaseModel
from typing_extensions import TypeVar, override

from mppr.io.base import IoMethod, Writer

T = TypeVar("T", bound=BaseModel)


@dataclass
class PydanticIoMethod(IoMethod[T]):
    path: Path
    to: type[T]

    @staticmethod
    def create(base_path: Path, stage_name: str, to: type[T]) -> "IoMethod[T]":
        return PydanticIoMethod(
            path=base_path / f"{stage_name}.jsonl",
            to=to,
        )

    @override
    def read(self) -> dict[str, T] | None:
        if not self.path.is_file():
            return None
        values = {}
        with jsonlines.open(self.path, "r") as f:
            for line in f:
                assert line.keys() == {"key", "value"}
                values[line["key"]] = self.to(**line["value"])
        return values

    @override
    def create_writer(self) -> "Writer[T]":
        return PydanticWriter(self.path)

    @override
    def get_path(self) -> Path:
        return self.path


class PydanticWriter(Writer[T]):
    def __init__(self, path: Path):
        self.path = path
        self.f = jsonlines.open(self.path, "a")

    @override
    def write(self, key: str, value: T):
        self.f.write({"key": key, "value": value.model_dump()})

    @override
    def close(self):
        self.f.close()
