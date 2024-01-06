import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

from mppr.io.base import IoMethod, Writer

T = TypeVar("T")


@dataclass
class PickleIoMethod(IoMethod[T]):
    path: Path

    @staticmethod
    def create(base_path: Path, stage_name: str) -> "IoMethod[T]":
        return PickleIoMethod(
            path=base_path / f"{stage_name}.pickle",
        )

    def read(self) -> dict[str, T] | None:
        if not self.path.is_file():
            return None
        values = {}
        with self.path.open("rb") as f:
            result = pickle.load(f)
            assert isinstance(result, dict)
            assert result.keys() == {"key", "value"}
            assert isinstance(result["key"], str)
            values[result["key"]] = result["value"]
        return values

    def create_writer(self) -> "Writer[T]":
        return PickleWriter(self.path)


class PickleWriter(Writer[T]):
    def __init__(self, path: Path):
        self.path = path
        self.f = self.path.open("wb")

    def write(self, key: str, value: T):
        pickle.dump({"key": key, "value": value}, self.f)

    def close(self):
        self.f.close()
