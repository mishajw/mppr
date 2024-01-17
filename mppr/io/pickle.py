import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from typing_extensions import TypeVar, override

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

    @override
    def read(self) -> Iterable[tuple[str, T]]:
        if not self.path.is_file():
            return
        values = {}
        with self.path.open("rb") as f:
            while True:
                try:
                    result = pickle.load(f)
                except EOFError:
                    break
                assert isinstance(result, dict)
                assert result.keys() == {"key", "value"}
                assert isinstance(result["key"], str)
                yield result["key"], result["value"]
        return values

    @override
    def create_writer(self) -> "Writer[T]":
        return PickleWriter(self.path)

    @override
    def get_path(self) -> Path:
        return self.path


class PickleWriter(Writer[T]):
    def __init__(self, path: Path):
        self.path = path
        self.f = self.path.open("ab")

    @override
    def write(self, key: str, value: T):
        pickle.dump({"key": key, "value": value}, self.f)

    @override
    def close(self):
        self.f.close()
