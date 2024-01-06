from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class IoMethod(ABC, Generic[T]):
    @abstractmethod
    def read(self) -> dict[str, T] | None:
        ...

    @abstractmethod
    def create_writer(self) -> "Writer[T]":
        ...


class Writer(ABC, Generic[T]):
    @abstractmethod
    def write(self, key: str, value: T):
        ...

    @abstractmethod
    def close(self):
        ...

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()
