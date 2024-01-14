from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class IoMethod(ABC, Generic[T]):
    @abstractmethod
    def read(self) -> dict[str, T] | None:
        """
        Reads the stage.
        """
        ...

    @abstractmethod
    def create_writer(self) -> "Writer[T]":
        """
        Creates a writer for the stage.
        """
        ...


class Writer(ABC, Generic[T]):
    @abstractmethod
    def write(self, key: str, value: T):
        """
        Writes a single value to the stage file.
        """
        ...

    @abstractmethod
    def close(self):
        """
        Closes the writer.
        """
        ...

    @abstractmethod
    def get_file_path(self) -> Path:
        """
        Gets the file path that we're writing to.
        """
        ...

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()
