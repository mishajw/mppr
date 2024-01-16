from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypeVar

from mppr.io.creator import ToType, create_io_method
from mppr.m_dict import MDict

T = TypeVar("T")
NewT = TypeVar("NewT")
JoinT = TypeVar("JoinT")


@dataclass
class MContext:
    dir: Path

    def __post_init__(self):
        self.dir.mkdir(parents=True, exist_ok=True)

    def init(
        self,
        stage_name: str,
        init_fn: Callable[[], dict[str, T]],
        to: ToType[T],
    ) -> MDict[T]:
        """
        Creates a mappable object from an init function.

        The function is only called if the stage does not exist.

        Args:
            stage_name: The name of the stage.
            base_dir: Where the stages are saved out to.
            init_fn: The function to call to initialize the stage.
            to: The class of the values in the stage. Used for deserialization.
        """

        io_method = create_io_method(self.dir, stage_name, to)
        read_values = io_method.read()
        if read_values is not None:
            return MDict(values=read_values, context=self)

        values = init_fn()
        with io_method.create_writer() as writer:
            for key, value in values.items():
                writer.write(key, value)
        return MDict(values, context=self)

    def load(self, stage_name: str, to: ToType[T]) -> MDict[T]:
        """
        Loads a previously created stage.

        Args:
            stage_name: The name of the stage.
            base_dir: Where the stages are saved out to.
            to: The class of the values in the stage. Used for deserialization.
        """

        io_method = create_io_method(self.dir, stage_name, to)
        values = io_method.read()
        assert values is not None, f"Stage {stage_name} does not exist"
        return MDict(values, context=self)

    def create(self, values: dict[str, T]) -> MDict[T]:
        """
        Creates a mappable object from a dictionary.
        """
        return MDict(values, context=self)
