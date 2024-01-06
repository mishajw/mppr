from pathlib import Path
from typing import Literal, TypeVar

from pydantic import BaseModel

from mppr.io.base import IoMethod
from mppr.io.pickle import PickleIoMethod
from mppr.io.pydantic import PydanticIoMethod

T = TypeVar("T")

ToType = type[T] | Literal["pickle"]


def create_io_method(
    base_dir: Path,
    stage_name: str,
    to: ToType[T],
) -> IoMethod[T]:
    """
    Creates an IO method for the given type.
    """

    if to == "pickle":
        return PickleIoMethod.create(base_dir, stage_name)
    elif issubclass(to, BaseModel):
        return PydanticIoMethod.create(base_dir, stage_name, to)
    else:
        raise NotImplementedError(f"Unsupported type: {to}")
