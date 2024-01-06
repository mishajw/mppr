from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

from mppr.io.base import IoMethod
from mppr.io.pydantic import PydanticIoMethod

T = TypeVar("T")


def create_io_method(
    base_dir: Path,
    stage_name: str,
    to: type[T],
) -> IoMethod[T]:
    if issubclass(to, BaseModel):
        return PydanticIoMethod.create(base_dir, stage_name, to)
    else:
        raise NotImplementedError(f"Unsupported type: {to}")
