from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Generic, TypeVar
from urllib.parse import urlparse

import boto3
import pandas as pd
from tqdm import tqdm

from mppr.io.base import IoMethod
from mppr.io.creator import ToType, create_io_method

if TYPE_CHECKING:
    from mppr.mcontext import MContext

T = TypeVar("T")
NewT = TypeVar("NewT")
JoinT = TypeVar("JoinT")


@dataclass
class MDict(Generic[T]):
    """
    Wrapper around a dictionary that allows for resumable processing.
    """

    values: dict[str, T]
    context: MContext

    def map_cached(
        self,
        stage_name: str,
        fn: Callable[[str, T], NewT],
        to: ToType[NewT],
    ) -> "MDict[NewT]":
        """
        Maps a function over the values in the stage.

        Resumable: If the stage already exists, the function is only called on
        the values that have not been processed yet.

        Args:
            stage_name: The name of the stage.
            fn: The function to call on each value.
            to: The class of the values in the stage. Used for deserialization.
        """

        io_method = create_io_method(self.context.dir, stage_name, to)
        mapped_values: dict[str, NewT] = self._load_for_map(stage_name, io_method)

        with io_method.create_writer() as writer:
            with tqdm(
                total=len(self.values),
                initial=len(mapped_values),
                desc=stage_name,
            ) as pbar:
                for key, value in self.values.items():
                    if key not in mapped_values:
                        mapped_value = fn(key, value)
                        mapped_values[key] = mapped_value
                        writer.write(key, mapped_value)
                        pbar.update(1)

        return MDict(mapped_values, self.context)

    async def amap_cached(
        self,
        stage_name: str,
        fn: Callable[[str, T], Awaitable[NewT]],
        to: ToType[NewT],
    ) -> "MDict[NewT]":
        """
        Asynchronous version of map.
        """

        io_method = create_io_method(self.context.dir, stage_name, to)
        mapped_values: dict[str, NewT] = self._load_for_map(stage_name, io_method)

        with io_method.create_writer() as writer:
            with tqdm(
                desc=stage_name,
                total=len(self.values),
                initial=len(mapped_values),
            ) as pbar:
                for key, value in self.values.items():
                    if key not in mapped_values:
                        mapped_value = await fn(key, value)
                        mapped_values[key] = mapped_value
                        writer.write(key, mapped_value)
                        pbar.update(1)

        return MDict(mapped_values, self.context)

    def _load_for_map(
        self,
        stage_name: str,
        io_method: IoMethod[NewT],
    ) -> dict[str, NewT]:
        read_values = {}
        with tqdm(desc=f"{stage_name} load", total=len(self.values)) as pbar:
            for key, value in io_method.read():
                if key in self.values:
                    read_values[key] = value
                    pbar.update(1)
        return read_values

    def join(
        self,
        other: "MDict[JoinT]",
        fn: Callable[[str, T, JoinT], NewT],
    ) -> "MDict[NewT]":
        """
        Joins two mappable objects together.
        """
        return MDict(
            {
                key: fn(key, self.values[key], other.values[key])
                for key in self.values.keys()
                if key in other.values
            },
            self.context,
        )

    def flat_map(
        self,
        fn: Callable[[str, T], dict[str, NewT]],
    ) -> "MDict[NewT]":
        """
        Maps each row into multiple rows.
        """
        return MDict(
            {
                new_key: new_value
                for key, value in self.values.items()
                for new_key, new_value in fn(key, value).items()
            },
            self.context,
        )

    def filter(
        self,
        fn: Callable[[str, T], bool],
    ) -> "MDict[T]":
        """
        Filters the values in the stage.
        """
        return MDict(
            {key: value for key, value in self.values.items() if fn(key, value)},
            self.context,
        )

    def upload(self, path: str | Path, to: ToType[T]) -> "MDict[T]":
        """
        Uploads the values in the map to a file or S3.

        Args:
            path: The path to upload to. Can be a local file path, or an S3 path (s3://bucket/path).
            to: The class of the values in the stage. Used for deserialization.
        """
        if isinstance(path, Path):
            self._upload_to_file(path, to)
            return self
        parsed_url = urlparse(path)
        if parsed_url.scheme in ["", "file"]:
            self._upload_to_file(Path(parsed_url.path), to)
        elif parsed_url.scheme == "s3":
            s3_bucket = parsed_url.netloc
            s3_path = parsed_url.path.lstrip("/")
            self._upload_to_s3(s3_bucket, s3_path, to)
        return self

    def _upload_to_file(self, path: Path, to: ToType[T]) -> None:
        with create_io_method(path.parent, path.name, to).create_writer() as writer:
            for key, value in tqdm(self.values.items(), desc="upload write"):
                writer.write(key, value)

    def _upload_to_s3(self, s3_bucket: str, s3_path: str, to: ToType[T]) -> None:
        with TemporaryDirectory() as dir:
            temp_dir = Path(dir)
            temp_dir.mkdir(parents=True, exist_ok=True)
            io_method = create_io_method(temp_dir, "to-upload", to)
            with io_method.create_writer() as writer:
                for key, value in tqdm(self.values.items(), desc="upload write"):
                    writer.write(key, value)
            boto3.resource("s3").Bucket(s3_bucket).upload_file(
                str(io_method.get_path()),
                s3_path,
            )

    def sort(self, fn: Callable[[str, T], Any]) -> "MDict[T]":
        """
        Sorts the values by a key function.
        """
        return MDict(
            dict(sorted(self.values.items(), key=lambda x: fn(x[0], x[1]))),
            self.context,
        )

    def get(self) -> list[T]:
        """
        Gets the values in the map.
        """
        return list(self.values.values())

    def limit(self, n: int) -> "MDict[T]":
        """
        Limits the number of values in the map.
        """
        return MDict(dict(list(self.values.items())[:n]), self.context)

    def to_dataframe(
        self,
        fn: Callable[[T], dict[str, Any]],
    ) -> pd.DataFrame:
        """
        Creates a dataframe out of the mappable.
        """
        return pd.DataFrame(
            [fn(value) for value in self.values.values()],
            index=list(self.values.keys()),
        )
