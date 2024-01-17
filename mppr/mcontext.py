from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypeVar
from urllib.parse import urlparse

import boto3

from mppr.io.creator import ToType, create_io_method
from mppr.mdict import MDict

T = TypeVar("T")
NewT = TypeVar("NewT")
JoinT = TypeVar("JoinT")


@dataclass
class MContext:
    dir: Path

    def __post_init__(self):
        self.dir.mkdir(parents=True, exist_ok=True)

    def create_cached(
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

    def create(self, values: dict[str, T]) -> MDict[T]:
        """
        Creates a mappable object from a dictionary.
        """
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

    def download_cached(
        self, stage_name: str, *, path: str | Path, to: ToType[T]
    ) -> MDict[T]:
        """
        Downloads a stage from a file or S3.

        Args:
            path: The path to download from. Can be a local file path, or an S3 path (s3://bucket/path).
            to: The class of the values in the stage. Used for deserialization.
        """
        io_method = create_io_method(self.dir, stage_name, to)
        values = io_method.read()
        if values is not None:
            return MDict(values, context=self)
        self._download(from_path=path, to_path=io_method.get_path())
        values = io_method.read()
        assert (
            values is not None
        ), f"Unexpected state: Couldn't find values after downloading from {path}"
        return MDict(values, context=self)

    def _download(
        self,
        *,
        from_path: str | Path,
        to_path: Path,
    ) -> None:
        if isinstance(from_path, Path):
            self._download_from_file(
                from_path=from_path,
                to_path=to_path,
            )
            return
        parsed_url = urlparse(from_path)
        if parsed_url.scheme in ["", "file"]:
            self._download_from_file(
                from_path=Path(parsed_url.path),
                to_path=to_path,
            )
        elif parsed_url.scheme == "s3":
            s3_bucket = parsed_url.netloc
            s3_path = parsed_url.path.lstrip("/")
            self._download_from_s3(
                s3_bucket=s3_bucket,
                s3_path=s3_path,
                to_path=to_path,
            )
        else:
            raise NotImplementedError(f"Unsupported path: {from_path}")

    def _download_from_file(
        self,
        *,
        from_path: Path,
        to_path: Path,
    ) -> None:
        assert not to_path.exists(), to_path
        from_path.rename(to_path)

    def _download_from_s3(
        self,
        *,
        s3_bucket: str,
        s3_path: str,
        to_path: Path,
    ) -> None:
        boto3.resource("s3").Bucket(s3_bucket).download_file(
            s3_path,
            str(to_path),
        )
        assert (
            to_path.is_file()
        ), f"Couldn't find download file: from=s3://{s3_bucket}/{s3_path}, to={to_path}"
