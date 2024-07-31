"""IO managers for text data."""

from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING, Self

from dagster._seven.temp_dir import get_system_temp_directory

from .aws_utils import s3
from .base import BaseIOManager

if TYPE_CHECKING:
    from dagster import (
        InputContext,
        OutputContext,
    )


class TextFileIOManager(BaseIOManager, ABC):
    """IOManager will take in a text string and store it as a text file.

    Downstream ops can load this file as a string.
    """

    filetype: str = "txt"

    def handle_output(
        self: Self,
        context: OutputContext,
        asset: str,
    ) -> None:
        """Write text asset to a file."""
        super().handle_output(context, asset)

    def load_input(
        self: Self,
        context: InputContext,
    ) -> str:
        """Load a text asset as a string."""
        super().load_input(context)

    def _add_metadata(self, context: OutputContext, asset: str, path: str) -> None:  # noqa: ARG002
        context.add_output_metadata({"path": path})

    @property
    def _extension(self: Self) -> str:
        return f".{self.filetype}"


class LocalTextFileIOManager(TextFileIOManager):
    """IOManager for local text file storage."""

    directory: str = get_system_temp_directory()

    @property
    def _base_path(self: Self) -> str:
        return self.directory

    def _write(self: Self, asset: str, path: str) -> None:
        """Write a string to a file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with Path.open(path, "w") as f:
            f.write(asset)

    def _read(self: Self, path: str) -> str:
        """Read a file as a string."""
        with Path.open(path) as f:
            return f.read()


class S3TextFileIOManager(TextFileIOManager):
    """IOManager for S3 text file storage."""

    bucket_name: str

    @property
    def _base_path(self: Self) -> str:
        return s3.get_bucketpath(self.bucket_name)

    def _write(self: Self, text: str, path: str) -> None:
        """Write a string as a text file to an S3 bucket."""
        s3.create_bucket(bucket_name=self.bucket_name)
        s3.write_to_s3(body=text, path=path)

    def _read(self: Self, path: str) -> str:
        """Read a file as a string from an S3 bucket."""
        return s3.read_from_s3(path)
