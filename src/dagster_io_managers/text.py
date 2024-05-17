"""IO managers for text data."""

from __future__ import annotations

from pathlib import Path
from typing import Self

from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)
from dagster._seven.temp_dir import get_system_temp_directory

from .aws_utils import s3


class TextFileIOManager(ConfigurableIOManager):
    """IOManager will take in a text string and store it as a text file.

    Downstream ops can load this file as a string.
    """

    filetype: str = "txt"

    @property
    def _base_path(self: Self) -> None:
        raise NotImplementedError

    def handle_output(
        self: Self,
        context: OutputContext,
        obj: str,
    ) -> None:
        """Write string text to a file asset."""
        path = self._get_path(context)

        if isinstance(obj, str):
            self.write_to_file(text=obj, path=path)
        else:
            msg = f"Outputs of type {type(obj)} not supported."
            raise TypeError(msg)

        context.add_output_metadata({"path": self.get_output_path(path)})

    def load_input(
        self: Self,
        context: InputContext,
    ) -> str:
        """Load a file asset as a string."""
        path = self._get_path(context)
        return self.read_from_file(path)

    def _get_path(
        self: Self,
        context: InputContext | OutputContext,
    ) -> str:
        key = f"{context.asset_key.path[-1]}.{self.filetype}"
        if self._base_path:
            return f"{self._base_path}/{key}"
        return key

    def get_output_path(self: Self, path: str) -> str:
        """Return the path to the output file."""
        return path


class LocalTextFileIOManager(TextFileIOManager):
    """IOManager for local text file storage."""

    directory: str = get_system_temp_directory()

    @property
    def _base_path(self: Self) -> str:
        return self.directory

    def write_to_file(self: Self, text: str, path: str) -> None:
        """Write a string to a file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with Path.open(path, "w") as f:
            f.write(text)

    def read_from_file(self: Self, path: str) -> str:
        """Read a file as a string."""
        with Path.open(path) as f:
            return f.read()


class S3TextFileIOManager(TextFileIOManager):
    """IOManager for S3 text file storage."""

    bucket_name: str

    @property
    def _base_path(self: Self) -> str:
        return ""

    def write_to_file(self: Self, text: str, path: str) -> None:
        """Write a string as a text file to an S3 bucket."""
        s3.create_bucket(bucket_name=self.bucket_name)
        s3.write_to_s3(bucket_name=self.bucket_name, key=path, body=text)

    def read_from_file(self: Self, path: str) -> str:
        """Read a file as a string from an S3 bucket."""
        return s3.read_from_s3(self.bucket_name, path)

    def get_output_path(self: Self, path: str) -> str:
        """Return the path to the output file."""
        return s3.get_filepath(self.bucket_name, path)
