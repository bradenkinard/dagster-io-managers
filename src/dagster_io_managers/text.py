"""IO managers for text data."""

from __future__ import annotations

from pathlib import Path
from typing import Self

import boto3
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)
from dagster._seven.temp_dir import get_system_temp_directory


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
        if self.base_path:
            return f"{self.base_path}/{key}"
        return key

    def get_output_path(self: Self, path: str) -> str:
        """Return the path to the output file."""
        return path


class LocalTextFileIOManager(TextFileIOManager):
    """IOManager for local text file storage."""

    base_path: str = get_system_temp_directory()

    def write_to_file(self: Self, text: str, path: str) -> None:
        """Write a string to a file."""
        with Path.open(path, "w") as f:
            f.write(text)

    def read_from_file(self: Self, path: str) -> str:
        """Read a file as a string."""
        with Path.open(path) as f:
            return f.read()


class S3TextFileIOManager(TextFileIOManager):
    """IOManager for S3 text file storage."""

    bucket: str
    base_path: str = ""

    def write_to_file(self: Self, text: str, path: str) -> None:
        """Write a string as a text file to an S3 bucket."""
        s3 = boto3.resource("s3").Bucket(self.bucket)
        s3.Object(path).put(Body=text)

    def read_from_file(self: Self, path: str) -> str:
        """Read a file as a string from an S3 bucket."""
        s3 = boto3.client("s3")
        return (
            s3.get_object(Bucket=self.bucket, Key=path)["Body"].read().decode("utf-8")
        )

    def get_output_path(self: Self, path: str) -> str:
        """Return the path to the output file."""
        return f"s3://{self.bucket}/{path}"
