"""IO managers for Huggingface datasets."""

from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import Self

from dagster import (
    EnvVar,
    InputContext,
    OutputContext,
)
from dagster._seven.temp_dir import get_system_temp_directory
from datasets import DatasetDict, load_from_disk

from .aws_utils import s3
from .base import BaseIOManager


class DatasetIOManager(BaseIOManager, ABC):
    """IOManager for a huggingface dataset."""

    def handle_output(
        self: Self,
        context: OutputContext,
        asset: DatasetDict,
    ) -> None:
        """Write a dataset asset to disk."""
        super().handle_output(context, asset)

    def load_input(
        self: Self,
        context: InputContext,
    ) -> DatasetDict:
        """Load a dataset asset from disk."""
        super().load_input(context)

    def _add_metadata(
        self: Self,
        context: OutputContext,
        asset: DatasetDict,
        path: str,
    ) -> None:
        context.add_output_metadata({"row_count": asset.num_rows, "path": path})

    def _read(self: Self, path: str) -> DatasetDict:
        return load_from_disk(path, storage_options=self._storage_options)

    def _write(self: Self, asset: DatasetDict, path: str) -> None:
        self._create_path()
        asset.save_to_disk(path, storage_options=self._storage_options)

    @property
    def _extension(self: Self) -> str:
        return ""

    @property
    def _storage_options(self: Self) -> dict:
        raise NotImplementedError


class LocalDatasetIOManager(DatasetIOManager):
    """IOManager for local Huggingface dataset."""

    directory: str = get_system_temp_directory()

    @property
    def _base_path(self: Self) -> str:
        return self.directory

    @property
    def _storage_options(self: Self) -> dict:
        return {}

    def _create_path(self: Self) -> None:
        Path(self._base_path).mkdir(parents=True, exist_ok=True)


class S3DatasetIOManager(DatasetIOManager):
    """IOManager for Huggingface dataset in S3."""

    bucket_name: str

    aws_access_key: str = EnvVar("AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = EnvVar("AWS_SECRET_ACCESS_KEY")
    endpoint_url: str = EnvVar("AWS_ENDPOINT_URL")

    @property
    def _base_path(self: Self) -> str:
        return s3.get_bucketpath(self.bucket_name)

    def _create_path(self: Self) -> None:
        s3.create_bucket(bucket_name=self.bucket_name)

    @property
    def _storage_options(self: Self) -> dict:
        options = {
            "key": self.aws_access_key,
            "secret": self.aws_secret_access_key,
        }

        if self.endpoint_url:
            options["endpoint_url"] = self.endpoint_url

        return options
