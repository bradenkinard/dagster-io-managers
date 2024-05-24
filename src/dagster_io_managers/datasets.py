"""IO managers for Huggingface datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Self

from dagster import (
    ConfigurableIOManager,
    EnvVar,
    InputContext,
    OutputContext,
)
from dagster._seven.temp_dir import get_system_temp_directory
from datasets import DatasetDict, load_from_disk

from .aws_utils import s3


class DatasetIOManager(ConfigurableIOManager):
    """IOManager for a huggingface dataset."""

    @property
    def _base_path(self: Self) -> None:
        raise NotImplementedError

    def _create_path(self: Self) -> None:
        raise NotImplementedError

    def handle_output(
        self: Self,
        context: OutputContext,
        obj: DatasetDict,
    ) -> None:
        """Serialize a dataset object to disk."""
        self._create_directory()
        path = self._get_path(context)

        if isinstance(obj, DatasetDict):
            obj.save_to_disk(path, storage_options=self._storage_options)
        else:
            msg = f"Outputs of type {type(obj)} not supported."
            raise TypeError(msg)

        context.add_output_metadata({"row_count": obj.num_rows, "path": path})

    def load_input(
        self: Self,
        context: InputContext,
    ) -> DatasetDict:
        """Load a serialized dataset from disk to memory."""
        path = self._get_path(context)
        return load_from_disk(path, storage_options=self._storage_options)

    @property
    def _storage_options(self: Self) -> dict:
        raise NotImplementedError

    def _get_path(
        self: Self,
        context: InputContext | OutputContext,
    ) -> str:
        key = context.asset_key.path[-1]

        return f"{self._base_path}/{key}"


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
