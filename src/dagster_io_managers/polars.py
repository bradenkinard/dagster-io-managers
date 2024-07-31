"""IO managers for polars dataframes."""

from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import Self

import polars as pl
import s3fs
from dagster import (
    InputContext,
    MetadataValue,
    OutputContext,
)
from dagster._seven.temp_dir import get_system_temp_directory

from .aws_utils import s3
from .base import BaseIOManager


class PolarsIOManager(BaseIOManager, ABC):
    """IOManager will take in a polars dataframe and store it as parquet.

    Downstream ops can load this dataframe as a polars dataframe.
    """

    def handle_output(
        self: Self,
        context: OutputContext,
        asset: pl.DataFrame,
    ) -> None:
        """Write a polars dataframe to a parquet file."""
        self.super().handle_output(context, asset)

    def load_input(
        self: Self,
        context: InputContext,
    ) -> pl.DataFrame:
        """Load a polars dataframe from a parquet file."""
        self.super().load_input(context)

    def _add_metadata(
        self,
        context: OutputContext,
        asset: pl.DataFrame,
        path: str,
    ) -> None:
        row_count = len(asset)
        context.log.info("Row count: %s", row_count)
        with pl.Config() as cfg:
            cfg.set_tbl_formatting("ASCII_MARKDOWN")
            context.add_output_metadata(
                {
                    "row_count": row_count,
                    "path": path,
                    "example_rows": MetadataValue.md(str(asset.head())),
                },
            )

    @property
    def _extension(self: Self) -> str:
        return ".parquet"

    def _read(self, path: str) -> pl.DataFrame:
        return pl.read_parquet(path)


class LocalPolarsIOManager(PolarsIOManager):
    """IOManager for local polars dataframe storage."""

    directory: str = get_system_temp_directory()

    @property
    def _base_path(self: Self) -> str:
        return self.directory

    def _write(self: Self, asset: pl.DataFrame, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        asset.write_parquet(path)


class S3PolarsIOManager(PolarsIOManager):
    """IOManager for S3 polars dataframe storage."""

    bucket_name: str

    @property
    def _base_path(self: Self) -> str:
        return s3.get_bucketpath(self.bucket_name)

    def _write(self: Self, asset: pl.DataFrame, path: str) -> None:
        s3.create_bucket(bucket_name=self.bucket_name)
        fs = s3fs.S3FileSystem()
        with fs.open(path, mode="wb") as f:
            asset.write_parquet(f)
