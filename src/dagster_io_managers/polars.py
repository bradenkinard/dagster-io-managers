"""IO managers for polars dataframes."""

from __future__ import annotations

from pathlib import Path
from typing import Self

import polars as pl
import s3fs
from dagster import (
    ConfigurableIOManager,
    InputContext,
    MetadataValue,
    OutputContext,
)
from dagster._seven.temp_dir import get_system_temp_directory


class PolarsIOManager(ConfigurableIOManager):
    """IOManager will take in a polars dataframe and store it as parquet.

    Downstream ops can load this dataframe as a polars dataframe.
    """

    @property
    def _base_path(self: Self) -> str:
        raise NotImplementedError

    def handle_output(
        self: Self,
        context: OutputContext,
        obj: pl.DataFrame,
    ) -> None:
        """Write a polars dataframe to a parquet file."""
        path = self._get_path(context)

        if isinstance(obj, pl.DataFrame):
            row_count = len(obj)
            context.log.info("Row count: %s", row_count)

            self._write_dataframe(obj, path)
        else:
            msg = f"Outputs of type {type(obj)} not supported."
            raise TypeError(msg)

        with pl.Config() as cfg:
            cfg.set_tbl_formatting("ASCII_MARKDOWN")
            context.add_output_metadata(
                {
                    "row_count": row_count,
                    "path": path,
                    "example_rows": MetadataValue.md(obj.head()),
                }
            )

    def load_input(
        self: Self,
        context: InputContext,
    ) -> pl.DataFrame:
        """Load a polars dataframe from a parquet file."""
        path = self._get_path(context)
        return pl.read_parquet(path)

    def _get_path(
        self: Self,
        context: InputContext | OutputContext,
    ) -> str:
        return f"{self._base_path}/{context.asset_key.path[-1]}.parquet"


class LocalPolarsIOManager(PolarsIOManager):
    """IOManager for local polars dataframe storage."""

    directory: str = get_system_temp_directory()

    @property
    def _base_path(self: Self) -> str:
        return self.directory

    def _write_dataframe(self: Self, df: pl.DataFrame, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path)


class S3PolarsIOManager(PolarsIOManager):
    """IOManager for S3 polars dataframe storage."""

    bucket: str
    base_path: str = ""

    @property
    def _base_path(self: Self) -> str:
        return "s3://" + self.bucket

    def _write_dataframe(self: Self, df: pl.DataFrame, path: str) -> None:
        fs = s3fs.S3FileSystem()
        with fs.open(path, mode="wb") as f:
            df.write_parquet(f)
