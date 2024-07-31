"""Base class for IO Managers."""

# ruff: noqa: ANN401

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Self

from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)


class BaseIOManager(ConfigurableIOManager, ABC):
    """IOManager will take in an asset object and save it to an output format.

    Downstream ops can load this asset as the original object class.
    """

    def handle_output(
        self: Self,
        context: OutputContext,
        asset: Any,
    ) -> None:
        """Write an asset to the output file."""
        path = self._get_path(context)
        self._write(asset, path)
        self._add_metadata(context, asset, path)

    def load_input(
        self: Self,
        context: InputContext,
    ) -> Any:
        """Load an asset from a output file."""
        path = self._get_path(context)
        return self._read(path)

    def _get_path(
        self: Self,
        context: InputContext | OutputContext,
    ) -> str:
        asset_name = self._get_asset_name(context)
        return f"{self._base_path}/{asset_name}{self._extension}"

    def _get_asset_name(self: Self, context: InputContext | OutputContext) -> str:
        return context.asset_key.path[-1]

    @abstractmethod
    def _add_metadata(
        self,
        context: OutputContext,
        asset: Any,
        path: str,
    ) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def _base_path(self: Self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def _extension(self: Self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _write(self, asset: Any, path: str) -> None:
        raise NotImplementedError

    def _read(self, path: str) -> Any:
        raise NotImplementedError
