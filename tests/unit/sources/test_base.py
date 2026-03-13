"""Tests for SourceHandler base class contract."""

from __future__ import annotations

import pytest


class TestSourceHandlerABC:
    """SourceHandler is an abstract base class that defines the handler contract."""

    def test_cannot_instantiate_directly(self) -> None:
        """SourceHandler is abstract — instantiating it must raise TypeError."""
        from dashboardmd.sources.base import SourceHandler

        with pytest.raises(TypeError):
            SourceHandler()  # type: ignore[abstract]

    def test_subclass_must_implement_register(self) -> None:
        """A subclass that doesn't implement register() cannot be instantiated."""
        from dashboardmd.sources.base import SourceHandler

        class IncompleteHandler(SourceHandler):
            def describe(self) -> dict:
                return {}

        with pytest.raises(TypeError):
            IncompleteHandler()  # type: ignore[abstract]

    def test_subclass_must_implement_describe(self) -> None:
        """A subclass that doesn't implement describe() cannot be instantiated."""
        from dashboardmd.sources.base import SourceHandler

        class IncompleteHandler(SourceHandler):
            def register(self, conn, table_name):  # type: ignore[override]
                pass

        with pytest.raises(TypeError):
            IncompleteHandler()  # type: ignore[abstract]

    def test_valid_subclass_instantiates(self) -> None:
        """A subclass implementing both methods can be instantiated."""
        from dashboardmd.sources.base import SourceHandler

        class ValidHandler(SourceHandler):
            def register(self, conn, table_name):  # type: ignore[override]
                pass

            def describe(self) -> dict:
                return {"columns": []}

        handler = ValidHandler()
        assert handler is not None

    def test_register_signature(self) -> None:
        """register() takes a DuckDB connection and a table name string."""
        from dashboardmd.sources.base import SourceHandler

        import inspect

        sig = inspect.signature(SourceHandler.register)
        params = list(sig.parameters.keys())
        assert "conn" in params
        assert "table_name" in params

    def test_describe_returns_dict(self) -> None:
        """describe() must return a dict with schema metadata."""
        from dashboardmd.sources.base import SourceHandler

        class ValidHandler(SourceHandler):
            def register(self, conn, table_name):  # type: ignore[override]
                pass

            def describe(self) -> dict:
                return {"columns": [("id", "INTEGER"), ("name", "VARCHAR")]}

        handler = ValidHandler()
        result = handler.describe()
        assert isinstance(result, dict)
        assert "columns" in result
