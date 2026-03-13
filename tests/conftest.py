"""Shared pytest fixtures for dashboardmd test suite."""

import pytest


@pytest.fixture
def tmp_output_dir(tmp_path):
    """Temporary directory for dashboard output."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir
