"""Tests for the CLI entry point."""

from __future__ import annotations

from pathlib import Path

import pytest


class TestCLIVersion:
    """CLI --version flag."""

    def test_version_flag(self, capsys: pytest.CaptureFixture[str]) -> None:
        """--version prints the version string."""
        from dashboardmd.cli import main

        main(["--version"])
        captured = capsys.readouterr()
        assert "dashboardmd" in captured.out
        assert "0.1.0" in captured.out

    def test_no_args_prints_help(self, capsys: pytest.CaptureFixture[str]) -> None:
        """No arguments prints help text."""
        from dashboardmd.cli import main

        main([])
        captured = capsys.readouterr()
        assert "usage" in captured.out.lower() or "dashboardmd" in captured.out.lower()


class TestCLIDiscover:
    """CLI discover command."""

    def test_discover_prints_entities(
        self, samples_dir: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """discover command prints discovered entities."""
        from dashboardmd.cli import main

        main(["discover", str(samples_dir)])
        captured = capsys.readouterr()
        assert "orders" in captured.out
        assert "customers" in captured.out

    def test_discover_with_output(
        self, samples_dir: Path, tmp_output_dir: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """discover --output generates a markdown dashboard."""
        from dashboardmd.cli import main

        output_path = tmp_output_dir / "auto.md"
        main(["discover", str(samples_dir), "--output", str(output_path)])
        assert output_path.exists()

    def test_discover_empty_dir(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """discover on empty directory prints 'no data files' message."""
        from dashboardmd.cli import main

        main(["discover", str(tmp_path)])
        captured = capsys.readouterr()
        assert "no data files" in captured.out.lower()


class TestCLIQuery:
    """CLI query command."""

    def test_query_against_data_dir(
        self, samples_dir: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """query command executes SQL against data files."""
        from dashboardmd.cli import main

        main(["query", str(samples_dir), "SELECT COUNT(*) AS n FROM orders"])
        captured = capsys.readouterr()
        assert "20" in captured.out


class TestCLIRun:
    """CLI run command."""

    def test_run_nonexistent_script(self) -> None:
        """run with missing script exits with error."""
        from dashboardmd.cli import main

        with pytest.raises(SystemExit):
            main(["run", "/nonexistent/script.py"])

    def test_run_valid_script(self, tmp_path: Path) -> None:
        """run executes a Python script."""
        from dashboardmd.cli import main

        script = tmp_path / "test_script.py"
        script.write_text("x = 1 + 1\n")
        main(["run", str(script)])
