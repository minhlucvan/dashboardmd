"""CLI entry point for dashboardmd.

Usage:
    dashboardmd run <script.py>       Run a dashboard script
    dashboardmd discover <dir>        Auto-discover entities from data files
    dashboardmd query <dir> <sql>     Run SQL against data files
    dashboardmd refresh <script.py>   Re-run and show metric diffs
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="dashboardmd",
        description="Code-first analytics dashboard platform.",
    )
    parser.add_argument("--version", action="store_true", help="Show version and exit")
    subparsers = parser.add_subparsers(dest="command")

    # run
    run_parser = subparsers.add_parser("run", help="Run a dashboard script")
    run_parser.add_argument("script", help="Path to a Python dashboard script")

    # discover
    discover_parser = subparsers.add_parser("discover", help="Auto-discover entities from data files")
    discover_parser.add_argument("directory", help="Directory containing data files")
    discover_parser.add_argument("--output", "-o", help="Output markdown file path")

    # query
    query_parser = subparsers.add_parser("query", help="Run SQL against data files in a directory")
    query_parser.add_argument("directory", help="Directory containing data files")
    query_parser.add_argument("sql", help="SQL query to execute")

    # refresh
    refresh_parser = subparsers.add_parser("refresh", help="Re-run a dashboard script and show metric diffs")
    refresh_parser.add_argument("script", help="Path to a Python dashboard script")

    args = parser.parse_args(argv)

    if args.version:
        from dashboardmd import __version__

        print(f"dashboardmd {__version__}")
        return

    if args.command is None:
        parser.print_help()
        return

    if args.command == "run":
        _cmd_run(args.script)
    elif args.command == "discover":
        _cmd_discover(args.directory, args.output)
    elif args.command == "query":
        _cmd_query(args.directory, args.sql)
    elif args.command == "refresh":
        _cmd_refresh(args.script)


def _cmd_run(script: str) -> None:
    """Execute a Python dashboard script."""
    path = Path(script)
    if not path.exists():
        print(f"Error: Script not found: {script}", file=sys.stderr)
        sys.exit(1)
    exec(compile(path.read_text(), str(path), "exec"), {"__name__": "__main__"})


def _cmd_discover(directory: str, output: str | None) -> None:
    """Discover entities and optionally generate an auto-dashboard."""
    from dashboardmd.suggest import auto_join, discover, suggest_measures

    entities = discover(directory)
    if not entities:
        print(f"No data files found in {directory}")
        return

    print(f"Discovered {len(entities)} entities:\n")
    for entity in entities:
        # Add suggested measures
        entity.measures = suggest_measures(entity)
        dims = [d.name for d in entity.dimensions]
        measures = [m.name for m in entity.measures]
        print(f"  {entity.name}")
        print(f"    Dimensions: {', '.join(dims)}")
        print(f"    Measures:   {', '.join(measures)}")
        print()

    relationships = auto_join(entities)
    if relationships:
        print(f"Detected {len(relationships)} relationships:")
        for rel in relationships:
            print(f"  {rel.from_entity}.{rel.on[0]} → {rel.to_entity}.{rel.on[1]}")
        print()

    if output:
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(
            title="Auto-Generated Dashboard",
            entities=entities,
            relationships=relationships,
            output=output,
        )
        dash.auto_dashboard()
        md = dash.save()
        print(f"Dashboard saved to {output} ({len(md)} bytes)")


def _cmd_query(directory: str, sql: str) -> None:
    """Register all data files in a directory and run SQL."""
    from dashboardmd.analyst import Analyst
    from dashboardmd.suggest import discover

    entities = discover(directory)
    if not entities:
        print(f"No data files found in {directory}")
        return

    analyst = Analyst()
    for entity in entities:
        analyst.add(entity.name, str(entity.source))

    result = analyst.sql(sql)
    print(result.to_markdown_table())


def _cmd_refresh(script: str) -> None:
    """Re-run a dashboard script and display metric diffs."""
    # For refresh, we run the script and look for Dashboard instances
    print(f"Refreshing {script}...")
    _cmd_run(script)
    print("Dashboard refreshed. Use the refresh() API for metric diff tracking.")


if __name__ == "__main__":
    main()
