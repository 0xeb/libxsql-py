# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
#
# This file is licensed under the Human-Origin Source License v1.0.
# See LICENSE.

"""Command-line interface for querying and serving libxsql databases."""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING

import apsw

from . import __version__ as _package_version
from .connection import connect
from .errors import LibxsqlError
from .script import (
    ScriptOptions,
    script_result_to_csv,
    script_result_to_json,
    script_result_to_text,
    script_result_to_tsv,
)
from .thinclient import HttpQueryServer, HttpQueryServerConfig

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .script import ScriptResult


def _version() -> str:
    return _package_version


def build_parser() -> argparse.ArgumentParser:
    """Build the public command-line parser."""
    parser = argparse.ArgumentParser(
        prog="libxsql",
        description="Query SQLite and Python virtual tables through libxsql.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_version()}",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    version_parser = commands.add_parser("version", help="show package versions")
    version_parser.set_defaults(handler=_version_command)

    query_parser = commands.add_parser("query", help="execute a SQL script")
    query_parser.add_argument("sql", help="SQL text, or '-' to read standard input")
    query_parser.add_argument(
        "--database",
        "-d",
        default=":memory:",
        help="SQLite filename or URI (default: in-memory)",
    )
    query_parser.add_argument(
        "--format",
        "-f",
        choices=("text", "json", "csv", "tsv"),
        default="text",
    )
    query_parser.add_argument("--continue-on-error", action="store_true")
    query_parser.add_argument("--include-sql", action="store_true")
    query_parser.add_argument("--timeout", type=float, default=None, metavar="SECONDS")
    query_parser.add_argument(
        "--clipboard",
        action="store_true",
        help="copy output through the optional clipboard extra",
    )
    query_parser.set_defaults(handler=_query_command)

    serve_parser = commands.add_parser("serve", help="serve queries over HTTP")
    serve_parser.add_argument(
        "--database",
        "-d",
        default=":memory:",
        help="SQLite filename or URI (default: in-memory)",
    )
    serve_parser.add_argument("--bind", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=0)
    serve_parser.add_argument("--token", default=None)
    serve_parser.add_argument("--timeout", type=float, default=None, metavar="SECONDS")
    serve_parser.add_argument("--max-queue", type=int, default=0)
    serve_parser.add_argument(
        "--allow-insecure-no-auth",
        action="store_true",
        help="allow an unauthenticated non-loopback bind",
    )
    serve_parser.set_defaults(handler=_serve_command)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command-line interface and return a process status."""
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        return int(arguments.handler(arguments))
    except (LibxsqlError, OSError, ValueError, ImportError) as exc:
        parser.exit(1, f"libxsql: error: {exc}\n")


def _version_command(arguments: argparse.Namespace) -> int:
    del arguments
    payload = {
        "libxsql": _version(),
        "apsw": apsw.apswversion(),
        "sqlite": apsw.sqlitelibversion(),
    }
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    return 0


def _query_command(arguments: argparse.Namespace) -> int:
    script = sys.stdin.read() if arguments.sql == "-" else str(arguments.sql)
    options = ScriptOptions(
        continue_on_error=bool(arguments.continue_on_error),
        include_sql=bool(arguments.include_sql),
        timeout_ms=(
            round(float(arguments.timeout) * 1_000) if arguments.timeout is not None else 0
        ),
    )
    with connect(str(arguments.database)) as connection:
        result = connection.run_script(script, options)
    rendered = _render(
        result,
        str(arguments.format),
        include_sql=bool(arguments.include_sql),
    )
    if arguments.clipboard:
        _copy_to_clipboard(rendered)
    sys.stdout.write(rendered)
    if rendered and not rendered.endswith("\n"):
        sys.stdout.write("\n")
    return 0 if result.success else 1


def _serve_command(arguments: argparse.Namespace) -> int:
    connection = connect(
        str(arguments.database),
        default_query_timeout=(float(arguments.timeout) if arguments.timeout is not None else None),
    )

    def execute(sql: str, options: ScriptOptions) -> object:
        return connection.run_script(sql, options)

    config = HttpQueryServerConfig(
        tool_name="libxsql",
        help_text=(
            "POST /query with UTF-8 SQL. Use ?format=json|text|csv|tsv, "
            "?continue_on_error=1, and ?include_sql=1.\n"
        ),
        bind_address=str(arguments.bind),
        port=int(arguments.port),
        auth_token=arguments.token,
        query_handler=execute,
        use_queue=True,
        max_queue=int(arguments.max_queue),
        allow_insecure_no_auth=bool(arguments.allow_insecure_no_auth),
    )
    server = HttpQueryServer(config)
    try:
        port = server.start()
        sys.stdout.write(f"http://{config.bind_address}:{port}\n")
        sys.stdout.flush()
        server.run_until_stopped()
    except KeyboardInterrupt:
        return 130
    finally:
        server.stop()
        connection.close()
    return 0


def _render(result: ScriptResult, output_format: str, *, include_sql: bool) -> str:
    if output_format == "json":
        return script_result_to_json(result, include_sql=include_sql)
    if output_format == "csv":
        return script_result_to_csv(result)
    if output_format == "tsv":
        return script_result_to_tsv(result)
    return script_result_to_text(result)


def _copy_to_clipboard(text: str) -> None:
    try:
        import pyperclip  # noqa: PLC0415 - optional clipboard extra
    except ImportError:
        msg = "clipboard output requires `pip install libxsql[clipboard]`"
        raise ImportError(msg) from None
    pyperclip.copy(text)


__all__ = ["build_parser", "main"]
