# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Canonical multi-statement execution and result formatting."""

from __future__ import annotations

import csv
import inspect
import io
import json
import os
import tempfile
import time
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import apsw
from anyio import to_thread

from .errors import LibxsqlError, PartialQueryError, QueryCancelledError, QueryTimeoutError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Iterator

    from .connection import AsyncConnection, AsyncCursor, Connection
    from .types import SQLiteRow, SQLiteValue

_TABLE_INFO_COLUMN_COUNT = 6


@dataclass(frozen=True, slots=True)
class ScriptOptions:
    """Options for multi-statement SQL execution."""

    continue_on_error: bool = False
    include_sql: bool = False
    timeout_ms: int = 0
    partial_on_timeout: bool = True
    should_cancel: Callable[[], bool] | None = None

    def __post_init__(self) -> None:
        """Validate option bounds."""
        if self.timeout_ms < 0:
            message = "timeout_ms must be non-negative"
            raise ValueError(message)


@dataclass(frozen=True, slots=True)
class StatementResult:
    """Result of one SQL statement."""

    statement_index: int = 0
    success: bool = False
    columns: tuple[str, ...] = ()
    rows: tuple[SQLiteRow, ...] = ()
    row_count: int = 0
    elapsed_ms: float = 0.0
    error: str | None = None
    sql: str = ""
    timed_out: bool = False
    partial: bool = False
    warnings: tuple[str, ...] = ()

    def is_null_cell(self, row_index: int, column_index: int) -> bool:
        """Whether an in-range cell is SQL NULL."""
        try:
            return self.rows[row_index][column_index] is None
        except IndexError:
            return False


ScriptStatementResult = StatementResult
"""Compatibility alias matching the C++ result type name."""


@dataclass(frozen=True, slots=True)
class ScriptResult:
    """Aggregated result of a multi-statement script."""

    success: bool = True
    statement_count: int = 0
    results: tuple[StatementResult, ...] = ()
    row_count_total: int = 0
    elapsed_ms_total: float = 0.0
    first_error_index: int | None = None
    parse_error: str = ""


@dataclass(frozen=True, slots=True)
class ExportOptions:
    """Options for SQL table exports."""

    include_header: bool = True
    include_drop: bool = True
    include_schema: bool = True
    include_data: bool = True
    wrap_transaction: bool = False


class TableStyle(StrEnum):
    """Rendering styles supported by :func:`print_table`."""

    BORDERLESS = "borderless"
    BOXED = "boxed"


@dataclass(frozen=True, slots=True)
class TablePrintOptions:
    """Options controlling human-readable table rendering."""

    no_result: str = "(no result)"
    newline_after_no_result: bool = False
    style: TableStyle = TableStyle.BORDERLESS
    boxed_row_count_footer: bool = True


@dataclass(frozen=True, slots=True)
class _ColumnInfo:
    name: str
    declared_type: str
    not_null: bool
    default_value: str | None
    primary_key: bool


def quote_identifier(identifier: str) -> str:
    """Return a safely double-quoted SQLite identifier."""
    if "\x00" in identifier:
        message = "SQLite identifiers cannot contain NUL"
        raise ValueError(message)
    return '"' + identifier.replace('"', '""') + '"'


def collect_statements(script: str) -> list[str]:
    """Split SQL using SQLite's own complete-statement parser."""
    statements: list[str] = []
    current: list[str] = []
    for character in script:
        current.append(character)
        candidate = "".join(current)
        if apsw.complete(candidate):
            if trimmed := candidate.strip():
                statements.append(trimmed)
            current.clear()
    if tail := "".join(current).strip():
        statements.append(tail)
    return statements


split_script = collect_statements
"""Alias for :func:`collect_statements`."""


def _pre_statement_cancellation(
    statement_index: int,
    statement: str,
    options: ScriptOptions,
) -> StatementResult | None:
    if options.should_cancel is None:
        return None
    try:
        if not options.should_cancel():
            return None
        error = "query cancelled"
    except Exception as caught:  # noqa: BLE001 - callback failures are result data.
        error = str(caught)
    return StatementResult(
        statement_index=statement_index,
        success=False,
        error=error,
        sql=statement if options.include_sql else "",
    )


@dataclass(slots=True)
class _StickyCancellation:
    predicate: Callable[[], bool]
    cancelled: bool = False
    failure: BaseException | None = None

    def __call__(self) -> bool:
        if self.failure is not None:
            raise self.failure
        if self.cancelled:
            return True
        try:
            self.cancelled = self.predicate()
        except BaseException as error:
            self.failure = error
            raise
        return self.cancelled


def _sticky_script_options(options: ScriptOptions) -> ScriptOptions:
    predicate = options.should_cancel
    if predicate is None or isinstance(predicate, _StickyCancellation):
        return options
    return replace(options, should_cancel=_StickyCancellation(predicate))


def _partial_statement_result(
    error: PartialQueryError,
    *,
    statement_index: int,
    statement: str,
    include_sql: bool,
) -> StatementResult:
    query_result = error.result
    return StatementResult(
        statement_index=statement_index,
        success=False,
        columns=query_result.columns,
        rows=query_result.rows,
        row_count=query_result.row_count,
        elapsed_ms=query_result.elapsed_ms,
        error=str(error),
        sql=statement if include_sql else "",
        timed_out=query_result.timed_out,
        partial=query_result.partial,
        warnings=query_result.warnings,
    )


def run_database_script(
    connection: Connection,
    script: str,
    options: ScriptOptions | None = None,
) -> ScriptResult:
    """Execute a script against a synchronous connection."""
    effective_options = _sticky_script_options(options or ScriptOptions())
    statements = collect_statements(script)
    results: list[StatementResult] = []
    first_error: int | None = None
    for statement_index, statement in enumerate(statements):
        if cancelled := _pre_statement_cancellation(
            statement_index,
            statement,
            effective_options,
        ):
            results.append(cancelled)
            first_error = statement_index
            break
        try:
            timeout = effective_options.timeout_ms / 1_000 if effective_options.timeout_ms else None
            if effective_options.should_cancel is None:
                query_result = connection.query(
                    statement,
                    timeout=timeout,
                    partial_on_timeout=effective_options.partial_on_timeout,
                )
            else:
                query_result = connection.query(
                    statement,
                    timeout=timeout,
                    should_cancel=effective_options.should_cancel,
                    partial_on_timeout=effective_options.partial_on_timeout,
                )
            result = StatementResult(
                statement_index=statement_index,
                success=True,
                columns=query_result.columns,
                rows=query_result.rows,
                row_count=query_result.row_count,
                elapsed_ms=query_result.elapsed_ms,
                sql=statement if effective_options.include_sql else "",
                timed_out=query_result.timed_out,
                partial=query_result.partial,
                warnings=query_result.warnings,
            )
        except PartialQueryError as error:
            result = _partial_statement_result(
                error,
                statement_index=statement_index,
                statement=statement,
                include_sql=effective_options.include_sql,
            )
        except Exception as error:  # noqa: BLE001 - statement failures are result data.
            result = StatementResult(
                statement_index=statement_index,
                success=False,
                error=str(error),
                sql=statement if effective_options.include_sql else "",
            )
        results.append(result)
        if not result.success:
            first_error = statement_index if first_error is None else first_error
            if not effective_options.continue_on_error:
                break
    return _aggregate_results(statements, results, first_error)


async def run_database_script_async(
    connection: AsyncConnection,
    script: str,
    options: ScriptOptions | None = None,
) -> ScriptResult:
    """Execute a script against an asynchronous connection."""
    effective_options = _sticky_script_options(options or ScriptOptions())
    statements = collect_statements(script)
    results: list[StatementResult] = []
    first_error: int | None = None
    for statement_index, statement in enumerate(statements):
        if cancelled := _pre_statement_cancellation(
            statement_index,
            statement,
            effective_options,
        ):
            results.append(cancelled)
            first_error = statement_index
            break
        try:
            timeout = effective_options.timeout_ms / 1_000 if effective_options.timeout_ms else None
            if effective_options.should_cancel is None:
                query_result = await connection.query(
                    statement,
                    timeout=timeout,
                    partial_on_timeout=effective_options.partial_on_timeout,
                )
            else:
                query_result = await connection.query(
                    statement,
                    timeout=timeout,
                    should_cancel=effective_options.should_cancel,
                    partial_on_timeout=effective_options.partial_on_timeout,
                )
            result = StatementResult(
                statement_index=statement_index,
                success=True,
                columns=query_result.columns,
                rows=query_result.rows,
                row_count=query_result.row_count,
                elapsed_ms=query_result.elapsed_ms,
                sql=statement if effective_options.include_sql else "",
                timed_out=query_result.timed_out,
                partial=query_result.partial,
                warnings=query_result.warnings,
            )
        except PartialQueryError as error:
            result = _partial_statement_result(
                error,
                statement_index=statement_index,
                statement=statement,
                include_sql=effective_options.include_sql,
            )
        except Exception as error:  # noqa: BLE001 - statement failures are result data.
            result = StatementResult(
                statement_index=statement_index,
                success=False,
                error=str(error),
                sql=statement if effective_options.include_sql else "",
            )
        results.append(result)
        if not result.success:
            first_error = statement_index if first_error is None else first_error
            if not effective_options.continue_on_error:
                break
    return _aggregate_results(statements, results, first_error)


def execute_script(connection: Connection, script: str) -> tuple[StatementResult, ...]:
    """Run a script fail-fast and return statements that produced columns."""
    result = run_database_script(
        connection,
        script,
        ScriptOptions(include_sql=True),
    )
    if not result.success:
        error = next(
            (statement.error for statement in result.results if statement.error),
            "script execution failed",
        )
        raise LibxsqlError(error)
    return tuple(statement for statement in result.results if statement.columns)


def export_tables(
    connection: Connection,
    tables: Iterable[str],
    output_path: str | os.PathLike[str],
    options: ExportOptions | None = None,
) -> Path:
    """Export tables as a replayable SQL script using an atomic file replace.

    An empty ``tables`` iterable exports every ordinary table in ``main``.
    Identifiers are always quoted and values are emitted as typed SQL literals.
    """
    effective_options = options or ExportOptions()
    names = _sync_table_names(connection, tables)
    text = _render_export_sync(connection, names, effective_options)
    return _write_export(output_path, text)


async def export_tables_async(
    connection: AsyncConnection,
    tables: Iterable[str],
    output_path: str | os.PathLike[str],
    options: ExportOptions | None = None,
) -> Path:
    """Asynchronously export tables without blocking the event loop on file I/O."""
    effective_options = options or ExportOptions()
    requested = tuple(tables)
    if requested:
        names = requested
    else:
        result = await connection.query(
            "SELECT name FROM main.sqlite_schema WHERE type='table' ORDER BY name"
        )
        names = tuple(str(row[0]) for row in result.rows)
    chunks = _export_header(names, effective_options)
    for name in names:
        quoted_name = quote_identifier(name)
        column_result = await connection.query(f"PRAGMA table_info({quoted_name})")
        columns = _columns_from_rows(column_result.rows)
        if not columns:
            continue
        rows = (
            (await connection.query(f"SELECT * FROM {quoted_name}")).rows  # noqa: S608
            if effective_options.include_data
            else ()
        )
        chunks.extend(_render_export_table(name, columns, rows, effective_options))
    chunks.extend(_export_footer(effective_options))
    text = "".join(chunks)
    return await to_thread.run_sync(_write_export, output_path, text)


def _sync_table_names(
    connection: Connection,
    tables: Iterable[str],
) -> tuple[str, ...]:
    requested = tuple(tables)
    if requested:
        return requested
    result = connection.query(
        "SELECT name FROM main.sqlite_schema WHERE type='table' ORDER BY name"
    )
    return tuple(str(row[0]) for row in result.rows)


def _render_export_sync(
    connection: Connection,
    names: tuple[str, ...],
    options: ExportOptions,
) -> str:
    chunks = _export_header(names, options)
    for name in names:
        quoted_name = quote_identifier(name)
        columns = _columns_from_rows(connection.query(f"PRAGMA table_info({quoted_name})").rows)
        if not columns:
            continue
        rows = (
            connection.query(f"SELECT * FROM {quoted_name}").rows  # noqa: S608
            if options.include_data
            else ()
        )
        chunks.extend(_render_export_table(name, columns, rows, options))
    chunks.extend(_export_footer(options))
    return "".join(chunks)


def _columns_from_rows(rows: Iterable[SQLiteRow]) -> tuple[_ColumnInfo, ...]:
    return tuple(
        _ColumnInfo(
            name=str(row[1]),
            declared_type="" if row[2] is None else str(row[2]),
            not_null=bool(row[3]),
            default_value=None if row[4] is None else str(row[4]),
            primary_key=bool(row[5]),
        )
        for row in rows
        if len(row) >= _TABLE_INFO_COLUMN_COUNT
    )


def _export_header(names: tuple[str, ...], options: ExportOptions) -> list[str]:
    chunks: list[str] = []
    if options.include_header:
        chunks.extend(("-- SQL Export\n", f"-- Tables: {len(names)}\n", "\n"))
    if options.wrap_transaction:
        chunks.extend(("BEGIN TRANSACTION;\n", "\n"))
    return chunks


def _export_footer(options: ExportOptions) -> tuple[str, ...]:
    return ("COMMIT;\n",) if options.wrap_transaction else ()


def _render_export_table(
    name: str,
    columns: tuple[_ColumnInfo, ...],
    rows: Iterable[SQLiteRow],
    options: ExportOptions,
) -> list[str]:
    quoted_name = quote_identifier(name)
    chunks = [f"-- Table: {name}\n"]
    if options.include_drop:
        chunks.append(f"DROP TABLE IF EXISTS {quoted_name};\n")
    if options.include_schema:
        chunks.append(f"CREATE TABLE {quoted_name} (\n")
        for index, column in enumerate(columns):
            definition = f"    {quote_identifier(column.name)}"
            if column.declared_type:
                definition += f" {column.declared_type}"
            if column.primary_key:
                definition += " PRIMARY KEY"
            if column.not_null:
                definition += " NOT NULL"
            if column.default_value is not None:
                definition += f" DEFAULT {column.default_value}"
            definition += "," if index + 1 < len(columns) else ""
            chunks.append(definition + "\n")
        chunks.extend((");\n", "\n"))
    row_count = 0
    if options.include_data:
        for row in rows:
            literals = ", ".join(_sql_literal(value) for value in row)
            chunks.append(f"INSERT INTO {quoted_name} VALUES ({literals});\n")  # noqa: S608
            row_count += 1
    chunks.extend((f"-- {row_count} rows exported\n", "\n"))
    return chunks


def _sql_literal(value: SQLiteValue) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bytes):
        return f"X'{value.hex().upper()}'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, float):
        return format(value, ".16g")
    return str(value)


def _write_export(output_path: str | os.PathLike[str], text: str) -> Path:
    path = Path(output_path)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
            delete=False,
        ) as output:
            temporary_path = Path(output.name)
            output.write(text)
            output.flush()
            os.fsync(output.fileno())
        temporary_path.replace(path)
    except BaseException:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise
    return path


def run_script_with_executor(
    script: str,
    options: ScriptOptions,
    executor: Callable[[str, int], StatementResult],
) -> ScriptResult:
    """Execute split statements with a caller-provided result function."""
    options = _sticky_script_options(options)
    statements = collect_statements(script)
    results: list[StatementResult] = []
    first_error: int | None = None
    for statement_index, statement in enumerate(statements):
        if cancelled := _pre_statement_cancellation(statement_index, statement, options):
            results.append(cancelled)
            first_error = statement_index
            break
        candidate = executor(statement, statement_index)
        result = StatementResult(
            statement_index=statement_index,
            success=candidate.success and candidate.error is None,
            columns=candidate.columns,
            rows=candidate.rows,
            row_count=len(candidate.rows),
            elapsed_ms=candidate.elapsed_ms,
            error=candidate.error,
            sql=statement if options.include_sql else "",
            timed_out=candidate.timed_out,
            partial=candidate.partial,
            warnings=candidate.warnings,
        )
        results.append(result)
        if not result.success:
            first_error = statement_index if first_error is None else first_error
            if not options.continue_on_error:
                break
    return _aggregate_results(statements, results, first_error)


def _aggregate_results(
    statements: list[str],
    results: list[StatementResult],
    first_error: int | None,
) -> ScriptResult:
    return ScriptResult(
        success=first_error is None,
        statement_count=len(statements),
        results=tuple(results),
        row_count_total=sum(result.row_count for result in results),
        elapsed_ms_total=sum(result.elapsed_ms for result in results),
        first_error_index=first_error,
    )


def _display_cell(value: SQLiteValue) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.decode("latin-1")
    if isinstance(value, float):
        return format(value, ".15g")
    return str(value)


def _json_cell_value(value: SQLiteValue) -> SQLiteValue:
    if isinstance(value, bytes):
        return _display_cell(value)
    return value


def _elapsed_json(value: float) -> int | float:
    return int(value) if value.is_integer() else value


def _statement_object(
    statement: StatementResult,
    *,
    include_sql: bool,
) -> dict[str, Any]:
    output: dict[str, Any] = {
        "statement_index": statement.statement_index,
        "success": statement.success,
    }
    if include_sql and statement.sql:
        output["sql"] = statement.sql
    output.update(
        {
            "columns": list(statement.columns),
            "rows": [[_json_cell_value(cell) for cell in row] for row in statement.rows],
            "row_count": statement.row_count,
            "elapsed_ms": _elapsed_json(statement.elapsed_ms),
            "error": statement.error,
        }
    )
    if statement.timed_out:
        output["timed_out"] = True
    if statement.partial:
        output["partial"] = True
    if statement.warnings:
        output["warnings"] = list(statement.warnings)
    return output


def script_result_to_object(
    result: ScriptResult,
    *,
    include_sql: bool = False,
) -> dict[str, Any]:
    """Return the canonical JSON-compatible envelope."""
    if result.parse_error:
        return {
            "success": False,
            "statement_count": 0,
            "results": [],
            "row_count_total": 0,
            "elapsed_ms_total": 0,
            "first_error_index": None,
            "parse_error": result.parse_error,
        }
    return {
        "success": result.success,
        "statement_count": result.statement_count,
        "results": [
            _statement_object(statement, include_sql=include_sql) for statement in result.results
        ],
        "row_count_total": result.row_count_total,
        "elapsed_ms_total": _elapsed_json(result.elapsed_ms_total),
        "first_error_index": result.first_error_index,
    }


def script_result_to_json(
    result: ScriptResult,
    *,
    include_sql: bool = False,
) -> str:
    """Serialize a result using the canonical compact JSON envelope."""
    return json.dumps(
        script_result_to_object(result, include_sql=include_sql),
        ensure_ascii=False,
        separators=(",", ":"),
    )


def script_result_to_jsonl(result: ScriptResult) -> str:
    """Serialize one column-keyed JSON object per row."""
    if result.parse_error:
        return _compact_json_object((("error", result.parse_error),)) + "\n"
    lines: list[str] = []
    for statement in result.results:
        if not statement.success:
            lines.append(
                _compact_json_object(
                    (
                        ("statement_index", statement.statement_index),
                        ("error", statement.error or ""),
                    )
                )
            )
            continue
        keys = _unique_json_object_keys(statement.columns)
        lines.extend(
            _compact_json_object(
                tuple(
                    (
                        column,
                        _display_cell(row[index])
                        if index < len(row) and row[index] is not None
                        else None,
                    )
                    for index, column in enumerate(keys)
                )
            )
            for row in statement.rows
        )
        if statement.timed_out or statement.partial or statement.warnings:
            metadata: list[tuple[str, object]] = [("statement_index", statement.statement_index)]
            if statement.timed_out:
                metadata.append(("timed_out", True))
            if statement.partial:
                metadata.append(("partial", True))
            if statement.warnings:
                metadata.append(("warnings", statement.warnings))
            lines.append(_compact_json_object(tuple(metadata)))
    return "".join(f"{line}\n" for line in lines)


def _unique_json_object_keys(columns: tuple[str, ...]) -> tuple[str, ...]:
    used: set[str] = set()
    keys: list[str] = []
    for column in columns:
        candidate = column
        suffix = 2
        while candidate in used:
            candidate = f"{column}#{suffix}"
            suffix += 1
        used.add(candidate)
        keys.append(candidate)
    return tuple(keys)


def _ndjson_metadata_fields(
    statement_index: int,
    *,
    error: str | None = None,
    timed_out: bool,
    partial: bool,
    warnings: tuple[str, ...],
) -> tuple[tuple[str, object], ...]:
    fields: list[tuple[str, object]] = [("statement_index", statement_index)]
    if error is not None:
        fields.append(("error", error))
    if timed_out:
        fields.append(("timed_out", True))
    if partial:
        fields.append(("partial", True))
    if warnings:
        fields.append(("warnings", warnings))
    return tuple(fields)


def _stream_timeout_seconds(options: ScriptOptions) -> float | None:
    return options.timeout_ms / 1_000 if options.timeout_ms else None


def _cursor_is_readonly(cursor: object) -> bool:
    """Return statement mutability, defaulting legacy cursor adapters to reads."""
    return bool(getattr(cursor, "is_readonly", True))


@dataclass(slots=True)
class _StreamWriter:
    write: Callable[[str], bool | None]
    aborted: bool = False

    def emit(self, chunk: str) -> None:
        if not self.aborted and chunk:
            self.aborted = self.write(chunk) is False

    def stopped(self) -> bool:
        return self.aborted


@dataclass(slots=True)
class _AsyncStreamWriter:
    write: Callable[[str], bool | Awaitable[bool | None] | None]
    aborted: bool = False

    async def emit_async(self, chunk: str) -> None:
        if self.aborted or not chunk:
            return
        outcome = self.write(chunk)
        if inspect.isawaitable(outcome):
            outcome = await outcome
        self.aborted = outcome is False

    def stopped(self) -> bool:
        return self.aborted


def _stream_partial_state(
    error: BaseException,
    *,
    partial_capable: bool,
    rows_seen: int = 0,
    options: ScriptOptions,
) -> tuple[bool, bool, bool, tuple[str, ...], str | None]:
    # A zero-row cancel/timeout is a hard error, not an empty partial — an empty
    # "partial" would read as a valid truncation of the real result.
    if (
        isinstance(error, QueryCancelledError)
        and error.readonly
        and partial_capable
        and rows_seen > 0
    ):
        return True, False, True, ("query cancelled; returning partial rows",), None
    if isinstance(error, QueryTimeoutError):
        if error.readonly and partial_capable and options.partial_on_timeout and rows_seen > 0:
            return True, True, True, ("query timed out; returning partial rows",), None
        return False, True, False, (), str(error)
    if isinstance(error, PartialQueryError):
        result = error.result
        return (
            False,
            result.timed_out,
            result.partial,
            result.warnings,
            str(error),
        )
    if partial_capable and rows_seen > 0:
        return (
            False,
            False,
            True,
            ("query failed; returning partial rows",),
            str(error),
        )
    return False, False, False, (), str(error)


def _interrupted_result_columns(error: BaseException) -> tuple[str, ...]:
    if isinstance(error, (QueryCancelledError, QueryTimeoutError)):
        return error.result_columns
    if isinstance(error, PartialQueryError):
        return error.result.columns
    return ()


def _stream_statement_head(
    statement_index: int,
    statement: str,
    columns: tuple[str, ...],
    options: ScriptOptions,
) -> str:
    fields = [f'"statement_index":{statement_index}']
    if options.include_sql:
        fields.append(f'"sql":{json.dumps(statement, ensure_ascii=False)}')
    fields.extend(
        (
            f'"columns":{json.dumps(columns, ensure_ascii=False, separators=(",", ":"))}',
            '"rows":[',
        )
    )
    return "{" + ",".join(fields)


def _stream_statement_tail(  # noqa: PLR0913 - mirrors the public envelope fields.
    *,
    row_count: int,
    elapsed_ms: float,
    success: bool,
    error: str | None,
    timed_out: bool,
    partial: bool,
    warnings: tuple[str, ...],
) -> str:
    fields = [
        f'"row_count":{row_count}',
        f'"elapsed_ms":{json.dumps(_elapsed_json(elapsed_ms))}',
        f'"success":{"true" if success else "false"}',
        f'"error":{json.dumps(error, ensure_ascii=False)}',
    ]
    if timed_out:
        fields.append('"timed_out":true')
    if partial:
        fields.append('"partial":true')
    if warnings:
        fields.append(
            f'"warnings":{json.dumps(warnings, ensure_ascii=False, separators=(",", ":"))}'
        )
    return "]," + ",".join(fields) + "}"


def stream_database_script_json(  # noqa: C901, PLR0912, PLR0915
    connection: Connection,
    script: str,
    options: ScriptOptions,
    write: Callable[[str], bool | None],
) -> None:
    """Execute a script and stream its canonical JSON envelope.

    Rows are encoded and released one at a time. Returning ``False`` from
    ``write`` stops execution immediately, which lets HTTP adapters propagate a
    disconnected consumer without buffering the remaining result.
    """
    options = _sticky_script_options(options)
    statements = collect_statements(script)
    sink = _StreamWriter(write)
    sink.emit(f'{{"statement_count":{len(statements)},"results":[')
    row_count_total = 0
    elapsed_ms_total = 0.0
    first_error_index: int | None = None

    for statement_index, statement in enumerate(statements):
        if sink.stopped():
            break
        if statement_index:
            sink.emit(",")
        if sink.stopped():
            break
        if cancelled := _pre_statement_cancellation(statement_index, statement, options):
            sink.emit(_stream_statement_head(statement_index, statement, (), options))
            sink.emit(
                _stream_statement_tail(
                    row_count=0,
                    elapsed_ms=0.0,
                    success=False,
                    error=cancelled.error,
                    timed_out=False,
                    partial=False,
                    warnings=(),
                )
            )
            first_error_index = statement_index
            break

        started = time.perf_counter()
        cursor = connection.cursor()
        columns: tuple[str, ...] = ()
        row_count = 0
        success = True
        error_message: str | None = None
        timed_out = False
        partial = False
        warnings: tuple[str, ...] = ()
        executed = False
        readonly = True
        buffered_rows: list[str] = []
        try:
            try:
                cursor.execute(
                    statement,
                    timeout=_stream_timeout_seconds(options),
                    should_cancel=options.should_cancel,
                )
                columns = cursor.columns
                readonly = _cursor_is_readonly(cursor)
                executed = True
            except Exception as caught:  # noqa: BLE001 - statement failures are result data.
                columns = cursor.columns
                readonly = _cursor_is_readonly(cursor)
                success, timed_out, partial, warnings, error_message = _stream_partial_state(
                    caught,
                    partial_capable=bool(columns) and readonly,
                    options=options,
                )

            sink.emit(_stream_statement_head(statement_index, statement, columns, options))
            first_row = True
            while executed and not sink.stopped():
                try:
                    row = cursor.fetchone()
                    if row is None:
                        break
                    encoded = json.dumps(
                        [_json_cell_value(cell) for cell in row],
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                except Exception as caught:  # noqa: BLE001 - statement failures are result data.
                    success, timed_out, partial, warnings, error_message = _stream_partial_state(
                        caught,
                        partial_capable=bool(columns) and readonly,
                        rows_seen=row_count,
                        options=options,
                    )
                    break
                if readonly:
                    sink.emit(encoded if first_row else "," + encoded)
                else:
                    buffered_rows.append(encoded)
                first_row = False
                row_count += 1
            if success and not readonly:
                for index, encoded in enumerate(buffered_rows):
                    sink.emit(encoded if index == 0 else "," + encoded)
                    if sink.stopped():
                        break
            elif not success and not readonly:
                row_count = 0
        finally:
            cursor.close()

        if sink.stopped():
            break
        elapsed_ms = (time.perf_counter() - started) * 1_000
        sink.emit(
            _stream_statement_tail(
                row_count=row_count,
                elapsed_ms=elapsed_ms,
                success=success,
                error=error_message,
                timed_out=timed_out,
                partial=partial,
                warnings=warnings,
            )
        )
        row_count_total += row_count
        elapsed_ms_total += elapsed_ms
        if not success:
            if first_error_index is None:
                first_error_index = statement_index
            if not options.continue_on_error:
                break

    footer = (
        f'],"row_count_total":{row_count_total},'
        f'"elapsed_ms_total":{json.dumps(_elapsed_json(elapsed_ms_total))},'
        f'"first_error_index":{json.dumps(first_error_index)},'
        f'"success":{"false" if first_error_index is not None else "true"}}}'
    )
    sink.emit(footer)


async def stream_database_script_json_async(  # noqa: C901, PLR0912, PLR0915
    connection: AsyncConnection,
    script: str,
    options: ScriptOptions,
    write: Callable[[str], bool | Awaitable[bool | None] | None],
) -> None:
    """Asynchronously execute and stream the canonical JSON envelope.

    ``write`` may be synchronous or return an awaitable. Awaitable sinks provide
    natural backpressure under both asyncio and Trio.
    """
    options = _sticky_script_options(options)
    statements = collect_statements(script)
    sink = _AsyncStreamWriter(write)
    await sink.emit_async(f'{{"statement_count":{len(statements)},"results":[')
    row_count_total = 0
    elapsed_ms_total = 0.0
    first_error_index: int | None = None

    for statement_index, statement in enumerate(statements):
        if sink.stopped():
            break
        if statement_index:
            await sink.emit_async(",")
        if sink.stopped():
            break
        if cancelled := _pre_statement_cancellation(statement_index, statement, options):
            await sink.emit_async(_stream_statement_head(statement_index, statement, (), options))
            await sink.emit_async(
                _stream_statement_tail(
                    row_count=0,
                    elapsed_ms=0.0,
                    success=False,
                    error=cancelled.error,
                    timed_out=False,
                    partial=False,
                    warnings=(),
                )
            )
            first_error_index = statement_index
            break

        started = time.perf_counter()
        row_count = 0
        success = True
        error_message: str | None = None
        timed_out = False
        partial = False
        warnings: tuple[str, ...] = ()
        columns: tuple[str, ...] = ()
        cursor: AsyncCursor | None = None
        readonly = True
        buffered_rows: list[str] = []
        try:
            try:
                cursor = await connection.execute(
                    statement,
                    timeout=_stream_timeout_seconds(options),
                    should_cancel=options.should_cancel,
                )
                columns = cursor.columns
                readonly = _cursor_is_readonly(cursor)
            except Exception as caught:  # noqa: BLE001 - statement failures are result data.
                columns = _interrupted_result_columns(caught)
                readonly = getattr(caught, "readonly", True)
                success, timed_out, partial, warnings, error_message = _stream_partial_state(
                    caught,
                    partial_capable=bool(columns) and readonly,
                    options=options,
                )

            await sink.emit_async(
                _stream_statement_head(statement_index, statement, columns, options)
            )
            if cursor is not None:
                first_row = True
                while not sink.stopped():
                    try:
                        row = await cursor.fetchone()
                        if row is None:
                            break
                        encoded = json.dumps(
                            [_json_cell_value(cell) for cell in row],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    except Exception as caught:  # noqa: BLE001 - statement failures are data.
                        success, timed_out, partial, warnings, error_message = (
                            _stream_partial_state(
                                caught,
                                partial_capable=bool(columns) and readonly,
                                rows_seen=row_count,
                                options=options,
                            )
                        )
                        break
                    if readonly:
                        await sink.emit_async(encoded if first_row else "," + encoded)
                    else:
                        buffered_rows.append(encoded)
                    first_row = False
                    row_count += 1
                if success and not readonly:
                    for index, encoded in enumerate(buffered_rows):
                        await sink.emit_async(encoded if index == 0 else "," + encoded)
                        if sink.stopped():
                            break
                elif not success and not readonly:
                    row_count = 0
        finally:
            if cursor is not None:
                await cursor.aclose()

        if sink.stopped():
            break
        elapsed_ms = (time.perf_counter() - started) * 1_000
        await sink.emit_async(
            _stream_statement_tail(
                row_count=row_count,
                elapsed_ms=elapsed_ms,
                success=success,
                error=error_message,
                timed_out=timed_out,
                partial=partial,
                warnings=warnings,
            )
        )
        row_count_total += row_count
        elapsed_ms_total += elapsed_ms
        if not success:
            if first_error_index is None:
                first_error_index = statement_index
            if not options.continue_on_error:
                break

    footer = (
        f'],"row_count_total":{row_count_total},'
        f'"elapsed_ms_total":{json.dumps(_elapsed_json(elapsed_ms_total))},'
        f'"first_error_index":{json.dumps(first_error_index)},'
        f'"success":{"false" if first_error_index is not None else "true"}}}'
    )
    await sink.emit_async(footer)


def stream_database_script_ndjson(  # noqa: C901, PLR0912, PLR0915 - streaming state machine
    connection: Connection,
    script: str,
    options: ScriptOptions,
    write: Callable[[str], bool | None],
) -> None:
    """Execute and stream one bounded-memory NDJSON record at a time."""
    options = _sticky_script_options(options)
    statements = collect_statements(script)
    sink = _StreamWriter(write)

    def emit(fields: tuple[tuple[str, object], ...]) -> None:
        sink.emit(_compact_json_object(fields) + "\n")

    for statement_index, statement in enumerate(statements):
        if sink.stopped():
            break
        if cancelled := _pre_statement_cancellation(statement_index, statement, options):
            emit(
                (
                    ("statement_index", statement_index),
                    ("error", cancelled.error or "query cancelled"),
                )
            )
            break
        cursor = connection.cursor()
        stop_after_statement = False
        try:
            try:
                cursor.execute(
                    statement,
                    timeout=_stream_timeout_seconds(options),
                    should_cancel=options.should_cancel,
                )
            except Exception as error:  # noqa: BLE001 - statement failures are stream data.
                success, timed_out, partial, warnings, error_message = _stream_partial_state(
                    error,
                    partial_capable=bool(cursor.columns) and _cursor_is_readonly(cursor),
                    options=options,
                )
                emit(
                    _ndjson_metadata_fields(
                        statement_index,
                        error=None if success else (error_message or str(error)),
                        timed_out=timed_out,
                        partial=partial,
                        warnings=warnings,
                    )
                )
                stop_after_statement = not success and not options.continue_on_error
            else:
                columns = cursor.columns
                keys = _unique_json_object_keys(columns)
                readonly = _cursor_is_readonly(cursor)
                row_count = 0
                success = True
                buffered_rows: list[tuple[tuple[str, object], ...]] = []
                while True:
                    try:
                        row = cursor.fetchone()
                        if row is None:
                            break
                        fields = tuple(
                            (
                                column,
                                _display_cell(row[index])
                                if index < len(row) and row[index] is not None
                                else None,
                            )
                            for index, column in enumerate(keys)
                        )
                    except Exception as error:  # noqa: BLE001 - statement failures are data.
                        success, timed_out, partial, warnings, error_message = (
                            _stream_partial_state(
                                error,
                                partial_capable=bool(columns) and readonly,
                                rows_seen=row_count,
                                options=options,
                            )
                        )
                        emit(
                            _ndjson_metadata_fields(
                                statement_index,
                                error=None if success else (error_message or str(error)),
                                timed_out=timed_out,
                                partial=partial,
                                warnings=warnings,
                            )
                        )
                        stop_after_statement = not success and not options.continue_on_error
                        break
                    row_count += 1
                    if readonly:
                        emit(fields)
                    else:
                        buffered_rows.append(fields)
                    if sink.stopped():
                        break
                if success and not readonly:
                    for buffered_fields in buffered_rows:
                        emit(buffered_fields)
                        if sink.stopped():
                            break
        finally:
            cursor.close()
        if stop_after_statement:
            break


async def stream_database_script_ndjson_async(  # noqa: C901, PLR0912, PLR0915
    connection: AsyncConnection,
    script: str,
    options: ScriptOptions,
    write: Callable[[str], bool | Awaitable[bool | None] | None],
) -> None:
    """Asynchronously stream bounded-memory NDJSON with sink backpressure."""
    options = _sticky_script_options(options)
    statements = collect_statements(script)
    sink = _AsyncStreamWriter(write)

    async def emit(fields: tuple[tuple[str, object], ...]) -> None:
        await sink.emit_async(_compact_json_object(fields) + "\n")

    for statement_index, statement in enumerate(statements):
        if sink.stopped():
            break
        if cancelled := _pre_statement_cancellation(statement_index, statement, options):
            await emit(
                (
                    ("statement_index", statement_index),
                    ("error", cancelled.error or "query cancelled"),
                )
            )
            break
        try:
            cursor = await connection.execute(
                statement,
                timeout=_stream_timeout_seconds(options),
                should_cancel=options.should_cancel,
            )
        except Exception as error:  # noqa: BLE001 - statement failures are stream data.
            columns = _interrupted_result_columns(error)
            readonly = getattr(error, "readonly", True)
            success, timed_out, partial, warnings, error_message = _stream_partial_state(
                error,
                partial_capable=bool(columns) and readonly,
                options=options,
            )
            await emit(
                _ndjson_metadata_fields(
                    statement_index,
                    error=None if success else (error_message or str(error)),
                    timed_out=timed_out,
                    partial=partial,
                    warnings=warnings,
                )
            )
            if not success and not options.continue_on_error:
                break
            continue
        stop_after_statement = False
        try:
            columns = cursor.columns
            keys = _unique_json_object_keys(columns)
            readonly = _cursor_is_readonly(cursor)
            row_count = 0
            success = True
            buffered_rows: list[tuple[tuple[str, object], ...]] = []
            while not sink.stopped():
                try:
                    row = await cursor.fetchone()
                    if row is None:
                        break
                    fields = tuple(
                        (
                            column,
                            _display_cell(row[index])
                            if index < len(row) and row[index] is not None
                            else None,
                        )
                        for index, column in enumerate(keys)
                    )
                except Exception as error:  # noqa: BLE001 - statement failures are data.
                    success, timed_out, partial, warnings, error_message = _stream_partial_state(
                        error,
                        partial_capable=bool(columns) and readonly,
                        rows_seen=row_count,
                        options=options,
                    )
                    await emit(
                        _ndjson_metadata_fields(
                            statement_index,
                            error=None if success else (error_message or str(error)),
                            timed_out=timed_out,
                            partial=partial,
                            warnings=warnings,
                        )
                    )
                    stop_after_statement = not success and not options.continue_on_error
                    break
                row_count += 1
                if readonly:
                    await emit(fields)
                else:
                    buffered_rows.append(fields)
            if success and not readonly:
                for buffered_fields in buffered_rows:
                    await emit(buffered_fields)
                    if sink.stopped():
                        break
        finally:
            await cursor.aclose()
        if stop_after_statement:
            break


def _compact_json_object(fields: tuple[tuple[str, object], ...]) -> str:
    return (
        "{"
        + ",".join(
            f"{json.dumps(key, ensure_ascii=False)}:"
            f"{json.dumps(value, ensure_ascii=False, separators=(',', ':'))}"
            for key, value in fields
        )
        + "}"
    )


def iter_script_result_json(
    result: ScriptResult,
    *,
    include_sql: bool = False,
) -> Iterator[str]:
    """Yield canonical JSON in encoder-sized chunks."""
    encoder = json.JSONEncoder(ensure_ascii=False, separators=(",", ":"))
    yield from encoder.iterencode(script_result_to_object(result, include_sql=include_sql))


def stream_script_result_json(
    result: ScriptResult,
    write: Callable[[str], object],
    *,
    include_sql: bool = False,
) -> None:
    """Stream canonical JSON chunks to ``write``."""
    for chunk in iter_script_result_json(result, include_sql=include_sql):
        write(chunk)


def json_to_script_result(payload: str | bytes | bytearray) -> ScriptResult:
    """Parse a tolerant canonical JSON envelope."""
    failure = ScriptResult(
        success=False,
        parse_error="json_to_script_result: could not parse result JSON",
    )
    try:
        decoded: object = json.loads(payload)
    except (TypeError, ValueError):
        return failure
    if not isinstance(decoded, dict):
        return failure
    root = cast("dict[str, object]", decoded)
    raw_results: object = root.get("results", [])
    results: list[StatementResult] = []
    if isinstance(raw_results, list):
        for candidate_statement in cast("list[object]", raw_results):
            if not isinstance(candidate_statement, dict):
                continue
            raw_statement = cast("dict[str, object]", candidate_statement)
            raw_rows: object = raw_statement.get("rows", [])
            rows: list[SQLiteRow] = []
            if isinstance(raw_rows, list):
                rows.extend(
                    tuple(_json_cell(cell) for cell in cast("list[object]", candidate_row))
                    for candidate_row in cast("list[object]", raw_rows)
                    if isinstance(candidate_row, list)
                )
            raw_columns: object = raw_statement.get("columns", [])
            columns = (
                tuple(str(column) for column in cast("list[object]", raw_columns))
                if isinstance(raw_columns, list)
                else ()
            )
            raw_warnings: object = raw_statement.get("warnings", [])
            warnings = (
                tuple(str(warning) for warning in cast("list[object]", raw_warnings))
                if isinstance(raw_warnings, list)
                else ()
            )
            results.append(
                StatementResult(
                    statement_index=_as_int(raw_statement.get("statement_index"), 0),
                    success=bool(raw_statement.get("success", False)),
                    columns=columns,
                    rows=tuple(rows),
                    row_count=_as_int(raw_statement.get("row_count"), len(rows)),
                    elapsed_ms=_as_float(raw_statement.get("elapsed_ms")),
                    error=(
                        str(raw_statement["error"])
                        if raw_statement.get("error") is not None
                        else None
                    ),
                    sql=str(raw_statement.get("sql", "")),
                    timed_out=bool(raw_statement.get("timed_out", False)),
                    partial=bool(raw_statement.get("partial", False)),
                    warnings=warnings,
                )
            )
    parse_error = str(root.get("parse_error", ""))
    error_value = root.get("error")
    if not parse_error and root.get("success") is False and isinstance(error_value, str):
        parse_error = error_value
    first_error = root.get("first_error_index")
    return ScriptResult(
        success=bool(root.get("success", False)),
        statement_count=_as_int(root.get("statement_count"), 0),
        results=tuple(results),
        row_count_total=_as_int(root.get("row_count_total"), 0),
        elapsed_ms_total=_as_float(root.get("elapsed_ms_total")),
        first_error_index=first_error if isinstance(first_error, int) else None,
        parse_error=parse_error,
    )


def _json_cell(value: object) -> SQLiteValue:
    if isinstance(value, bool):
        return int(value)
    if value is None or isinstance(value, (str, int, float)):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _as_int(value: object, default: int) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else default


def _as_float(value: object) -> float:
    return float(value) if isinstance(value, (int, float)) else 0.0


def print_table(
    columns: Iterable[str],
    rows: Iterable[Iterable[SQLiteValue]],
    options: TablePrintOptions | None = None,
) -> str:
    """Render a compact table compatible with the shared core."""
    effective = options or TablePrintOptions()
    column_tuple = tuple(columns)
    row_tuple = tuple(
        tuple("NULL" if (display := _display_cell(cell)) is None else display for cell in row)
        for row in rows
    )
    if not column_tuple:
        if effective.style is TableStyle.BOXED:
            return ""
        suffix = "\n" if effective.newline_after_no_result else ""
        return effective.no_result + suffix
    widths = [len(column) for column in column_tuple]
    for row in row_tuple:
        for index, cell in enumerate(row[: len(widths)]):
            widths[index] = max(widths[index], len(cell))

    if effective.style is TableStyle.BOXED:
        rule = "+" + "".join("-" * (width + 2) + "+" for width in widths)

        def render_boxed(values: Iterable[str]) -> str:
            cells = tuple(values)
            return (
                "| "
                + "".join(
                    (cells[index] if index < len(cells) else "").ljust(width) + " | "
                    for index, width in enumerate(widths)
                )
                + "\n"
            )

        rendered = (
            rule
            + "\n"
            + render_boxed(column_tuple)
            + rule
            + "\n"
            + "".join(render_boxed(row) for row in row_tuple)
            + rule
            + "\n"
        )
        if effective.boxed_row_count_footer:
            rendered += f"{len(row_tuple)} row(s)\n"
        return rendered

    def render(values: Iterable[str]) -> str:
        cells = tuple(values)
        return "  ".join(
            (cells[index] if index < len(cells) else "").ljust(width)
            for index, width in enumerate(widths)
        ).rstrip()

    lines = [
        render(column_tuple),
        "  ".join("-" * width for width in widths),
        *(render(row) for row in row_tuple),
    ]
    return "\n".join(lines) + "\n"


def script_result_to_text(result: ScriptResult) -> str:
    """Render a script result as human-readable text."""
    if result.parse_error:
        return f"PARSE ERROR: {result.parse_error}"
    sections: list[str] = []
    for statement in result.results:
        plural = "" if statement.row_count == 1 else "s"
        section = (
            f"-- statement {statement.statement_index + 1}/{result.statement_count} "
            f"({_elapsed_json(statement.elapsed_ms)} ms, "
            f"{statement.row_count} row{plural})\n"
        )
        if statement.success:
            section += print_table(statement.columns, statement.rows)
        else:
            section += f"ERROR: {statement.error or ''}\n"
        for warning in statement.warnings:
            section += f"-- warning: {warning}\n"
        if statement.timed_out:
            section += "-- query timed out; results are partial\n"
        elif statement.partial:
            section += "-- results are partial\n"
        sections.append(section.rstrip("\n"))
    return "\n\n".join(sections) + ("\n" if sections else "")


def script_result_to_csv(result: ScriptResult) -> str:
    """Render result sets as RFC 4180-style comma-delimited text."""
    return _script_result_to_delimited(result, csv_mode=True)


def script_result_to_tsv(result: ScriptResult) -> str:
    """Render result sets as one-record-per-line tab-delimited text."""
    return _script_result_to_delimited(result, csv_mode=False)


def _script_result_to_delimited(result: ScriptResult, *, csv_mode: bool) -> str:
    if result.parse_error:
        return f"# PARSE ERROR: {result.parse_error}\n"
    sections: list[str] = []
    multiple = len(result.results) > 1
    for statement in result.results:
        output = io.StringIO(newline="")
        if multiple:
            output.write(f"# statement {statement.statement_index + 1}/{result.statement_count}\n")
        if not statement.success:
            output.write(f"# ERROR: {statement.error or ''}\n")
        elif not statement.columns:
            output.write("(no result)\n")
        elif csv_mode:
            writer = csv.writer(output, lineterminator="\n")
            writer.writerow(statement.columns)
            writer.writerows(
                tuple(_display_cell(cell) or "" for cell in row) for row in statement.rows
            )
        else:
            output.write("\t".join(_sanitize_tsv(column) for column in statement.columns) + "\n")
            for row in statement.rows:
                output.write(
                    "\t".join(_sanitize_tsv(_display_cell(cell) or "") for cell in row) + "\n"
                )
        sections.append(output.getvalue().rstrip("\n"))
    return "\n\n".join(sections) + ("\n" if sections else "")


def _sanitize_tsv(value: str) -> str:
    return value.replace("\t", " ").replace("\r", " ").replace("\n", " ")


__all__ = [
    "ExportOptions",
    "ScriptOptions",
    "ScriptResult",
    "ScriptStatementResult",
    "StatementResult",
    "TablePrintOptions",
    "TableStyle",
    "collect_statements",
    "execute_script",
    "export_tables",
    "export_tables_async",
    "iter_script_result_json",
    "json_to_script_result",
    "print_table",
    "quote_identifier",
    "run_database_script",
    "run_database_script_async",
    "run_script_with_executor",
    "script_result_to_csv",
    "script_result_to_json",
    "script_result_to_jsonl",
    "script_result_to_object",
    "script_result_to_text",
    "script_result_to_tsv",
    "split_script",
    "stream_database_script_json",
    "stream_database_script_json_async",
    "stream_database_script_ndjson",
    "stream_database_script_ndjson_async",
    "stream_script_result_json",
]
