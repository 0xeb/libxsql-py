# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Pythonic synchronous and asynchronous APSW connection wrappers."""

# APSW's published value aliases contain an untyped buffer arm. All values are
# normalized at this module boundary; internal collaborators intentionally
# share private lifetime state.
# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownMemberType=false

from __future__ import annotations

import contextlib
import contextvars
import inspect
import itertools
import re
import sys
import threading
import time
import weakref
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Never, Protocol, Self, cast, overload

import anyio
import apsw
from anyio.lowlevel import current_token as current_event_loop_token

from .errors import (
    ClosedError,
    ConfigurationError,
    PartialQueryError,
    QueryCancelledError,
    QueryTimeoutError,
    ReadOnlyError,
    ReentrancyError,
    RegistrationError,
    ThreadingError,
    UnsupportedRuntimeError,
)
from .types import Bindings, ColumnDescription, QueryResult, SQLiteRow, SQLiteValue

if TYPE_CHECKING:
    from collections.abc import Generator
    from os import PathLike
    from pathlib import Path
    from types import TracebackType

    from .functions import AsyncScalarFunction, ContextScalarFunction, ScalarFunction
    from .script import ExportOptions, ScriptOptions, ScriptResult
    from .vtable import Registration, TableDefinition

_DEFAULT_OPEN_FLAGS = apsw.SQLITE_OPEN_READWRITE | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_URI
_SAVEPOINT_IDS = itertools.count()
_TIMEOUT_WARNING = "query timed out; results are partial"
_CANCEL_WARNING = "query cancelled; results are partial"
_QUERY_ERROR_WARNING = "query failed; results are partial"
_MAX_FUNCTION_NAME_BYTES = 255
_MAX_FUNCTION_ARITY = 127
_AGGREGATE_PROTOCOL_SIZE = 3
_ACTIVE_CALLBACK_CONNECTIONS: contextvars.ContextVar[frozenset[int]] = contextvars.ContextVar(
    "libxsql_active_callback_connections", default=frozenset()
)
_ACTIVE_INTERRUPT_CHECKER: contextvars.ContextVar[Callable[[], bool] | None] = (
    contextvars.ContextVar("libxsql_active_interrupt_checker", default=None)
)
_SQL_TOKEN_PATTERN = re.compile(
    r"""
    '(?:''|[^'])*'
    | "(?:""|[^"])*"
    | `(?:``|[^`])*`
    | \[[^\]]*\]
    | --[^\r\n]*
    | /\*.*?\*/
    | (?P<word>[A-Za-z_][A-Za-z0-9_]*)
    """,
    re.DOTALL | re.VERBOSE,
)
_AuthorizerCallback = Callable[
    [int, str | None, str | None, str | None, str | None],
    int,
]


@dataclass(slots=True)
class _CancellationState:
    deadline: float | None
    should_cancel: Callable[[], bool] | None
    interrupt_event: threading.Event
    timed_out: bool = False
    cancelled: bool = False
    failure: BaseException | None = None
    defer_interrupt: bool = False

    def check(self) -> bool:  # noqa: PLR0911 - ordered sticky cancellation causes.
        """Record and report the active query's cancellation cause."""
        if self.failure is not None or self.cancelled:
            return True
        if self.interrupt_event.is_set():
            # An explicit interrupt belongs to the operation that observes it.
            # Keep this state sticky while allowing a different cursor queued
            # behind the connection lock to proceed normally.
            self.interrupt_event.clear()
            self.cancelled = True
            return True
        if self.should_cancel is not None:
            try:
                if self.should_cancel():
                    self.cancelled = True
                    return True
            except BaseException as error:
                self.failure = error
                return True
        if self.timed_out:
            return True
        if self.deadline is not None and time.perf_counter() >= self.deadline:
            self.timed_out = True
            return True
        return False

    def progress(self) -> bool:
        """Poll while optionally deferring SQLite's transaction-wide interrupt."""
        interrupted = self.check()
        return interrupted and not self.defer_interrupt


@dataclass(slots=True)
class _PreparedStatementMetadata:
    """Metadata observed by an execution tracer before APSW starts stepping."""

    description: tuple[tuple[str, str | None], ...] = ()
    readonly: bool = True

    @property
    def columns(self) -> tuple[str, ...]:
        """Return only prepared result-column names."""
        return tuple(item[0] for item in self.description)

    def capture(self, cursor: apsw.Cursor) -> None:
        """Capture the prepared statement's result description and mutability."""
        try:
            raw_description = cursor.get_description()
        except apsw.ExecutionCompleteError:
            raw_description = ()
        self.description = tuple(
            (str(name), declared_type or None) for name, declared_type in raw_description
        )
        with contextlib.suppress(apsw.ExecutionCompleteError):
            self.readonly = bool(cursor.is_readonly)


def _raise_for_cancellation(
    cancellation: _CancellationState,
    *,
    result_columns: tuple[str, ...] = (),
    readonly: bool = True,
) -> None:
    """Raise the normalized exception when ``cancellation`` has fired."""
    if not cancellation.check():
        return
    if cancellation.failure is not None:
        raise cancellation.failure
    if cancellation.cancelled:
        raise QueryCancelledError(
            "query cancelled",
            result_columns=result_columns,
            readonly=readonly,
        )
    raise QueryTimeoutError(result_columns=result_columns, readonly=readonly)


def callback_interrupted() -> bool:
    """Return whether the active libxsql callback should stop work.

    The function is deliberately false outside a library-managed callback.
    Long-running cache builders, filters, generators, and scalar callbacks can
    poll it cheaply and return as soon as it becomes true.
    """
    checker = _ACTIVE_INTERRUPT_CHECKER.get()
    return bool(checker is not None and checker())


@contextlib.contextmanager
def _interruption_scope(checker: Callable[[], bool]) -> Generator[None, None, None]:
    token = _ACTIVE_INTERRUPT_CHECKER.set(checker)
    try:
        yield
    finally:
        _ACTIVE_INTERRUPT_CHECKER.reset(token)


def _ensure_supported_runtime() -> None:
    """Reject unsupported Python implementations and disabled-GIL runtimes."""
    if sys.implementation.name != "cpython":
        raise UnsupportedRuntimeError(
            f"libxsql requires CPython 3.11 or newer; {sys.implementation.name} is not supported"
        )
    is_gil_enabled = getattr(sys, "_is_gil_enabled", None)
    if callable(is_gil_enabled) and not is_gil_enabled():
        raise UnsupportedRuntimeError(
            "libxsql requires a GIL-enabled CPython runtime; "
            "disabled-GIL execution is not currently safe in APSW"
        )


def _normalize_value(value: object) -> SQLiteValue:
    if value is None or isinstance(value, (float, str, bytes)):
        return value
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise OverflowError("SQLite INTEGER values must fit in a signed 64-bit integer")
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    raise TypeError(
        "SQLite values must be None, int, float, str, bytes, bytearray, or memoryview; "
        f"got {type(value).__name__}"
    )


def _normalize_bindings(bindings: Bindings | None) -> Bindings | None:
    if bindings is None:
        return None
    if isinstance(bindings, Mapping):
        return {str(key): _normalize_value(value) for key, value in bindings.items()}
    if isinstance(bindings, (str, bytes, bytearray, memoryview)):
        raise TypeError("statement bindings must be a sequence or mapping, not a scalar value")
    return tuple(_normalize_value(value) for value in bindings)


def _normalize_row(row: Iterable[object]) -> SQLiteRow:
    return tuple(_normalize_value(value) for value in row)


def _validate_function_registration(name: str, num_args: int) -> None:
    if not name or "\x00" in name:
        message = "SQL function name must be non-empty and contain no NUL"
        raise ConfigurationError(message)
    if len(name.encode("utf-8")) > _MAX_FUNCTION_NAME_BYTES:
        message = "SQL function name must be at most 255 UTF-8 bytes"
        raise ConfigurationError(message)
    if num_args < -1 or num_args > _MAX_FUNCTION_ARITY:
        message = "num_args must be -1 (variadic) or between 0 and 127"
        raise ConfigurationError(message)


def _is_dml_returning(sql: str) -> bool:
    words = {
        match.group("word").casefold()
        for match in _SQL_TOKEN_PATTERN.finditer(sql)
        if match.group("word") is not None
    }
    return "returning" in words and bool(words & {"insert", "update", "delete"})


def _is_rollback_statement(sql: str) -> bool:
    return (
        next(
            (
                match.group("word").casefold()
                for match in _SQL_TOKEN_PATTERN.finditer(sql)
                if match.group("word") is not None
            ),
            "",
        )
        == "rollback"
    )


def _is_missing_savepoint(error: BaseException) -> bool:
    return isinstance(error, apsw.SQLError) and "no such savepoint" in str(error).casefold()


@contextlib.contextmanager
def _callback_scope(
    connection_key: int,
    interruption_checker: Callable[[], bool] | None = None,
) -> Generator[None, None, None]:
    active = _ACTIVE_CALLBACK_CONNECTIONS.get()
    token = _ACTIVE_CALLBACK_CONNECTIONS.set(active | {connection_key})
    interrupt_token = (
        _ACTIVE_INTERRUPT_CHECKER.set(interruption_checker)
        if interruption_checker is not None
        else None
    )
    try:
        yield
    finally:
        if interrupt_token is not None:
            _ACTIVE_INTERRUPT_CHECKER.reset(interrupt_token)
        _ACTIVE_CALLBACK_CONNECTIONS.reset(token)


def _check_callback_reentrancy(connection: object) -> None:
    if id(connection) in _ACTIVE_CALLBACK_CONNECTIONS.get():
        message = "a libxsql connection cannot be used recursively from its own callback"
        raise ReentrancyError(message)


class _RegistrationAuthorizer:
    """Compose an existing authorizer with live virtual-table write capabilities."""

    def __init__(
        self,
        registrations: list[Registration],
        previous: _AuthorizerCallback | None,
    ) -> None:
        self.registrations = registrations
        self.previous = previous
        self._drop_target: tuple[str, str] | None = None

    def __call__(
        self,
        action: int,
        parameter_one: str | None,
        parameter_two: str | None,
        database: str | None,
        source: str | None,
    ) -> int:
        """Authorize one SQLite prepare operation."""
        if self.previous is not None:
            previous_result = self.previous(
                action,
                parameter_one,
                parameter_two,
                database,
                source,
            )
            if previous_result != apsw.SQLITE_OK:
                self._drop_target = None
                return previous_result

        registration = self._find_registration(parameter_one, database)
        target = self._target(parameter_one, database)
        if action == apsw.SQLITE_DROP_VTABLE:
            self._drop_target = target if registration is not None else None
            return apsw.SQLITE_OK
        if action == apsw.SQLITE_DELETE and target == self._drop_target:
            self._drop_target = None
            return apsw.SQLITE_OK
        self._drop_target = None

        if registration is None:
            return apsw.SQLITE_OK
        return self._authorize_write(registration, action, parameter_two)

    @staticmethod
    def _authorize_write(
        registration: Registration,
        action: int,
        column_name: str | None,
    ) -> int:
        definition = registration.definition
        if action == apsw.SQLITE_INSERT:
            allowed = definition.insert_row is not None
        elif action == apsw.SQLITE_DELETE:
            allowed = definition.delete_row is not None
        elif action == apsw.SQLITE_UPDATE:
            column = next(
                (
                    item
                    for item in definition.columns
                    if column_name is not None and item.name.casefold() == column_name.casefold()
                ),
                None,
            )
            allowed = column is not None and (
                definition.update_row is not None or column.set is not None
            )
        else:
            return apsw.SQLITE_OK
        if allowed:
            return apsw.SQLITE_OK
        operation = {
            apsw.SQLITE_INSERT: "INSERT",
            apsw.SQLITE_DELETE: "DELETE",
            apsw.SQLITE_UPDATE: "UPDATE",
        }[action]
        if action == apsw.SQLITE_UPDATE and column_name is not None:
            message = (
                f"column {column_name!r} in table {registration.table_name!r} "
                f"is read-only and does not support {operation}"
            )
        else:
            message = (
                f"table {registration.table_name!r} is read-only and does not support {operation}"
            )
        raise ReadOnlyError(message)

    def _find_registration(
        self,
        table_name: str | None,
        database: str | None,
    ) -> Registration | None:
        target = self._target(table_name, database)
        if target is None:
            return None
        for registration in reversed(self.registrations):
            try:
                if (
                    registration.is_active
                    and self._target(registration.table_name, registration.schema) == target
                ):
                    return registration
            except AttributeError:
                # Cleanup is intentionally best-effort, including for a
                # malformed child injected by an adapter.
                continue
        return None

    @staticmethod
    def _target(
        table_name: str | None,
        database: str | None,
    ) -> tuple[str, str] | None:
        if table_name is None or database is None:
            return None
        return database.casefold(), table_name.casefold()


async def _await_callback_result_in_scope(
    awaitable: Awaitable[object],
    connection_key: int,
    interruption_checker: Callable[[], bool] | None = None,
) -> object:
    with _callback_scope(connection_key, interruption_checker):
        return await awaitable


def _resolve_async_callback(
    value: object,
    runner: Callable[[Awaitable[object]], object],
    connection_key: int,
    interruption_checker: Callable[[], bool] | None = None,
) -> object:
    if not inspect.isawaitable(value):
        return value
    return runner(
        _await_callback_result_in_scope(
            value,
            connection_key,
            interruption_checker,
        )
    )


def _async_callback_runner(
    connection: object,
) -> Callable[[Awaitable[object]], object]:
    controller = getattr(connection, "async_controller", None)
    runner = getattr(controller, "async_run_coro", None)
    if not callable(runner):
        message = "APSW async callback routing is unavailable on this connection"
        raise UnsupportedRuntimeError(message)
    return cast("Callable[[Awaitable[object]], object]", runner)


class _AggregateObject(Protocol):
    def step(self, *values: object) -> object:
        """Accumulate one aggregate row."""
        ...

    def final(self) -> object:
        """Produce the final aggregate value."""
        ...


class _AsyncAggregateBridge:
    """Synchronous APSW adapter for sync or async aggregate protocols."""

    def __init__(
        self,
        target: object,
        runner: Callable[[Awaitable[object]], object],
        connection_key: int,
        interruption_checker: Callable[[], bool],
    ) -> None:
        self._runner = runner
        self._connection_key = connection_key
        self._interruption_checker = interruption_checker
        self._step: Callable[..., object]
        self._final: Callable[..., object]
        if isinstance(target, tuple) and len(target) == _AGGREGATE_PROTOCOL_SIZE:
            aggregate_tuple = cast(
                "tuple[object, Callable[..., object], Callable[..., object]]",
                target,
            )
            self._state, self._step, self._final = aggregate_tuple
            self._tuple_protocol = True
        else:
            self._state = target
            aggregate = cast("_AggregateObject", target)
            self._step = aggregate.step
            self._final = aggregate.final
            self._tuple_protocol = False

    def step(self, *values: object) -> None:
        """Forward one aggregate row through the configured event-loop runner."""
        with _callback_scope(self._connection_key, self._interruption_checker):
            result = (
                self._step(self._state, *values) if self._tuple_protocol else self._step(*values)
            )
        _resolve_async_callback(
            result,
            self._runner,
            self._connection_key,
            self._interruption_checker,
        )

    def final(self) -> SQLiteValue:
        """Resolve and normalize the aggregate's final SQLite value."""
        with _callback_scope(self._connection_key, self._interruption_checker):
            result = self._final(self._state) if self._tuple_protocol else self._final()
        resolved = _resolve_async_callback(
            result,
            self._runner,
            self._connection_key,
            self._interruption_checker,
        )
        return _normalize_value(resolved)


def _validate_timeout(timeout: float | None) -> float | None:
    if timeout is None:
        return None
    value = float(timeout)
    if value < 0:
        raise ConfigurationError("timeout must be non-negative or None")
    return value or None


def _is_async_timeout(error: BaseException) -> bool:
    """Recognize APSW deadline errors on every supported AnyIO backend."""
    if isinstance(error, (TimeoutError, apsw.InterruptError)):
        return True
    try:
        from trio import TooSlowError
    except ImportError:
        return False
    return isinstance(error, TooSlowError)


class Cursor(Iterator[SQLiteRow]):
    """A streaming synchronous SQLite cursor.

    Cursors retain their parent connection and release timeout handlers
    deterministically when exhausted or closed.
    """

    def __init__(self, connection: Connection, raw_cursor: apsw.Cursor | None = None) -> None:
        """Initialize an idle cursor.

        Args:
            connection: Owning libxsql connection.
            raw_cursor: Existing APSW cursor, when wrapping an executed statement.
        """
        self._connection = connection
        self._raw = raw_cursor if raw_cursor is not None else connection.raw_connection.cursor()
        self._closed = False
        self._complete = False
        self._columns: tuple[str, ...] = ()
        self._description: tuple[ColumnDescription, ...] = ()
        self._prepared_description: tuple[tuple[str, str | None], ...] = ()
        self._prepared_readonly = True
        self._rows_seen = 0
        self._deadline: float | None = None
        self._timed_out = False
        self._cancellation: _CancellationState | None = None
        self._readonly = True
        self._returning_savepoint: str | None = None
        if raw_cursor is not None:
            self._refresh_metadata()
        connection._track_cursor(self)

    @property
    def raw_cursor(self) -> apsw.Cursor:
        """Return the underlying APSW cursor."""
        self._ensure_open()
        return self._raw

    @property
    def connection(self) -> Connection:
        """Return the owning connection."""
        return self._connection

    @property
    def columns(self) -> tuple[str, ...]:
        """Return result column names."""
        return self._columns

    @property
    def description(self) -> tuple[ColumnDescription, ...]:
        """Return a DB-API-shaped result description."""
        return self._description

    @property
    def rowcount(self) -> int:
        """Return rows fetched, or changed rows for a completed mutation.

        A streaming query reports ``-1`` until it is exhausted.
        """
        if self._columns and not self._complete:
            return -1
        if self._columns:
            return self._rows_seen
        return self._connection.changes

    @property
    def lastrowid(self) -> int:
        """Return the connection's most recent inserted rowid."""
        return self._connection.last_insert_rowid

    @property
    def is_readonly(self) -> bool:
        """Whether SQLite classified the active statement as read-only."""
        return self._readonly

    @property
    def closed(self) -> bool:
        """Whether the cursor is closed."""
        return self._closed

    def execute(
        self,
        sql: str,
        bindings: Bindings | None = None,
        *,
        timeout: float | None = None,
        should_cancel: Callable[[], bool] | None = None,
        can_cache: bool = True,
    ) -> Self:
        """Execute SQL and return this cursor.

        Args:
            sql: One SQL statement.
            bindings: Positional or named values.
            timeout: Deadline in seconds. ``None`` uses the connection default.
            should_cancel: Optional cooperative cancellation predicate.
            can_cache: Whether APSW may use its prepared-statement cache.
        """
        self._ensure_open()
        self._connection._check()
        self._finish_timeout()
        self._complete = False
        self._rows_seen = 0
        self._columns = ()
        self._description = ()
        self._start_operation(timeout, should_cancel)
        try:
            self._begin_returning_savepoint(sql)
            normalized = _normalize_bindings(bindings)
            cancellation = self._require_cancellation()
            self._raise_if_interrupted()
            with (
                self._capture_statement_metadata(cancellation),
                self._active_operation(cancellation),
                _interruption_scope(cancellation.check),
            ):
                if normalized is None:
                    self._raw.execute(sql, can_cache=can_cache)
                else:
                    self._raw.execute(sql, normalized, can_cache=can_cache)
            self._refresh_metadata()
            if _is_rollback_statement(sql):
                self._connection._refresh_registration_tables()
            if self.columns:
                self._raise_if_interrupted()
        except BaseException as error:
            self._raise_execution_error(error)
        if not self._columns:
            self._complete = True
            self._finish_returning_savepoint(commit=True)
            self._finish_timeout()
        return self

    def executemany(
        self,
        sql: str,
        bindings: Iterable[Bindings],
        *,
        timeout: float | None = None,
        should_cancel: Callable[[], bool] | None = None,
        can_cache: bool = True,
    ) -> Self:
        """Execute the same SQL for each bindings collection."""
        self._ensure_open()
        self._connection._check()
        self._finish_timeout()
        self._complete = False
        self._rows_seen = 0
        self._columns = ()
        self._description = ()
        self._start_operation(timeout, should_cancel)
        normalized = (_normalize_bindings(item) for item in bindings)
        try:
            self._begin_returning_savepoint(sql)
            cancellation = self._require_cancellation()
            self._raise_if_interrupted()
            with (
                self._capture_statement_metadata(cancellation),
                self._active_operation(cancellation),
                _interruption_scope(cancellation.check),
            ):
                self._raw.executemany(
                    sql,
                    cast("Iterable[Any]", normalized),
                    can_cache=can_cache,
                )
            self._refresh_metadata()
            if self.columns:
                self._raise_if_interrupted()
        except BaseException as error:
            self._raise_execution_error(error)
        if not self._columns:
            self._complete = True
            self._finish_returning_savepoint(commit=True)
            self._finish_timeout()
        return self

    def fetchone(self) -> SQLiteRow | None:
        """Return the next row, or ``None`` when exhausted."""
        try:
            return next(self)
        except StopIteration:
            return None

    def fetchmany(self, size: int = 1) -> list[SQLiteRow]:
        """Return at most ``size`` rows."""
        if size < 0:
            raise ValueError("size must be non-negative")
        return list(itertools.islice(self, size))

    def fetchall(self) -> list[SQLiteRow]:
        """Consume and return all remaining rows."""
        return list(self)

    def close(self) -> None:
        """Close the cursor and discard any unconsumed work."""
        if self._closed:
            return
        self._connection._check()
        self._closed = True
        self._complete = True
        try:
            self._raw.close(force=True)
        finally:
            try:
                self._finish_returning_savepoint(commit=False)
            finally:
                self._finish_timeout()
                self._connection._discard_cursor(self)

    def __iter__(self) -> Self:
        """Return this streaming iterator."""
        self._ensure_open()
        return self

    def __next__(self) -> SQLiteRow:
        """Return the next result row."""
        self._ensure_open()
        self._connection._check()
        if self._complete:
            raise StopIteration
        try:
            cancellation = self._cancellation
            if cancellation is None:
                row = next(self._raw)
            else:
                self._raise_if_interrupted()
                with (
                    self._active_operation(cancellation),
                    _interruption_scope(cancellation.check),
                ):
                    row = next(self._raw)
                self._raise_if_interrupted()
        except StopIteration:
            self._complete = True
            self._finish_returning_savepoint(commit=True)
            self._finish_timeout()
            raise
        except BaseException as error:
            self._raise_execution_error(error)
        self._rows_seen += 1
        return _normalize_row(row)

    def __enter__(self) -> Self:
        """Enter a cursor lifetime context."""
        self._ensure_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the cursor."""
        del exc_type, exc_value, traceback
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise ClosedError("cursor is closed")
        self._connection._check()

    def _refresh_metadata(self) -> None:
        try:
            apsw_description = self._raw.get_description()
        except apsw.ExecutionCompleteError:
            raw_description = self._prepared_description
        else:
            raw_description = tuple(
                (str(name), declared_type or None) for name, declared_type in apsw_description
            )
            self._prepared_description = raw_description
        self._columns = tuple(item[0] for item in raw_description)
        self._description = tuple(
            (name, declared_type or None, None, None, None, None, None)
            for name, declared_type in raw_description
        )
        try:
            self._readonly = bool(self._raw.is_readonly)
        except apsw.ExecutionCompleteError:
            self._readonly = self._prepared_readonly

    @contextlib.contextmanager
    def _capture_statement_metadata(
        self,
        cancellation: _CancellationState | None = None,
    ) -> Generator[None, None, None]:
        """Capture prepared metadata and optionally reject cancellation before stepping."""
        previous = cast(
            "Callable[[apsw.Cursor, str, object], bool] | None",
            self._raw.get_exec_trace(),
        )
        inherited = cast(
            "Callable[[apsw.Cursor, str, object], bool] | None",
            self._connection.raw_connection.get_exec_trace(),
        )
        chained = previous if previous is not None else inherited
        self._prepared_description = ()
        self._prepared_readonly = True

        def capture(cursor: apsw.Cursor, statement: str, bindings: object) -> bool:
            try:
                raw_description = cursor.get_description()
            except apsw.ExecutionCompleteError:
                raw_description = ()
            self._prepared_description = tuple(
                (str(name), declared_type or None) for name, declared_type in raw_description
            )
            self._columns = tuple(name for name, _ in self._prepared_description)
            with contextlib.suppress(apsw.ExecutionCompleteError):
                self._prepared_readonly = bool(cursor.is_readonly)
            proceed = True if chained is None else chained(cursor, statement, bindings)
            if proceed and cancellation is not None:
                _raise_for_cancellation(
                    cancellation,
                    result_columns=self._columns,
                    readonly=self._prepared_readonly,
                )
            return proceed

        self._raw.set_exec_trace(capture)
        try:
            yield
        finally:
            self._raw.set_exec_trace(previous)

    @contextlib.contextmanager
    def _active_operation(
        self,
        cancellation: _CancellationState,
    ) -> Generator[None, None, None]:
        """Expose this cursor's cancellation state only while SQLite is stepping."""
        previous = self._connection._active_interruption_state
        self._connection._active_interruption_state = cancellation
        try:
            yield
        finally:
            if self._connection._active_interruption_state is cancellation:
                self._connection._active_interruption_state = previous

    def _start_operation(
        self,
        timeout: float | None,
        should_cancel: Callable[[], bool] | None,
    ) -> None:
        effective = self._connection._default_query_timeout if timeout is None else timeout
        effective = _validate_timeout(effective)
        self._connection._interrupt_event.clear()
        self._deadline = None if effective is None else time.perf_counter() + effective
        self._timed_out = False
        self._cancellation = _CancellationState(
            self._deadline,
            should_cancel,
            self._connection._interrupt_event,
        )

    def _finish_timeout(self) -> None:
        if self._cancellation is None:
            return
        if self._connection._active_interruption_state is self._cancellation:
            self._connection._active_interruption_state = None
        self._connection._interrupt_event.clear()
        self._deadline = None
        self._cancellation = None

    def _require_cancellation(self) -> _CancellationState:
        cancellation = self._cancellation
        if cancellation is None:
            message = "cursor has no active operation"
            raise RuntimeError(message)
        return cancellation

    def _begin_returning_savepoint(self, sql: str) -> None:
        cancellation = self._require_cancellation()
        interruption_enabled = (
            cancellation.deadline is not None or cancellation.should_cancel is not None
        )
        if not interruption_enabled or not _is_dml_returning(sql):
            return
        name = f"libxsql_query_{next(_SAVEPOINT_IDS)}"
        self._connection._execute_control_sql(f'SAVEPOINT "{name}"')
        self._returning_savepoint = name
        cancellation.defer_interrupt = True

    def _finish_returning_savepoint(self, *, commit: bool) -> None:
        name = self._returning_savepoint
        if name is None:
            return
        self._returning_savepoint = None
        rollback_error: BaseException | None = None
        if not commit:
            try:
                self._connection._execute_control_sql(f'ROLLBACK TO SAVEPOINT "{name}"')
            except BaseException as error:
                if _is_missing_savepoint(error):
                    return
                rollback_error = error
        try:
            self._connection._execute_control_sql(f'RELEASE SAVEPOINT "{name}"')
        except BaseException as error:
            if _is_missing_savepoint(error):
                return
            if rollback_error is None:
                raise
        if rollback_error is not None:
            raise rollback_error

    def _raise_if_interrupted(self) -> None:
        _raise_for_cancellation(
            self._require_cancellation(),
            result_columns=self.columns,
            readonly=self.is_readonly,
        )

    def _raise_execution_error(self, error: BaseException) -> Never:
        cancellation = self._cancellation
        interrupted = cancellation is not None and cancellation.check()
        failure = None if cancellation is None else cancellation.failure
        timed_out = self._timed_out or (cancellation is not None and cancellation.timed_out)
        cancelled = cancellation is not None and cancellation.cancelled
        with contextlib.suppress(Exception):
            self._refresh_metadata()
        rollback_failure: BaseException | None = None
        if self._returning_savepoint is not None:
            with contextlib.suppress(Exception):
                self._raw.close(force=True)
            self._raw = self._connection.raw_connection.cursor()
            try:
                self._finish_returning_savepoint(commit=False)
            except BaseException as cleanup_error:
                rollback_failure = cleanup_error
        self._finish_timeout()
        if rollback_failure is not None:
            raise rollback_failure from error
        if failure is not None:
            raise failure from error
        if cancelled and (interrupted or isinstance(error, apsw.InterruptError)):
            raise QueryCancelledError(
                "query cancelled",
                result_columns=self.columns,
                readonly=self.is_readonly,
            ) from error
        if timed_out and (interrupted or isinstance(error, (apsw.InterruptError, TimeoutError))):
            raise QueryTimeoutError(
                result_columns=self.columns,
                readonly=self.is_readonly,
            ) from error
        raise error


class Connection:
    """A Pythonic, thread-affine APSW connection."""

    def __init__(
        self,
        path: str = ":memory:",
        *,
        flags: int | None = None,
        vfs: str | None = None,
        statement_cache_size: int = 100,
        busy_timeout: float | None = None,
        default_query_timeout: float | None = None,
    ) -> None:
        """Open a SQLite database.

        Args:
            path: Database filename, URI, or ``":memory:"``.
            flags: SQLite open flags. Defaults to read-write/create/URI.
            vfs: Optional SQLite VFS name.
            statement_cache_size: APSW prepared-statement cache capacity.
            busy_timeout: Lock wait timeout in seconds.
            default_query_timeout: Per-statement execution timeout in seconds.
        """
        _ensure_supported_runtime()
        if statement_cache_size < 0:
            raise ConfigurationError("statement_cache_size must be non-negative")
        open_flags = _DEFAULT_OPEN_FLAGS if flags is None else flags
        raw = apsw.Connection(
            path,
            flags=open_flags,
            vfs=vfs,
            statementcachesize=statement_cache_size,
        )
        self._initialize(raw, owns=True, default_query_timeout=default_query_timeout)
        if busy_timeout is not None:
            if busy_timeout < 0:
                self.close()
                raise ConfigurationError("busy_timeout must be non-negative or None")
            raw.set_busy_timeout(round(busy_timeout * 1_000))
        from .functions import register_blob_concat

        register_blob_concat(self)

    def _initialize(
        self,
        raw: apsw.Connection,
        *,
        owns: bool,
        default_query_timeout: float | None,
    ) -> None:
        self._raw = raw
        self._owns = owns
        self._closed = False
        self._thread_id = threading.get_ident()
        self._default_query_timeout = _validate_timeout(default_query_timeout)
        self._interrupt_event = threading.Event()
        self._active_interruption_state: _CancellationState | None = None
        self._progress_id = object()
        raw.set_progress_handler(
            self._dispatch_progress,
            1_000,
            id=self._progress_id,
        )
        self._cursors: weakref.WeakSet[Cursor] = weakref.WeakSet()
        self._registrations: list[Registration] = []
        self._scalar_functions: dict[tuple[str, int], str] = {}
        self._aggregate_functions: dict[tuple[str, int], str] = {}
        previous_authorizer = raw.authorizer
        self._registration_authorizer: _RegistrationAuthorizer | None = _RegistrationAuthorizer(
            self._registrations,
            previous_authorizer,
        )
        raw.set_authorizer(self._registration_authorizer)
        self._transaction_depth = 0

    @classmethod
    def wrap(
        cls,
        raw_connection: apsw.Connection,
        *,
        owns: bool = False,
        default_query_timeout: float | None = None,
        register_builtins: bool = True,
    ) -> Self:
        """Wrap an existing APSW connection.

        Args:
            raw_connection: Existing synchronous APSW connection.
            owns: Close the APSW connection when this wrapper closes.
            default_query_timeout: Default per-statement timeout in seconds.
            register_builtins: Register libxsql built-in functions.
        """
        _ensure_supported_runtime()
        if not isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
            raw_connection,
            apsw.Connection,
        ):
            raise TypeError("raw_connection must be an apsw.Connection")
        instance = cls.__new__(cls)
        instance._initialize(
            raw_connection,
            owns=owns,
            default_query_timeout=default_query_timeout,
        )
        if register_builtins:
            from .functions import register_blob_concat

            register_blob_concat(instance)
        return instance

    @property
    def raw_connection(self) -> apsw.Connection:
        """Return the underlying APSW connection."""
        self._check()
        return self._raw

    @property
    def closed(self) -> bool:
        """Whether this wrapper is closed."""
        return self._closed

    @property
    def last_insert_rowid(self) -> int:
        """Return the rowid from the most recent successful insert."""
        self._check()
        return int(self._raw.last_insert_rowid())

    @property
    def changes(self) -> int:
        """Return rows changed by the most recently completed statement."""
        self._check()
        return int(self._raw.changes())

    @property
    def in_transaction(self) -> bool:
        """Whether SQLite currently has an active transaction."""
        self._check()
        return bool(self._raw.in_transaction)

    def cursor(self) -> Cursor:
        """Create an idle streaming cursor."""
        self._check()
        return Cursor(self)

    def execute(
        self,
        sql: str,
        bindings: Bindings | None = None,
        *,
        timeout: float | None = None,
        should_cancel: Callable[[], bool] | None = None,
        can_cache: bool = True,
    ) -> Cursor:
        """Execute SQL and return a streaming cursor."""
        return self.cursor().execute(
            sql,
            bindings,
            timeout=timeout,
            should_cancel=should_cancel,
            can_cache=can_cache,
        )

    def executemany(
        self,
        sql: str,
        bindings: Iterable[Bindings],
        *,
        timeout: float | None = None,
        should_cancel: Callable[[], bool] | None = None,
        can_cache: bool = True,
    ) -> Cursor:
        """Execute SQL once for each set of bindings."""
        return self.cursor().executemany(
            sql,
            bindings,
            timeout=timeout,
            should_cancel=should_cancel,
            can_cache=can_cache,
        )

    def query(
        self,
        sql: str,
        bindings: Bindings | None = None,
        *,
        timeout: float | None = None,
        should_cancel: Callable[[], bool] | None = None,
        partial_on_timeout: bool = True,
    ) -> QueryResult:
        """Materialize a query result.

        A timed-out read-only statement returns its complete row prefix when
        ``partial_on_timeout`` is true. Mutations, including statements with a
        ``RETURNING`` clause, roll back and raise.
        """
        started = time.perf_counter()
        rows: list[SQLiteRow] = []
        cursor = self.cursor()
        columns: tuple[str, ...] = ()
        try:
            cursor.execute(
                sql,
                bindings,
                timeout=timeout,
                should_cancel=should_cancel,
            )
            columns = cursor.columns
            rows.extend(cursor)
        except QueryTimeoutError as error:
            elapsed_ms = (time.perf_counter() - started) * 1_000
            columns = error.result_columns or cursor.columns
            # A zero-row cancel/timeout is a hard error, not an empty partial — an
            # empty "partial" would read as a valid truncation of the real result.
            if partial_on_timeout and columns and cursor.is_readonly and rows:
                return QueryResult(
                    columns=columns,
                    rows=tuple(rows),
                    elapsed_ms=elapsed_ms,
                    timed_out=True,
                    partial=True,
                    warnings=(_TIMEOUT_WARNING,),
                )
            raise QueryTimeoutError(
                elapsed_ms=elapsed_ms,
                result_columns=columns,
                readonly=error.readonly,
            ) from None
        except QueryCancelledError as error:
            elapsed_ms = (time.perf_counter() - started) * 1_000
            columns = error.result_columns or cursor.columns
            if columns and cursor.is_readonly and rows:
                return QueryResult(
                    columns=columns,
                    rows=tuple(rows),
                    elapsed_ms=elapsed_ms,
                    partial=True,
                    warnings=(_CANCEL_WARNING,),
                )
            raise QueryCancelledError(
                "query cancelled",
                result_columns=columns,
                readonly=error.readonly,
            ) from None
        except Exception as error:
            elapsed_ms = (time.perf_counter() - started) * 1_000
            columns = cursor.columns or columns
            if rows and columns and cursor.is_readonly:
                result = QueryResult(
                    columns=columns,
                    rows=tuple(rows),
                    elapsed_ms=elapsed_ms,
                    partial=True,
                    warnings=(_QUERY_ERROR_WARNING,),
                )
                raise PartialQueryError(str(error), result=result) from error
            raise
        finally:
            cursor.close()
        return QueryResult(
            columns=columns,
            rows=tuple(rows),
            elapsed_ms=(time.perf_counter() - started) * 1_000,
        )

    def scalar(
        self,
        sql: str,
        bindings: Bindings | None = None,
        *,
        default: SQLiteValue = None,
        timeout: float | None = None,
        should_cancel: Callable[[], bool] | None = None,
    ) -> SQLiteValue:
        """Return the first cell from a materialized query."""
        return self.query(
            sql,
            bindings,
            timeout=timeout,
            should_cancel=should_cancel,
        ).scalar(default)

    def transaction(self, mode: str = "deferred") -> Transaction:
        """Create a transaction/savepoint context manager."""
        return Transaction(self, mode)

    @overload
    def register_function(
        self,
        name: str,
        function: ScalarFunction | None,
        num_args: int = -1,
        *,
        deterministic: bool = False,
        flags: int = 0,
        with_context: Literal[False] = False,
    ) -> None: ...

    @overload
    def register_function(
        self,
        name: str,
        function: ContextScalarFunction | None,
        num_args: int = -1,
        *,
        deterministic: bool = False,
        flags: int = 0,
        with_context: Literal[True],
    ) -> None: ...

    def register_function(
        self,
        name: str,
        function: Any,
        num_args: int = -1,
        *,
        deterministic: bool = False,
        flags: int = 0,
        with_context: bool = False,
    ) -> None:
        """Register a scalar SQL function."""
        from .functions import register_function

        if with_context:
            register_function(
                self,
                name,
                function,
                num_args,
                deterministic=deterministic,
                flags=flags,
                with_context=True,
            )
        else:
            register_function(
                self,
                name,
                function,
                num_args,
                deterministic=deterministic,
                flags=flags,
                with_context=False,
            )

    def register_aggregate(
        self,
        name: str,
        factory: Any,
        num_args: int = -1,
        *,
        flags: int = 0,
    ) -> None:
        """Register an aggregate SQL function."""
        from .functions import register_aggregate

        register_aggregate(self, name, factory, num_args, flags=flags)

    def register(
        self,
        definition: TableDefinition[Any],
        *,
        table_name: str | None = None,
        schema: str = "temp",
    ) -> Registration:
        """Register an immutable virtual-table definition."""
        from .vtable import register_definition

        self._check()
        registration = register_definition(
            self._raw,
            definition,
            table_name=table_name,
            schema=schema,
            interrupt_checker=self._callback_interruption_checker,
        )
        self._registrations.append(registration)
        return registration

    def run_script(
        self,
        script: str,
        options: ScriptOptions | None = None,
    ) -> ScriptResult:
        """Execute a multi-statement SQL script."""
        from .script import run_database_script

        return run_database_script(self, script, options)

    def export_tables(
        self,
        tables: Iterable[str],
        output_path: str | PathLike[str],
        options: ExportOptions | None = None,
    ) -> Path:
        """Export tables to an atomically replaced SQL script."""
        from .script import export_tables

        return export_tables(self, tables, output_path, options)

    def interrupt(self) -> None:
        """Interrupt active SQLite work; safe to call from another thread."""
        if self._closed:
            raise ClosedError("connection is closed")
        self._interrupt_event.set()
        state = self._active_interruption_state
        if state is None or not state.defer_interrupt:
            self._raw.interrupt()

    def close(self) -> None:
        """Close child resources and, when owned, the APSW connection."""
        if self._closed:
            return
        self._check_thread()
        for cursor in tuple(self._cursors):
            with contextlib.suppress(Exception):
                cursor.close()
        self._cursors.clear()
        for registration in reversed(self._registrations):
            with contextlib.suppress(Exception):
                registration.close()
        self._registrations.clear()
        self._unregister_functions()
        self._restore_registration_authorizer()
        with contextlib.suppress(Exception):
            self._raw.set_progress_handler(None, id=self._progress_id)
        self._closed = True
        if self._owns:
            self._raw.close(force=True)

    def _track_cursor(self, cursor: Cursor) -> None:
        self._cursors.add(cursor)

    def _discard_cursor(self, cursor: Cursor) -> None:
        self._cursors.discard(cursor)

    def _record_scalar_registration(
        self,
        name: str,
        num_args: int,
        *,
        registered: bool,
    ) -> None:
        key = name.casefold(), num_args
        if registered:
            self._aggregate_functions.pop(key, None)
            self._scalar_functions[key] = name
        else:
            self._scalar_functions.pop(key, None)
            self._aggregate_functions.pop(key, None)

    def _record_aggregate_registration(
        self,
        name: str,
        num_args: int,
        *,
        registered: bool,
    ) -> None:
        key = name.casefold(), num_args
        if registered:
            self._scalar_functions.pop(key, None)
            self._aggregate_functions[key] = name
        else:
            self._aggregate_functions.pop(key, None)
            self._scalar_functions.pop(key, None)

    def _unregister_functions(self) -> None:
        if self._owns:
            self._scalar_functions.clear()
            self._aggregate_functions.clear()
            return

        failures: list[tuple[str, str, int, Exception]] = []
        for key, name in tuple(self._scalar_functions.items()):
            num_args = key[1]
            try:
                self._raw.create_scalar_function(name, None, num_args)
            except Exception as error:
                failures.append(("scalar", name, num_args, error))
            else:
                self._scalar_functions.pop(key, None)
        for key, name in tuple(self._aggregate_functions.items()):
            num_args = key[1]
            try:
                self._raw.create_aggregate_function(name, None, num_args)
            except Exception as error:
                failures.append(("aggregate", name, num_args, error))
            else:
                self._aggregate_functions.pop(key, None)

        if failures:
            overloads = ", ".join(
                f"{kind} {name!r}/{num_args}" for kind, name, num_args, _ in failures
            )
            message = f"could not unregister wrapper-owned SQL function overloads: {overloads}"
            raise RegistrationError(message) from failures[0][3]

    def _restore_registration_authorizer(self) -> None:
        authorizer = self._registration_authorizer
        if authorizer is not None and self._raw.authorizer is authorizer:
            self._raw.set_authorizer(authorizer.previous)
        self._registration_authorizer = None

    def __enter__(self) -> Self:
        """Enter a connection lifetime context."""
        self._check()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the connection."""
        del exc_type, exc_value, traceback
        self.close()

    def _check_thread(self) -> None:
        if threading.get_ident() != self._thread_id:
            raise ThreadingError(
                "synchronous Connection objects may only be used from their creation thread"
            )

    def _check(self) -> None:
        if self._closed:
            raise ClosedError("connection is closed")
        _check_callback_reentrancy(self)
        self._check_thread()

    def _callback_interruption_checker(self) -> bool:
        state = self._active_interruption_state
        return bool(state is not None and state.check())

    def _dispatch_progress(self) -> bool:
        """Poll only the cursor operation currently stepping this connection."""
        state = self._active_interruption_state
        return False if state is None else state.progress()

    def _execute_control_sql(self, sql: str) -> None:
        cursor = self._raw.cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close(force=True)

    def _refresh_registration_tables(self) -> None:
        for registration in self._registrations:
            if registration._active:
                schema = registration.schema.replace('"', '""')
                cursor = self._raw.cursor()
                try:
                    row = cursor.execute(
                        f'SELECT sql FROM "{schema}".sqlite_schema '  # noqa: S608
                        "WHERE type='table' AND name=?",
                        (registration.table_name,),
                    ).fetchone()
                finally:
                    cursor.close(force=True)
                create_sql = None if row is None or row[0] is None else str(row[0])
                registration._refresh_table_state(create_sql)


class Transaction:
    """A synchronous transaction that nests using SQLite savepoints."""

    _VALID_MODES = frozenset({"deferred", "immediate", "exclusive"})

    def __init__(self, connection: Connection, mode: str) -> None:
        """Initialize a transaction context without beginning it."""
        normalized = mode.strip().lower()
        if normalized not in self._VALID_MODES:
            raise ConfigurationError("transaction mode must be deferred, immediate, or exclusive")
        self._connection = connection
        self._mode = normalized
        self._savepoint: str | None = None
        self._entered = False

    def __enter__(self) -> Self:
        """Begin the transaction or nested savepoint."""
        if self._entered:
            raise RuntimeError("transaction context cannot be entered twice")
        connection = self._connection
        connection._check()
        nested = connection._transaction_depth > 0 or connection.in_transaction
        if nested:
            self._savepoint = f"libxsql_{next(_SAVEPOINT_IDS)}"
            cursor = connection.execute(f'SAVEPOINT "{self._savepoint}"')
        else:
            cursor = connection.execute(f"BEGIN {self._mode.upper()}")
        cursor.close()
        connection._transaction_depth += 1
        self._entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        """Commit on success, otherwise roll back."""
        del exc_value, traceback
        if not self._entered:
            return False
        connection = self._connection
        try:
            if self._savepoint is not None:
                if exc_type is None:
                    cursor = connection.execute(f'RELEASE SAVEPOINT "{self._savepoint}"')
                    cursor.close()
                else:
                    cursor = connection.execute(f'ROLLBACK TO SAVEPOINT "{self._savepoint}"')
                    cursor.close()
                    cursor = connection.execute(f'RELEASE SAVEPOINT "{self._savepoint}"')
                    cursor.close()
            elif exc_type is None:
                cursor = connection.execute("COMMIT")
                cursor.close()
            else:
                cursor = connection.execute("ROLLBACK")
                cursor.close()
        finally:
            connection._transaction_depth = max(0, connection._transaction_depth - 1)
            self._entered = False
        return False


class AsyncCursor(AsyncIterator[SQLiteRow]):
    """A streaming APSW cursor bound to one async connection worker."""

    def __init__(
        self,
        connection: AsyncConnection,
        raw_cursor: Any,
        *,
        cancellation: _CancellationState,
        deadline: float | None = None,
        prepared_metadata: _PreparedStatementMetadata | None = None,
        returning_savepoint: str | None = None,
    ) -> None:
        """Wrap an already-executed APSW async cursor."""
        self._connection = connection
        self._raw = raw_cursor
        self._deadline = deadline
        self._cancellation = cancellation
        self._closed = False
        self._complete = False
        self._rows_seen = 0
        self._columns: tuple[str, ...] = ()
        self._description: tuple[ColumnDescription, ...] = ()
        self._prepared_metadata = prepared_metadata or _PreparedStatementMetadata()
        self._readonly = self._prepared_metadata.readonly
        self._returning_savepoint = returning_savepoint
        self._refresh_metadata()
        self._complete = not self._columns
        self._raw_iterator = raw_cursor.__aiter__()
        connection._track_cursor(self)

    @property
    def raw_cursor(self) -> Any:
        """Return the underlying APSW async cursor."""
        if self._closed:
            raise ClosedError("cursor is closed")
        self._connection._check()
        return self._raw

    @property
    def connection(self) -> AsyncConnection:
        """Return the owning async connection."""
        return self._connection

    @property
    def columns(self) -> tuple[str, ...]:
        """Return result column names."""
        return self._columns

    @property
    def description(self) -> tuple[ColumnDescription, ...]:
        """Return a DB-API-shaped result description."""
        return self._description

    @property
    def rowcount(self) -> int:
        """Return fetched row count after exhaustion, otherwise ``-1``."""
        return self._rows_seen if self._complete else -1

    @property
    def is_readonly(self) -> bool:
        """Whether SQLite classified the active statement as read-only."""
        return self._readonly

    @property
    def closed(self) -> bool:
        """Whether this cursor is closed."""
        return self._closed

    async def fetchone(self) -> SQLiteRow | None:
        """Return the next row, or ``None`` when exhausted."""
        try:
            return await anext(self)
        except StopAsyncIteration:
            return None

    async def fetchmany(self, size: int = 1) -> list[SQLiteRow]:
        """Return at most ``size`` rows."""
        if size < 0:
            raise ValueError("size must be non-negative")
        rows: list[SQLiteRow] = []
        for _ in range(size):
            row = await self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    async def fetchall(self) -> list[SQLiteRow]:
        """Consume and return all remaining rows."""
        return [row async for row in self]

    async def aclose(self) -> None:
        """Close the cursor and discard unconsumed work."""
        if self._closed:
            return
        self._connection._check()
        self._closed = True
        self._complete = True
        try:
            if self._returning_savepoint is None:
                await self._connection._run_async_operation(
                    self._cancellation,
                    lambda: self._raw.aclose(force=True),
                    check_before=False,
                    check_after=False,
                )
            else:
                await self._connection._close_control_cursor(self._raw)
                await self._finish_returning_savepoint(commit=False)
        finally:
            self._connection._discard_cursor(self)

    def __aiter__(self) -> Self:
        """Return this async row iterator."""
        if self._closed:
            raise ClosedError("cursor is closed")
        self._connection._check()
        return self

    async def __anext__(self) -> SQLiteRow:  # noqa: C901 - cancellation cleanup state machine
        """Return the next result row."""
        if self._closed:
            raise ClosedError("cursor is closed")
        self._connection._check()
        if self._complete:
            raise StopAsyncIteration
        try:
            row = await self._connection._run_async_operation(
                self._cancellation,
                lambda: self._with_deadline(self._raw_iterator.__anext__()),
            )
        except StopAsyncIteration:
            self._complete = True
            await self._finish_returning_savepoint(commit=True)
            raise
        except BaseException as error:
            rollback_failure: BaseException | None = None
            if self._returning_savepoint is not None:
                try:
                    await self._connection._close_control_cursor(self._raw)
                    await self._finish_returning_savepoint(commit=False)
                except BaseException as cleanup_error:
                    rollback_failure = cleanup_error
                self._closed = True
                self._complete = True
                self._connection._discard_cursor(self)
            if rollback_failure is not None:
                raise rollback_failure from error
            if self._cancellation.failure is not None:
                raise self._cancellation.failure from error
            if self._cancellation.cancelled:
                raise QueryCancelledError(
                    "query cancelled",
                    result_columns=self.columns,
                    readonly=self.is_readonly,
                ) from error
            if self._cancellation.timed_out or _is_async_timeout(error):
                raise QueryTimeoutError(
                    result_columns=self.columns,
                    readonly=self.is_readonly,
                ) from error
            raise
        self._rows_seen += 1
        return _normalize_row(row)

    async def _finish_returning_savepoint(self, *, commit: bool) -> None:
        name = self._returning_savepoint
        if name is None:
            return
        self._returning_savepoint = None
        await self._connection._finish_query_savepoint(name, commit=commit)

    async def __aenter__(self) -> Self:
        """Enter an async cursor lifetime context."""
        if self._closed:
            raise ClosedError("cursor is closed")
        self._connection._check()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the cursor."""
        del exc_type, exc_value, traceback
        await self.aclose()

    async def _with_deadline(self, awaitable: Any) -> Any:
        if self._deadline is None or self._cancellation.defer_interrupt:
            return await awaitable
        from apsw import aio

        token = aio.deadline.set(self._deadline)
        try:
            return await awaitable
        finally:
            aio.deadline.reset(token)

    def _refresh_metadata(self) -> None:
        try:
            apsw_description = self._raw.get_description()
        except apsw.ExecutionCompleteError:
            raw_description = self._prepared_metadata.description
        else:
            raw_description = tuple(
                (str(name), declared_type or None) for name, declared_type in apsw_description
            )
        self._columns = tuple(item[0] for item in raw_description)
        self._description = tuple(
            (name, declared_type or None, None, None, None, None, None)
            for name, declared_type in raw_description
        )
        with contextlib.suppress(apsw.ExecutionCompleteError):
            self._readonly = bool(self._raw.is_readonly)


class AsyncConnection:
    """An AnyIO-compatible APSW connection with a dedicated worker thread."""

    def _initialize(
        self,
        raw: Any,
        *,
        owns: bool,
        default_query_timeout: float | None,
    ) -> None:
        self._raw = raw
        self._owns = owns
        self._closed = False
        self._event_loop_token = self._current_event_loop_token()
        self._default_query_timeout = _validate_timeout(default_query_timeout)
        self._interrupt_event = threading.Event()
        self._active_interruption_state: _CancellationState | None = None
        self._operation_lock = anyio.Lock()
        self._cursors: weakref.WeakSet[AsyncCursor] = weakref.WeakSet()
        self._registrations: list[Registration] = []
        self._scalar_functions: dict[tuple[str, int], str] = {}
        self._aggregate_functions: dict[tuple[str, int], str] = {}
        self._registration_authorizer: _RegistrationAuthorizer | None = None
        self._authorizer_lock = anyio.Lock()
        self._transaction_depth = 0

    @classmethod
    def wrap(
        cls,
        raw_connection: Any,
        *,
        owns: bool = False,
        default_query_timeout: float | None = None,
    ) -> Self:
        """Wrap an existing APSW ``AsyncConnection``."""
        _ensure_supported_runtime()
        if not callable(getattr(raw_connection, "async_run", None)):
            raise TypeError("raw_connection must be an apsw.AsyncConnection")
        instance = cls()
        instance._initialize(
            raw_connection,
            owns=owns,
            default_query_timeout=default_query_timeout,
        )
        return instance

    @property
    def raw_connection(self) -> Any:
        """Return the underlying APSW async connection."""
        self._check()
        return self._raw

    @property
    def closed(self) -> bool:
        """Whether this wrapper is closed."""
        return self._closed

    async def in_transaction(self) -> bool:
        """Return whether SQLite currently has an active transaction."""
        self._check()
        return bool(await self._raw.async_run(lambda: self._raw.in_transaction))

    async def execute(
        self,
        sql: str,
        bindings: Bindings | None = None,
        *,
        timeout: float | None = None,
        should_cancel: Callable[[], bool] | None = None,
        can_cache: bool = True,
    ) -> AsyncCursor:
        """Execute SQL and return a streaming async cursor."""
        self._check()
        deadline = self._deadline(timeout)
        remaining = None if deadline is None else max(0.0, deadline - anyio.current_time())
        cancellation = _CancellationState(
            None if remaining is None else time.perf_counter() + remaining,
            should_cancel,
            self._interrupt_event,
        )
        normalized = _normalize_bindings(bindings)
        prepared_metadata = _PreparedStatementMetadata()
        returning_savepoint = await self._begin_query_savepoint(sql, cancellation)
        try:

            async def operation() -> Any:
                if deadline is None or cancellation.defer_interrupt:
                    return (
                        await self._raw.execute(sql, can_cache=can_cache)
                        if normalized is None
                        else await self._raw.execute(sql, normalized, can_cache=can_cache)
                    )
                from apsw import aio

                token = aio.deadline.set(deadline)
                try:
                    return (
                        await self._raw.execute(sql, can_cache=can_cache)
                        if normalized is None
                        else await self._raw.execute(sql, normalized, can_cache=can_cache)
                    )
                finally:
                    aio.deadline.reset(token)

            raw_cursor = await self._run_async_operation(
                cancellation,
                operation,
                check_before=True,
                check_after=False,
                clear_interrupt=True,
                prepared_metadata=prepared_metadata,
            )
            if _is_rollback_statement(sql):
                await self._refresh_registration_tables()
        except BaseException as error:
            if returning_savepoint is not None:
                await self._finish_query_savepoint(returning_savepoint, commit=False)
            if cancellation.cancelled:
                raise QueryCancelledError(
                    "query cancelled",
                    result_columns=prepared_metadata.columns,
                    readonly=prepared_metadata.readonly,
                ) from error
            if cancellation.timed_out or _is_async_timeout(error):
                raise QueryTimeoutError(
                    result_columns=prepared_metadata.columns,
                    readonly=prepared_metadata.readonly,
                ) from error
            raise
        return AsyncCursor(
            self,
            raw_cursor,
            deadline=deadline,
            cancellation=cancellation,
            prepared_metadata=prepared_metadata,
            returning_savepoint=returning_savepoint,
        )

    async def executemany(
        self,
        sql: str,
        bindings: Iterable[Bindings],
        *,
        timeout: float | None = None,
        should_cancel: Callable[[], bool] | None = None,
        can_cache: bool = True,
    ) -> AsyncCursor:
        """Execute SQL once for each set of bindings."""
        self._check()
        deadline = self._deadline(timeout)
        remaining = None if deadline is None else max(0.0, deadline - anyio.current_time())
        cancellation = _CancellationState(
            None if remaining is None else time.perf_counter() + remaining,
            should_cancel,
            self._interrupt_event,
        )
        normalized = (_normalize_bindings(item) for item in bindings)
        prepared_metadata = _PreparedStatementMetadata()
        returning_savepoint = await self._begin_query_savepoint(sql, cancellation)
        try:

            async def operation() -> Any:
                if deadline is None or cancellation.defer_interrupt:
                    return await self._raw.executemany(
                        sql,
                        normalized,
                        can_cache=can_cache,
                    )
                from apsw import aio

                token = aio.deadline.set(deadline)
                try:
                    return await self._raw.executemany(
                        sql,
                        normalized,
                        can_cache=can_cache,
                    )
                finally:
                    aio.deadline.reset(token)

            raw_cursor = await self._run_async_operation(
                cancellation,
                operation,
                check_before=True,
                check_after=False,
                clear_interrupt=True,
                prepared_metadata=prepared_metadata,
            )
        except BaseException as error:
            if returning_savepoint is not None:
                await self._finish_query_savepoint(returning_savepoint, commit=False)
            if cancellation.cancelled:
                raise QueryCancelledError(
                    "query cancelled",
                    result_columns=prepared_metadata.columns,
                    readonly=prepared_metadata.readonly,
                ) from error
            if cancellation.timed_out or _is_async_timeout(error):
                raise QueryTimeoutError(
                    result_columns=prepared_metadata.columns,
                    readonly=prepared_metadata.readonly,
                ) from error
            raise
        return AsyncCursor(
            self,
            raw_cursor,
            deadline=deadline,
            cancellation=cancellation,
            prepared_metadata=prepared_metadata,
            returning_savepoint=returning_savepoint,
        )

    async def query(
        self,
        sql: str,
        bindings: Bindings | None = None,
        *,
        timeout: float | None = None,
        should_cancel: Callable[[], bool] | None = None,
        partial_on_timeout: bool = True,
    ) -> QueryResult:
        """Materialize a query without blocking the event loop."""
        started = time.perf_counter()
        rows: list[SQLiteRow] = []
        try:
            cursor = await self.execute(
                sql,
                bindings,
                timeout=timeout,
                should_cancel=should_cancel,
            )
        except QueryTimeoutError as error:
            # Timed out before any row was fetched: a zero-row timeout is a hard
            # error, not an empty partial.
            raise QueryTimeoutError(
                elapsed_ms=(time.perf_counter() - started) * 1_000,
                result_columns=error.result_columns,
                readonly=error.readonly,
            ) from None
        except QueryCancelledError as error:
            # Cancelled before any row was fetched: a zero-row cancel is an error.
            raise QueryCancelledError(
                "query cancelled",
                result_columns=error.result_columns,
                readonly=error.readonly,
            ) from None
        columns = cursor.columns
        try:
            async for row in cursor:
                # Preserve already-fetched rows when a later iteration times out.
                rows.append(row)  # noqa: PERF401
        except QueryTimeoutError as error:
            elapsed_ms = (time.perf_counter() - started) * 1_000
            columns = error.result_columns or columns
            # A zero-row cancel/timeout is a hard error, not an empty partial.
            if partial_on_timeout and columns and cursor.is_readonly and rows:
                return QueryResult(
                    columns=columns,
                    rows=tuple(rows),
                    elapsed_ms=elapsed_ms,
                    timed_out=True,
                    partial=True,
                    warnings=(_TIMEOUT_WARNING,),
                )
            raise QueryTimeoutError(
                elapsed_ms=elapsed_ms,
                result_columns=columns,
                readonly=error.readonly,
            ) from None
        except QueryCancelledError as error:
            columns = error.result_columns or columns
            if cursor.is_readonly and rows:
                return QueryResult(
                    columns=columns,
                    rows=tuple(rows),
                    elapsed_ms=(time.perf_counter() - started) * 1_000,
                    partial=True,
                    warnings=(_CANCEL_WARNING,),
                )
            raise QueryCancelledError(
                "query cancelled",
                result_columns=columns,
                readonly=False,
            ) from None
        except Exception as error:
            elapsed_ms = (time.perf_counter() - started) * 1_000
            columns = cursor.columns or columns
            if rows and columns and cursor.is_readonly:
                result = QueryResult(
                    columns=columns,
                    rows=tuple(rows),
                    elapsed_ms=elapsed_ms,
                    partial=True,
                    warnings=(_QUERY_ERROR_WARNING,),
                )
                raise PartialQueryError(str(error), result=result) from error
            raise
        finally:
            await cursor.aclose()
        return QueryResult(
            columns=columns,
            rows=tuple(rows),
            elapsed_ms=(time.perf_counter() - started) * 1_000,
        )

    async def scalar(
        self,
        sql: str,
        bindings: Bindings | None = None,
        *,
        default: SQLiteValue = None,
        timeout: float | None = None,
        should_cancel: Callable[[], bool] | None = None,
    ) -> SQLiteValue:
        """Return the first cell from a materialized async query."""
        return (
            await self.query(
                sql,
                bindings,
                timeout=timeout,
                should_cancel=should_cancel,
            )
        ).scalar(default)

    def transaction(self, mode: str = "deferred") -> AsyncTransaction:
        """Create an async transaction/savepoint context manager."""
        return AsyncTransaction(self, mode)

    @overload
    async def register_function(
        self,
        name: str,
        function: AsyncScalarFunction | None,
        num_args: int = -1,
        *,
        deterministic: bool = False,
        flags: int = 0,
        with_context: Literal[False] = False,
    ) -> None: ...

    @overload
    async def register_function(
        self,
        name: str,
        function: ContextScalarFunction | None,
        num_args: int = -1,
        *,
        deterministic: bool = False,
        flags: int = 0,
        with_context: Literal[True],
    ) -> None: ...

    async def register_function(
        self,
        name: str,
        function: Callable[..., object] | None,
        num_args: int = -1,
        *,
        deterministic: bool = False,
        flags: int = 0,
        with_context: bool = False,
    ) -> None:
        """Register a synchronous or asynchronous scalar SQL function."""
        self._check()
        _validate_function_registration(name, num_args)
        if with_context and function is not None and inspect.iscoroutinefunction(function):
            message = "context-aware SQL functions must be synchronous"
            raise ConfigurationError(message)

        callback: Callable[..., SQLiteValue] | None
        if function is None:
            callback = None
        else:
            runner = _async_callback_runner(self._raw)
            scalar_function = function
            connection_key = id(self)
            interruption_checker = self._callback_interruption_checker

            def invoke(*arguments: object) -> SQLiteValue:
                from .functions import FunctionContext, _FunctionContextState

                with _callback_scope(connection_key, interruption_checker):
                    if with_context:
                        context = FunctionContext(_FunctionContextState(self._raw))
                        try:
                            callback_result = scalar_function(context, *arguments)
                        finally:
                            context._expire()
                    else:
                        callback_result = scalar_function(*arguments)
                if with_context and inspect.isawaitable(callback_result):
                    close = getattr(callback_result, "close", None)
                    if callable(close):
                        close()
                    message = "context-aware SQL functions must be synchronous"
                    raise ConfigurationError(message)
                result = _resolve_async_callback(
                    callback_result,
                    runner,
                    connection_key,
                    interruption_checker,
                )
                return _normalize_value(result)

            callback = invoke

        await self._raw.create_scalar_function(
            name,
            callback,
            num_args,
            deterministic=deterministic,
            flags=flags,
        )
        self._record_scalar_registration(
            name,
            num_args,
            registered=function is not None,
        )

    async def register_aggregate(
        self,
        name: str,
        factory: Callable[[], object] | None,
        num_args: int = -1,
        *,
        flags: int = 0,
    ) -> None:
        """Register a synchronous or asynchronous aggregate SQL function."""
        self._check()
        _validate_function_registration(name, num_args)
        bridge_factory: Callable[[], _AsyncAggregateBridge] | None
        if factory is None:
            bridge_factory = None
        else:
            runner = _async_callback_runner(self._raw)
            aggregate_factory = factory
            connection_key = id(self)
            interruption_checker = self._callback_interruption_checker

            def make_aggregate() -> _AsyncAggregateBridge:
                with _callback_scope(connection_key, interruption_checker):
                    factory_result = aggregate_factory()
                target = _resolve_async_callback(
                    factory_result,
                    runner,
                    connection_key,
                    interruption_checker,
                )
                return _AsyncAggregateBridge(
                    target,
                    runner,
                    connection_key,
                    interruption_checker,
                )

            bridge_factory = make_aggregate

        await self._raw.create_aggregate_function(
            name,
            cast("Any", bridge_factory),
            num_args,
            flags=flags,
        )
        self._record_aggregate_registration(
            name,
            num_args,
            registered=factory is not None,
        )

    async def register(
        self,
        definition: TableDefinition[Any],
        *,
        table_name: str | None = None,
        schema: str = "temp",
    ) -> Registration:
        """Register an immutable virtual-table definition asynchronously."""
        from .vtable import async_register_definition

        self._check()
        await self._install_registration_authorizer()
        connection_key = id(self)

        def await_in_callback_scope(awaitable: Awaitable[object]) -> Awaitable[object]:
            return _await_callback_result_in_scope(
                awaitable,
                connection_key,
                self._callback_interruption_checker,
            )

        registration = await async_register_definition(
            self._raw,
            definition,
            table_name=table_name,
            schema=schema,
            awaitable_scope=await_in_callback_scope,
            interrupt_checker=self._callback_interruption_checker,
        )
        self._registrations.append(registration)
        return registration

    async def run_script(
        self,
        script: str,
        options: ScriptOptions | None = None,
    ) -> ScriptResult:
        """Execute a multi-statement SQL script asynchronously."""
        from .script import run_database_script_async

        return await run_database_script_async(self, script, options)

    async def export_tables(
        self,
        tables: Iterable[str],
        output_path: str | PathLike[str],
        options: ExportOptions | None = None,
    ) -> Path:
        """Export tables without blocking the event loop on file I/O."""
        from .script import export_tables_async

        return await export_tables_async(self, tables, output_path, options)

    def interrupt(self) -> None:
        """Interrupt active SQLite work; safe from another thread."""
        if self._closed:
            raise ClosedError("connection is closed")
        self._interrupt_event.set()
        state = self._active_interruption_state
        if state is None or not state.defer_interrupt:
            self._raw.interrupt()

    async def aclose(self) -> None:
        """Close child resources and the owned APSW worker connection."""
        if self._closed:
            return
        self._check()
        for cursor in tuple(self._cursors):
            with contextlib.suppress(Exception):
                await cursor.aclose()
        self._cursors.clear()
        for registration in reversed(self._registrations):
            with contextlib.suppress(Exception):
                await registration.aclose()
        self._registrations.clear()
        await self._unregister_functions()
        await self._restore_registration_authorizer()
        self._closed = True
        if self._owns:
            await self._raw.aclose(force=True)

    def _track_cursor(self, cursor: AsyncCursor) -> None:
        self._cursors.add(cursor)

    def _discard_cursor(self, cursor: AsyncCursor) -> None:
        self._cursors.discard(cursor)

    async def _begin_query_savepoint(
        self,
        sql: str,
        cancellation: _CancellationState,
    ) -> str | None:
        interruption_enabled = (
            cancellation.deadline is not None or cancellation.should_cancel is not None
        )
        if not interruption_enabled or not _is_dml_returning(sql):
            return None
        name = f"libxsql_query_{next(_SAVEPOINT_IDS)}"
        await self._execute_control_sql(f'SAVEPOINT "{name}"')
        cancellation.defer_interrupt = True
        return name

    async def _finish_query_savepoint(self, name: str, *, commit: bool) -> None:
        rollback_error: BaseException | None = None
        if not commit:
            try:
                await self._execute_control_sql(f'ROLLBACK TO SAVEPOINT "{name}"')
            except BaseException as error:
                if _is_missing_savepoint(error):
                    return
                rollback_error = error
        try:
            await self._execute_control_sql(f'RELEASE SAVEPOINT "{name}"')
        except BaseException as error:
            if _is_missing_savepoint(error):
                return
            if rollback_error is None:
                raise
        if rollback_error is not None:
            raise rollback_error

    async def _execute_control_sql(self, sql: str) -> None:
        async with self._operation_lock:
            cursor = await self._raw.execute(sql)
            await cursor.aclose(force=True)

    async def _close_control_cursor(self, cursor: Any) -> None:
        async with self._operation_lock:
            await cursor.aclose(force=True)

    async def _refresh_registration_tables(self) -> None:
        async with self._operation_lock:
            for registration in self._registrations:
                if not registration._active:
                    continue
                schema = registration.schema.replace('"', '""')
                cursor = await self._raw.execute(
                    f'SELECT sql FROM "{schema}".sqlite_schema '  # noqa: S608
                    "WHERE type='table' AND name=?",
                    (registration.table_name,),
                )
                try:
                    row = await cursor.fetchone()
                finally:
                    await cursor.aclose(force=True)
                create_sql = None if row is None or row[0] is None else str(row[0])
                registration._refresh_table_state(create_sql)

    async def _run_async_operation(  # noqa: C901 - serialized cancellation state machine
        self,
        cancellation: _CancellationState,
        operation: Callable[[], Awaitable[Any]],
        *,
        check_before: bool = True,
        check_after: bool = True,
        clear_interrupt: bool = False,
        prepared_metadata: _PreparedStatementMetadata | None = None,
    ) -> Any:
        progress_id = object()
        async with self._operation_lock:
            with self._capture_async_statement_metadata(prepared_metadata, cancellation):
                if clear_interrupt:
                    self._interrupt_event.clear()
                self._active_interruption_state = cancellation
                set_progress_handler = getattr(self._raw, "set_progress_handler", None)
                try:
                    if callable(set_progress_handler):
                        await cast(
                            "Awaitable[object]",
                            set_progress_handler(
                                cancellation.progress,
                                1_000,
                                id=progress_id,
                            ),
                        )
                    if check_before:
                        _raise_for_cancellation(
                            cancellation,
                            result_columns=(
                                () if prepared_metadata is None else prepared_metadata.columns
                            ),
                            readonly=(
                                True if prepared_metadata is None else prepared_metadata.readonly
                            ),
                        )
                    try:
                        with _interruption_scope(cancellation.check):
                            result = await operation()
                    except BaseException as error:
                        if isinstance(error, StopAsyncIteration):
                            raise
                        interrupted = cancellation.check()
                        if cancellation.failure is not None:
                            raise cancellation.failure from error
                        if cancellation.cancelled and interrupted:
                            raise QueryCancelledError(
                                "query cancelled",
                                result_columns=(
                                    () if prepared_metadata is None else prepared_metadata.columns
                                ),
                                readonly=(
                                    True
                                    if prepared_metadata is None
                                    else prepared_metadata.readonly
                                ),
                            ) from error
                        if cancellation.timed_out and interrupted:
                            raise QueryTimeoutError(
                                result_columns=(
                                    () if prepared_metadata is None else prepared_metadata.columns
                                ),
                                readonly=(
                                    True
                                    if prepared_metadata is None
                                    else prepared_metadata.readonly
                                ),
                            ) from error
                        raise
                    if check_after:
                        _raise_for_cancellation(
                            cancellation,
                            result_columns=(
                                () if prepared_metadata is None else prepared_metadata.columns
                            ),
                            readonly=(
                                True if prepared_metadata is None else prepared_metadata.readonly
                            ),
                        )
                    return result
                finally:
                    if callable(set_progress_handler):
                        with contextlib.suppress(Exception):
                            await cast(
                                "Awaitable[object]",
                                set_progress_handler(None, id=progress_id),
                            )
                    if self._active_interruption_state is cancellation:
                        self._active_interruption_state = None

    @contextlib.contextmanager
    def _capture_async_statement_metadata(
        self,
        metadata: _PreparedStatementMetadata | None,
        cancellation: _CancellationState | None = None,
    ) -> Generator[None, None, None]:
        """Temporarily compose metadata capture with a connection execution tracer."""
        if metadata is None:
            yield
            return
        get_exec_trace = getattr(self._raw, "get_exec_trace", None)
        set_exec_trace = getattr(self._raw, "set_exec_trace", None)
        if not callable(get_exec_trace) or not callable(set_exec_trace):
            yield
            return
        previous = cast(
            "Callable[[apsw.Cursor, str, object], bool] | None",
            get_exec_trace(),
        )

        def capture(cursor: apsw.Cursor, statement: str, bindings: object) -> bool:
            metadata.capture(cursor)
            proceed = True if previous is None else previous(cursor, statement, bindings)
            if proceed and cancellation is not None:
                _raise_for_cancellation(
                    cancellation,
                    result_columns=metadata.columns,
                    readonly=metadata.readonly,
                )
            return proceed

        set_exec_trace(capture)
        try:
            yield
        finally:
            set_exec_trace(previous)

    def _record_scalar_registration(
        self,
        name: str,
        num_args: int,
        *,
        registered: bool,
    ) -> None:
        key = name.casefold(), num_args
        if registered:
            self._aggregate_functions.pop(key, None)
            self._scalar_functions[key] = name
        else:
            self._scalar_functions.pop(key, None)
            self._aggregate_functions.pop(key, None)

    def _record_aggregate_registration(
        self,
        name: str,
        num_args: int,
        *,
        registered: bool,
    ) -> None:
        key = name.casefold(), num_args
        if registered:
            self._scalar_functions.pop(key, None)
            self._aggregate_functions[key] = name
        else:
            self._aggregate_functions.pop(key, None)
            self._scalar_functions.pop(key, None)

    async def _unregister_functions(self) -> None:
        if self._owns:
            self._scalar_functions.clear()
            self._aggregate_functions.clear()
            return

        failures: list[tuple[str, str, int, Exception]] = []
        for key, name in tuple(self._scalar_functions.items()):
            num_args = key[1]
            try:
                await self._raw.create_scalar_function(name, None, num_args)
            except Exception as error:
                failures.append(("scalar", name, num_args, error))
            else:
                self._scalar_functions.pop(key, None)
        for key, name in tuple(self._aggregate_functions.items()):
            num_args = key[1]
            try:
                await self._raw.create_aggregate_function(name, None, num_args)
            except Exception as error:
                failures.append(("aggregate", name, num_args, error))
            else:
                self._aggregate_functions.pop(key, None)

        if failures:
            overloads = ", ".join(
                f"{kind} {name!r}/{num_args}" for kind, name, num_args, _ in failures
            )
            message = f"could not unregister wrapper-owned SQL function overloads: {overloads}"
            raise RegistrationError(message) from failures[0][3]

    async def _install_registration_authorizer(self) -> None:
        async with self._authorizer_lock:
            if self._registration_authorizer is not None:
                return
            previous_authorizer = cast("_AuthorizerCallback | None", self._raw.authorizer)
            authorizer = _RegistrationAuthorizer(self._registrations, previous_authorizer)
            await self._raw.set_authorizer(authorizer)
            self._registration_authorizer = authorizer

    async def _restore_registration_authorizer(self) -> None:
        async with self._authorizer_lock:
            authorizer = self._registration_authorizer
            if authorizer is not None and self._raw.authorizer is authorizer:
                await self._raw.set_authorizer(authorizer.previous)
            self._registration_authorizer = None

    async def __aenter__(self) -> Self:
        """Enter an async connection lifetime context."""
        self._check()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the connection."""
        del exc_type, exc_value, traceback
        await self.aclose()

    def _deadline(self, timeout: float | None) -> float | None:
        effective = self._default_query_timeout if timeout is None else timeout
        effective = _validate_timeout(effective)
        return None if effective is None else anyio.current_time() + effective

    def _callback_interruption_checker(self) -> bool:
        state = self._active_interruption_state
        return bool(state is not None and state.check())

    def _check(self) -> None:
        if self._closed:
            raise ClosedError("connection is closed")
        if self._current_event_loop_token() != self._event_loop_token:
            raise ThreadingError(
                "AsyncConnection objects may only be used from their creation event loop"
            )
        _check_callback_reentrancy(self)

    @staticmethod
    def _current_event_loop_token() -> object:
        try:
            return current_event_loop_token()
        except RuntimeError as error:
            raise ThreadingError(
                "AsyncConnection objects require their creation event loop"
            ) from error


class AsyncTransaction:
    """An asynchronous transaction that nests using savepoints."""

    def __init__(self, connection: AsyncConnection, mode: str) -> None:
        """Initialize an async transaction context without beginning it."""
        normalized = mode.strip().lower()
        if normalized not in Transaction._VALID_MODES:
            raise ConfigurationError("transaction mode must be deferred, immediate, or exclusive")
        self._connection = connection
        self._mode = normalized
        self._savepoint: str | None = None
        self._entered = False

    async def __aenter__(self) -> Self:
        """Begin the transaction or nested savepoint."""
        if self._entered:
            raise RuntimeError("transaction context cannot be entered twice")
        connection = self._connection
        connection._check()
        nested = connection._transaction_depth > 0 or await connection.in_transaction()
        if nested:
            self._savepoint = f"libxsql_{next(_SAVEPOINT_IDS)}"
            cursor = await connection.execute(f'SAVEPOINT "{self._savepoint}"')
        else:
            cursor = await connection.execute(f"BEGIN {self._mode.upper()}")
        await cursor.aclose()
        connection._transaction_depth += 1
        self._entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        """Commit on success, otherwise roll back."""
        del exc_value, traceback
        if not self._entered:
            return False
        connection = self._connection
        try:
            statements: tuple[str, ...]
            if self._savepoint is not None:
                statements = (
                    (f'RELEASE SAVEPOINT "{self._savepoint}"',)
                    if exc_type is None
                    else (
                        f'ROLLBACK TO SAVEPOINT "{self._savepoint}"',
                        f'RELEASE SAVEPOINT "{self._savepoint}"',
                    )
                )
            else:
                statements = ("COMMIT" if exc_type is None else "ROLLBACK",)
            for statement in statements:
                cursor = await connection.execute(statement)
                await cursor.aclose()
        finally:
            connection._transaction_depth = max(0, connection._transaction_depth - 1)
            self._entered = False
        return False


def connect(
    path: str = ":memory:",
    *,
    flags: int | None = None,
    vfs: str | None = None,
    statement_cache_size: int = 100,
    busy_timeout: float | None = None,
    default_query_timeout: float | None = None,
) -> Connection:
    """Open and return a synchronous :class:`Connection`."""
    return Connection(
        path,
        flags=flags,
        vfs=vfs,
        statement_cache_size=statement_cache_size,
        busy_timeout=busy_timeout,
        default_query_timeout=default_query_timeout,
    )


async def connect_async(
    path: str = ":memory:",
    *,
    flags: int | None = None,
    vfs: str | None = None,
    statement_cache_size: int = 100,
    busy_timeout: float | None = None,
    default_query_timeout: float | None = None,
) -> AsyncConnection:
    """Open an APSW async connection on a dedicated AnyIO-compatible worker."""
    _ensure_supported_runtime()
    if statement_cache_size < 0:
        raise ConfigurationError("statement_cache_size must be non-negative")
    if busy_timeout is not None and busy_timeout < 0:
        raise ConfigurationError("busy_timeout must be non-negative or None")
    open_flags = _DEFAULT_OPEN_FLAGS if flags is None else flags
    raw = await apsw.Connection.as_async(
        path,
        flags=open_flags,
        vfs=vfs,
        statementcachesize=statement_cache_size,
    )
    connection = AsyncConnection()
    connection._initialize(
        raw,
        owns=True,
        default_query_timeout=default_query_timeout,
    )
    try:
        await connection._install_registration_authorizer()
        if busy_timeout is not None:
            await raw.set_busy_timeout(round(busy_timeout * 1_000))
        from .functions import BlobConcat

        await raw.create_aggregate_function("blob_concat", cast("Any", BlobConcat), 1)
    except BaseException:
        await connection.aclose()
        raise
    return connection


__all__ = [
    "AsyncConnection",
    "AsyncCursor",
    "AsyncTransaction",
    "Connection",
    "Cursor",
    "Transaction",
    "callback_interrupted",
    "connect",
    "connect_async",
]
