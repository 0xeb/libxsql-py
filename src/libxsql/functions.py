# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Scalar and aggregate SQL function registration."""

# APSW's callback value aliases expose an untyped buffer arm; this module
# normalizes every callback input/output at the boundary.
# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false

from __future__ import annotations

import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Concatenate, Literal, Protocol, TypeVar, cast, overload

import apsw

from .errors import ClosedError, ConfigurationError
from .types import Bindings, QueryResult, SQLiteBindable, SQLiteRow, SQLiteValue

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

_MAX_FUNCTION_NAME_BYTES = 255
_MAX_FUNCTION_ARITY = 127
_MAX_BYTE = 255
_MIN_SQLITE_INTEGER = -(2**63)
_MAX_SQLITE_INTEGER = 2**63
_AGGREGATE_PROTOCOL_SIZE = 3

ScalarFunction = Callable[..., SQLiteBindable]
"""A Python callable exposed as a SQLite scalar function."""

AsyncScalarFunction = Callable[..., SQLiteBindable | Awaitable[SQLiteBindable]]
"""A synchronous or awaitable scalar callable for :class:`AsyncConnection`."""

ContextScalarFunction = Callable[Concatenate["FunctionContext", ...], SQLiteBindable]
"""A scalar callable whose first argument is a scoped :class:`FunctionContext`."""

AggregateFactory = Callable[[], Any] | type[Any]
"""A factory returning an object with ``step`` and ``final`` methods."""

_StateT = TypeVar("_StateT")


@dataclass(slots=True)
class _FunctionContextState:
    raw_connection: Any
    active: bool = True


@dataclass(frozen=True, slots=True)
class FunctionContext:
    """Scoped access to safe same-connection queries from a scalar callback.

    Contexts are valid only for the dynamic extent of their callback. They do
    not expose the underlying APSW connection and cannot be retained for later
    use.
    """

    _state: _FunctionContextState

    def query(
        self,
        sql: str,
        bindings: Bindings | None = None,
    ) -> QueryResult:
        """Materialize a nested query on the callback's SQLite connection."""
        started = time.perf_counter()
        columns, rows = self._query_rows(sql, bindings)
        return QueryResult(
            columns=columns,
            rows=rows,
            elapsed_ms=(time.perf_counter() - started) * 1_000,
        )

    def query_each(
        self,
        sql: str,
        callback: Callable[[SQLiteRow], None],
        bindings: Bindings | None = None,
    ) -> None:
        """Invoke a synchronous ``callback`` once per nested-query row."""
        cursor, _ = self._execute(sql, bindings)
        try:
            from .connection import _normalize_row  # noqa: PLC0415

            for row in cursor:
                outcome = cast(
                    "Callable[[SQLiteRow], object]",
                    callback,
                )(_normalize_row(row))
                if inspect.isawaitable(outcome):
                    close = getattr(outcome, "close", None)
                    if callable(close):
                        close()
                    message = "FunctionContext.query_each callbacks must be synchronous"
                    raise ConfigurationError(message)
        finally:
            cursor.close(force=True)

    def scalar(
        self,
        sql: str,
        bindings: Bindings | None = None,
        *,
        default: SQLiteValue = None,
    ) -> SQLiteValue:
        """Return the first nested-query cell, or ``default``."""
        return self.query(sql, bindings).scalar(default)

    def _query_rows(
        self,
        sql: str,
        bindings: Bindings | None,
    ) -> tuple[tuple[str, ...], tuple[SQLiteRow, ...]]:
        cursor, columns = self._execute(sql, bindings)
        try:
            from .connection import _normalize_row  # noqa: PLC0415

            rows = tuple(_normalize_row(row) for row in cursor)
            return columns, rows
        finally:
            cursor.close(force=True)

    def _execute(
        self,
        sql: str,
        bindings: Bindings | None,
    ) -> tuple[Any, tuple[str, ...]]:
        self._ensure_active()
        from .connection import _normalize_bindings  # noqa: PLC0415

        cursor = apsw.Connection.cursor(self._state.raw_connection)
        normalized = _normalize_bindings(bindings)
        prepared_columns: tuple[str, ...] = ()
        previous = cast(
            "Callable[[apsw.Cursor, str, object], bool] | None",
            cursor.get_exec_trace(),
        )
        inherited = cast(
            "Callable[[apsw.Cursor, str, object], bool] | None",
            self._state.raw_connection.get_exec_trace(),
        )
        chained = previous if previous is not None else inherited

        def capture(prepared: apsw.Cursor, statement: str, values: object) -> bool:
            nonlocal prepared_columns
            try:
                description = prepared.get_description()
            except apsw.ExecutionCompleteError:
                description = ()
            prepared_columns = tuple(str(item[0]) for item in description)
            return True if chained is None else chained(prepared, statement, values)

        cursor.set_exec_trace(capture)
        try:
            if normalized is None:
                apsw.Cursor.execute(cursor, sql)
            else:
                apsw.Cursor.execute(cursor, sql, normalized)
        except BaseException:
            cursor.set_exec_trace(previous)
            cursor.close(force=True)
            raise
        else:
            cursor.set_exec_trace(previous)
        try:
            description = cursor.get_description()
        except apsw.ExecutionCompleteError:
            columns = prepared_columns
        else:
            columns = tuple(str(item[0]) for item in description)
        return cursor, columns

    def _ensure_active(self) -> None:
        if not self._state.active:
            message = "function context is no longer active"
            raise ClosedError(message)

    def _expire(self) -> None:
        self._state.active = False


class _SyncConnection(Protocol):
    @property
    def raw_connection(self) -> apsw.Connection:
        """Return the underlying APSW connection."""
        ...

    def _record_scalar_registration(
        self,
        name: str,
        num_args: int,
        *,
        registered: bool,
    ) -> None:
        """Record one wrapper-owned scalar overload."""
        ...

    def _record_aggregate_registration(
        self,
        name: str,
        num_args: int,
        *,
        registered: bool,
    ) -> None:
        """Record one wrapper-owned aggregate overload."""
        ...

    def _callback_interruption_checker(self) -> bool:
        """Report whether the active callback should stop."""
        ...


class _AggregateObject(Protocol):
    def step(self, *values: object) -> object:
        """Accumulate one aggregate row."""
        ...

    def final(self) -> object:
        """Produce the final aggregate result."""
        ...


def _callback_context(
    connection_key: int,
    interruption_checker: Callable[[], bool],
) -> AbstractContextManager[None]:
    from .connection import _callback_scope  # noqa: PLC0415  # pyright: ignore[reportPrivateUsage]

    return _callback_scope(connection_key, interruption_checker)


class _SyncAggregateBridge:
    def __init__(
        self,
        target: object,
        connection_key: int,
        interruption_checker: Callable[[], bool],
    ) -> None:
        """Adapt APSW's object and tuple aggregate protocols."""
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
        """Run one step inside the owning connection's callback scope."""
        with _callback_context(self._connection_key, self._interruption_checker):
            if self._tuple_protocol:
                self._step(self._state, *values)
            else:
                self._step(*values)

    def final(self) -> SQLiteValue:
        """Run and normalize the final callback."""
        with _callback_context(self._connection_key, self._interruption_checker):
            result = self._final(self._state) if self._tuple_protocol else self._final()
        return _normalize_result(result)


def _validate_registration(name: str, num_args: int) -> None:
    if not name or "\x00" in name:
        message = "SQL function name must be non-empty and contain no NUL"
        raise ConfigurationError(message)
    if len(name.encode("utf-8")) > _MAX_FUNCTION_NAME_BYTES:
        message = "SQL function name must be at most 255 UTF-8 bytes"
        raise ConfigurationError(message)
    if num_args < -1 or num_args > _MAX_FUNCTION_ARITY:
        message = "num_args must be -1 (variadic) or between 0 and 127"
        raise ConfigurationError(message)


def _normalize_result(value: object) -> SQLiteValue:
    if value is None or isinstance(value, (float, str, bytes)):
        return value
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        if not _MIN_SQLITE_INTEGER <= value < _MAX_SQLITE_INTEGER:
            message = "SQLite INTEGER results must fit in a signed 64-bit integer"
            raise OverflowError(message)
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    message = (
        "SQL function results must be None, int, float, str, bytes, "
        f"bytearray, or memoryview; got {type(value).__name__}"
    )
    raise TypeError(message)


@overload
def register_function(
    connection: _SyncConnection,
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
    connection: _SyncConnection,
    name: str,
    function: ContextScalarFunction | None,
    num_args: int = -1,
    *,
    deterministic: bool = False,
    flags: int = 0,
    with_context: Literal[True],
) -> None: ...


def register_function(  # noqa: PLR0913 - mirrors SQLite registration arguments
    connection: _SyncConnection,
    name: str,
    function: ScalarFunction | None,
    num_args: int = -1,
    *,
    deterministic: bool = False,
    flags: int = 0,
    with_context: bool = False,
) -> None:
    """Register or unregister a scalar SQL function.

    Callback exceptions intentionally cross the APSW boundary unchanged.

    Args:
        connection: Open synchronous libxsql connection.
        name: SQL function name.
        function: Callable, or ``None`` to unregister.
        num_args: Arity; ``-1`` means variadic.
        deterministic: Whether equal inputs always produce equal output.
        flags: Additional SQLite function flags.
        with_context: Pass a scoped :class:`FunctionContext` first.
    """
    _validate_registration(name, num_args)
    if function is None:
        connection.raw_connection.create_scalar_function(
            name,
            None,
            num_args,
            deterministic=deterministic,
            flags=flags,
        )
        connection._record_scalar_registration(name, num_args, registered=False)  # noqa: SLF001
        return

    if with_context and inspect.iscoroutinefunction(function):
        message = "context-aware SQL functions must be synchronous"
        raise ConfigurationError(message)

    connection_key = id(connection)
    raw_connection = connection.raw_connection
    interruption_checker = connection._callback_interruption_checker  # noqa: SLF001

    def invoke(*arguments: apsw.SQLiteValue) -> apsw.SQLiteValue:
        with _callback_context(connection_key, interruption_checker):
            if with_context:
                context = FunctionContext(_FunctionContextState(raw_connection))
                try:
                    result: Any = function(context, *arguments)
                finally:
                    context._expire()  # noqa: SLF001
            else:
                result = function(*arguments)
        if inspect.isawaitable(result):
            close = getattr(result, "close", None)
            if callable(close):
                close()
            message = "context-aware SQL functions must be synchronous"
            raise ConfigurationError(message)
        return _normalize_result(result)

    connection.raw_connection.create_scalar_function(
        name,
        invoke,
        num_args,
        deterministic=deterministic,
        flags=flags,
    )
    connection._record_scalar_registration(name, num_args, registered=True)  # noqa: SLF001


def register_aggregate(
    connection: _SyncConnection,
    name: str,
    factory: AggregateFactory | None,
    num_args: int = -1,
    *,
    flags: int = 0,
) -> None:
    """Register or unregister an aggregate SQL function.

    The factory follows APSW's native aggregate protocol: it returns an object
    with ``step(*values)`` and ``final()`` methods, or APSW's context tuple.
    """
    _validate_registration(name, num_args)
    if factory is None:
        connection.raw_connection.create_aggregate_function(
            name,
            None,
            num_args,
            flags=flags,
        )
        connection._record_aggregate_registration(name, num_args, registered=False)  # noqa: SLF001
        return
    connection_key = id(connection)
    interruption_checker = connection._callback_interruption_checker  # noqa: SLF001

    def bridge_factory() -> _SyncAggregateBridge:
        with _callback_context(connection_key, interruption_checker):
            target = factory()
        return _SyncAggregateBridge(target, connection_key, interruption_checker)

    connection.raw_connection.create_aggregate_function(
        name,
        bridge_factory,
        num_args,
        flags=flags,
    )
    connection._record_aggregate_registration(name, num_args, registered=True)  # noqa: SLF001


def aggregate(
    *,
    initial: Callable[[], _StateT],
    step: Callable[[_StateT, SQLiteValue], None],
    final: Callable[[_StateT], SQLiteValue],
) -> type[Any]:
    """Create an APSW aggregate class from Pythonic state callbacks.

    Args:
        initial: Creates state for one aggregate invocation.
        step: Accumulates one SQLite value into state.
        final: Converts final state to a SQLite value.
    """

    class CallbackAggregate:
        def __init__(self) -> None:
            """Create isolated state for one aggregate invocation."""
            self._state = initial()

        def step(self, value: SQLiteValue) -> None:
            step(self._state, value)

        def final(self) -> SQLiteValue:
            return _normalize_result(final(self._state))

    return CallbackAggregate


class BlobConcat:
    """Implementation of the built-in ``blob_concat(value)`` aggregate."""

    __slots__ = ("_buffer", "_has_input")

    def __init__(self) -> None:
        """Create an empty aggregate state."""
        self._buffer = bytearray()
        self._has_input = False

    def step(self, value: SQLiteValue) -> None:
        """Append one BLOB or byte-valued INTEGER; ignore SQL NULL."""
        if value is None:
            return
        if isinstance(value, bytes):
            self._has_input = True
            self._buffer.extend(value)
            return
        if isinstance(value, int) and not isinstance(value, bool):
            if not 0 <= value <= _MAX_BYTE:
                message = "blob_concat: INTEGER input out of byte range [0, 255]"
                raise ValueError(message)
            self._has_input = True
            self._buffer.append(value)
            return
        message = "blob_concat: input must be BLOB, INTEGER 0-255, or NULL"
        raise TypeError(message)

    def final(self) -> bytes | None:
        """Return concatenated bytes, or SQL NULL when no non-NULL input exists."""
        return bytes(self._buffer) if self._has_input else None


def register_blob_concat(connection: _SyncConnection) -> None:
    """Register libxsql's built-in ``blob_concat`` aggregate."""
    register_aggregate(connection, "blob_concat", BlobConcat, 1)


__all__ = [
    "AggregateFactory",
    "AsyncScalarFunction",
    "BlobConcat",
    "ContextScalarFunction",
    "FunctionContext",
    "ScalarFunction",
    "aggregate",
    "register_aggregate",
    "register_blob_concat",
    "register_function",
]
