# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Typed, Pythonic SQLite virtual-table definitions backed by APSW.

The public layer in this module is deliberately declarative.  Builders collect
callbacks and validate their relationships; :class:`TableDefinition` values
are immutable and reusable across connections.  Registration-specific state,
including shared caches, is never stored on a definition.
"""

# APSW intentionally types SQLite's native buffer value as unknown, and these
# package-private adapter objects collaborate as one virtual-table implementation.
# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false
# pyright: reportUnknownParameterType=false, reportUnknownVariableType=false
# pyright: reportPrivateUsage=false

from __future__ import annotations

import contextlib
import inspect
import json
import math
import operator
import threading
import uuid
import weakref
from bisect import bisect_left, bisect_right
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from contextlib import suppress
from dataclasses import dataclass, field, replace
from enum import Enum, IntEnum
from types import MappingProxyType, TracebackType
from typing import (
    Any,
    Final,
    Generic,
    ParamSpec,
    Protocol,
    Self,
    TypeAlias,
    TypeVar,
    cast,
    overload,
)

import apsw

from .errors import (
    ClosedError,
    ConfigurationError,
    FullScanError,
    PartialUpdateError,
    ProtocolError,
    ReadOnlyError,
    RegistrationError,
)
from .types import SQLiteValue

RowT = TypeVar("RowT")
ResultT = TypeVar("ResultT")
SourceRowT = TypeVar("SourceRowT")
CallbackResultT = TypeVar("CallbackResultT")
CallbackParams = ParamSpec("CallbackParams")

SourceResult: TypeAlias = (
    Iterable[RowT] | AsyncIterable[RowT] | Awaitable[Iterable[RowT] | AsyncIterable[RowT]]
)
SourceFactory: TypeAlias = Callable[[], SourceResult[RowT]]
StatefulSourceFactory: TypeAlias = Callable[[object], SourceResult[RowT]]
Source: TypeAlias = Iterable[RowT] | SourceFactory[RowT]
ProjectionSource: TypeAlias = Callable[["Projection"], SourceResult[RowT]]
ContextSource: TypeAlias = Callable[["GeneratorContext"], SourceResult[RowT]]
GeneratorSource: TypeAlias = Source[RowT] | ContextSource[RowT]
ColumnGetter: TypeAlias = Callable[[RowT], object]
ColumnSetter: TypeAlias = Callable[[RowT, SQLiteValue], bool | None]
RowidGetter: TypeAlias = Callable[[RowT], int]
RowLookup: TypeAlias = Callable[[int], RowT | None]
StatefulRowLookup: TypeAlias = Callable[[object, int], RowT | None]
FilterFactory: TypeAlias = Callable[[SQLiteValue], SourceResult[RowT]]
ConstraintFactory: TypeAlias = Callable[[tuple["Constraint", ...]], SourceResult[RowT]]
CountCallback: TypeAlias = Callable[[], int]
EstimateCallback: TypeAlias = Callable[[], int]
ModifyHook: TypeAlias = Callable[[str], None]
CommitHook: TypeAlias = Callable[[], None]
TransactionStateFactory: TypeAlias = Callable[[], object]
TransactionPrepareHook: TypeAlias = Callable[[object], None]
TransactionFinalHook: TypeAlias = Callable[[object], None]
TransactionSavepointHook: TypeAlias = Callable[[object, int], None]
IndexDeleteCallback: TypeAlias = Callable[[int], bool | None]
RowDeleteCallback: TypeAlias = Callable[[RowT], bool | None]
InsertCallback: TypeAlias = Callable[[Mapping[str, SQLiteValue]], int | bool | None]
UpdateCallback: TypeAlias = Callable[[int, Mapping[str, SQLiteValue]], bool | None]
AsyncRunner: TypeAlias = Callable[[Any], Any]
AwaitableScope: TypeAlias = Callable[[Awaitable[object]], Awaitable[object]]
InterruptChecker: TypeAlias = Callable[[], bool]

_MIN_ROWID: Final = -(1 << 63)
_MAX_ROWID: Final = (1 << 63) - 1
_DEFAULT_ESTIMATED_ROWS: Final = 1_000_000
_DEFAULT_ESTIMATED_COST: Final = 1_000_000.0


class _SupportsGetItem(Protocol):
    def __getitem__(self, key: object, /) -> object: ...


class TableKind(Enum):
    """The source and lifetime strategy used by a virtual table."""

    INDEX = "index"
    CACHED = "cached"
    GENERATOR = "generator"


class ColumnType(Enum):
    """SQLite declared types supported by libxsql."""

    INTEGER = "INTEGER"
    TEXT = "TEXT"
    REAL = "REAL"
    BLOB = "BLOB"
    ANY = ""


class ConstraintOp(IntEnum):
    """SQLite virtual-table constraint operators."""

    EQ = apsw.SQLITE_INDEX_CONSTRAINT_EQ
    GT = apsw.SQLITE_INDEX_CONSTRAINT_GT
    LE = apsw.SQLITE_INDEX_CONSTRAINT_LE
    LT = apsw.SQLITE_INDEX_CONSTRAINT_LT
    GE = apsw.SQLITE_INDEX_CONSTRAINT_GE
    MATCH = apsw.SQLITE_INDEX_CONSTRAINT_MATCH
    LIKE = apsw.SQLITE_INDEX_CONSTRAINT_LIKE
    GLOB = apsw.SQLITE_INDEX_CONSTRAINT_GLOB
    REGEXP = apsw.SQLITE_INDEX_CONSTRAINT_REGEXP
    NE = apsw.SQLITE_INDEX_CONSTRAINT_NE
    IS_NOT = apsw.SQLITE_INDEX_CONSTRAINT_ISNOT
    IS_NOT_NULL = apsw.SQLITE_INDEX_CONSTRAINT_ISNOTNULL
    IS_NULL = apsw.SQLITE_INDEX_CONSTRAINT_ISNULL
    IS = apsw.SQLITE_INDEX_CONSTRAINT_IS
    LIMIT = apsw.SQLITE_INDEX_CONSTRAINT_LIMIT
    OFFSET = apsw.SQLITE_INDEX_CONSTRAINT_OFFSET
    FUNCTION = apsw.SQLITE_INDEX_CONSTRAINT_FUNCTION


@dataclass(frozen=True, slots=True)
class TransactionHooks:
    """Complete connection-local SQLite virtual-table transaction lifecycle.

    ``prepare_commit`` and the savepoint callbacks are fallible: raising from
    them rejects the SQLite operation. ``commit`` and ``rollback`` are final
    notifications with no usable SQLite failure channel, so accidental
    exceptions are contained by the adapter.
    """

    state_factory: TransactionStateFactory | None = None
    prepare_commit: TransactionPrepareHook | None = None
    commit: TransactionFinalHook | None = None
    rollback: TransactionFinalHook | None = None
    savepoint: TransactionSavepointHook | None = None
    release: TransactionSavepointHook | None = None
    rollback_to: TransactionSavepointHook | None = None


@dataclass(frozen=True, slots=True)
class Column(Generic[RowT]):
    """One immutable virtual-table column definition."""

    name: str
    type: ColumnType
    get: ColumnGetter[RowT] | None
    set: ColumnSetter[RowT] | None = None
    nullable: bool = False
    hidden: bool = False

    @property
    def writable(self) -> bool:
        """Whether the column has a setter."""
        return self.set is not None


@dataclass(frozen=True, slots=True)
class ConstraintSpec:
    """A constraint requested by a generator constraint plan."""

    column: str
    op: ConstraintOp = ConstraintOp.EQ
    required: bool = True
    exact: bool = True


@dataclass(frozen=True, slots=True)
class Constraint:
    """A concrete constraint delivered to a source factory."""

    column: str | None
    column_index: int
    op: ConstraintOp
    value: SQLiteValue


@dataclass(frozen=True, slots=True)
class OrderTerm:
    """One requested or guaranteed ordering term."""

    column: str
    column_index: int
    descending: bool = False


@dataclass(frozen=True, slots=True)
class Projection:
    """Columns used by a scan, including predicates and result expressions."""

    columns: tuple[str, ...]
    column_indices: tuple[int, ...]
    includes_rowid: bool = False

    def uses(self, column: str | int) -> bool:
        """Return whether a column participates in the scan."""
        if isinstance(column, int):
            return column in self.column_indices
        return column in self.columns


@dataclass(frozen=True, slots=True)
class FilterContext:
    """Planner context common to filtered and generated scans."""

    projection: Projection
    constraints: tuple[Constraint, ...] = ()
    order_by: tuple[OrderTerm, ...] = ()


@dataclass(frozen=True, slots=True)
class GeneratorContext(FilterContext):
    """Context supplied to a context-aware generator."""

    limit: int | None = None
    offset: int = 0


@dataclass(frozen=True, slots=True)
class Index(Generic[RowT]):
    """A materialized equality and range index."""

    column: str
    key: ColumnGetter[RowT]


@dataclass(frozen=True, slots=True)
class Filter(Generic[RowT]):
    """A single-constraint source factory."""

    column: str
    op: ConstraintOp
    factory: FilterFactory[RowT]
    estimated_cost: float
    estimated_rows: int
    exact: bool


@dataclass(frozen=True, slots=True)
class ConstraintFilter(Generic[RowT]):
    """A multi-constraint generator source factory."""

    specs: tuple[ConstraintSpec, ...]
    factory: ConstraintFactory[RowT]
    estimated_cost: float
    estimated_rows: int
    order_by: tuple[OrderTerm, ...] = ()


@dataclass(frozen=True, slots=True)
class TableDefinition(Generic[RowT]):
    """An immutable, connection-independent virtual-table definition."""

    name: str
    kind: TableKind
    columns: tuple[Column[RowT], ...]
    source: GeneratorSource[RowT] | None = None
    stateful_source: StatefulSourceFactory[RowT] | None = None
    projection_source: ProjectionSource[RowT] | None = None
    context_source: ContextSource[RowT] | None = None
    count_rows: CountCallback | None = None
    estimate_rows: EstimateCallback | None = None
    row_at: Callable[[int], RowT] | None = None
    rowid: RowidGetter[RowT] | None = None
    row_lookup: RowLookup[RowT] | None = None
    stateful_row_lookup: StatefulRowLookup[RowT] | None = None
    filters: tuple[Filter[RowT], ...] = ()
    indexes: tuple[Index[RowT], ...] = ()
    constraint_filters: tuple[ConstraintFilter[RowT], ...] = ()
    before_modify: ModifyHook | None = None
    after_modify: ModifyHook | None = None
    transaction_hooks: TransactionHooks = TransactionHooks()
    delete_row: IndexDeleteCallback | RowDeleteCallback[RowT] | None = None
    insert_row: InsertCallback | None = None
    update_row: UpdateCallback | None = None
    full_scan_message: str | None = None
    shared: bool = False

    @property
    def schema(self) -> str:
        """Return the SQLite declaration passed to ``sqlite3_declare_vtab``."""
        parts: list[str] = []
        for column in self.columns:
            declaration = _quote_identifier(column.name)
            if column.type is not ColumnType.ANY:
                declaration += f" {column.type.value}"
            if column.hidden:
                declaration += " HIDDEN"
            parts.append(declaration)
        return f"CREATE TABLE {_quote_identifier(self.name)}({', '.join(parts)})"

    @property
    def writable(self) -> bool:
        """Whether any write operation is supported."""
        return (
            self.delete_row is not None
            or self.insert_row is not None
            or self.update_row is not None
            or any(column.writable for column in self.columns)
        )

    def column_index(self, name: str) -> int:
        """Return a column index, raising for an unknown name."""
        for index, column in enumerate(self.columns):
            if column.name == name:
                return index
        msg = f"unknown column {name!r} in table {self.name!r}"
        raise ConfigurationError(msg)


def required(
    column: str,
    op: ConstraintOp = ConstraintOp.EQ,
    *,
    exact: bool = True,
) -> ConstraintSpec:
    """Create a required generator constraint."""
    return ConstraintSpec(column, op, required=True, exact=exact)


def optional(
    column: str,
    op: ConstraintOp = ConstraintOp.EQ,
    *,
    exact: bool = True,
) -> ConstraintSpec:
    """Create an optional generator constraint."""
    return ConstraintSpec(column, op, required=False, exact=exact)


def required_eq(column: str) -> ConstraintSpec:
    """Create a required equality constraint."""
    return required(column)


def optional_eq(column: str) -> ConstraintSpec:
    """Create an optional equality constraint."""
    return optional(column)


def required_gt(column: str) -> ConstraintSpec:
    """Create a required greater-than constraint."""
    return required(column, ConstraintOp.GT)


def optional_gt(column: str) -> ConstraintSpec:
    """Create an optional greater-than constraint."""
    return optional(column, ConstraintOp.GT)


def required_ge(column: str) -> ConstraintSpec:
    """Create a required greater-than-or-equal constraint."""
    return required(column, ConstraintOp.GE)


def optional_ge(column: str) -> ConstraintSpec:
    """Create an optional greater-than-or-equal constraint."""
    return optional(column, ConstraintOp.GE)


def required_lt(column: str) -> ConstraintSpec:
    """Create a required less-than constraint."""
    return required(column, ConstraintOp.LT)


def optional_lt(column: str) -> ConstraintSpec:
    """Create an optional less-than constraint."""
    return optional(column, ConstraintOp.LT)


def required_le(column: str) -> ConstraintSpec:
    """Create a required less-than-or-equal constraint."""
    return required(column, ConstraintOp.LE)


def optional_le(column: str) -> ConstraintSpec:
    """Create an optional less-than-or-equal constraint."""
    return optional(column, ConstraintOp.LE)


def required_like(column: str) -> ConstraintSpec:
    """Create a required LIKE superset constraint."""
    return required(column, ConstraintOp.LIKE, exact=False)


def optional_like(column: str) -> ConstraintSpec:
    """Create an optional LIKE superset constraint."""
    return optional(column, ConstraintOp.LIKE, exact=False)


class _BaseTableBuilder(Generic[RowT]):
    """Shared fluent implementation for all table families."""

    def __init__(
        self,
        name: str,
        kind: TableKind,
        source: GeneratorSource[RowT] | None,
    ) -> None:
        self._name = _validate_identifier(name, "table")
        self._kind = kind
        self._source = source
        self._stateful_source: StatefulSourceFactory[RowT] | None = None
        self._projection_source: ProjectionSource[RowT] | None = None
        self._context_source: ContextSource[RowT] | None = None
        self._columns: list[Column[RowT]] = []
        self._count_rows: CountCallback | None = None
        self._estimate_rows: EstimateCallback | None = None
        self._row_at: Callable[[int], RowT] | None = None
        self._rowid: RowidGetter[RowT] | None = None
        self._row_lookup: RowLookup[RowT] | None = None
        self._stateful_row_lookup: StatefulRowLookup[RowT] | None = None
        self._filters: list[Filter[RowT]] = []
        self._indexes: list[Index[RowT]] = []
        self._constraint_filters: list[ConstraintFilter[RowT]] = []
        self._before_modify: ModifyHook | None = None
        self._after_modify: ModifyHook | None = None
        self._transaction_hooks = TransactionHooks()
        self._delete_row: IndexDeleteCallback | RowDeleteCallback[RowT] | None = None
        self._insert_row: InsertCallback | None = None
        self._update_row: UpdateCallback | None = None
        self._full_scan_message: str | None = None
        self._shared = False

    def _copy_state_to(self, target: _BaseTableBuilder[SourceRowT]) -> None:
        """Copy fluent state into a freshly row-typed builder."""
        target.__dict__ = self.__dict__.copy()
        target._columns = cast("list[Column[SourceRowT]]", list(self._columns))
        target._filters = cast("list[Filter[SourceRowT]]", list(self._filters))
        target._indexes = cast("list[Index[SourceRowT]]", list(self._indexes))
        target._constraint_filters = cast(
            "list[ConstraintFilter[SourceRowT]]",
            list(self._constraint_filters),
        )

    def count(self, callback: CountCallback) -> Self:
        """Set the exact, inexpensive row-count callback."""
        if not callable(cast("object", callback)):
            msg = "count callback must be callable"
            raise ConfigurationError(msg)
        self._count_rows = callback
        return self

    def estimate_rows(self, estimate: int | EstimateCallback) -> Self:
        """Set the inexpensive row estimate used by SQLite's planner."""
        raw_estimate = cast("object", estimate)
        if isinstance(raw_estimate, bool):
            msg = "estimated row count must be a non-negative integer or callback"
            raise ConfigurationError(msg)
        if isinstance(raw_estimate, int):
            _validate_count(raw_estimate, "estimated row count")
            self._estimate_rows = lambda value=raw_estimate: value
        elif callable(raw_estimate):
            self._estimate_rows = cast("EstimateCallback", raw_estimate)
        else:
            msg = "estimated row count must be a non-negative integer or callback"
            raise ConfigurationError(msg)
        return self

    def row_at(self, callback: Callable[[int], RowT]) -> Self:
        """Set advanced positional row access for an index/live table."""
        self._row_at = callback
        return self

    def rowid(self, callback: RowidGetter[RowT]) -> Self:
        """Set a stable rowid getter."""
        self._rowid = callback
        return self

    def row_lookup(self, callback: RowLookup[RowT]) -> Self:
        """Set stable row reconstruction by rowid for mutation."""
        self._row_lookup = callback
        return self

    def stateful_row_lookup(self, callback: StatefulRowLookup[RowT]) -> Self:
        """Set row reconstruction receiving registration transaction state."""
        self._stateful_row_lookup = callback
        return self

    def before_modify(self, callback: ModifyHook) -> Self:
        """Run a hook before each successful write attempt."""
        self._before_modify = callback
        return self

    def on_modify(self, callback: ModifyHook) -> Self:
        """Alias for :meth:`before_modify`."""
        return self.before_modify(callback)

    def after_modify(self, callback: ModifyHook) -> Self:
        """Run a hook after each completed write and cache invalidation."""
        self._after_modify = callback
        return self

    def on_commit(self, callback: CommitHook) -> Self:
        """Run a fallible compatibility hook before a write transaction commits.

        New code should use :meth:`transaction_hooks`. Mapping this legacy
        spelling to ``prepare_commit`` preserves its historical exception
        behavior while using SQLite's only reliable commit-failure phase.
        """
        if not callable(cast("object", callback)):
            msg = "commit callback must be callable"
            raise ConfigurationError(msg)

        def prepare_commit(_state: object) -> None:
            callback()

        self._transaction_hooks = replace(
            self._transaction_hooks,
            prepare_commit=prepare_commit,
        )
        return self

    def transaction_hooks(self, hooks: TransactionHooks) -> Self:
        """Install a complete registration-local transaction lifecycle."""
        self._transaction_hooks = hooks
        return self

    def insertable(self, callback: InsertCallback) -> Self:
        """Enable insertion from a name-to-value mapping."""
        self._insert_row = callback
        return self

    def updatable(self, callback: UpdateCallback) -> Self:
        """Enable preferred atomic row updates."""
        self._update_row = callback
        return self

    def column(  # noqa: PLR0913 - selectors are the deliberate fluent contract.
        self,
        name: str,
        python_type: type[object] | ColumnType,
        *,
        get: ColumnGetter[RowT] | None = None,
        attr: str | None = None,
        key: object | None = None,
        set: ColumnSetter[RowT] | None = None,  # noqa: A002 - deliberate public keyword.
        nullable: bool = False,
        hidden: bool = False,
    ) -> Self:
        """Add a column using a getter, attribute, mapping key, or its name."""
        column_name = _validate_identifier(name, "column")
        if any(column.name == column_name for column in self._columns):
            msg = f"duplicate column {column_name!r}"
            raise ConfigurationError(msg)
        selectors = sum(value is not None for value in (get, attr, key))
        if selectors > 1:
            msg = "column accepts only one of get, attr, or key"
            raise ConfigurationError(msg)
        getter: ColumnGetter[RowT] | None
        if get is not None:
            getter = get
        elif attr is not None:
            getter = cast("ColumnGetter[RowT]", operator.attrgetter(attr))
        elif key is not None:

            def get_item(row: RowT, item: object = key) -> object:
                return operator.getitem(cast("_SupportsGetItem", row), item)

            getter = get_item
        elif hidden:
            getter = None
        else:
            getter = _default_getter(column_name)
        self._columns.append(
            Column(
                name=column_name,
                type=_column_type(python_type),
                get=getter,
                set=set,
                nullable=nullable,
                hidden=hidden,
            ),
        )
        return self

    def column_int(
        self,
        name: str,
        get: ColumnGetter[RowT] | None = None,
        *,
        attr: str | None = None,
        key: object | None = None,
    ) -> Self:
        """Add an integer column."""
        return self.column(name, int, get=get, attr=attr, key=key)

    def column_i64(
        self,
        name: str,
        get: ColumnGetter[RowT] | None = None,
        *,
        attr: str | None = None,
        key: object | None = None,
    ) -> Self:
        """Alias for :meth:`column_int`."""
        return self.column_int(name, get, attr=attr, key=key)

    def column_int64(
        self,
        name: str,
        get: ColumnGetter[RowT] | None = None,
        *,
        attr: str | None = None,
        key: object | None = None,
    ) -> Self:
        """Alias for :meth:`column_int`."""
        return self.column_int(name, get, attr=attr, key=key)

    def column_text(
        self,
        name: str,
        get: ColumnGetter[RowT] | None = None,
        *,
        attr: str | None = None,
        key: object | None = None,
        nullable: bool = False,
    ) -> Self:
        """Add a text column."""
        return self.column(name, str, get=get, attr=attr, key=key, nullable=nullable)

    def column_text_nullable(
        self,
        name: str,
        get: ColumnGetter[RowT] | None = None,
        *,
        attr: str | None = None,
        key: object | None = None,
    ) -> Self:
        """Add nullable text."""
        return self.column_text(name, get, attr=attr, key=key, nullable=True)

    def column_double(
        self,
        name: str,
        get: ColumnGetter[RowT] | None = None,
        *,
        attr: str | None = None,
        key: object | None = None,
    ) -> Self:
        """Add a floating-point column."""
        return self.column(name, float, get=get, attr=attr, key=key)

    def column_blob(
        self,
        name: str,
        get: ColumnGetter[RowT] | None = None,
        *,
        attr: str | None = None,
        key: object | None = None,
        nullable: bool = False,
    ) -> Self:
        """Add a bytes column."""
        return self.column(name, bytes, get=get, attr=attr, key=key, nullable=nullable)

    def column_int_rw(
        self,
        name: str,
        get: ColumnGetter[RowT],
        setter: ColumnSetter[RowT],
        *,
        nullable: bool = False,
    ) -> Self:
        """Add a writable integer column."""
        return self.column(name, int, get=get, set=setter, nullable=nullable)

    def column_i64_rw(
        self,
        name: str,
        get: ColumnGetter[RowT],
        setter: ColumnSetter[RowT],
        *,
        nullable: bool = False,
    ) -> Self:
        """Alias for :meth:`column_int_rw`."""
        return self.column_int_rw(name, get, setter, nullable=nullable)

    def column_int64_rw(
        self,
        name: str,
        get: ColumnGetter[RowT],
        setter: ColumnSetter[RowT],
        *,
        nullable: bool = False,
    ) -> Self:
        """Alias for :meth:`column_int_rw`."""
        return self.column_int_rw(name, get, setter, nullable=nullable)

    def column_text_rw(
        self,
        name: str,
        get: ColumnGetter[RowT],
        setter: ColumnSetter[RowT],
        *,
        nullable: bool = False,
    ) -> Self:
        """Add a writable text column."""
        return self.column(name, str, get=get, set=setter, nullable=nullable)

    def column_text_nullable_rw(
        self,
        name: str,
        get: ColumnGetter[RowT],
        setter: ColumnSetter[RowT],
    ) -> Self:
        """Add nullable writable text."""
        return self.column_text_rw(name, get, setter, nullable=True)

    def column_double_rw(
        self,
        name: str,
        get: ColumnGetter[RowT],
        setter: ColumnSetter[RowT],
        *,
        nullable: bool = False,
    ) -> Self:
        """Add a writable floating-point column."""
        return self.column(name, float, get=get, set=setter, nullable=nullable)

    def column_blob_rw(
        self,
        name: str,
        get: ColumnGetter[RowT],
        setter: ColumnSetter[RowT],
        *,
        nullable: bool = False,
    ) -> Self:
        """Add a writable bytes column."""
        return self.column(name, bytes, get=get, set=setter, nullable=nullable)

    def hidden_column(
        self,
        name: str,
        python_type: type[object] | ColumnType,
        *,
        nullable: bool = True,
    ) -> Self:
        """Add a hidden input column."""
        return self.column(name, python_type, nullable=nullable, hidden=True)

    def hidden_column_int(self, name: str) -> Self:
        """Add a hidden integer input."""
        return self.hidden_column(name, int)

    def hidden_column_i64(self, name: str) -> Self:
        """Alias for :meth:`hidden_column_int`."""
        return self.hidden_column_int(name)

    def hidden_column_int64(self, name: str) -> Self:
        """Alias for :meth:`hidden_column_int`."""
        return self.hidden_column_int(name)

    def hidden_column_text(self, name: str) -> Self:
        """Add a hidden text input."""
        return self.hidden_column(name, str)

    def filter(  # noqa: PLR0913 - planner estimates are part of the fluent contract.
        self,
        column: str,
        op: ConstraintOp,
        factory: FilterFactory[RowT],
        *,
        estimated_cost: float = 10.0,
        estimated_rows: int = 10,
        exact: bool | None = None,
    ) -> Self:
        """Add a source optimized for one SQLite constraint."""
        _validate_cost(estimated_cost)
        _validate_count(estimated_rows, "estimated rows")
        if exact is None:
            exact = op in {
                ConstraintOp.EQ,
                ConstraintOp.GE,
                ConstraintOp.GT,
                ConstraintOp.LE,
                ConstraintOp.LT,
                ConstraintOp.IS,
                ConstraintOp.IS_NULL,
            }
        self._filters.append(
            Filter(column, ConstraintOp(op), factory, estimated_cost, estimated_rows, exact),
        )
        return self

    def filter_eq(
        self,
        column: str,
        factory: FilterFactory[RowT],
        estimated_cost: float = 10.0,
        estimated_rows: int = 10,
    ) -> Self:
        """Add an exact equality filter."""
        return self.filter(
            column,
            ConstraintOp.EQ,
            factory,
            estimated_cost=estimated_cost,
            estimated_rows=estimated_rows,
            exact=True,
        )

    def filter_eq_text(
        self,
        column: str,
        factory: FilterFactory[RowT],
        estimated_cost: float = 10.0,
        estimated_rows: int = 10,
    ) -> Self:
        """Alias for an equality filter whose values are text."""
        return self.filter_eq(column, factory, estimated_cost, estimated_rows)

    def filter_prefix(
        self,
        column: str,
        factory: FilterFactory[RowT],
        estimated_cost: float = 10.0,
        estimated_rows: int = 10,
    ) -> Self:
        """Add a LIKE-prefix superset filter; SQLite performs the final check."""
        return self.filter(
            column,
            ConstraintOp.LIKE,
            factory,
            estimated_cost=estimated_cost,
            estimated_rows=estimated_rows,
            exact=False,
        )

    def index(
        self,
        column: str,
        *,
        key: ColumnGetter[RowT] | None = None,
    ) -> Self:
        """Add a materialized equality and range index."""
        self._indexes.append(Index(column, key or self._getter_for(column)))
        return self

    def index_on(
        self,
        column: str,
        key: ColumnGetter[RowT] | None = None,
    ) -> Self:
        """Alias for :meth:`index`."""
        return self.index(column, key=key)

    def _getter_for(self, name: str) -> ColumnGetter[RowT]:
        for column in self._columns:
            if column.name == name:
                if column.get is None:
                    msg = f"hidden column {name!r} cannot be materialized as an index"
                    raise ConfigurationError(msg)
                return column.get
        msg = f"unknown column {name!r}"
        raise ConfigurationError(msg)

    def _validate(self) -> None:  # noqa: C901, PLR0912 - central cross-option validation.
        if not self._columns:
            msg = "a virtual table requires at least one column"
            raise ConfigurationError(msg)
        names = {column.name for column in self._columns}
        for filter_definition in self._filters:
            if filter_definition.column not in names:
                msg = f"filter references unknown column {filter_definition.column!r}"
                raise ConfigurationError(msg)
        for index_definition in self._indexes:
            if index_definition.column not in names:
                msg = f"index references unknown column {index_definition.column!r}"
                raise ConfigurationError(msg)
        for constraint_filter in self._constraint_filters:
            for spec in constraint_filter.specs:
                if spec.column not in names:
                    msg = f"constraint references unknown column {spec.column!r}"
                    raise ConfigurationError(msg)
        if self._kind is TableKind.INDEX:
            if self._source is None and (self._count_rows is None or self._row_at is None):
                msg = "index table requires rows or both count() and row_at()"
                raise ConfigurationError(msg)
        elif (
            self._source is None
            and self._stateful_source is None
            and self._projection_source is None
            and self._context_source is None
        ):
            msg = f"{self._kind.value} table requires a row source"
            raise ConfigurationError(msg)
        if (
            self._kind is TableKind.CACHED
            and self._shared
            and self._source is None
            and self._stateful_source is None
        ):
            msg = (
                "shared cached tables require a complete source via "
                "cached_table(..., source) or cache_builder()"
            )
            raise ConfigurationError(msg)
        if (
            self._kind is TableKind.GENERATOR
            and (self._delete_row is not None or any(column.writable for column in self._columns))
            and self._row_lookup is None
            and self._stateful_row_lookup is None
        ):
            msg = "writable generator tables require row_lookup()"
            raise ConfigurationError(msg)
        if (
            self._filters
            and self._rowid is None
            and (
                self._delete_row is not None
                or self._update_row is not None
                or any(column.writable for column in self._columns)
            )
        ):
            msg = "writable filtered tables require rowid()"
            raise ConfigurationError(msg)
        if self._source is not None and not callable(self._source):
            iterator = iter(self._source)
            if iterator is self._source:
                msg = "a one-shot iterator must be supplied through a factory"
                raise ConfigurationError(msg)

    def _definition(self) -> TableDefinition[RowT]:
        self._validate()
        return TableDefinition(
            name=self._name,
            kind=self._kind,
            columns=tuple(self._columns),
            source=self._source,
            stateful_source=self._stateful_source,
            projection_source=self._projection_source,
            context_source=self._context_source,
            count_rows=self._count_rows,
            estimate_rows=self._estimate_rows,
            row_at=self._row_at,
            rowid=self._rowid,
            row_lookup=self._row_lookup,
            stateful_row_lookup=self._stateful_row_lookup,
            filters=tuple(self._filters),
            indexes=tuple(self._indexes),
            constraint_filters=tuple(self._constraint_filters),
            before_modify=self._before_modify,
            after_modify=self._after_modify,
            transaction_hooks=self._transaction_hooks,
            delete_row=self._delete_row,
            insert_row=self._insert_row,
            update_row=self._update_row,
            full_scan_message=self._full_scan_message,
            shared=self._shared,
        )


class TableBuilder(_BaseTableBuilder[RowT]):
    """Fluent builder for a live sequence/index virtual table."""

    def deletable(self, callback: IndexDeleteCallback) -> Self:
        """Enable deletion by positional rowid."""
        self._delete_row = callback
        return self

    def build(self) -> TableDefinition[RowT]:
        """Validate and freeze the definition."""
        return self._definition()


class CachedTableBuilder(_BaseTableBuilder[RowT]):
    """Fluent builder for a materialized virtual table."""

    def cache_builder(self, source: SourceFactory[RowT]) -> Self:
        """Set the row factory used to materialize a scan."""
        self._source = source
        return self

    def stateful_cache_builder(self, source: StatefulSourceFactory[RowT]) -> Self:
        """Set a row factory receiving this registration's transaction state."""
        self._stateful_source = source
        return self

    def projection_cache_builder(self, source: ProjectionSource[RowT]) -> Self:
        """Set a projection-aware factory for query-scoped materialization."""
        self._projection_source = source
        return self

    def shared_cache(self, *, enabled: bool = True) -> Self:
        """Persist complete rows from the full source until invalidation."""
        self._shared = enabled
        return self

    def deletable(self, callback: RowDeleteCallback[RowT]) -> Self:
        """Enable deletion after resolving the target row."""
        self._delete_row = callback
        return self

    def build(self) -> TableDefinition[RowT]:
        """Validate and freeze the definition."""
        return self._definition()


class CachedTableBuilderStart(CachedTableBuilder[object]):
    """Source-less cached builder whose source establishes the row type."""

    def cache_builder(  # type: ignore[override]
        self,
        source: SourceFactory[SourceRowT],
    ) -> CachedTableBuilder[SourceRowT]:
        """Bind a full row factory on a fresh, row-typed builder."""
        target: CachedTableBuilder[SourceRowT] = CachedTableBuilder(
            self._name,
            self._kind,
            None,
        )
        self._copy_state_to(target)
        target._source = source
        return target

    def projection_cache_builder(  # type: ignore[override]
        self,
        source: ProjectionSource[SourceRowT],
    ) -> CachedTableBuilder[SourceRowT]:
        """Bind a projection factory on a fresh, row-typed builder."""
        target: CachedTableBuilder[SourceRowT] = CachedTableBuilder(
            self._name,
            self._kind,
            None,
        )
        self._copy_state_to(target)
        target._projection_source = source
        return target

    def stateful_cache_builder(  # type: ignore[override]
        self,
        source: StatefulSourceFactory[SourceRowT],
    ) -> CachedTableBuilder[SourceRowT]:
        """Bind a registration-state-aware factory on a row-typed builder."""
        target: CachedTableBuilder[SourceRowT] = CachedTableBuilder(
            self._name,
            self._kind,
            None,
        )
        self._copy_state_to(target)
        target._stateful_source = source
        return target


class GeneratorTableBuilder(_BaseTableBuilder[RowT]):
    """Fluent builder for a streaming virtual table."""

    def generator(
        self,
        source: SourceFactory[RowT] | ContextSource[RowT],
    ) -> Self:
        """Set the iterator factory used for ordinary scans."""
        self._source = source
        return self

    def row_count(self, callback: CountCallback) -> Self:
        """Set the exact count used by bare ``COUNT(*)`` without iteration."""
        return self.count(callback)

    def projection_generator(
        self,
        source: ProjectionSource[RowT],
    ) -> Self:
        """Set a projection-aware iterator factory."""
        self._projection_source = source
        return self

    def context_generator(
        self,
        source: ContextSource[RowT],
    ) -> Self:
        """Set a factory receiving complete planner context."""
        self._context_source = source
        return self

    def constraint_filter(
        self,
        specs: Iterable[ConstraintSpec],
        factory: ConstraintFactory[RowT],
        *,
        estimated_cost: float = 1.0,
        estimated_rows: int = 1,
    ) -> Self:
        """Add a multi-constraint generator plan."""
        normalized = tuple(specs)
        if not normalized:
            msg = "constraint_filter requires at least one constraint"
            raise ConfigurationError(msg)
        if len({(spec.column, spec.op) for spec in normalized}) != len(normalized):
            msg = "constraint_filter contains duplicate column/operator pairs"
            raise ConfigurationError(msg)
        _validate_cost(estimated_cost)
        _validate_count(estimated_rows, "estimated rows")
        self._constraint_filters.append(
            ConstraintFilter(
                specs=normalized,
                factory=factory,
                estimated_cost=estimated_cost,
                estimated_rows=estimated_rows,
            ),
        )
        return self

    def parametric_filter(
        self,
        columns: Iterable[str],
        factory: Callable[[tuple[SQLiteValue, ...]], SourceResult[RowT]],
        *,
        estimated_cost: float = 1.0,
        estimated_rows: int = 1,
    ) -> Self:
        """Add an equality filter requiring every named input column."""
        names = tuple(columns)
        specs = tuple(required_eq(name) for name in names)

        def adapt(constraints: tuple[Constraint, ...]) -> SourceResult[RowT]:
            return factory(tuple(constraint.value for constraint in constraints))

        return self.constraint_filter(
            specs,
            adapt,
            estimated_cost=estimated_cost,
            estimated_rows=estimated_rows,
        )

    def order_by_consumed(self, column: str, *, descending: bool = False) -> Self:
        """Declare ordering guaranteed by the most recent constraint plan."""
        column_index = self._column_position(column)
        if not self._constraint_filters:
            msg = "order_by_consumed() requires a preceding constraint_filter()"
            raise ConfigurationError(msg)
        constraint_filter = self._constraint_filters[-1]
        self._constraint_filters[-1] = ConstraintFilter(
            specs=constraint_filter.specs,
            factory=constraint_filter.factory,
            estimated_cost=constraint_filter.estimated_cost,
            estimated_rows=constraint_filter.estimated_rows,
            order_by=(OrderTerm(column, column_index, descending),),
        )
        return self

    def deletable(self, callback: RowDeleteCallback[RowT]) -> Self:
        """Enable deletion after resolving the target row."""
        self._delete_row = callback
        return self

    def full_scan_error(self, message: str) -> Self:
        """Reject scans for which no configured constraint plan is usable."""
        if not message:
            msg = "full-scan error message must not be empty"
            raise ConfigurationError(msg)
        self._full_scan_message = message
        return self

    def _column_position(self, name: str) -> int:
        for index, column in enumerate(self._columns):
            if column.name == name:
                return index
        msg = f"unknown column {name!r}"
        raise ConfigurationError(msg)

    def build(self) -> TableDefinition[RowT]:
        """Validate and freeze the definition."""
        return self._definition()


class GeneratorTableBuilderStart(GeneratorTableBuilder[object]):
    """Source-less generator builder whose source establishes the row type."""

    def generator(  # type: ignore[override]
        self,
        source: SourceFactory[SourceRowT] | ContextSource[SourceRowT],
    ) -> GeneratorTableBuilder[SourceRowT]:
        """Bind an ordinary source on a fresh, row-typed builder."""
        target: GeneratorTableBuilder[SourceRowT] = GeneratorTableBuilder(
            self._name,
            self._kind,
            None,
        )
        self._copy_state_to(target)
        target._source = source
        return target

    def projection_generator(  # type: ignore[override]
        self,
        source: ProjectionSource[SourceRowT],
    ) -> GeneratorTableBuilder[SourceRowT]:
        """Bind a projection source on a fresh, row-typed builder."""
        target: GeneratorTableBuilder[SourceRowT] = GeneratorTableBuilder(
            self._name,
            self._kind,
            None,
        )
        self._copy_state_to(target)
        target._projection_source = source
        return target

    def context_generator(  # type: ignore[override]
        self,
        source: ContextSource[SourceRowT],
    ) -> GeneratorTableBuilder[SourceRowT]:
        """Bind a context source on a fresh, row-typed builder."""
        target: GeneratorTableBuilder[SourceRowT] = GeneratorTableBuilder(
            self._name,
            self._kind,
            None,
        )
        self._copy_state_to(target)
        target._context_source = source
        return target


@overload
def table(name: str, rows: Sequence[RowT]) -> TableBuilder[RowT]: ...


@overload
def table(name: str, rows: None = None) -> TableBuilder[object]: ...


def table(
    name: str,
    rows: Sequence[RowT] | None = None,
) -> TableBuilder[RowT] | TableBuilder[object]:
    """Start a live sequence/index table definition."""
    if rows is None:
        return TableBuilder(name, TableKind.INDEX, None)
    return TableBuilder(name, TableKind.INDEX, rows)


@overload
def cached_table(name: str, source: Source[RowT]) -> CachedTableBuilder[RowT]: ...


@overload
def cached_table(name: str, source: None = None) -> CachedTableBuilderStart: ...


def cached_table(
    name: str,
    source: Source[RowT] | None = None,
) -> CachedTableBuilder[RowT] | CachedTableBuilderStart:
    """Start a query-scoped cached table definition."""
    if source is None:
        return CachedTableBuilderStart(name, TableKind.CACHED, None)
    return CachedTableBuilder(name, TableKind.CACHED, source)


@overload
def generator_table(name: str, source: SourceFactory[RowT]) -> GeneratorTableBuilder[RowT]: ...


@overload
def generator_table(name: str, source: ContextSource[RowT]) -> GeneratorTableBuilder[RowT]: ...


@overload
def generator_table(name: str, source: Iterable[RowT]) -> GeneratorTableBuilder[RowT]: ...


@overload
def generator_table(name: str, source: None = None) -> GeneratorTableBuilderStart: ...


def generator_table(
    name: str,
    source: GeneratorSource[RowT] | None = None,
) -> GeneratorTableBuilder[RowT] | GeneratorTableBuilderStart:
    """Start a streaming generator table definition."""
    if source is None:
        return GeneratorTableBuilderStart(name, TableKind.GENERATOR, None)
    return GeneratorTableBuilder(name, TableKind.GENERATOR, source)


@dataclass(frozen=True, slots=True)
class _RowEntry(Generic[RowT]):
    rowid: int
    row: RowT


def _empty_equality_cache() -> dict[int, dict[SQLiteValue, tuple[int, ...]]]:
    return {}


def _empty_sorted_positions() -> dict[int, tuple[tuple[SQLiteValue, int], ...]]:
    return {}


@dataclass(slots=True)
class _Snapshot(Generic[RowT]):
    entries: tuple[_RowEntry[RowT], ...]
    equality: dict[int, dict[SQLiteValue, tuple[int, ...]]] = field(
        default_factory=_empty_equality_cache,
    )
    sorted_positions: dict[int, tuple[tuple[SQLiteValue, int], ...]] = field(
        default_factory=_empty_sorted_positions,
    )


@dataclass(frozen=True, slots=True)
class _TransactionSnapshot:
    touched: bool
    wrote: bool
    prepared: bool


class _TransactionLifecycle:
    """Registration-local bookkeeping around public transaction hooks."""

    def __init__(self, hooks: TransactionHooks) -> None:
        self.hooks = hooks
        self.state = hooks.state_factory() if hooks.state_factory is not None else object()
        self.touched = False
        self.wrote = False
        self.prepared = False
        self.savepoints: dict[int, _TransactionSnapshot] = {}

    def begin(self) -> None:
        self._clear()

    def begin_statement(self) -> None:
        """Notify connection-local state that a new vtab scan is starting."""
        callback = getattr(self.state, "_libxsql_begin_statement", None)
        if callable(callback):
            cast("Callable[[], None]", callback)()

    def touch(self) -> None:
        self.touched = True

    def mark_written(self) -> None:
        self.wrote = True
        self.prepared = False

    def sync(self, registration: _RegistrationState[object]) -> None:
        if not self.wrote or self.prepared:
            return
        if self.hooks.prepare_commit is not None:
            _invoke_callback(registration, self.hooks.prepare_commit, self.state)
        self.prepared = True

    def commit(self, registration: _RegistrationState[object]) -> None:
        wrote = self.wrote
        self._clear()
        if wrote and self.hooks.commit is not None:
            with suppress(BaseException):
                _invoke_callback(registration, self.hooks.commit, self.state)

    def rollback(self, registration: _RegistrationState[object]) -> None:
        touched = self.touched
        self._clear()
        if touched and self.hooks.rollback is not None:
            with suppress(BaseException):
                _invoke_callback(registration, self.hooks.rollback, self.state)

    def savepoint(self, registration: _RegistrationState[object], level: int) -> None:
        self.savepoints[level] = _TransactionSnapshot(
            self.touched,
            self.wrote,
            self.prepared,
        )
        try:
            if self.hooks.savepoint is not None:
                _invoke_callback(registration, self.hooks.savepoint, self.state, level)
        except BaseException:
            self.savepoints.pop(level, None)
            raise

    def release(self, registration: _RegistrationState[object], level: int) -> None:
        if self.hooks.release is not None:
            _invoke_callback(registration, self.hooks.release, self.state, level)
        self.savepoints = {
            saved: snapshot for saved, snapshot in self.savepoints.items() if saved < level
        }

    def rollback_to(self, registration: _RegistrationState[object], level: int) -> None:
        if self.hooks.rollback_to is not None:
            _invoke_callback(registration, self.hooks.rollback_to, self.state, level)
        snapshot = self.savepoints.get(
            level,
            _TransactionSnapshot(touched=False, wrote=False, prepared=False),
        )
        self.touched = snapshot.touched
        self.wrote = snapshot.wrote
        self.prepared = snapshot.prepared
        self.savepoints = {
            saved: state for saved, state in self.savepoints.items() if saved <= level
        }

    def _clear(self) -> None:
        self.touched = False
        self.wrote = False
        self.prepared = False
        self.savepoints.clear()


class _RegistrationState(Generic[RowT]):
    def __init__(
        self,
        definition: TableDefinition[RowT],
        async_runner: AsyncRunner | None,
        awaitable_scope: AwaitableScope | None = None,
        interrupt_checker: InterruptChecker | None = None,
    ) -> None:
        self.definition = definition
        self.async_runner = async_runner
        self.awaitable_scope = awaitable_scope
        self.interrupt_checker = interrupt_checker
        self.transaction = _TransactionLifecycle(definition.transaction_hooks)
        self.lock = threading.RLock()
        self.shared_snapshot: _Snapshot[RowT] | None = None
        self.epoch = 0
        self.table_active = True

    def invalidate(self) -> None:
        with self.lock:
            self.epoch += 1
            self.shared_snapshot = None

    def snapshot(self, projection: Projection) -> _Snapshot[RowT]:
        if not self.definition.shared:
            return _materialize_snapshot(
                self.definition,
                projection,
                self.run_coroutine,
                self.transaction.state,
                use_projection=True,
            )
        with self.lock:
            if self.shared_snapshot is None:
                snapshot = _materialize_snapshot(
                    self.definition,
                    projection,
                    self.run_coroutine,
                    self.transaction.state,
                    use_projection=False,
                )
                self.shared_snapshot = snapshot
            return self.shared_snapshot

    def run_coroutine(self, awaitable: Awaitable[ResultT]) -> ResultT:
        runner = self.async_runner
        if runner is None:
            _discard_awaitable(cast("Awaitable[object]", awaitable))
            msg = "async virtual-table sources require an APSW connection in async mode"
            raise ProtocolError(msg)
        prepared: Awaitable[object] = _await_value(awaitable)
        if self.awaitable_scope is not None:
            prepared = self.awaitable_scope(prepared)
        return cast("ResultT", runner(prepared))

    @contextlib.contextmanager
    def callback_scope(self) -> Generator[None, None, None]:
        """Expose the active query's cooperative-cancellation checker."""
        if self.interrupt_checker is None:
            yield
            return
        from .connection import _interruption_scope  # noqa: PLC0415

        with _interruption_scope(self.interrupt_checker):
            yield


def _invoke_callback(
    state: _RegistrationState[object],
    callback: Callable[CallbackParams, CallbackResultT],
    *args: CallbackParams.args,
    **kwargs: CallbackParams.kwargs,
) -> CallbackResultT:
    with state.callback_scope():
        return callback(*args, **kwargs)


class _Module(Generic[RowT]):
    def __init__(self, state: _RegistrationState[RowT]) -> None:
        self.state = state

    def Create(
        self,
        connection: apsw.Connection,
        _module_name: str,
        _database_name: str,
        _table_name: str,
        *_args: SQLiteValue,
    ) -> tuple[str, _Table[RowT]]:
        connection.vtab_config(apsw.SQLITE_VTAB_CONSTRAINT_SUPPORT, 1)
        self.state.table_active = True
        return self.state.definition.schema, _Table(self.state)

    Connect = Create

    def ShadowName(self, _table_suffix: str) -> bool:
        return False


class _PlanKind(IntEnum):
    FULL = 0
    FILTER = 1
    CONSTRAINT_FILTER = 2
    INDEX_EQ = 3
    INDEX_RANGE = 4
    ROWID = 5
    COUNT = 6
    REJECT = 7


@dataclass(frozen=True, slots=True)
class _PlannedConstraint:
    source_index: int
    column_index: int
    op: ConstraintOp
    exact: bool


@dataclass(frozen=True, slots=True)
class _Plan:
    kind: _PlanKind
    target: int = -1
    constraints: tuple[_PlannedConstraint, ...] = ()
    projection: Projection = Projection((), ())
    order_by: tuple[OrderTerm, ...] = ()
    limit_arg: int = -1
    offset_arg: int = -1

    def encode(self) -> str:
        payload = {
            "k": int(self.kind),
            "t": self.target,
            "c": [
                [item.source_index, item.column_index, int(item.op), item.exact]
                for item in self.constraints
            ],
            "p": [
                list(self.projection.columns),
                list(self.projection.column_indices),
                self.projection.includes_rowid,
            ],
            "o": [[term.column, term.column_index, term.descending] for term in self.order_by],
            "l": self.limit_arg,
            "f": self.offset_arg,
        }
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)

    @classmethod
    def decode(cls, text: str | None) -> _Plan:
        if not text:
            return cls(_PlanKind.FULL)
        try:
            payload = cast("dict[str, object]", json.loads(text))
            raw_constraints = cast("list[list[object]]", payload["c"])
            raw_projection = cast("list[object]", payload["p"])
            raw_order = cast("list[list[object]]", payload["o"])
            return cls(
                kind=_PlanKind(cast("int", payload["k"])),
                target=cast("int", payload["t"]),
                constraints=tuple(
                    _PlannedConstraint(
                        cast("int", item[0]),
                        cast("int", item[1]),
                        ConstraintOp(cast("int", item[2])),
                        cast("bool", item[3]),
                    )
                    for item in raw_constraints
                ),
                projection=Projection(
                    tuple(cast("list[str]", raw_projection[0])),
                    tuple(cast("list[int]", raw_projection[1])),
                    cast("bool", raw_projection[2]),
                ),
                order_by=tuple(
                    OrderTerm(
                        cast("str", item[0]),
                        cast("int", item[1]),
                        cast("bool", item[2]),
                    )
                    for item in raw_order
                ),
                limit_arg=cast("int", payload["l"]),
                offset_arg=cast("int", payload["f"]),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            msg = "SQLite returned an invalid libxsql virtual-table plan"
            raise ProtocolError(msg) from error


class _Table(Generic[RowT]):
    def __init__(self, state: _RegistrationState[RowT]) -> None:
        self.state = state
        self.definition = state.definition
        self._active_rows: dict[int, tuple[int, RowT]] = {}
        self._active_lock = threading.RLock()

    def BestIndexObject(self, info: apsw.IndexInfo) -> bool:
        planner = _Planner(self.definition, info)
        plan, estimated_cost, estimated_rows, order_consumed = planner.choose()
        for argument_index, constraint in enumerate(plan.constraints, start=1):
            info.set_aConstraintUsage_argvIndex(constraint.source_index, argument_index)
            info.set_aConstraintUsage_omit(constraint.source_index, constraint.exact)
        next_argument = len(plan.constraints) + 1
        limit_arg = -1
        offset_arg = -1
        if planner.can_push_limit(plan, order_consumed=order_consumed):
            if planner.limit_constraint is not None:
                limit_arg = next_argument - 1
                info.set_aConstraintUsage_argvIndex(planner.limit_constraint, next_argument)
                info.set_aConstraintUsage_omit(planner.limit_constraint, omit=True)
                next_argument += 1
            if planner.offset_constraint is not None:
                offset_arg = next_argument - 1
                info.set_aConstraintUsage_argvIndex(planner.offset_constraint, next_argument)
                info.set_aConstraintUsage_omit(planner.offset_constraint, omit=True)
        plan = _Plan(
            plan.kind,
            plan.target,
            plan.constraints,
            plan.projection,
            plan.order_by,
            limit_arg,
            offset_arg,
        )
        info.idxNum = int(plan.kind)
        info.idxStr = plan.encode()
        info.estimatedCost = estimated_cost
        info.estimatedRows = estimated_rows
        info.orderByConsumed = order_consumed
        if plan.kind is _PlanKind.ROWID:
            info.idxFlags = apsw.SQLITE_INDEX_SCAN_UNIQUE
        return True

    def Open(self) -> _Cursor[RowT]:
        return _Cursor(self)

    def Disconnect(self) -> None:
        """Release a SQLite reference to this table."""

    def Destroy(self) -> None:
        """Retire an executed DROP while retaining module cleanup state."""
        self.state.table_active = False
        self.state.invalidate()

    def Begin(self) -> None:
        self.state.transaction.begin()

    def Sync(self) -> None:
        """Run the fallible prepare phase for a transaction that wrote."""
        self.state.transaction.sync(cast("_RegistrationState[object]", self.state))

    def Commit(self) -> None:
        self.state.transaction.commit(cast("_RegistrationState[object]", self.state))

    def Rollback(self) -> None:
        self.state.table_active = True
        self.state.transaction.rollback(cast("_RegistrationState[object]", self.state))
        self.state.invalidate()

    def Savepoint(self, level: int) -> None:
        self.state.transaction.savepoint(
            cast("_RegistrationState[object]", self.state),
            level,
        )

    def Release(self, level: int) -> None:
        self.state.transaction.release(
            cast("_RegistrationState[object]", self.state),
            level,
        )

    def RollbackTo(self, level: int) -> None:
        self.state.table_active = True
        self.state.transaction.rollback_to(
            cast("_RegistrationState[object]", self.state),
            level,
        )
        self.state.invalidate()

    def Rename(self, _new_name: str) -> None:
        msg = "registered libxsql tables cannot be renamed"
        raise RegistrationError(msg)

    def UpdateDeleteRow(self, rowid: int) -> None:
        callback = self.definition.delete_row
        if callback is None:
            msg = f"table {self.definition.name!r} does not support DELETE"
            raise ReadOnlyError(msg)
        _validate_rowid(rowid)
        if self.definition.kind is TableKind.INDEX:
            argument: object = rowid
        else:
            argument = self._lookup_row(rowid)
        self.state.transaction.touch()
        self._before("delete")
        result = _invoke_callback(
            cast("_RegistrationState[object]", self.state),
            cast("Callable[[object], bool | None]", callback),
            argument,
        )
        _require_callback_success("delete", value=result)
        self._modified("delete")

    def UpdateInsertRow(
        self,
        rowid: int | None,
        fields: tuple[apsw.SQLiteValue, ...],
    ) -> int | None:
        callback = self.definition.insert_row
        if callback is None:
            msg = f"table {self.definition.name!r} does not support INSERT"
            raise ReadOnlyError(msg)
        if rowid is not None:
            _validate_rowid(rowid)
        if len(fields) != len(self.definition.columns):
            msg = "SQLite supplied the wrong number of insert fields"
            raise ProtocolError(msg)
        values: dict[str, SQLiteValue] = {}
        for column, value in zip(self.definition.columns, fields, strict=True):
            if column.hidden:
                continue
            values[column.name] = _coerce_column_value(column, value)
        self.state.transaction.touch()
        self._before("insert")
        result = cast(
            "object",
            _invoke_callback(
                cast("_RegistrationState[object]", self.state),
                callback,
                MappingProxyType(values),
            ),
        )
        if result is False:
            msg = "insert callback rejected the row"
            raise ProtocolError(msg)
        assigned: int | None
        if isinstance(result, bool) or result is None:
            assigned = rowid
        elif isinstance(result, int):
            assigned = _validate_rowid(result)
        else:
            msg = "insert callback must return an integer rowid, bool, or None"
            raise ProtocolError(msg)
        self._modified("insert")
        return assigned

    def UpdateChangeRow(  # noqa: C901 - APSW supplies one operation callback.
        self,
        rowid: int,
        new_rowid: int,
        fields: tuple[apsw.SQLiteValue | apsw.no_change, ...],
    ) -> None:
        _validate_rowid(rowid)
        _validate_rowid(new_rowid)
        if rowid != new_rowid:
            msg = "changing a virtual-table rowid is not supported"
            raise ReadOnlyError(msg)
        if len(fields) != len(self.definition.columns):
            msg = "SQLite supplied the wrong number of update fields"
            raise ProtocolError(msg)
        changed: list[tuple[Column[RowT], SQLiteValue]] = []
        for column, value in zip(self.definition.columns, fields, strict=True):
            if value is apsw.no_change:
                continue
            changed.append((column, _coerce_column_value(column, value)))
        if not changed:
            return
        atomic = self.definition.update_row
        if atomic is None:
            readonly = [column.name for column, _value in changed if column.set is None]
            if readonly:
                names = ", ".join(readonly)
                msg = f"column(s) {names} are read-only"
                raise ReadOnlyError(msg)
        self.state.transaction.touch()
        self._before("update")
        if atomic is not None:
            changes = MappingProxyType({column.name: value for column, value in changed})
            result = _invoke_callback(
                cast("_RegistrationState[object]", self.state),
                atomic,
                rowid,
                changes,
            )
            _require_callback_success("update", value=result)
        else:
            row = self._lookup_row(rowid)
            applied: list[str] = []
            for column, value in changed:
                # The immutable preflight above proved every changed column writable.
                setter = cast("ColumnSetter[RowT]", column.set)
                try:
                    result = _invoke_callback(
                        cast("_RegistrationState[object]", self.state),
                        setter,
                        row,
                        value,
                    )
                    _require_callback_success(
                        f"setter for {column.name}",
                        value=result,
                    )
                except Exception as error:
                    if not applied:
                        raise
                    msg = f"update failed while applying column {column.name!r}"
                    raise PartialUpdateError(msg, applied_columns=applied) from error
                applied.append(column.name)
        self._modified("update")

    def _lookup_row(self, rowid: int) -> RowT:
        if self.definition.stateful_row_lookup is not None:
            row = _invoke_callback(
                cast("_RegistrationState[object]", self.state),
                self.definition.stateful_row_lookup,
                self.state.transaction.state,
                rowid,
            )
            if row is None:
                msg = f"rowid {rowid} no longer exists"
                raise ProtocolError(msg)
            return row
        if self.definition.row_lookup is not None:
            row = _invoke_callback(
                cast("_RegistrationState[object]", self.state),
                self.definition.row_lookup,
                rowid,
            )
            if row is None:
                msg = f"rowid {rowid} no longer exists"
                raise ProtocolError(msg)
            return row
        with self._active_lock:
            for active_rowid, row in reversed(tuple(self._active_rows.values())):
                if active_rowid == rowid:
                    return row
        if self.definition.kind is TableKind.INDEX and self.definition.row_at is not None:
            return _invoke_callback(
                cast("_RegistrationState[object]", self.state),
                self.definition.row_at,
                rowid,
            )
        source = self.definition.source
        if (
            self.definition.kind is TableKind.INDEX
            and source is not None
            and not callable(source)
            and isinstance(source, Sequence)
        ):
            try:
                sequence_source: Sequence[RowT] = source
                return sequence_source[rowid]
            except IndexError as error:
                msg = f"rowid {rowid} no longer exists"
                raise ProtocolError(msg) from error
        msg = "row_lookup() is required to reconstruct this row for mutation"
        raise ReadOnlyError(msg)

    def _set_active(self, cursor: _Cursor[RowT], entry: _RowEntry[RowT] | None) -> None:
        with self._active_lock:
            key = id(cursor)
            if entry is None:
                self._active_rows.pop(key, None)
            else:
                self._active_rows[key] = (entry.rowid, entry.row)

    def _before(self, operation: str) -> None:
        if self.definition.before_modify is not None:
            _invoke_callback(
                cast("_RegistrationState[object]", self.state),
                self.definition.before_modify,
                operation,
            )

    def _modified(self, operation: str) -> None:
        self.state.transaction.mark_written()
        self.state.invalidate()
        if self.definition.after_modify is not None:
            _invoke_callback(
                cast("_RegistrationState[object]", self.state),
                self.definition.after_modify,
                operation,
            )


class _Planner(Generic[RowT]):
    def __init__(self, definition: TableDefinition[RowT], info: apsw.IndexInfo) -> None:
        self.definition = definition
        self.info = info
        self.usable: list[tuple[int, int, ConstraintOp]] = []
        self.limit_constraint: int | None = None
        self.offset_constraint: int | None = None
        for source_index in range(info.nConstraint):
            if not info.get_aConstraint_usable(source_index):
                continue
            op = ConstraintOp(info.get_aConstraint_op(source_index))
            column_index = info.get_aConstraint_iColumn(source_index)
            if op is ConstraintOp.LIMIT:
                self.limit_constraint = source_index
            elif op is ConstraintOp.OFFSET:
                self.offset_constraint = source_index
            else:
                self.usable.append((source_index, column_index, op))
        self.projection = _projection(definition, info.colUsed)
        self.order_by = tuple(
            OrderTerm(
                _column_name(definition, info.get_aOrderBy_iColumn(index)) or "rowid",
                info.get_aOrderBy_iColumn(index),
                info.get_aOrderBy_desc(index),
            )
            for index in range(info.nOrderBy)
        )

    def choose(self) -> tuple[_Plan, float, int, bool]:  # noqa: PLR0911
        rowid = self._rowid_plan()
        if rowid is not None:
            return rowid
        constraint_filter, missing_constraint_target = self._constraint_filter_plan()
        if constraint_filter is not None:
            return constraint_filter
        regular_filter = self._filter_plan()
        if regular_filter is not None:
            return regular_filter
        index_plan = self._index_plan()
        if index_plan is not None:
            return index_plan
        if missing_constraint_target >= 0:
            plan = _Plan(
                _PlanKind.REJECT,
                target=missing_constraint_target,
                projection=self.projection,
                order_by=self.order_by,
            )
            return plan, 1.0, 1, False
        if (
            not self.usable
            and not self.projection.column_indices
            and not self.projection.includes_rowid
            and self.definition.count_rows is not None
            and not self.definition.writable
        ):
            count = _checked_callback_count(self.definition.count_rows, "row count")
            plan = _Plan(_PlanKind.COUNT, projection=self.projection, order_by=self.order_by)
            return plan, 1.0, count, False
        if self.definition.full_scan_message is not None:
            plan = _Plan(_PlanKind.REJECT, projection=self.projection, order_by=self.order_by)
            return plan, 1.0, 1, False
        rows = _estimated_rows(self.definition)
        plan = _Plan(_PlanKind.FULL, projection=self.projection, order_by=self.order_by)
        return plan, float(max(rows, 1)), rows, False

    def can_push_limit(self, plan: _Plan, *, order_consumed: bool) -> bool:
        if self.limit_constraint is None and self.offset_constraint is None:
            return False
        if self.order_by and not order_consumed:
            return False
        consumed = {constraint.source_index for constraint in plan.constraints if constraint.exact}
        return all(source_index in consumed for source_index, _column, _op in self.usable)

    def _rowid_plan(self) -> tuple[_Plan, float, int, bool] | None:
        if self.definition.row_lookup is None and self.definition.stateful_row_lookup is None:
            return None
        for source_index, column_index, op in self.usable:
            if column_index == -1 and op is ConstraintOp.EQ:
                constraint = _PlannedConstraint(source_index, -1, op, exact=True)
                plan = _Plan(
                    _PlanKind.ROWID,
                    constraints=(constraint,),
                    projection=self.projection,
                    order_by=self.order_by,
                )
                return plan, 1.0, 1, False
        return None

    def _constraint_filter_plan(
        self,
    ) -> tuple[tuple[_Plan, float, int, bool] | None, int]:
        best: tuple[tuple[float, int, int], tuple[_Plan, float, int, bool]] | None = None
        missing_target = -1
        for target, filter_definition in enumerate(self.definition.constraint_filters):
            matches: list[_PlannedConstraint] = []
            required_missing = False
            for spec in filter_definition.specs:
                column_index = self.definition.column_index(spec.column)
                match = next(
                    (
                        (source_index, candidate_column, op)
                        for source_index, candidate_column, op in self.usable
                        if candidate_column == column_index and op is spec.op
                    ),
                    None,
                )
                if match is None:
                    if spec.required:
                        required_missing = True
                    continue
                matches.append(
                    _PlannedConstraint(match[0], match[1], match[2], spec.exact),
                )
            if required_missing:
                if missing_target < 0:
                    missing_target = target
                continue
            if not matches:
                continue
            plan = _Plan(
                _PlanKind.CONSTRAINT_FILTER,
                target,
                tuple(matches),
                self.projection,
                self.order_by,
            )
            order_consumed = self._order_consumed(filter_definition.order_by)
            candidate = (
                (
                    filter_definition.estimated_cost,
                    -int(order_consumed),
                    -len(matches),
                ),
                (
                    plan,
                    filter_definition.estimated_cost,
                    filter_definition.estimated_rows,
                    order_consumed,
                ),
            )
            if best is None or candidate[0] < best[0]:
                best = candidate
        return (None if best is None else best[1]), missing_target

    def _filter_plan(self) -> tuple[_Plan, float, int, bool] | None:
        best: tuple[float, tuple[_Plan, float, int, bool]] | None = None
        for target, filter_definition in enumerate(self.definition.filters):
            column_index = self.definition.column_index(filter_definition.column)
            for source_index, candidate_column, op in self.usable:
                if candidate_column != column_index or op is not filter_definition.op:
                    continue
                constraint = _PlannedConstraint(
                    source_index,
                    column_index,
                    op,
                    filter_definition.exact,
                )
                plan = _Plan(
                    _PlanKind.FILTER,
                    target,
                    (constraint,),
                    self.projection,
                    self.order_by,
                )
                candidate = (
                    filter_definition.estimated_cost,
                    (
                        plan,
                        filter_definition.estimated_cost,
                        filter_definition.estimated_rows,
                        False,
                    ),
                )
                if best is None or candidate[0] < best[0]:
                    best = candidate
        return None if best is None else best[1]

    def _index_plan(self) -> tuple[_Plan, float, int, bool] | None:
        for target, index_definition in enumerate(self.definition.indexes):
            column_index = self.definition.column_index(index_definition.column)
            for source_index, candidate_column, op in self.usable:
                if candidate_column == column_index and op is ConstraintOp.EQ:
                    constraint = _PlannedConstraint(
                        source_index,
                        column_index,
                        op,
                        exact=True,
                    )
                    plan = _Plan(
                        _PlanKind.INDEX_EQ,
                        target,
                        (constraint,),
                        self.projection,
                        self.order_by,
                    )
                    return plan, 2.0, max(1, _estimated_rows(self.definition) // 10), False
            lower = next(
                (
                    (source_index, column_index, op)
                    for source_index, candidate_column, op in self.usable
                    if candidate_column == column_index and op in {ConstraintOp.GE, ConstraintOp.GT}
                ),
                None,
            )
            upper = next(
                (
                    (source_index, column_index, op)
                    for source_index, candidate_column, op in self.usable
                    if candidate_column == column_index and op in {ConstraintOp.LE, ConstraintOp.LT}
                ),
                None,
            )
            bounds = tuple(
                _PlannedConstraint(
                    source_index,
                    candidate_column,
                    op,
                    exact=True,
                )
                for item in (lower, upper)
                if item is not None
                for source_index, candidate_column, op in (item,)
            )
            if bounds:
                plan = _Plan(
                    _PlanKind.INDEX_RANGE,
                    target,
                    bounds,
                    self.projection,
                    self.order_by,
                )
                order_consumed = (
                    len(self.order_by) == 1
                    and self.order_by[0].column_index == column_index
                    and not self.order_by[0].descending
                )
                return (
                    plan,
                    5.0,
                    max(1, _estimated_rows(self.definition) // 4),
                    order_consumed,
                )
        return None

    def _order_consumed(self, guaranteed: tuple[OrderTerm, ...]) -> bool:
        return bool(self.order_by) and self.order_by == guaranteed


class _Cursor(Generic[RowT]):
    def __init__(self, table_object: _Table[RowT]) -> None:
        self.table = table_object
        self.definition = table_object.definition
        self._iterator: Iterator[_RowEntry[RowT]] | None = None
        self._async_iterator: AsyncIterator[RowT] | None = None
        self._current: _RowEntry[RowT] | None = None
        self._eof = True
        self._closed = False
        self._hidden_values: dict[int, SQLiteValue] = {}
        self._seen_rowids: set[int] = set()
        self._next_position = 0
        self._count_only_context: GeneratorContext | None = None

    def Filter(
        self,
        index_number: int,
        index_name: str | None,
        constraint_args: tuple[apsw.SQLiteValue, ...] | None,
    ) -> None:
        with self.table.state.callback_scope():
            self.table.state.transaction.begin_statement()
            self._filter(index_number, index_name, constraint_args)

    def _filter(  # noqa: C901, PLR0912, PLR0915 - dispatches immutable SQLite plans.
        self,
        _index_number: int,
        index_name: str | None,
        constraint_args: tuple[apsw.SQLiteValue, ...] | None,
    ) -> None:
        self._reset_scan()
        plan = _Plan.decode(index_name)
        args = tuple(constraint_args or ())
        expected = len(plan.constraints)
        if plan.limit_arg >= 0:
            expected += 1
        if plan.offset_arg >= 0:
            expected += 1
        if len(args) != expected:
            msg = f"SQLite supplied {len(args)} plan arguments; expected {expected}"
            raise ProtocolError(msg)
        constraints = tuple(
            Constraint(
                _column_name(self.definition, planned.column_index),
                planned.column_index,
                planned.op,
                _normalize_sqlite_value(args[index]),
            )
            for index, planned in enumerate(plan.constraints)
        )
        self._hidden_values = {
            constraint.column_index: constraint.value
            for constraint in constraints
            if constraint.column_index >= 0
            and self.definition.columns[constraint.column_index].hidden
        }
        limit = (
            _optional_nonnegative_int(args[plan.limit_arg], "LIMIT")
            if plan.limit_arg >= 0
            else None
        )
        offset = (
            _optional_nonnegative_int(args[plan.offset_arg], "OFFSET")
            if plan.offset_arg >= 0
            else 0
        )
        context = GeneratorContext(plan.projection, constraints, plan.order_by, limit, offset)
        entries: Iterable[_RowEntry[RowT]]
        if plan.kind is _PlanKind.REJECT:
            raise FullScanError(_rejection_message(self.definition, plan.target))
        if plan.kind is _PlanKind.COUNT:
            if self.definition.shared:
                count = len(self.table.state.snapshot(plan.projection).entries)
            else:
                count_callback = self.definition.count_rows
                if count_callback is None:
                    msg = "count plan selected without a count callback"
                    raise ProtocolError(msg)
                count = _checked_callback_count(count_callback, "row count")
            self._count_only_context = context
            entries = (_RowEntry(rowid, cast("RowT", None)) for rowid in range(count))
        elif plan.kind is _PlanKind.ROWID:
            value = constraints[0].value
            if not isinstance(value, int) or isinstance(value, bool):
                entries = ()
            else:
                stateful_row_lookup = self.definition.stateful_row_lookup
                row_lookup = self.definition.row_lookup
                if stateful_row_lookup is not None:
                    row = stateful_row_lookup(
                        self.table.state.transaction.state,
                        value,
                    )
                    entries = () if row is None else (_RowEntry(value, row),)
                elif row_lookup is None:
                    entries = ()
                else:
                    row = row_lookup(value)
                    entries = () if row is None else (_RowEntry(value, row),)
        elif plan.kind is _PlanKind.FILTER:
            single_filter = self.definition.filters[plan.target]
            rows = single_filter.factory(constraints[0].value)
            entries = self._entries_from_source(rows)
        elif plan.kind is _PlanKind.CONSTRAINT_FILTER:
            constraint_filter = self.definition.constraint_filters[plan.target]
            rows = constraint_filter.factory(constraints)
            entries = self._entries_from_source(rows)
        elif plan.kind in {_PlanKind.INDEX_EQ, _PlanKind.INDEX_RANGE}:
            snapshot = self.table.state.snapshot(plan.projection)
            positions = _indexed_positions(snapshot, self.definition, plan, constraints)
            entries = (snapshot.entries[position] for position in positions)
        else:
            entries = self._full_scan(context)
        if offset or limit is not None:
            entries = _window(entries, offset, limit)
        self._iterator = iter(entries)
        self._advance()

    def Eof(self) -> bool:
        return self._eof

    def Next(self) -> None:
        with self.table.state.callback_scope():
            self._advance()

    def Column(self, number: int) -> SQLiteValue:
        with self.table.state.callback_scope():
            if number in self._hidden_values:
                return self._hidden_values[number]
            self._materialize_count_only_row()
            current = self._require_current()
            try:
                column = self.definition.columns[number]
            except IndexError as error:
                msg = f"column index {number} is out of range"
                raise ProtocolError(msg) from error
            if column.get is None:
                return None
            return _coerce_column_value(column, column.get(current.row))

    def ColumnNoChange(self, _number: int) -> apsw.no_change:
        return cast("apsw.no_change", apsw.no_change)

    def Rowid(self) -> int:
        with self.table.state.callback_scope():
            self._materialize_count_only_row()
            return self._require_current().rowid

    def Close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._close_iterators()
        finally:
            self.table._set_active(self, None)
            self._count_only_context = None
            self._current = None
            self._eof = True

    def _full_scan(self, context: GeneratorContext) -> Iterable[_RowEntry[RowT]]:
        if self.definition.kind is TableKind.INDEX:
            if self.definition.source is None:
                count_callback = self.definition.count_rows
                row_at = self.definition.row_at
                if count_callback is None or row_at is None:
                    msg = "index table has no row source"
                    raise ProtocolError(msg)
                count = _checked_callback_count(count_callback, "row count")
                return (
                    _RowEntry(
                        _rowid(self.definition, row := row_at(position), position),
                        row,
                    )
                    for position in range(count)
                )
            return self._entries_from_source(
                _invoke_source(cast("Source[RowT]", self.definition.source)),
            )
        if self.definition.kind is TableKind.CACHED:
            return self.table.state.snapshot(context.projection).entries
        if self.definition.context_source is not None:
            return self._entries_from_source(self.definition.context_source(context))
        if self.definition.projection_source is not None:
            return self._entries_from_source(
                self.definition.projection_source(context.projection),
            )
        source = self.definition.source
        if source is None:
            msg = "generator has no row source"
            raise ProtocolError(msg)
        return self._entries_from_source(_invoke_generator_source(source, context))

    def _entries_from_source(self, source: SourceResult[RowT]) -> Iterable[_RowEntry[RowT]]:
        resolved = _resolve_source(source, self.table.state.run_coroutine)
        if isinstance(resolved, AsyncIterable):
            self._async_iterator = aiter(resolved)
            return _AsyncEntryIterator(self)
        iterator = iter(resolved)
        return _EntryIterator(self, iterator)

    def _entry(self, row: RowT) -> _RowEntry[RowT]:
        rowid = _rowid(self.definition, row, self._next_position)
        self._next_position += 1
        if rowid in self._seen_rowids:
            msg = f"row source produced duplicate rowid {rowid}"
            raise ProtocolError(msg)
        self._seen_rowids.add(rowid)
        return _RowEntry(rowid, row)

    def _materialize_count_only_row(self) -> None:
        context = self._count_only_context
        if context is None:
            return
        phantom = self._require_current()
        position = phantom.rowid - context.offset
        self._count_only_context = None
        self._close_iterators()
        self.table._set_active(self, None)
        self._current = None
        self._eof = True
        self._seen_rowids.clear()
        self._next_position = 0
        entries: Iterable[_RowEntry[RowT]] = self._full_scan(context)
        if context.offset or context.limit is not None:
            entries = _window(entries, context.offset, context.limit)
        self._iterator = iterator = iter(entries)
        current: _RowEntry[RowT] | None = None
        try:
            for _index in range(position + 1):
                current = next(iterator)
        except StopIteration as error:
            self._close_iterators()
            msg = "row_count() exceeds the rows produced by the row source"
            raise ProtocolError(msg) from error
        self._current = cast("_RowEntry[RowT]", current)
        self._eof = False
        self.table._set_active(self, self._current)

    def _advance(self) -> None:
        self.table._set_active(self, None)
        iterator = self._iterator
        if iterator is None:
            self._current = None
            self._eof = True
            return
        try:
            current = next(iterator)
        except StopIteration:
            self._current = None
            self._eof = True
            return
        self._current = current
        self._eof = False
        self.table._set_active(self, current)

    def _require_current(self) -> _RowEntry[RowT]:
        if self._current is None or self._eof:
            msg = "virtual-table cursor is not positioned on a row"
            raise ProtocolError(msg)
        return self._current

    def _reset_scan(self) -> None:
        if self._closed:
            msg = "virtual-table cursor is closed"
            raise ClosedError(msg)
        self._close_iterators()
        self.table._set_active(self, None)
        self._iterator = None
        self._async_iterator = None
        self._current = None
        self._eof = True
        self._hidden_values.clear()
        self._seen_rowids.clear()
        self._next_position = 0
        self._count_only_context = None

    def _close_iterators(self) -> None:
        iterator = self._iterator
        async_iterator = self._async_iterator
        self._iterator = None
        self._async_iterator = None
        if async_iterator is not None:
            close = getattr(async_iterator, "aclose", None)
            if close is not None:
                self.table.state.run_coroutine(cast("Awaitable[object]", close()))
        if iterator is not None:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()


class _EntryIterator(Iterator[_RowEntry[RowT]], Generic[RowT]):
    def __init__(self, cursor: _Cursor[RowT], rows: Iterator[RowT]) -> None:
        self.cursor = cursor
        self.rows = rows

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> _RowEntry[RowT]:
        return self.cursor._entry(next(self.rows))

    def close(self) -> None:
        close = getattr(self.rows, "close", None)
        if close is not None:
            close()


class _AsyncEntryIterator(Iterator[_RowEntry[RowT]], Generic[RowT]):
    def __init__(self, cursor: _Cursor[RowT]) -> None:
        self.cursor = cursor

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> _RowEntry[RowT]:
        iterator = self.cursor._async_iterator
        if iterator is None:
            raise StopIteration
        try:
            row = self.cursor.table.state.run_coroutine(anext(iterator))
        except StopAsyncIteration as error:
            raise StopIteration from error
        return self.cursor._entry(row)


class Registration:
    """A live connection-local virtual-table registration."""

    def __init__(  # noqa: PLR0913, PLR0917 - owns six distinct resources/names.
        self,
        connection: apsw.Connection,
        definition: TableDefinition[object],
        state: _RegistrationState[object],
        module_name: str,
        table_name: str,
        schema: str,
    ) -> None:
        """Initialize an active registration."""
        self._connection = weakref.ref(connection)
        self._definition = definition
        self._state = state
        self._module_name = module_name
        self._table_name = table_name
        self._schema = schema
        self._active = True
        self._lock = threading.RLock()

    @property
    def _table_active(self) -> bool:
        return self._state.table_active

    @_table_active.setter
    def _table_active(self, active: bool) -> None:
        self._state.table_active = active

    def _refresh_table_state(self, create_sql: str | None) -> None:
        """Refresh liveness after a successfully executed transaction rollback."""
        if not self._active:
            return
        expected_module = self._module_name.casefold()
        self._table_active = (
            create_sql is not None
            and "create virtual table" in create_sql.casefold()
            and expected_module in create_sql.casefold()
        )

    @property
    def definition(self) -> TableDefinition[object]:
        """Return the immutable registered definition."""
        return self._definition

    @property
    def table_name(self) -> str:
        """Return the connection-local SQL table name."""
        return self._table_name

    @property
    def schema(self) -> str:
        """Return the SQLite schema containing the table."""
        return self._schema

    @property
    def is_active(self) -> bool:
        """Whether the registration has not been closed."""
        return self._active and self._table_active

    def invalidate(self) -> None:
        """Invalidate this registration's persistent cache, if any."""
        if not self.is_active:
            msg = "virtual-table registration is closed"
            raise ClosedError(msg)
        self._state.invalidate()

    def unregister(self) -> None:
        """Drop the virtual table and unregister its private APSW module."""
        with self._lock:
            if not self._active:
                return
            connection = self._connection()
            if connection is None:
                self._active = False
                self._table_active = False
                return
            try:
                if self._table_active:
                    result = connection.execute(
                        f"DROP TABLE IF EXISTS "
                        f"{_qualified_identifier(self._schema, self._table_name)}",
                    )
                    _reject_awaitable(
                        result,
                        "use await registration.aclose() for async connections",
                    )
                    self._table_active = False
                module_result = _call_create_module(connection, self._module_name, None)
                _reject_awaitable(
                    module_result,
                    "use await registration.aclose() for async connections",
                )
            except Exception as error:
                msg = f"could not unregister virtual table {self._table_name!r}"
                raise RegistrationError(msg) from error
            self._active = False
            self._table_active = False
            self._state.invalidate()

    close = unregister

    async def aclose(self) -> None:
        """Asynchronously drop the table and unregister its APSW module."""
        with self._lock:
            if not self._active:
                return
            connection = self._connection()
            if connection is None:
                self._active = False
                self._table_active = False
                return
            try:
                if self._table_active:
                    await _await_maybe(
                        connection.execute(
                            f"DROP TABLE IF EXISTS "
                            f"{_qualified_identifier(self._schema, self._table_name)}",
                        ),
                    )
                    self._table_active = False
                await _await_maybe(_call_create_module(connection, self._module_name, None))
            except Exception as error:
                msg = f"could not unregister virtual table {self._table_name!r}"
                raise RegistrationError(msg) from error
            self._active = False
            self._table_active = False
            self._state.invalidate()

    def __enter__(self) -> Self:
        """Return this active registration."""
        if not self.is_active:
            msg = "virtual-table registration is closed"
            raise ClosedError(msg)
        return self

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Close the registration when leaving a synchronous context."""
        self.close()

    async def __aenter__(self) -> Self:
        """Return this active registration."""
        if not self.is_active:
            msg = "virtual-table registration is closed"
            raise ClosedError(msg)
        return self

    async def __aexit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Close the registration when leaving an asynchronous context."""
        await self.aclose()


def register_definition(
    connection: apsw.Connection,
    definition: TableDefinition[RowT],
    *,
    table_name: str | None = None,
    schema: str = "temp",
    interrupt_checker: InterruptChecker | None = None,
) -> Registration:
    """Register and create one virtual table on an APSW connection."""
    name = _validate_identifier(table_name or definition.name, "table")
    schema_name = _validate_identifier(schema, "schema")
    module_name = f"_libxsql_{uuid.uuid4().hex}"
    state = _RegistrationState(
        definition,
        _connection_async_runner(connection),
        interrupt_checker=interrupt_checker,
    )
    module = _Module(state)
    try:
        result = _call_create_module(
            connection,
            module_name,
            cast("apsw.VTModule", module),
            use_bestindex_object=True,
            use_no_change=True,
            iVersion=3,
            read_only=not definition.writable,
        )
        _reject_awaitable(result, "use async_register_definition() for async connections")
        result = connection.execute(
            f"CREATE VIRTUAL TABLE {_qualified_identifier(schema_name, name)} "
            f"USING {_quote_identifier(module_name)}",
        )
        _reject_awaitable(result, "use async_register_definition() for async connections")
    except Exception as error:
        with suppress(Exception):
            cleanup = _call_create_module(connection, module_name, None)
            if inspect.isawaitable(cleanup):
                _discard_awaitable(cast("Awaitable[object]", cleanup))
        msg = f"could not register virtual table {name!r}"
        raise RegistrationError(msg) from error
    return Registration(
        connection,
        cast("TableDefinition[object]", definition),
        cast("_RegistrationState[object]", state),
        module_name,
        name,
        schema_name,
    )


async def async_register_definition(  # noqa: PLR0913 - explicit adapter configuration
    connection: apsw.Connection,
    definition: TableDefinition[RowT],
    *,
    table_name: str | None = None,
    schema: str = "temp",
    awaitable_scope: AwaitableScope | None = None,
    interrupt_checker: InterruptChecker | None = None,
) -> Registration:
    """Register and create one virtual table on an APSW async connection."""
    name = _validate_identifier(table_name or definition.name, "table")
    schema_name = _validate_identifier(schema, "schema")
    module_name = f"_libxsql_{uuid.uuid4().hex}"
    state = _RegistrationState(
        definition,
        _connection_async_runner(connection),
        awaitable_scope,
        interrupt_checker,
    )
    module = _Module(state)
    try:
        await _await_maybe(
            _call_create_module(
                connection,
                module_name,
                cast("apsw.VTModule", module),
                use_bestindex_object=True,
                use_no_change=True,
                iVersion=3,
                read_only=not definition.writable,
            ),
        )
        await _await_maybe(
            connection.execute(
                f"CREATE VIRTUAL TABLE {_qualified_identifier(schema_name, name)} "
                f"USING {_quote_identifier(module_name)}",
            ),
        )
    except Exception as error:
        with contextlib.suppress(Exception):
            await _await_maybe(_call_create_module(connection, module_name, None))
        msg = f"could not register virtual table {name!r}"
        raise RegistrationError(msg) from error
    return Registration(
        connection,
        cast("TableDefinition[object]", definition),
        cast("_RegistrationState[object]", state),
        module_name,
        name,
        schema_name,
    )


def _connection_async_runner(connection: apsw.Connection) -> AsyncRunner | None:
    try:
        controller = connection.async_controller
    except (AttributeError, TypeError):
        return None
    runner = getattr(controller, "async_run_coro", None)
    if not callable(runner):
        return None
    return cast("AsyncRunner", runner)


def _call_create_module(
    connection: apsw.Connection,
    module_name: str,
    module: apsw.VTModule | None,
    **options: object,
) -> object:
    """Call APSW's sync-or-async module API through one typed boundary."""
    create_module = cast("Callable[..., object]", connection.create_module)
    return create_module(module_name, module, **options)


def _validate_identifier(value: str, kind: str) -> str:
    raw_value = cast("object", value)
    if not isinstance(raw_value, str) or not raw_value or "\x00" in raw_value:
        msg = f"{kind} name must be a non-empty string without NUL characters"
        raise ConfigurationError(msg)
    return raw_value


def _quote_identifier(value: str) -> str:
    return f'"{value.replace(chr(34), chr(34) * 2)}"'


def _qualified_identifier(schema: str, name: str) -> str:
    return f"{_quote_identifier(schema)}.{_quote_identifier(name)}"


def _column_type(value: type[object] | ColumnType) -> ColumnType:
    if isinstance(value, ColumnType):
        return value
    if value is int or value is bool:
        return ColumnType.INTEGER
    if value is str:
        return ColumnType.TEXT
    if value is float:
        return ColumnType.REAL
    if value in {bytes, bytearray, memoryview}:
        return ColumnType.BLOB
    if value is object:
        return ColumnType.ANY
    msg = f"unsupported SQLite column type {value!r}"
    raise ConfigurationError(msg)


def _default_getter(name: str) -> ColumnGetter[RowT]:
    def get(row: RowT) -> object:
        if isinstance(row, Mapping):
            return cast("Mapping[object, object]", row)[name]
        return getattr(row, name)

    return get


def _normalize_sqlite_value(value: object) -> SQLiteValue:  # noqa: PLR0911
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        if not _MIN_ROWID <= value <= _MAX_ROWID:
            msg = f"integer {value} does not fit SQLite's signed 64-bit range"
            raise ProtocolError(msg)
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    msg = f"value of type {type(value).__name__} is not a native SQLite value"
    raise ProtocolError(msg)


def _sqlite_sort_key(value: SQLiteValue) -> tuple[int, int | float | str | bytes]:
    """Return SQLite's NULL, numeric, text, and blob storage-class ordering."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return 0, 0
    if isinstance(value, (int, float)):
        return 1, value
    if isinstance(value, str):
        return 2, value
    return 3, value


def _coerce_column_value(column: Column[RowT], value: object) -> SQLiteValue:
    normalized = _normalize_sqlite_value(value)
    if normalized is None:
        if not column.nullable and not column.hidden:
            msg = f"column {column.name!r} is not nullable"
            raise ProtocolError(msg)
        return None
    if column.type is ColumnType.INTEGER:
        if not isinstance(normalized, int):
            msg = f"column {column.name!r} requires an integer"
            raise ProtocolError(msg)
    elif column.type is ColumnType.REAL:
        if not isinstance(normalized, (int, float)):
            msg = f"column {column.name!r} requires a real number"
            raise ProtocolError(msg)
        normalized = float(normalized)
    elif column.type is ColumnType.TEXT and not isinstance(normalized, str):
        msg = f"column {column.name!r} requires text"
        raise ProtocolError(msg)
    elif column.type is ColumnType.BLOB and not isinstance(normalized, bytes):
        msg = f"column {column.name!r} requires bytes"
        raise ProtocolError(msg)
    return normalized


def _validate_count(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        msg = f"{label} must be a non-negative integer"
        raise ConfigurationError(msg)
    return value


def _checked_callback_count(callback: CountCallback, label: str) -> int:
    value = cast("object", callback())
    try:
        return _validate_count(value, label)
    except ConfigurationError as error:
        raise ProtocolError(str(error)) from error


def _validate_cost(value: float) -> float:
    raw_value = cast("object", value)
    if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
        msg = "estimated cost must be a finite non-negative number"
        raise ConfigurationError(msg)
    result = float(raw_value)
    if not math.isfinite(result) or result < 0:
        msg = "estimated cost must be a finite non-negative number"
        raise ConfigurationError(msg)
    return result


def _validate_rowid(value: int) -> int:
    raw_value = cast("object", value)
    if isinstance(raw_value, bool) or not isinstance(raw_value, int):
        msg = "rowid must be an integer"
        raise ProtocolError(msg)
    if not _MIN_ROWID <= raw_value <= _MAX_ROWID:
        msg = f"rowid {raw_value} is outside SQLite's signed 64-bit range"
        raise ProtocolError(msg)
    if raw_value < 0:
        msg = f"negative rowid {raw_value} is not accepted by libxsql table adapters"
        raise ProtocolError(msg)
    return raw_value


def _rowid(definition: TableDefinition[RowT], row: RowT, position: int) -> int:
    value = definition.rowid(row) if definition.rowid is not None else position
    return _validate_rowid(value)


def _estimated_rows(definition: TableDefinition[RowT]) -> int:
    if definition.estimate_rows is not None:
        return _checked_callback_count(definition.estimate_rows, "estimated row count")
    return _DEFAULT_ESTIMATED_ROWS


def _projection(
    definition: TableDefinition[RowT],
    used_columns: set[int],
) -> Projection:
    column_indices = tuple(
        sorted(index for index in used_columns if 0 <= index < len(definition.columns)),
    )
    return Projection(
        tuple(definition.columns[index].name for index in column_indices),
        column_indices,
        -1 in used_columns,
    )


def _column_name(definition: TableDefinition[RowT], index: int) -> str | None:
    if index == -1:
        return None
    try:
        return definition.columns[index].name
    except IndexError as error:
        msg = f"SQLite referred to invalid virtual-table column {index}"
        raise ProtocolError(msg) from error


def _rejection_message(definition: TableDefinition[RowT], target: int) -> str:
    if definition.full_scan_message is not None:
        return definition.full_scan_message
    if 0 <= target < len(definition.constraint_filters):
        specs = definition.constraint_filters[target].specs
        required_specs = tuple(spec for spec in specs if spec.required)
        rendered = ", ".join(f"{spec.column} {spec.op.name}" for spec in required_specs)
        return f"required virtual-table constraints are missing: {rendered}"
    return f"full scan of virtual table {definition.name!r} is not allowed"


def _invoke_source(source: Source[RowT]) -> SourceResult[RowT]:
    if callable(source):
        return source()
    return source


def _invoke_generator_source(
    source: GeneratorSource[RowT],
    context: GeneratorContext,
) -> SourceResult[RowT]:
    if not callable(source):
        return source
    try:
        signature = inspect.signature(source)
    except (TypeError, ValueError):
        msg = "generator source callable must expose an inspectable signature"
        raise ProtocolError(msg) from None
    try:
        signature.bind(context)
    except TypeError:
        try:
            signature.bind()
        except TypeError as error:
            msg = "generator source must accept either no arguments or one GeneratorContext"
            raise ProtocolError(msg) from error
        return cast("SourceFactory[RowT]", source)()
    return cast("ContextSource[RowT]", source)(context)


def _resolve_source(
    source: SourceResult[RowT],
    run_coroutine: Callable[[Awaitable[Any]], Any],
) -> Iterable[RowT] | AsyncIterable[RowT]:
    if inspect.isawaitable(source):
        return cast(
            "Iterable[RowT] | AsyncIterable[RowT]",
            run_coroutine(cast("Awaitable[Any]", source)),
        )
    return source


async def _await_value(awaitable: Awaitable[ResultT]) -> ResultT:
    return await awaitable


def _materialize_snapshot(
    definition: TableDefinition[RowT],
    projection: Projection,
    run_coroutine: Callable[[Awaitable[Any]], Any],
    transaction_state: object = None,
    *,
    use_projection: bool = True,
) -> _Snapshot[RowT]:
    if use_projection and definition.projection_source is not None:
        source = definition.projection_source(projection)
    elif definition.stateful_source is not None:
        source = definition.stateful_source(transaction_state)
    elif definition.source is not None:
        source = _invoke_source(cast("Source[RowT]", definition.source))
    elif definition.kind is TableKind.INDEX:
        count_callback = definition.count_rows
        row_at = definition.row_at
        if count_callback is None or row_at is None:
            msg = "index table has no materialization source"
            raise ProtocolError(msg)
        source = (
            row_at(index) for index in range(_checked_callback_count(count_callback, "row count"))
        )
    else:
        msg = "cached table has no materialization source"
        raise ProtocolError(msg)
    resolved = _resolve_source(source, run_coroutine)
    if isinstance(resolved, AsyncIterable):
        rows = cast("tuple[RowT, ...]", run_coroutine(_collect_async(resolved)))
    else:
        rows = tuple(resolved)
    entries: list[_RowEntry[RowT]] = []
    seen: set[int] = set()
    for position, row in enumerate(rows):
        rowid = _rowid(definition, row, position)
        if rowid in seen:
            msg = f"row source produced duplicate rowid {rowid}"
            raise ProtocolError(msg)
        seen.add(rowid)
        entries.append(_RowEntry(rowid, row))
    return _Snapshot(tuple(entries))


async def _collect_async(source: AsyncIterable[RowT]) -> tuple[RowT, ...]:
    return tuple([row async for row in source])


def _indexed_positions(  # noqa: C901 - exact/range index modes share snapshot setup.
    snapshot: _Snapshot[RowT],
    definition: TableDefinition[RowT],
    plan: _Plan,
    constraints: tuple[Constraint, ...],
) -> tuple[int, ...]:
    index_definition = definition.indexes[plan.target]
    if plan.kind is _PlanKind.INDEX_EQ:
        equality = snapshot.equality.get(plan.target)
        if equality is None:
            building: dict[SQLiteValue, list[int]] = {}
            for position, entry in enumerate(snapshot.entries):
                key = _normalize_sqlite_value(index_definition.key(entry.row))
                building.setdefault(key, []).append(position)
            equality = {key: tuple(value) for key, value in building.items()}
            snapshot.equality[plan.target] = equality
        return equality.get(constraints[0].value, ())
    sorted_positions = snapshot.sorted_positions.get(plan.target)
    if sorted_positions is None:
        sortable = tuple(
            (
                _normalize_sqlite_value(index_definition.key(entry.row)),
                position,
            )
            for position, entry in enumerate(snapshot.entries)
        )
        sorted_positions = tuple(sorted(sortable, key=lambda item: _sqlite_sort_key(item[0])))
        snapshot.sorted_positions[plan.target] = sorted_positions
    keys = tuple(_sqlite_sort_key(item[0]) for item in sorted_positions)
    if any(constraint.value is None for constraint in constraints):
        return ()
    low = bisect_right(keys, _sqlite_sort_key(None))
    high = len(keys)
    for constraint in constraints:
        value = _sqlite_sort_key(constraint.value)
        if constraint.op is ConstraintOp.GE:
            low = max(low, bisect_left(keys, value))
        elif constraint.op is ConstraintOp.GT:
            low = max(low, bisect_right(keys, value))
        elif constraint.op is ConstraintOp.LE:
            high = min(high, bisect_right(keys, value))
        elif constraint.op is ConstraintOp.LT:
            high = min(high, bisect_left(keys, value))
    if low >= high:
        return ()
    return tuple(position for _key, position in sorted_positions[low:high])


def _window(
    entries: Iterable[_RowEntry[RowT]],
    offset: int,
    limit: int | None,
) -> Iterator[_RowEntry[RowT]]:
    iterator = iter(entries)
    for _index in range(offset):
        try:
            next(iterator)
        except StopIteration:
            return
    if limit is None:
        yield from iterator
        return
    for _index in range(limit):
        try:
            yield next(iterator)
        except StopIteration:
            return


def _optional_nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        msg = f"{label} must be a non-negative integer"
        raise ProtocolError(msg)
    return value


def _require_callback_success(
    operation: str,
    *,
    value: object,
) -> None:
    if value is False:
        msg = f"{operation} callback rejected the operation"
        raise ProtocolError(msg)
    if value is not None and value is not True:
        msg = f"{operation} callback must return bool or None"
        raise ProtocolError(msg)


def _reject_awaitable(value: object, message: str) -> None:
    if inspect.isawaitable(value):
        _discard_awaitable(cast("Awaitable[object]", value))
        raise RegistrationError(message)


def _discard_awaitable(value: Awaitable[object]) -> None:
    close = getattr(value, "close", None)
    if close is not None:
        close()


async def _await_maybe(value: ResultT | Awaitable[ResultT]) -> ResultT:
    if inspect.isawaitable(value):
        return await cast("Awaitable[ResultT]", value)
    return value


__all__ = [
    "CachedTableBuilder",
    "CachedTableBuilderStart",
    "Column",
    "ColumnType",
    "Constraint",
    "ConstraintFilter",
    "ConstraintOp",
    "ConstraintSpec",
    "Filter",
    "FilterContext",
    "GeneratorContext",
    "GeneratorTableBuilder",
    "GeneratorTableBuilderStart",
    "Index",
    "OrderTerm",
    "Projection",
    "Registration",
    "TableBuilder",
    "TableDefinition",
    "TableKind",
    "TransactionHooks",
    "async_register_definition",
    "cached_table",
    "generator_table",
    "optional",
    "optional_eq",
    "optional_ge",
    "optional_gt",
    "optional_le",
    "optional_like",
    "optional_lt",
    "register_definition",
    "required",
    "required_eq",
    "required_ge",
    "required_gt",
    "required_le",
    "required_like",
    "required_lt",
    "table",
]
