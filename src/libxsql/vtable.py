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
from bisect import bisect_left, bisect_right, insort
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
ProjectionFilterFactory: TypeAlias = Callable[[SQLiteValue, "Projection"], SourceResult[RowT]]
ProjectionConstraintFactory: TypeAlias = Callable[
    [tuple["Constraint", ...], "Projection"],
    SourceResult[RowT],
]
CountCallback: TypeAlias = Callable[[], int]
EstimateCallback: TypeAlias = Callable[[], int]
ModifyHook: TypeAlias = Callable[[str], None]
CommitHook: TypeAlias = Callable[[], None]
TransactionStateFactory: TypeAlias = Callable[[], object]
TransactionPrepareHook: TypeAlias = Callable[[object], None]
TransactionFinalHook: TypeAlias = Callable[[object], None]
TransactionSavepointHook: TypeAlias = Callable[[object, int], None]
RowDeleteCallback: TypeAlias = Callable[[RowT], bool | None]
InsertCallback: TypeAlias = Callable[[Mapping[str, SQLiteValue]], int | bool | None]
UpdateCallback: TypeAlias = Callable[[RowT, Mapping[str, SQLiteValue]], bool | None]
AsyncRunner: TypeAlias = Callable[[Any], Any]
AwaitableScope: TypeAlias = Callable[[Awaitable[object]], Awaitable[object]]
InterruptChecker: TypeAlias = Callable[[], bool]

_MIN_ROWID: Final = -(1 << 63)
_MAX_ROWID: Final = (1 << 63) - 1
_U64_MAX: Final = (1 << 64) - 1
_DEFAULT_ESTIMATED_ROWS: Final = 1_000_000
_DEFAULT_ESTIMATED_COST: Final = 1_000_000.0
# A plan that can only refuse the query must be the one SQLite picks last: in a
# join it competes with plans where another table runs outer and feeds the
# required value in per row. Priced cheap it won, and a valid join failed.
# Alone it is still the only plan, so the refusal stands. (Same as the C++ core.)
_REJECT_COST: Final = 1e9


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


# Operators a cache-backed constraint filter can evaluate against an index key.
_CACHE_FILTER_OPS: Final = frozenset(
    {ConstraintOp.EQ, ConstraintOp.GT, ConstraintOp.GE, ConstraintOp.LT, ConstraintOp.LE},
)
# A LIKE/GLOB pattern starting with one of these gives a filter nothing to seek on.
_LIKE_UNUSABLE_LEADS: Final = frozenset("%_\\")
_PATTERN_OPS: Final = frozenset({ConstraintOp.LIKE, ConstraintOp.GLOB})


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
    """A single-constraint source factory.

    With ``projection`` set, ``factory`` also receives the scan's
    :class:`Projection`, so it can skip columns the query never reads.
    """

    column: str
    op: ConstraintOp
    factory: FilterFactory[RowT] | ProjectionFilterFactory[RowT]
    estimated_cost: float
    estimated_rows: int
    exact: bool
    projection: bool = False


@dataclass(frozen=True, slots=True)
class ConstraintFilter(Generic[RowT]):
    """A multi-constraint source factory.

    With ``projection`` set, ``factory`` also receives the scan's
    :class:`Projection`. A ``factory`` of ``None`` evaluates the matched
    constraints in memory against a cached table's materialized rows, comparing
    each column through its :meth:`CachedTableBuilder.index` key.
    """

    specs: tuple[ConstraintSpec, ...]
    factory: ConstraintFactory[RowT] | ProjectionConstraintFactory[RowT] | None
    estimated_cost: float
    estimated_rows: int
    order_by: tuple[OrderTerm, ...] = ()
    projection: bool = False


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
    delete_row: RowDeleteCallback[RowT] | None = None
    insert_row: InsertCallback | None = None
    update_row: UpdateCallback[RowT] | None = None
    full_scan_message: str | None = None
    shared: bool = False
    counts_as_write: bool = True

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
        self._delete_row: RowDeleteCallback[RowT] | None = None
        self._insert_row: InsertCallback | None = None
        self._update_row: UpdateCallback[RowT] | None = None
        self._full_scan_message: str | None = None
        self._shared = False
        self._counts_as_write = True

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
        """Set a stable rowid getter.

        The rowid is then a KEY, not a scan position: every plan reports it and
        ``WHERE rowid = ?`` matches it. On a cached or generator table it may be
        negative (an address at or above ``2**63``). A cached table with
        :meth:`row_lookup` or :meth:`stateful_row_lookup` must set it, returning
        exactly the key those callbacks resolve; a live table that sets it and
        is updatable or deletable must resolve its keys with a row lookup.
        """
        self._rowid = callback
        return self

    def row_lookup(self, callback: RowLookup[RowT]) -> Self:
        """Resolve a rowid (a KEY, never a position) back to its row for mutation.

        On a cached table this is the SOLE authority for UPDATE, DELETE and
        ``WHERE rowid = ?``: a rowid it returns ``None`` for fails the write with
        ``row <id> not found in <table>`` and matches no row on read, never
        another row by position. A cached table that sets it must also set
        :meth:`rowid`; building or registering one without it raises
        :class:`ConfigurationError`.
        """
        self._row_lookup = callback
        return self

    def stateful_row_lookup(self, callback: StatefulRowLookup[RowT]) -> Self:
        """Like :meth:`row_lookup`, also receiving registration transaction state.

        The same row-identity rule applies: a cached table needs :meth:`rowid`.
        """
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

    def counts_as_write(self, *, enabled: bool = True) -> Self:
        """Choose whether this table's row writes count in ``vtab_write_count``.

        Tables count by default. A session or bookkeeping table (settings,
        checkpoints, previews) passes ``enabled=False`` so writing it does not
        read as a change to the host.
        """
        self._counts_as_write = enabled
        return self

    def insertable(self, callback: InsertCallback) -> Self:
        """Enable insertion from a name-to-value mapping."""
        self._insert_row = callback
        return self

    def updatable(self, callback: UpdateCallback[RowT]) -> Self:
        """Enable preferred atomic row updates.

        ``callback(row, changes)`` receives the row the UPDATE's rowid resolves
        to -- the same row the column setters would receive -- and a mapping of
        only the columns the statement changed. A rowid that resolves to no row
        fails the statement before the callback runs.
        """
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
        return self._append_filter(
            column,
            op,
            factory,
            estimated_cost=estimated_cost,
            estimated_rows=estimated_rows,
            exact=exact,
            projection=False,
        )

    def _append_filter(  # noqa: PLR0913 - one row of the filter table.
        self,
        column: str,
        op: ConstraintOp,
        factory: FilterFactory[RowT] | ProjectionFilterFactory[RowT],
        *,
        estimated_cost: float,
        estimated_rows: int,
        exact: bool | None,
        projection: bool,
    ) -> Self:
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
            Filter(
                column,
                ConstraintOp(op),
                factory,
                estimated_cost,
                estimated_rows,
                exact,
                projection,
            ),
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
        writable = (
            self._delete_row is not None
            or self._update_row is not None
            or any(column.writable for column in self._columns)
        )
        if (
            self._kind is TableKind.GENERATOR
            and writable
            and self._row_lookup is None
            and self._stateful_row_lookup is None
        ):
            msg = "writable generator tables require row_lookup()"
            raise ConfigurationError(msg)
        # A filter factory's rows carry positions in the factory's output, not
        # in the table, so only a rowid() key can name them for a write.
        factory_filters = self._filters or any(
            constraint_filter.factory is not None for constraint_filter in self._constraint_filters
        )
        if factory_filters and self._rowid is None and writable:
            msg = "writable filtered tables require rowid()"
            raise ConfigurationError(msg)
        if self._source is not None and not callable(self._source):
            iterator = iter(self._source)
            if iterator is self._source:
                msg = "a one-shot iterator must be supplied through a factory"
                raise ConfigurationError(msg)

    def _definition(self) -> TableDefinition[RowT]:
        self._validate()
        definition = TableDefinition(
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
            counts_as_write=self._counts_as_write,
        )
        _require_row_identity(definition)
        return definition


class TableBuilder(_BaseTableBuilder[RowT]):
    """Fluent builder for a live sequence/index virtual table."""

    def deletable(self, callback: RowDeleteCallback[RowT]) -> Self:
        """Enable deletion of the row the rowid resolves to.

        Without :meth:`rowid` the rowid is the position the statement scanned
        the row at, resolved through :meth:`row_at` or the row sequence; a
        multi-row DELETE reads it at the position minus the rows the statement
        already deleted before it, so a callback that removes its row from the
        sequence still receives exactly the rows the statement matched. With
        :meth:`rowid`, the rowid resolves through the row lookup. The callback
        receives that row, as the column setters do.
        """
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

    def constraint_filter(
        self,
        specs: Iterable[ConstraintSpec],
        factory: ConstraintFactory[RowT],
        *,
        estimated_cost: float = 1.0,
        estimated_rows: int = 100,
    ) -> Self:
        """Add an exact multi-column/operator pushdown backed by ``factory``.

        Every required constraint must be usable before SQLite selects the
        plan. ``factory`` receives the matched constraints in declaration order
        and must return exactly the rows satisfying all of them. This covers
        relations a single-column filter or :meth:`index` cannot express, such
        as interval containment. Misconfiguration raises immediately instead of
        silently leaving the table without the pushdown.
        """
        normalized = _normalized_constraint_specs(specs, "constraint_filter")
        for spec in normalized:
            self._require_column(spec.column, "constraint_filter")
        self._constraint_filters.append(
            ConstraintFilter(
                specs=normalized,
                factory=factory,
                estimated_cost=_validate_cost(estimated_cost),
                estimated_rows=_validate_count(estimated_rows, "estimated rows"),
            ),
        )
        return self

    def constraint_cache_filter(
        self,
        specs: Iterable[ConstraintSpec],
        *,
        estimated_cost: float = 1.0,
        estimated_rows: int = 100,
    ) -> Self:
        """Add an exact multi-column/operator filter evaluated over the cached rows.

        Every referenced column must also be registered with :meth:`index`,
        whose key supplies the comparison value; only ``=``, ``<``, ``<=``,
        ``>`` and ``>=`` can be evaluated that way. It serves both caching
        models: a :meth:`shared_cache` table evaluates against its shared rows,
        any other table against rows materialized once per scan.
        """
        normalized = _normalized_constraint_specs(specs, "constraint_cache_filter")
        indexed = {index.column for index in self._indexes}
        for spec in normalized:
            self._require_column(spec.column, "constraint_cache_filter")
            if spec.column not in indexed:
                msg = (
                    f"constraint_cache_filter on table {self._name!r}: column "
                    f"{spec.column!r} needs a matching index() to supply its comparison value"
                )
                raise ConfigurationError(msg)
            if spec.op not in _CACHE_FILTER_OPS:
                msg = (
                    f"constraint_cache_filter on table {self._name!r}: {spec.op.name} is not "
                    f"evaluable against an index key (column {spec.column!r})"
                )
                raise ConfigurationError(msg)
        self._constraint_filters.append(
            ConstraintFilter(
                specs=normalized,
                factory=None,
                estimated_cost=_validate_cost(estimated_cost),
                estimated_rows=_validate_count(estimated_rows, "estimated rows"),
            ),
        )
        return self

    def _require_column(self, name: str, api: str) -> None:
        if not any(column.name == name for column in self._columns):
            msg = f"{api} on table {self._name!r}: unknown column {name!r}"
            raise ConfigurationError(msg)

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
        self._constraint_filters.append(
            ConstraintFilter(
                specs=_normalized_constraint_specs(specs, "constraint_filter"),
                factory=factory,
                estimated_cost=_validate_cost(estimated_cost),
                estimated_rows=_validate_count(estimated_rows, "estimated rows"),
            ),
        )
        return self

    def constraint_filter_projection(
        self,
        specs: Iterable[ConstraintSpec],
        factory: ProjectionConstraintFactory[RowT],
        *,
        estimated_cost: float = 1.0,
        estimated_rows: int = 1,
    ) -> Self:
        """Add a multi-constraint plan whose factory also receives the projection.

        Like :meth:`constraint_filter`, but ``factory`` is called as
        ``factory(constraints, projection)``, so a pushdown can skip an
        expensive column the query never reads.
        """
        self._constraint_filters.append(
            ConstraintFilter(
                specs=_normalized_constraint_specs(specs, "constraint_filter_projection"),
                factory=factory,
                estimated_cost=_validate_cost(estimated_cost),
                estimated_rows=_validate_count(estimated_rows, "estimated rows"),
                projection=True,
            ),
        )
        return self

    def filter_eq_projection(
        self,
        column: str,
        factory: ProjectionFilterFactory[RowT],
        estimated_cost: float = 10.0,
        estimated_rows: int = 10,
    ) -> Self:
        """Add an exact equality filter whose factory also receives the projection."""
        return self._append_filter(
            column,
            ConstraintOp.EQ,
            factory,
            estimated_cost=estimated_cost,
            estimated_rows=estimated_rows,
            exact=True,
            projection=True,
        )

    def filter_like_text(
        self,
        column: str,
        factory: Callable[[str, Projection], SourceResult[RowT]],
        estimated_cost: float = 10.0,
        estimated_rows: int = 1000,
    ) -> Self:
        """Add a ``LIKE`` superset filter receiving the raw pattern and the projection.

        The plan is used only when the pattern has a usable literal prefix (a
        leading wildcard falls back to the full scan), and SQLite always
        re-checks the real pattern, so ``factory`` may return a superset.
        """

        def adapt(value: SQLiteValue, projection: Projection) -> SourceResult[RowT]:
            return factory(_sqlite_text(value), projection)

        return self._append_filter(
            column,
            ConstraintOp.LIKE,
            adapt,
            estimated_cost=estimated_cost,
            estimated_rows=estimated_rows,
            exact=False,
            projection=True,
        )

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

    def parametric_filter_projection(
        self,
        columns: Iterable[str],
        factory: Callable[[tuple[SQLiteValue, ...], Projection], SourceResult[RowT]],
        *,
        estimated_cost: float = 1.0,
        estimated_rows: int = 1,
    ) -> Self:
        """Add a :meth:`parametric_filter` whose factory also receives the projection."""
        specs = tuple(required_eq(name) for name in columns)

        def adapt(
            constraints: tuple[Constraint, ...],
            projection: Projection,
        ) -> SourceResult[RowT]:
            return factory(tuple(constraint.value for constraint in constraints), projection)

        return self.constraint_filter_projection(
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
            projection=constraint_filter.projection,
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
        # The rows a positional cached write resolves its position in once the
        # shared snapshot is gone (C++ SharedCache::mutation_snapshot): for a
        # shared table the pre-mutation shared rows, kept by the statement's
        # first write until the next build; for a query-scoped table the rows
        # its first write materialized, dropped by the next scan.
        self.mutation_snapshot: _Snapshot[RowT] | None = None
        self.epoch = 0
        self.table_active = True
        # The owning connection's write counter; registration attaches the
        # connection's shared one.
        self.write_counter = _WriteCounter()

    def invalidate(self) -> None:
        with self.lock:
            self.epoch += 1
            self.shared_snapshot = None
            self.mutation_snapshot = None

    def invalidate_after_mutation(self) -> None:
        """Invalidate after a write, keeping the rows later writes of the statement name."""
        with self.lock:
            self.epoch += 1
            kept = self.shared_snapshot or self.mutation_snapshot
            self.mutation_snapshot = kept if kept is not None and kept.entries else None
            self.shared_snapshot = None

    def drop_statement_snapshot(self) -> None:
        """A new scan of a query-scoped table starts a new statement."""
        if not self.definition.shared:
            with self.lock:
                self.mutation_snapshot = None

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
                self.mutation_snapshot = None
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
        # A positional live table's per-statement snapshot (C++
        # detail::LiveStatementPositions): the scanned positions this
        # statement deleted, ascending. The next scan drops it.
        self._deleted_positions: list[int] = []

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
        _validate_rowid(rowid, key=_write_rowid_is_key(self.definition))
        self._ensure_mutation_snapshot()
        row = self._resolve_row(rowid, update=False)
        self.state.transaction.touch()
        self._before("delete")
        result = _invoke_callback(
            cast("_RegistrationState[object]", self.state),
            callback,
            row,
        )
        _require_callback_success("delete", value=result)
        if _positional_live(self.definition):
            insort(self._deleted_positions, rowid)
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
            values[column.name] = _coerce_written_value(column, value)
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
        keyed = _write_rowid_is_key(self.definition)
        _validate_rowid(rowid, key=keyed)
        _validate_rowid(new_rowid, key=keyed)
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
            changed.append((column, _coerce_written_value(column, value)))
        if not changed:
            return
        atomic = self.definition.update_row
        if atomic is None:
            readonly = [column.name for column, _value in changed if column.set is None]
            if readonly:
                names = ", ".join(readonly)
                msg = f"column(s) {names} are read-only"
                raise ReadOnlyError(msg)
        self._ensure_mutation_snapshot()
        self.state.transaction.touch()
        self._before("update")
        row = self._resolve_row(rowid, update=True)
        if atomic is not None:
            changes = MappingProxyType({column.name: value for column, value in changed})
            result = _invoke_callback(
                cast("_RegistrationState[object]", self.state),
                atomic,
                row,
                changes,
            )
            _require_callback_success("update", value=result)
        else:
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

    def _resolve_row(self, rowid: int, *, update: bool) -> RowT:
        """Turn an UPDATE/DELETE rowid back into the row its callbacks receive.

        The order is C++'s (cached_vtab_update; vtab_update for a live table):
        a table with a row lookup resolves only through it; a cached table
        without one resolves a cache POSITION; a positional live table reads the
        row the statement scanned at that position. Nothing else is guessed.
        """
        definition = self.definition
        if definition.stateful_row_lookup is not None or definition.row_lookup is not None:
            return self._lookup_row(rowid)
        if definition.kind is TableKind.CACHED:
            return self._cached_position_row(rowid, update=update)
        if _positional_live(definition):
            return self._live_position_row(rowid)
        # A keyed live table or a generator without a lookup: the builder and
        # registration refuse both when writable (see _require_row_identity).
        raise ReadOnlyError(_NO_ROW_RESOLVER)

    def _lookup_row(self, rowid: int) -> RowT:
        # A row lookup is the sole authority for a table that has one: every
        # rowid a cached table reports is the key it resolves (see
        # _require_row_identity), so a rowid it cannot resolve is a row that is
        # gone -- never a position to retry.
        if self.definition.stateful_row_lookup is not None:
            row = _invoke_callback(
                cast("_RegistrationState[object]", self.state),
                self.definition.stateful_row_lookup,
                self.state.transaction.state,
                rowid,
            )
            if row is None:
                raise ProtocolError(self._row_not_found(rowid))
            return row
        if self.definition.row_lookup is not None:
            row = _invoke_callback(
                cast("_RegistrationState[object]", self.state),
                self.definition.row_lookup,
                rowid,
            )
            if row is None:
                raise ProtocolError(self._row_not_found(rowid))
            return row
        raise ReadOnlyError(_NO_ROW_RESOLVER)

    def _cached_position_row(self, rowid: int, *, update: bool) -> RowT:
        """Resolve a cache position, mirroring cached_vtab_update's positional branches.

        Only a table without ``rowid()`` reports positions. A shared table finds
        the position in its shared rows, or in the pre-mutation rows an earlier
        write of the statement kept. A query-scoped table finds it in the
        statement's snapshot, else in a fresh build. A position nothing holds
        is no row; an UPDATE with no positional route at all is read-only.
        """
        definition = self.definition
        state = self.state
        if definition.rowid is None:
            with state.lock:
                rows = state.shared_snapshot or state.mutation_snapshot
            if rows is not None and 0 <= rowid < len(rows.entries):
                return rows.entries[rowid].row
            if not definition.shared and _has_cache_builder(definition) and rowid >= 0:
                rebuilt = self._full_rows()
                if rowid < len(rebuilt.entries):
                    return rebuilt.entries[rowid].row
                raise ProtocolError(self._row_not_found(rowid))
        if update:
            raise ReadOnlyError(_NO_ROW_RESOLVER)
        raise ProtocolError(self._row_not_found(rowid))

    def _live_position_row(self, rowid: int) -> RowT:
        """Read the row the statement scanned at a live table's position.

        A delete callback removes its row and moves every later row up one, so
        the row scanned at ``rowid`` now sits at ``rowid`` minus the positions
        this statement already deleted before it (C++
        ``LiveStatementPositions::current_index``). A position the statement
        already deleted names no row.
        """
        deleted = self._deleted_positions
        earlier = bisect_left(deleted, rowid)
        if earlier < len(deleted) and deleted[earlier] == rowid:
            raise ProtocolError(self._row_not_found(rowid))
        index = rowid - earlier
        definition = self.definition
        source = definition.source
        try:
            if source is None:
                row_at = cast("Callable[[int], RowT]", definition.row_at)
                return _invoke_callback(
                    cast("_RegistrationState[object]", self.state),
                    row_at,
                    index,
                )
            if isinstance(source, Sequence):
                sequence = cast("Sequence[RowT]", source)
                return sequence[index]
        except IndexError as error:
            raise ProtocolError(self._row_not_found(rowid)) from error
        rows = self._full_rows()
        if index < len(rows.entries):
            return rows.entries[index].row
        raise ProtocolError(self._row_not_found(rowid))

    def drop_statement_positions(self) -> None:
        """A new scan starts a new statement: forget the last one's deletes."""
        self._deleted_positions.clear()

    def _full_rows(self) -> _Snapshot[RowT]:
        state = self.state
        with state.callback_scope():
            return _materialize_snapshot(
                self.definition,
                _FULL_PROJECTION,
                state.run_coroutine,
                state.transaction.state,
                use_projection=False,
            )

    def _ensure_mutation_snapshot(self) -> None:
        """Snapshot a query-scoped positional table's rows at the statement's first write.

        Mirrors ensure_query_scoped_mutation_snapshot: a multi-row UPDATE/DELETE
        then resolves every position in the rows the statement scanned, not in a
        rebuild of rows an earlier write already shifted. A failed build fails
        the write before any callback runs and leaves nothing to reuse.
        """
        state = self.state
        if not _uses_mutation_snapshot(self.definition):
            return
        with state.lock:
            if state.mutation_snapshot is not None:
                return
        rows = self._full_rows()
        with state.lock:
            state.mutation_snapshot = rows

    def _row_not_found(self, rowid: int) -> str:
        return f"row {rowid} not found in {self.definition.name}"

    def _before(self, operation: str) -> None:
        if self.definition.before_modify is not None:
            _invoke_callback(
                cast("_RegistrationState[object]", self.state),
                self.definition.before_modify,
                operation,
            )

    def _modified(self, operation: str) -> None:
        self.state.transaction.mark_written()
        if self.definition.counts_as_write:
            self.state.write_counter.value += 1
        self.state.invalidate_after_mutation()
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
            elif (info.get_aConstraint_collation(source_index) or "BINARY").upper() == "BINARY":
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
        if constraint_filter is not None and constraint_filter[1] < self._single_column_cost():
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
            return plan, _REJECT_COST, 1, False
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
            return plan, _REJECT_COST, 1, False
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
                # Generator tables reject a scan missing a required input; a
                # cached table can always fall back to its own rows instead.
                if missing_target < 0 and self.definition.kind is not TableKind.CACHED:
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
                if op in _PATTERN_OPS and not _like_constraint_has_usable_prefix(
                    self.info,
                    source_index,
                ):
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

    def _single_column_cost(self) -> float:
        """Cost a constraint filter must beat to be chosen, for every table kind.

        The cheapest usable single-column filter (equality, or a LIKE/GLOB
        pattern with a usable prefix) and, for a shared cache, an equality index
        lookup priced at 1.0. The constraint filter must be strictly cheaper, so
        a single-column filter wins a tie. With neither, any constraint filter
        wins. Parametric filters are constraint filters and compete the same way.
        """
        best = _REJECT_COST
        for filter_definition in self.definition.filters:
            column_index = self.definition.column_index(filter_definition.column)
            for source_index, candidate_column, op in self.usable:
                if candidate_column != column_index or op is not filter_definition.op:
                    continue
                if op is ConstraintOp.EQ or (
                    op in _PATTERN_OPS
                    and _like_constraint_has_usable_prefix(self.info, source_index)
                ):
                    best = min(best, filter_definition.estimated_cost)
        if self.definition.shared:
            indexed = {
                self.definition.column_index(index.column) for index in self.definition.indexes
            }
            if any(
                candidate_column in indexed and op is ConstraintOp.EQ
                for _source, candidate_column, op in self.usable
            ):
                best = min(best, 1.0)
        return best

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
        # Rows a full-scan, index or cache-filter plan materialized for a table
        # without a shared cache: kept for this cursor's lifetime (a join's inner
        # loop filters many times) and dropped when a write invalidates the table.
        self._scoped_snapshot: tuple[int, Projection, _Snapshot[RowT]] | None = None

    def _index_snapshot(self, projection: Projection) -> _Snapshot[RowT]:
        state = self.table.state
        if self.definition.shared:
            return state.snapshot(projection)
        scoped = self._scoped_snapshot
        if scoped is not None and scoped[0] == state.epoch and scoped[1] == projection:
            return scoped[2]
        snapshot = state.snapshot(projection)
        self._scoped_snapshot = (state.epoch, projection, snapshot)
        return snapshot

    def Filter(
        self,
        index_number: int,
        index_name: str | None,
        constraint_args: tuple[apsw.SQLiteValue, ...] | None,
    ) -> None:
        with self.table.state.callback_scope():
            self.table.state.transaction.begin_statement()
            self.table.state.drop_statement_snapshot()
            self.table.drop_statement_positions()
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
            # Exact: `rowid = 1.0` and `rowid = '1'` find row 1, as they do
            # natively; `rowid = 1.5` and `rowid = 'a'` find nothing. The row
            # lookup is the sole authority: the rowid is a key of any sign, and
            # one it cannot resolve is no row, never a cached row by position.
            value = _equality_key(ColumnType.INTEGER, constraints[0].value)
            if not isinstance(value, int):
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
            value = constraints[0].value
            if single_filter.op is ConstraintOp.EQ and single_filter.exact:
                # SQLite trusts an exact equality filter's rows, so the factory
                # sees the value as the column compares it, and is not called
                # for one no row can equal (10.5 on an INTEGER column, NULL).
                column_type = self.definition.columns[plan.constraints[0].column_index].type
                value = _equality_key(column_type, value)
            if value is _NO_MATCH:
                entries = ()
            elif single_filter.projection:
                projection_factory = cast("ProjectionFilterFactory[RowT]", single_filter.factory)
                entries = self._entries_from_source(
                    projection_factory(value, plan.projection),
                )
            else:
                value_factory = cast("FilterFactory[RowT]", single_filter.factory)
                entries = self._entries_from_source(value_factory(value))
        elif plan.kind is _PlanKind.CONSTRAINT_FILTER:
            constraint_filter = self.definition.constraint_filters[plan.target]
            if constraint_filter.factory is None:
                snapshot = self._index_snapshot(plan.projection)
                compared = _with_column_affinity(self.definition, constraints)
                entries = tuple(
                    entry
                    for entry in snapshot.entries
                    if _cache_filter_matches(self.definition, entry.row, compared)
                )
            elif constraint_filter.projection:
                projection_constraints = cast(
                    "ProjectionConstraintFactory[RowT]",
                    constraint_filter.factory,
                )
                entries = self._entries_from_source(
                    projection_constraints(constraints, plan.projection),
                )
            else:
                plain_constraints = cast("ConstraintFactory[RowT]", constraint_filter.factory)
                entries = self._entries_from_source(plain_constraints(constraints))
        elif plan.kind in {_PlanKind.INDEX_EQ, _PlanKind.INDEX_RANGE}:
            snapshot = self._index_snapshot(plan.projection)
            positions = _indexed_positions(
                snapshot,
                self.definition,
                plan,
                _with_column_affinity(self.definition, constraints),
            )
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
            self._count_only_context = None
            self._scoped_snapshot = None
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
            # Cursor-lived like the index/cache-filter plans: a join's inner loop
            # re-filters one cursor per outer row, and rebuilding the rows each
            # time made an N-row join pay N full builds. A write invalidates it.
            return self._index_snapshot(context.projection).entries
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

    def _advance(self) -> None:
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
            _untrack_connection_state(self._state)

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
            _untrack_connection_state(self._state)

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


# Connection-wide invalidation (libxsql's cache_registry). A SQL function that
# changes the engine behind the tables -- renames a symbol, re-runs analysis --
# is not a write through a table and usually holds no Registration, so every
# live registration's state is reachable from its raw APSW connection. Keys and
# members are weak: a closed-and-dropped connection or a released module leaves
# nothing behind, and no entry keeps a connection alive.
_CONNECTION_STATES_LOCK = threading.Lock()
_CONNECTION_STATES: weakref.WeakKeyDictionary[
    apsw.Connection,
    weakref.WeakSet[_RegistrationState[object]],
] = weakref.WeakKeyDictionary()


def _track_connection_state(
    connection: apsw.Connection,
    state: _RegistrationState[object],
) -> None:
    with _CONNECTION_STATES_LOCK:
        states = _CONNECTION_STATES.get(connection)
        if states is None:
            states = weakref.WeakSet()
            _CONNECTION_STATES[connection] = states
        states.add(state)


class _WriteCounter:
    """One connection's count of accepted virtual-table row writes."""

    __slots__ = ("__weakref__", "value")

    def __init__(self) -> None:
        self.value = 0


# Per-connection virtual-table write counters (libxsql's vtab_write_count). Weak
# keys: a closed-and-dropped connection leaves nothing behind.
_WRITE_COUNTERS: weakref.WeakKeyDictionary[apsw.Connection, _WriteCounter] = (
    weakref.WeakKeyDictionary()
)


def _connection_write_counter(connection: apsw.Connection) -> _WriteCounter:
    with _CONNECTION_STATES_LOCK:
        counter = _WRITE_COUNTERS.get(connection)
        if counter is None:
            counter = _WriteCounter()
            _WRITE_COUNTERS[connection] = counter
        return counter


def vtab_write_count(connection: apsw.Connection) -> int:
    """Return the row writes ``connection``'s virtual tables accepted.

    Every INSERT, UPDATE or DELETE row write a table's callback carried out
    counts, on tables that count as writes (see
    :meth:`CachedTableBuilder.counts_as_write`). Temp and ordinary SQLite tables
    never count, unlike ``total_changes``; a refused write does not count; and a
    later ``ROLLBACK`` does not lower it, because SQLite does not undo a host
    write. Compare two readings to ask "did a host-backed table change since X?".
    """
    with _CONNECTION_STATES_LOCK:
        counter = _WRITE_COUNTERS.get(connection)
    return 0 if counter is None else counter.value


def _untrack_connection_state(state: _RegistrationState[object]) -> None:
    with _CONNECTION_STATES_LOCK:
        for connection, states in tuple(_CONNECTION_STATES.items()):
            states.discard(state)
            if not states:
                del _CONNECTION_STATES[connection]


def invalidate_cached_tables(connection: apsw.Connection) -> None:
    """Invalidate every table registered on ``connection``, as a write would.

    Call it from code that mutates the data source behind the tables outside
    SQL -- typically a SQL function that renames, re-analyzes, or redefines
    what the tables read. It reaches every live registration on the raw APSW
    connection, including ones made with :func:`register_definition` whose
    :class:`Registration` handle was dropped. Each table is invalidated exactly
    as a successful write through it would be: a scan already in flight keeps
    the rows it started with, and every later probe or statement rebuilds from
    the live source. The states are collected under the registry lock and
    invalidated outside it. A connection with no registrations is a no-op.
    """
    with _CONNECTION_STATES_LOCK:
        states = tuple(_CONNECTION_STATES.get(connection, ()))
    for state in states:
        state.invalidate()


def register_definition(
    connection: apsw.Connection,
    definition: TableDefinition[RowT],
    *,
    table_name: str | None = None,
    schema: str = "temp",
    interrupt_checker: InterruptChecker | None = None,
) -> Registration:
    """Register and create one virtual table on an APSW connection."""
    _require_row_identity(definition)
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
    _track_connection_state(connection, cast("_RegistrationState[object]", state))
    state.write_counter = _connection_write_counter(connection)
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
    _require_row_identity(definition)
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
    _track_connection_state(connection, cast("_RegistrationState[object]", state))
    state.write_counter = _connection_write_counter(connection)
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


class _NoMatch(Enum):
    """No value of the column's storage class can equal the constraint value."""

    NO_MATCH = "no-match"


_NO_MATCH: Final = _NoMatch.NO_MATCH


class _AffinityProbe:
    """A private SQLite connection that applies column affinity exactly.

    SQLite hands a virtual table the constraint's value without the column's
    affinity, but compares a native column only after applying it: numeric
    text becomes a number for an INTEGER or REAL column (by SQLite's own
    parser: whitespace, signs, exponents, too-large integers become reals,
    hex stays text), and a number becomes its SQLite text for a TEXT column.
    Storing the value into a column of that affinity and reading it back via
    RETURNING is SQLite applying exactly that rule.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._connection: apsw.Connection | None = None

    def apply(self, value: SQLiteValue, *, numeric: bool) -> SQLiteValue:
        statement = _NUMERIC_AFFINITY_SQL if numeric else _TEXT_AFFINITY_SQL
        with self._lock:
            if self._connection is None:
                connection = apsw.Connection(":memory:")
                connection.execute(
                    "CREATE TABLE probe(id INTEGER PRIMARY KEY, number NUMERIC, text TEXT)",
                )
                self._connection = connection
            row = self._connection.execute(statement, (value,)).fetchone()
        return _normalize_sqlite_value(cast("tuple[object]", row)[0])


_NUMERIC_AFFINITY_SQL: Final = (
    "INSERT OR REPLACE INTO probe(id, number) VALUES (1, ?) RETURNING number"
)
_TEXT_AFFINITY_SQL: Final = "INSERT OR REPLACE INTO probe(id, text) VALUES (1, ?) RETURNING text"
_AFFINITY_PROBE: Final = _AffinityProbe()
# The storage classes a value of each column type can hold (libxsql enforces them).
_STORAGE_CLASSES: Final[Mapping[ColumnType, tuple[type[object], ...]]] = MappingProxyType(
    {
        ColumnType.INTEGER: (int,),
        ColumnType.REAL: (int, float),
        ColumnType.TEXT: (str,),
        ColumnType.BLOB: (bytes,),
        ColumnType.ANY: (int, float, str, bytes),
    },
)


def _apply_affinity(column_type: ColumnType, value: SQLiteValue) -> SQLiteValue:
    """``value`` with the affinity SQLite applies when comparing it to the column."""
    if column_type in {ColumnType.INTEGER, ColumnType.REAL} and isinstance(value, str):
        return _AFFINITY_PROBE.apply(value, numeric=True)
    if column_type is ColumnType.TEXT and isinstance(value, (int, float)):
        return _AFFINITY_PROBE.apply(value, numeric=False)
    return value


def _equality_key(column_type: ColumnType, value: SQLiteValue) -> SQLiteValue | _NoMatch:
    """The value an equality lookup on the column should use, or ``_NO_MATCH``.

    libxsql enforces each typed column's storage class, so after affinity only
    a value of that class can equal a row: an integral number within int64 for
    INTEGER (returned as an ``int``), a number for REAL, text for TEXT, bytes
    for BLOB. NULL equals nothing.
    """
    value = _apply_affinity(column_type, value)
    if (
        column_type is ColumnType.INTEGER
        and isinstance(value, float)
        and value.is_integer()
        and _MIN_ROWID <= value <= _MAX_ROWID
    ):
        value = int(value)
    if value is None or not isinstance(value, _STORAGE_CLASSES[column_type]):
        return _NO_MATCH
    return value


def _with_column_affinity(
    definition: TableDefinition[RowT],
    constraints: tuple[Constraint, ...],
) -> tuple[Constraint, ...]:
    """Constraints whose values carry their column's affinity (see ``_apply_affinity``)."""
    return tuple(
        replace(
            constraint,
            value=_apply_affinity(
                definition.columns[constraint.column_index].type,
                constraint.value,
            ),
        )
        for constraint in constraints
    )


def exact_int64(value: SQLiteValue) -> int | None:
    """The int64 an INTEGER column value must equal to match ``value``, or ``None``.

    A constraint or parametric filter factory receives the constraint value
    exactly as SQLite handed it to the virtual table -- no column affinity --
    and its plan is trusted (SQLite does not re-check the rows). Call this
    instead of ``int(value)``: an integer is itself; a real only when it is
    integral and within int64 (``10.0`` -> 10, ``10.5`` -> ``None``); text only
    when SQLite's INTEGER affinity makes it such a number (``'10'``, ``' 10 '``,
    ``'1e1'`` -> 10; ``'0x10'`` and ``'a'`` -> ``None``); NULL and blobs never.
    ``None`` means no row can match, so the factory should yield no rows.
    """
    key = _equality_key(ColumnType.INTEGER, value)
    return key if isinstance(key, int) else None


def exact_text(value: SQLiteValue) -> str | None:
    """The text a TEXT column value must equal to match ``value``, or ``None``.

    Text is itself; a number gets TEXT affinity, which is SQLite's rendering
    of it (``10`` -> ``'10'``, ``10.5`` -> ``'10.5'``); NULL and blobs never
    equal text. ``None`` means no row can match.
    """
    key = _equality_key(ColumnType.TEXT, value)
    return key if isinstance(key, str) else None


@dataclass(frozen=True, slots=True)
class U64Span:
    """An inclusive span of unsigned 64-bit keys, ``lo <= hi``.

    Produced by :meth:`Int64KeyRange.unsigned_spans`.
    """

    lo: int
    hi: int


class Int64KeyRange:
    """The inclusive window of int64 keys satisfying every comparison applied.

    Each comparison is ``key <op> value`` evaluated the way SQLite compares an
    INTEGER column: ``constrain(ConstraintOp.GE, 14.5)`` keeps keys >= 15,
    ``constrain(ConstraintOp.LT, 'a')`` keeps every key (text sorts after all
    numbers), and a NULL operand -- or an EQ value no integer can equal --
    empties the window. A new range holds every int64.

    Use it in a constraint-filter factory whose GT/GE/LT/LE/EQ constraints are
    on one integer key column::

        keys = Int64KeyRange()
        for constraint in constraints:
            keys.constrain(constraint.op, constraint.value)
        if keys.empty:
            return ()
        return scan(keys.lower, keys.upper)  # both inclusive
    """

    __slots__ = ("_empty", "_lower", "_upper")

    def __init__(self) -> None:
        """Start with every int64 key."""
        self._lower = _MIN_ROWID
        self._upper = _MAX_ROWID
        self._empty = False

    def constrain(self, op: ConstraintOp, value: SQLiteValue) -> bool:
        """Narrow the window to keys satisfying ``key <op> value``.

        ``value`` is the raw constraint value, or an ``int`` the caller
        resolved itself (a symbol name parsed to an address, say). Returns ``False`` once the
        window is empty. LIKE (or any operator other than EQ, GT, GE, LT and
        LE) is not an integer key comparison: the window empties and
        :class:`~libxsql.ConfigurationError` is raised, so a mis-routed
        constraint fails the query instead of matching.
        """
        if op not in _CACHE_FILTER_OPS:
            self._empty = True
            msg = f"{op.name} is not an integer key comparison"
            raise ConfigurationError(msg)
        if op is ConstraintOp.EQ:
            key = exact_int64(value)
            if key is None:
                self._empty = True
                return False
            self._narrow(key, key)
        else:
            operand = _apply_affinity(ColumnType.INTEGER, value)
            if op in {ConstraintOp.GT, ConstraintOp.GE}:
                bound = _lower_key_bound(operand, strict=op is ConstraintOp.GT)
                if bound is not None:
                    self._narrow(bound, _MAX_ROWID)
            else:
                bound = _upper_key_bound(operand, strict=op is ConstraintOp.LT)
                if bound is not None:
                    self._narrow(_MIN_ROWID, bound)
        return not self._empty

    @property
    def empty(self) -> bool:
        """Whether no key satisfies the comparisons applied so far."""
        return self._empty

    @property
    def lower(self) -> int:
        """The smallest key in the window (the int64 minimum when unbounded below)."""
        return self._lower

    @property
    def upper(self) -> int:
        """The largest key in the window (the int64 maximum when unbounded above)."""
        return self._upper

    def contains(self, key: int) -> bool:
        """Whether ``key`` satisfies every comparison applied so far."""
        return not self._empty and self._lower <= key <= self._upper

    def unsigned_spans(self, *, descending: bool = False) -> list[U64Span]:
        """The window as unsigned 64-bit spans, in the order SQL sorts the keys.

        For tables whose key is really an unsigned 64-bit value (an address)
        stored as a bit-cast int64. Keys >= 2**63 reach SQL as negative
        integers, so SQL order is the high half first (most negative first),
        then 0, then the low half, while the engine walks unsigned. A walker
        that claims ``ORDER BY key``, or omits the constraint, must visit the
        spans in the order returned here:

        ================  ==========================================
        window            spans
        ================  ==========================================
        empty             none
        ``lo, hi >= 0``   ``[lo, hi]``
        ``lo, hi < 0``    ``[u(lo), u(hi)]``
        ``lo < 0 <= hi``  ``[u(lo), 2**64 - 1]``, then ``[0, hi]``
        ================  ==========================================

        where ``u()`` is the bit-cast to uint64. ``descending`` reverses the
        order of the spans; each span stays ``lo <= hi``, and the walker decides
        how to walk it (downward, for a descending walk). A span only says what
        SQL order requires: it does not clamp to anything the engine can
        address.

        Gotcha for anyone writing such queries: SQLite reads a decimal literal
        >= 2**63 (``18446744073709551613``) as a REAL, which no INTEGER key
        equals and which bounds the window past every key. Write a hex literal
        (``0xFFFFFFFFFFFFFFFD``, read as 64-bit two's complement) or the
        negative decimal (``-3``). And ``key >= 0xFFFFF80000000000`` also
        includes every low-half key; ``key BETWEEN 0xFFFFF80000000000 AND -1``
        is the high half.
        """
        if self._empty or self._lower > self._upper:
            return []
        lo = self._lower & _U64_MAX
        hi = self._upper & _U64_MAX
        if self._lower < 0 <= self._upper:
            spans = [U64Span(lo, _U64_MAX), U64Span(0, hi)]
        else:
            spans = [U64Span(lo, hi)]
        if descending:
            spans.reverse()
        return spans

    def _narrow(self, lower: int | _NoKeyBound, upper: int | _NoKeyBound) -> None:
        if lower is _EMPTY_BOUND or upper is _EMPTY_BOUND:
            self._empty = True
            return
        self._lower = max(self._lower, lower)
        self._upper = min(self._upper, upper)
        if self._lower > self._upper:
            self._empty = True


class _NoKeyBound(Enum):
    """A range bound no int64 key can satisfy."""

    EMPTY = "empty"


_EMPTY_BOUND: Final = _NoKeyBound.EMPTY


def _lower_key_bound(operand: SQLiteValue, *, strict: bool) -> int | _NoKeyBound | None:
    """The inclusive int64 lower bound meaning ``key > operand`` (or ``>=``).

    ``operand`` already carries INTEGER affinity. Returns ``_EMPTY_BOUND`` when
    no key qualifies and ``None`` when every key does.
    """
    if operand is None or isinstance(operand, (str, bytes)):
        # NULL compares with nothing; text and blobs sort after every number.
        return _EMPTY_BOUND
    if isinstance(operand, float):
        if math.isnan(operand) or operand == math.inf:
            return _EMPTY_BOUND
        if operand == -math.inf:
            return None
        # key > r <=> key >= floor(r) + 1;  key >= r <=> key >= ceil(r).
        bound = math.floor(operand) + 1 if strict else math.ceil(operand)
    else:
        bound = operand + 1 if strict else operand
    if bound > _MAX_ROWID:
        return _EMPTY_BOUND
    return None if bound <= _MIN_ROWID else bound


def _upper_key_bound(operand: SQLiteValue, *, strict: bool) -> int | _NoKeyBound | None:
    """The inclusive int64 upper bound meaning ``key < operand`` (or ``<=``).

    ``operand`` already carries INTEGER affinity. Returns ``_EMPTY_BOUND`` when
    no key qualifies and ``None`` when every key does.
    """
    if operand is None:
        return _EMPTY_BOUND
    if isinstance(operand, (str, bytes)):
        return None
    if isinstance(operand, float):
        if math.isnan(operand) or operand == -math.inf:
            return _EMPTY_BOUND
        if operand == math.inf:
            return None
        # key < r <=> key <= ceil(r) - 1;  key <= r <=> key <= floor(r).
        bound = math.ceil(operand) - 1 if strict else math.floor(operand)
    else:
        bound = operand - 1 if strict else operand
    if bound < _MIN_ROWID:
        return _EMPTY_BOUND
    return None if bound >= _MAX_ROWID else bound


def _normalized_constraint_specs(
    specs: Iterable[ConstraintSpec],
    api: str,
) -> tuple[ConstraintSpec, ...]:
    normalized = tuple(specs)
    if not normalized:
        msg = f"{api} requires at least one constraint"
        raise ConfigurationError(msg)
    if len({(spec.column, spec.op) for spec in normalized}) != len(normalized):
        msg = f"{api} contains duplicate column/operator pairs"
        raise ConfigurationError(msg)
    return normalized


def _sqlite_text(value: SQLiteValue) -> str:
    """Return SQLite's text form of a pattern argument (NULL becomes empty)."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _like_constraint_has_usable_prefix(info: apsw.IndexInfo, source_index: int) -> bool:
    """Whether a LIKE/GLOB pattern has a literal prefix a filter can seek on.

    Unknown at plan time (a bound parameter) counts as usable: the filter stays
    a superset that SQLite re-checks, so the worst case is a full walk. APSW
    reports SQL NULL the same way as unknown, which is safe for the same reason.
    """
    rhs = info.get_aConstraint_rhs(source_index)
    if rhs is None:
        return True
    text = _sqlite_text(_normalize_sqlite_value(rhs))
    return bool(text) and text[0] not in _LIKE_UNUSABLE_LEADS


def _cache_filter_matches(
    definition: TableDefinition[RowT],
    row: RowT,
    constraints: tuple[Constraint, ...],
) -> bool:
    """Evaluate a cache-backed constraint filter against one materialized row."""
    for constraint in constraints:
        if constraint.value is None:
            return False
        index_definition = next(
            index for index in definition.indexes if index.column == constraint.column
        )
        key = _normalize_sqlite_value(index_definition.key(row))
        if key is None:
            return False
        left = _sqlite_sort_key(key)
        right = _sqlite_sort_key(constraint.value)
        if not _compare_sort_keys(constraint.op, left, right):
            return False
    return True


def _compare_sort_keys(
    op: ConstraintOp,
    left: tuple[int, int | float | str | bytes],
    right: tuple[int, int | float | str | bytes],
) -> bool:
    """Apply one of the :data:`_CACHE_FILTER_OPS` in SQLite's value ordering."""
    if left[0] != right[0]:
        # Different storage classes: SQLite orders NULL < numeric < text < blob.
        order = (left[0] > right[0]) - (left[0] < right[0])
    else:
        order = cast("int", (left[1] > right[1]) - (left[1] < right[1]))  # type: ignore[operator]
    if op is ConstraintOp.EQ:
        return order == 0
    if op is ConstraintOp.GT:
        return order > 0
    if op is ConstraintOp.GE:
        return order >= 0
    if op is ConstraintOp.LT:
        return order < 0
    return order <= 0


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


# Why a value written to an INTEGER column equals no integer, by storage class.
_INTEGER_REFUSALS: Final[Mapping[type[object], str]] = MappingProxyType(
    {
        type(None): "NULL",
        bytes: "a blob",
        float: "a non-integral or out-of-range real",
        str: "non-integer text",
    },
)


def _coerce_written_value(column: Column[RowT], value: object) -> SQLiteValue:
    """The value an INSERT or UPDATE writes to ``column``, as its callback sees it.

    SQLite applies no column affinity to the values a virtual table's write
    receives, so ``16.0`` arrives as a real and ``'16'`` as text. An INTEGER
    column takes the integer the value equals under SQLite's INTEGER
    comparison (:func:`exact_int64`: ``16``, ``16.0``, ``'16'``, ``' 16 '``
    and ``'1e1'`` are integers) and refuses anything else -- a non-integral or
    out-of-range real, non-integer text, a blob, and NULL unless the column is
    nullable -- with ``column "<name>": expected an integer, got ...``. Other
    column types keep :func:`_coerce_column_value`.
    """
    if column.type is not ColumnType.INTEGER:
        return _coerce_column_value(column, value)
    normalized = _normalize_sqlite_value(value)
    if normalized is None and (column.nullable or column.hidden):
        return None
    # Typed writes reject incomplete numeric strings, independently of the
    # linked SQLite version's comparison-affinity treatment of embedded NUL.
    key = None if isinstance(normalized, str) and "\0" in normalized else exact_int64(normalized)
    if key is None:
        got = _INTEGER_REFUSALS[type(normalized)]
        msg = f'column "{column.name}": expected an integer, got {got}'
        raise ProtocolError(msg)
    return key


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


def _validate_rowid(value: int, *, key: bool = False) -> int:
    """Check a rowid; only a KEY (see :func:`_keyed_rowids`) may be negative."""
    raw_value = cast("object", value)
    if isinstance(raw_value, bool) or not isinstance(raw_value, int):
        msg = "rowid must be an integer"
        raise ProtocolError(msg)
    if not _MIN_ROWID <= raw_value <= _MAX_ROWID:
        msg = f"rowid {raw_value} is outside SQLite's signed 64-bit range"
        raise ProtocolError(msg)
    if raw_value < 0 and not key:
        msg = f"negative rowid {raw_value} is not accepted by libxsql table adapters"
        raise ProtocolError(msg)
    return raw_value


def _keyed_rowids(definition: TableDefinition[RowT]) -> bool:
    """Whether the table's rowids are keys that may be negative.

    A cached or generator table with ``rowid()`` reports ``rowid(row)`` from
    every plan, and ``row_lookup()`` resolves that key; a key may be negative
    (an address at or above ``2**63``). A live table's rowid stays a
    non-negative position or key, like the C++ live table's.
    """
    return definition.kind is not TableKind.INDEX and definition.rowid is not None


_NO_ROW_RESOLVER: Final = "row_lookup() is required to reconstruct this row for mutation"
# Positional writes materialize complete rows: a projection would change nothing
# a position names, and the full source is what a shared cache is built from.
_FULL_PROJECTION: Final = Projection((), ())


def _write_rowid_is_key(definition: TableDefinition[RowT]) -> bool:
    """Whether an UPDATE/DELETE rowid may be negative.

    A cached table takes a rowid of any sign, as the C++ cached xUpdate does: a
    key may be negative, and a negative position simply names no row. A live
    table refuses one before any callback, like the C++ live xUpdate.
    """
    return definition.kind is TableKind.CACHED or _keyed_rowids(definition)


def _has_cache_builder(definition: TableDefinition[RowT]) -> bool:
    """A full row source, as C++ ``has_cache_builder()`` (a projection builder is not one)."""
    return definition.source is not None or definition.stateful_source is not None


def _positional_live(definition: TableDefinition[RowT]) -> bool:
    """Whether a live table's rowid is a position: no ``rowid()`` and no row lookup."""
    return (
        definition.kind is TableKind.INDEX
        and definition.rowid is None
        and definition.row_lookup is None
        and definition.stateful_row_lookup is None
    )


def _uses_mutation_snapshot(definition: TableDefinition[RowT]) -> bool:
    """Whether writes snapshot the rows once per statement (query_scoped_uses_mutation_snapshot).

    Only a query-scoped cached table whose rowids are positions: no row lookup,
    no ``rowid()``, no single-column filter, and a full source to build from.
    """
    return (
        definition.kind is TableKind.CACHED
        and not definition.shared
        and _has_cache_builder(definition)
        and definition.row_lookup is None
        and definition.stateful_row_lookup is None
        and definition.rowid is None
        and not definition.filters
    )


def _require_row_identity(definition: TableDefinition[RowT]) -> None:
    """Reject a table that could read a position as a key, or a key as a position.

    A cached table's row lookup is the only thing UPDATE, DELETE and
    ``WHERE rowid = ?`` use to turn a rowid back into a row, so every rowid the
    table reports must be the key it resolves. Without ``rowid()`` a scan reports
    a POSITION, and a position read as a key names a different, existing row.
    Conversely a writable live table with ``rowid()`` reports keys, which
    ``row_at()`` or its sequence would read as positions, so it must resolve
    them with a row lookup. Checked when building and again when registering a
    hand-made definition.
    """
    has_lookup = definition.row_lookup is not None or definition.stateful_row_lookup is not None
    if (
        definition.kind is TableKind.INDEX
        and definition.rowid is not None
        and not has_lookup
        and (
            definition.delete_row is not None
            or definition.update_row is not None
            or any(column.writable for column in definition.columns)
        )
    ):
        msg = (
            f"live table {definition.name!r}: a writable table with rowid() requires "
            "row_lookup(): its rowids are keys, not the positions row_at() and the "
            "row sequence resolve"
        )
        raise ConfigurationError(msg)
    if definition.kind is not TableKind.CACHED or definition.rowid is not None:
        return
    if definition.row_lookup is not None:
        api = "row_lookup()"
    elif definition.stateful_row_lookup is not None:
        api = "stateful_row_lookup()"
    else:
        return
    msg = (
        f"cached table {definition.name!r}: {api} requires rowid(): scans must "
        f"report the same key {api} resolves, not a cache position"
    )
    raise ConfigurationError(msg)


def _rowid(definition: TableDefinition[RowT], row: RowT, position: int) -> int:
    value = definition.rowid(row) if definition.rowid is not None else position
    return _validate_rowid(value, key=_keyed_rowids(definition))


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
        lookup = constraints[0].value
        return () if lookup is None else equality.get(lookup, ())
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
    "Int64KeyRange",
    "OrderTerm",
    "Projection",
    "Registration",
    "TableBuilder",
    "TableDefinition",
    "TableKind",
    "TransactionHooks",
    "U64Span",
    "async_register_definition",
    "cached_table",
    "exact_int64",
    "exact_text",
    "generator_table",
    "invalidate_cached_tables",
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
    "vtab_write_count",
]
