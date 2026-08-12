# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Shared public value and result types."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import TypeAlias, overload

SQLiteValue: TypeAlias = int | float | str | bytes | None
"""A native SQLite value returned by libxsql."""

SQLiteBindable: TypeAlias = SQLiteValue | bytearray | memoryview
"""A value accepted for SQLite statement binding."""

SQLiteRow: TypeAlias = tuple[SQLiteValue, ...]
"""One immutable SQLite result row."""

Bindings: TypeAlias = Sequence[SQLiteBindable] | Mapping[str, SQLiteBindable]
"""Positional or named SQLite statement bindings."""

ColumnDescription: TypeAlias = tuple[
    str,
    str | None,
    None,
    None,
    None,
    None,
    None,
]
"""One DB-API-shaped result-column description."""


@dataclass(frozen=True, slots=True)
class QueryResult(Sequence[SQLiteRow]):
    """An immutable materialized SQL result.

    Native SQLite values are retained.  The sequence interface iterates over
    rows, while metadata records timeout and partial-result state.

    Attributes:
        columns: Result column names.
        rows: Materialized result rows.
        elapsed_ms: Wall-clock execution time in milliseconds.
        timed_out: Whether execution reached its deadline.
        partial: Whether ``rows`` is an incomplete prefix.
        warnings: Non-fatal notices associated with the result.
    """

    columns: tuple[str, ...] = ()
    rows: tuple[SQLiteRow, ...] = ()
    elapsed_ms: float = 0.0
    timed_out: bool = False
    partial: bool = False
    warnings: tuple[str, ...] = ()

    def __iter__(self) -> Iterator[SQLiteRow]:
        """Iterate over result rows."""
        return iter(self.rows)

    def __len__(self) -> int:
        """Return the number of materialized rows."""
        return len(self.rows)

    @overload
    def __getitem__(self, index: int) -> SQLiteRow: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[SQLiteRow, ...]: ...

    def __getitem__(self, index: int | slice) -> SQLiteRow | tuple[SQLiteRow, ...]:
        """Return a row or row slice."""
        return self.rows[index]

    @property
    def row_count(self) -> int:
        """Return the number of materialized rows."""
        return len(self.rows)

    def scalar(self, default: SQLiteValue = None) -> SQLiteValue:
        """Return the first cell, or ``default`` for an empty result."""
        if not self.rows or not self.rows[0]:
            return default
        return self.rows[0][0]

    def dictionaries(self) -> tuple[dict[str, SQLiteValue], ...]:
        """Return each row mapped by column name.

        Duplicate column names follow normal ``dict`` behavior: the last value
        wins.  Use :attr:`rows` when duplicate names must be preserved.
        """
        return tuple(dict(zip(self.columns, row, strict=False)) for row in self.rows)


__all__ = [
    "Bindings",
    "ColumnDescription",
    "QueryResult",
    "SQLiteBindable",
    "SQLiteRow",
    "SQLiteValue",
]
