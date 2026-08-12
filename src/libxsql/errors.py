# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Public exception hierarchy for :mod:`libxsql`."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .types import QueryResult


class LibxsqlError(Exception):
    """Base class for errors raised by libxsql."""


class ConfigurationError(LibxsqlError, ValueError):
    """A definition or connection option is invalid."""


class ClosedError(LibxsqlError):
    """An operation requires an open connection, cursor, or registration."""


class ThreadingError(LibxsqlError, RuntimeError):
    """A synchronous object was used from a thread that does not own it."""


class ReentrancyError(LibxsqlError, RuntimeError):
    """A callback attempted to re-enter its active connection."""


class RegistrationError(LibxsqlError):
    """A virtual table could not be registered or unregistered."""


class ReadOnlyError(LibxsqlError):
    """A write was attempted through a read-only definition."""


class FullScanError(LibxsqlError):
    """The planner rejected a query that required a forbidden full scan."""


class QueryTimeoutError(LibxsqlError, TimeoutError):
    """SQLite execution exceeded its configured deadline."""

    def __init__(
        self,
        message: str = "query timed out",
        *,
        elapsed_ms: float | None = None,
        result_columns: Iterable[str] = (),
        readonly: bool = True,
    ) -> None:
        """Initialize a timeout error.

        Args:
            message: Human-readable timeout description.
            elapsed_ms: Observed wall-clock execution time, when known.
            result_columns: Prepared result columns, when execution was interrupted.
            readonly: Whether SQLite classified the interrupted statement as read-only.
        """
        super().__init__(message)
        self.elapsed_ms = elapsed_ms
        self.result_columns = tuple(result_columns)
        self.readonly = readonly


class QueryCancelledError(LibxsqlError):
    """SQLite execution was cancelled cooperatively or interrupted."""

    def __init__(
        self,
        message: str = "query cancelled",
        *,
        result_columns: Iterable[str] = (),
        readonly: bool = True,
    ) -> None:
        """Initialize a cancellation error with optional prepared result columns."""
        super().__init__(message)
        self.result_columns = tuple(result_columns)
        self.readonly = readonly


class PartialQueryError(LibxsqlError):
    """A read-only query failed after producing a usable row prefix."""

    def __init__(self, message: str, *, result: QueryResult) -> None:
        """Initialize the error with its immutable partial query result."""
        super().__init__(message)
        self.result = result


class PartialUpdateError(LibxsqlError):
    """A non-atomic column update failed after applying earlier columns."""

    def __init__(
        self,
        message: str,
        *,
        applied_columns: Iterable[str] = (),
    ) -> None:
        """Initialize a partial update error.

        Args:
            message: Human-readable failure description.
            applied_columns: Columns successfully applied before failure.
        """
        self.applied_columns = tuple(applied_columns)
        if self.applied_columns:
            rendered = ", ".join(self.applied_columns)
            message = f"{message} (already applied: {rendered})"
        super().__init__(message)


class ProtocolError(LibxsqlError):
    """An HTTP or callback protocol contract was violated."""


class UnsupportedRuntimeError(LibxsqlError, RuntimeError):
    """The active Python runtime cannot safely host libxsql."""


__all__ = [
    "ClosedError",
    "ConfigurationError",
    "FullScanError",
    "LibxsqlError",
    "PartialQueryError",
    "PartialUpdateError",
    "ProtocolError",
    "QueryCancelledError",
    "QueryTimeoutError",
    "ReadOnlyError",
    "ReentrancyError",
    "RegistrationError",
    "ThreadingError",
    "UnsupportedRuntimeError",
]
