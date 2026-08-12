# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Canonical SQL capability discovery table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .errors import ConfigurationError
from .vtable import cached_table

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .vtable import TableDefinition


@dataclass(frozen=True, slots=True)
class SqlCapability:
    """One stable consumer-facing capability declaration."""

    name: str
    is_supported: bool
    notes: str


def define_sql_capabilities(
    capabilities: Iterable[SqlCapability],
) -> TableDefinition[SqlCapability]:
    """Build ``sql_capabilities(name, is_supported, notes)``.

    Rows are copied and sorted by name. Empty or duplicate names fail while
    building the definition so SQL consumers never see an ambiguous surface.
    """
    rows = tuple(sorted(capabilities, key=lambda capability: capability.name))
    for index, capability in enumerate(rows):
        if not capability.name:
            message = "sql_capabilities contains an empty capability name"
            raise ConfigurationError(message)
        if index and rows[index - 1].name == capability.name:
            message = f"sql_capabilities contains duplicate capability {capability.name!r}"
            raise ConfigurationError(message)
    return (
        cached_table("sql_capabilities", lambda: rows)
        .count(lambda: len(rows))
        .estimate_rows(lambda: len(rows))
        .column_text("name", lambda row: row.name)
        .column_int("is_supported", lambda row: int(row.is_supported))
        .column_text("notes", lambda row: row.notes)
        .build()
    )


__all__ = ["SqlCapability", "define_sql_capabilities"]
