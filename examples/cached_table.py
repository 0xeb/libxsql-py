# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Materialize cross-references and provide an optimized equality filter."""

from dataclasses import dataclass

import libxsql


@dataclass(frozen=True, slots=True)
class Xref:
    """One directed cross-reference."""

    source: int
    target: int
    kind: int


def main() -> None:
    """Query a cached source through both scan and filter plans."""
    cross_references = (
        Xref(0x1000, 0x2000, 1),
        Xref(0x1004, 0x2000, 1),
        Xref(0x1008, 0x3000, 2),
        Xref(0x100C, 0x2000, 1),
        Xref(0x2000, 0x3000, 1),
        Xref(0x2004, 0x4000, 2),
        Xref(0x3000, 0x4000, 1),
    )

    def references_to(target: libxsql.SQLiteValue) -> tuple[Xref, ...]:
        return tuple(reference for reference in cross_references if reference.target == target)

    definition = (
        libxsql.cached_table("xrefs", lambda: cross_references)
        .column_int64("from_ea", attr="source")
        .column_int64("to_ea", attr="target")
        .column_int("kind", attr="kind")
        .filter_eq("to_ea", references_to, estimated_rows=3)
        .build()
    )

    with libxsql.connect() as connection:
        connection.register(definition)
        full_scan = connection.query(
            "SELECT printf('0x%X', from_ea), printf('0x%X', to_ea) FROM xrefs ORDER BY rowid",
        )
        filtered = connection.query(
            """
            SELECT printf('0x%X', from_ea)
            FROM xrefs
            WHERE to_ea = 0x2000
            ORDER BY from_ea
            """,
        )

    print(f"full-scan={len(full_scan.rows)}")
    print("to-0x2000=" + ",".join(str(row[0]) for row in filtered.rows))


if __name__ == "__main__":
    main()
