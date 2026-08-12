# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Stream a bounded series selected through a hidden SQL input."""

from collections.abc import Iterator
from dataclasses import dataclass

import libxsql


@dataclass(frozen=True, slots=True)
class SeriesValue:
    """One generated integer."""

    value: int


def main() -> None:
    """Generate only the rows consumed by SQLite's LIMIT."""
    produced = 0

    def full_scan() -> tuple[SeriesValue, ...]:
        return ()

    def generate(values: tuple[libxsql.SQLiteValue, ...]) -> Iterator[SeriesValue]:
        nonlocal produced
        start = values[0]
        if not isinstance(start, int):
            message = "start must be an integer"
            raise TypeError(message)
        current = start
        while True:
            produced += 1
            yield SeriesValue(current)
            current += 1

    definition = (
        libxsql.generator_table("series", full_scan)
        .column_int("value", attr="value")
        .hidden_column_int("start")
        .parametric_filter(("start",), generate, estimated_rows=10)
        .build()
    )

    with libxsql.connect() as connection:
        connection.register(definition)
        result = connection.query(
            "SELECT value FROM series WHERE start = ? LIMIT 4",
            (7,),
        )

    print("values=" + ",".join(str(row[0]) for row in result.rows))
    print(f"produced={produced}")


if __name__ == "__main__":
    main()
