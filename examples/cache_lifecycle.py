# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Control the lifetime of a shared cached-table snapshot."""

from dataclasses import dataclass
from typing import cast

import libxsql


@dataclass(frozen=True, slots=True)
class Event:
    """One event in the backing store."""

    identifier: int
    label: str


def main() -> None:
    """Reuse a shared snapshot, then invalidate it explicitly."""
    events = [Event(1, "opened"), Event(2, "indexed")]
    build_count = 0

    def build_cache() -> tuple[Event, ...]:
        nonlocal build_count
        build_count += 1
        return tuple(events)

    definition = (
        libxsql.cached_table("events", build_cache)
        .column_int("id", attr="identifier")
        .column_text("label", attr="label")
        .index("id")
        .shared_cache()
        .build()
    )

    with libxsql.connect() as connection:
        registration = connection.register(definition)
        first = cast("int", connection.scalar("SELECT count(*) FROM events"))
        second = cast("int", connection.scalar("SELECT count(*) FROM events"))
        print(f"before: first={first} second={second} builds={build_count}")

        events.append(Event(3, "published"))
        registration.invalidate()
        refreshed = cast("int", connection.scalar("SELECT count(*) FROM events"))
        print(f"after: rows={refreshed} builds={build_count}")


if __name__ == "__main__":
    main()
