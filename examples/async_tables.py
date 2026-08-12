# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Expose an async iterator as a virtual table under asyncio or Trio."""

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Literal, cast

import anyio
from anyio.lowlevel import checkpoint

import libxsql

Backend = Literal["asyncio", "trio"]


@dataclass(frozen=True, slots=True)
class Record:
    """One asynchronously produced record."""

    identifier: int
    label: str


def selected_backend() -> Backend:
    """Return the backend selected by the example runner."""
    value = os.environ.get("LIBXSQL_EXAMPLE_BACKEND", "asyncio")
    if value not in {"asyncio", "trio"}:
        message = "LIBXSQL_EXAMPLE_BACKEND must be 'asyncio' or 'trio'"
        raise ValueError(message)
    return cast("Backend", value)


async def records(_context: libxsql.GeneratorContext) -> AsyncIterator[Record]:
    """Yield records without blocking the owning event loop."""
    for identifier, label in ((1, "alpha"), (2, "beta"), (3, "gamma")):
        await checkpoint()
        yield Record(identifier, label)


async def main() -> None:
    """Register and query an asynchronous generator table."""
    definition = (
        libxsql.generator_table("records", records)
        .column_int("id", attr="identifier")
        .column_text("label", attr="label")
        .build()
    )
    async with await libxsql.connect_async() as connection:
        await connection.register(definition)
        result = await connection.query(
            "SELECT id, label FROM records ORDER BY id LIMIT 2",
        )
    records_text = ",".join(f"{cast('int', row[0])}:{cast('str', row[1])}" for row in result.rows)
    print("records=" + records_text)


if __name__ == "__main__":
    anyio.run(main, backend=selected_backend())
