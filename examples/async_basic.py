# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Run a basic query with the selected AnyIO backend."""

import os
from typing import Literal, cast

import anyio

import libxsql

Backend = Literal["asyncio", "trio"]


def selected_backend() -> Backend:
    """Return the backend selected by the example runner."""
    value = os.environ.get("LIBXSQL_EXAMPLE_BACKEND", "asyncio")
    if value not in {"asyncio", "trio"}:
        message = "LIBXSQL_EXAMPLE_BACKEND must be 'asyncio' or 'trio'"
        raise ValueError(message)
    return cast("Backend", value)


async def main() -> None:
    """Run the asynchronous example."""
    async with await libxsql.connect_async() as connection:
        result = await connection.query("SELECT 40 + 2 AS answer")
    print(f"answer={cast('int', result.scalar())}")


if __name__ == "__main__":
    anyio.run(main, backend=selected_backend())
