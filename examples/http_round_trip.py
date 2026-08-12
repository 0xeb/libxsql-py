# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Execute a query through the loopback HTTP transport."""

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


def execute_query(
    sql: str,
    _options: libxsql.ScriptOptions,
) -> libxsql.QueryResult:
    """Serve the one deterministic query used by this example."""
    if sql.strip() != "SELECT 40 + 2 AS answer":
        message = "unexpected example query"
        raise ValueError(message)
    return libxsql.QueryResult(columns=("answer",), rows=((42,),))


async def main() -> None:
    """Start a loopback server, query it, and stop it normally."""
    config = libxsql.HttpQueryServerConfig(
        bind_address="127.0.0.1",
        port=0,
        query_handler=execute_query,
    )
    async with libxsql.AsyncHttpQueryServer(config) as server:
        client_config = libxsql.ClientConfig(port=server.port)
        async with libxsql.AsyncThinClient(client_config) as client:
            payload = await client.query_json("SELECT 40 + 2 AS answer")
            reachable = await client.ping()

    if not isinstance(payload, dict):
        message = "query response must be a JSON object"
        raise TypeError(message)
    document = cast("dict[str, object]", payload)
    rows = cast("list[list[object]]", document["rows"])
    print(f"answer={rows[0][0]}")
    print(f"status={'ready' if reachable else 'unreachable'}")


if __name__ == "__main__":
    anyio.run(main, backend=selected_backend())
