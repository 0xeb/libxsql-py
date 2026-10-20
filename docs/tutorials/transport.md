# Loopback HTTP transport

The optional transport layer combines a framework-neutral ASGI application,
Hypercorn serving, and HTTPX clients. Bind to loopback unless authentication
and deployment-specific network controls are configured.

The program below requests an operating-system-selected port, performs a real
HTTP query, checks server status, and closes both client and server. It never
prints the random port, so its output stays reproducible.

--8<-- "examples/http_round_trip.py"

With authentication configured, `GET /status`, `POST /query`, `POST /cancel`,
and `POST /shutdown` require the configured bearer or X-XSQL token by default.
Set `status_requires_auth=False` only for an intentionally public health probe.
`ThinClient.cancel()` and `AsyncThinClient.cancel()` request cooperative
cancellation of the active query.

`HttpQueryServerConfig` mirrors the C++ server's options. A
`streaming_executor` answers requests that carry `X-XSQL-Stream: 1` (JSON
envelope) or `X-XSQL-Stream: ndjson` row by row, and a stream the requested
`?format=` cannot produce is rejected with HTTP 400. `X-XSQL-Timeout` sets a
per-request timeout in milliseconds. Responses are gzip-compressed when the
client sends `Accept-Encoding: gzip`, and `keep_alive_max_count` (default 1000)
bounds the requests served on one connection. A `cancel_handler` runs after
`POST /cancel` cancels local work; if it raises, the route answers 502.
