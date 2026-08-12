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
