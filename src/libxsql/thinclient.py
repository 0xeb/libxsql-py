# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
#
# This file is licensed under the Human-Origin Source License v1.0.
# See LICENSE.

"""ASGI query service and HTTP clients for libxsql.

The ASGI application is dependency-light and can be mounted in any conforming
server.  The convenience server uses Hypercorn, while the clients use HTTPX.
Those packages are imported lazily and live in the ``thinclient`` extra.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import importlib
import inspect
import ipaddress
import json
import queue
import random
import re
import socket
import threading
import time
import zlib
from collections.abc import Awaitable, Callable, Generator, Mapping, MutableMapping
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Protocol, Self, TypeAlias, cast
from urllib.parse import parse_qs, urlencode

import anyio
from anyio import from_thread, to_thread

from .errors import ConfigurationError, ProtocolError
from .script import (
    ScriptOptions,
    ScriptResult,
    script_result_to_csv,
    script_result_to_json,
    script_result_to_jsonl,
    script_result_to_text,
    script_result_to_tsv,
)
from .types import QueryResult

if TYPE_CHECKING:
    from types import TracebackType

    from anyio.abc import TaskGroup

    from .connection import AsyncConnection, Connection

    AsgiScope: TypeAlias = MutableMapping[str, Any]
    AsgiMessage: TypeAlias = MutableMapping[str, Any]
    AsgiReceive: TypeAlias = Callable[[], Awaitable[AsgiMessage]]
    AsgiSend: TypeAlias = Callable[[AsgiMessage], Awaitable[None]]


QueryHandler = Callable[[str, ScriptOptions], object]
StatusHandler = Callable[[], object]
ExtraRouteHandler = Callable[["HttpRequest"], object]
CancelHandler = Callable[[], object]
StreamSink = Callable[[str | bytes], bool]
AsyncStreamSink = Callable[[str | bytes], Awaitable[bool]]
StreamingExecutor = Callable[..., object]
"""``executor(sql, options, ndjson, sink)`` for true chunked ``/query`` responses.

A synchronous executor runs on a worker thread and gets a :data:`StreamSink`; a
coroutine function gets an :data:`AsyncStreamSink`. Either sink returns
``False`` once the client has gone, after which the executor should stop.
"""
_MAX_PORT = 65_535
_HTTP_OK = 200
_HTTP_ERROR = 400
_STREAM_FLUSH_BYTES = 64 * 1024
_LEADING_INTEGER = re.compile(r"\s*([+-]?\d+)")
# Media types gzip applies to, decided on the type alone (parameters ignored).
_COMPRESSIBLE_TYPES = frozenset(
    {
        "image/svg+xml",
        "application/javascript",
        "application/json",
        "application/xml",
        "application/protobuf",
        "application/x-ndjson",
        "application/xhtml+xml",
    },
)


class _Compressor(Protocol):
    def compress(self, data: bytes, /) -> bytes:
        """Compress ``data``, returning whatever output is ready."""
        ...

    def flush(self) -> bytes:
        """Finish the stream and return the remaining output."""
        ...


class _HypercornServe(Protocol):
    def __call__(
        self,
        app: object,
        config: object,
        *,
        shutdown_trigger: Callable[[], Awaitable[None]],
    ) -> Awaitable[None]:
        """Serve one ASGI application until the trigger completes."""
        ...


def _empty_headers() -> Mapping[str, str]:
    return {}


def _empty_routes() -> Mapping[tuple[str, str], ExtraRouteHandler]:
    return {}


@dataclass(frozen=True, slots=True)
class HttpRequest:
    """A normalized HTTP request passed to custom route handlers."""

    method: str
    path: str
    query: Mapping[str, tuple[str, ...]]
    headers: Mapping[str, str]
    body: bytes

    def parameter(self, name: str, default: str | None = None) -> str | None:
        """Return the first query-string value named ``name``."""
        values = self.query.get(name)
        return values[0] if values else default


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """An ASGI-independent HTTP response."""

    status: int
    body: bytes
    content_type: str = "application/json"
    headers: Mapping[str, str] = field(default_factory=_empty_headers)
    request_shutdown: bool = False

    @classmethod
    def text(
        cls,
        body: str,
        *,
        status: int = 200,
        content_type: str = "text/plain; charset=utf-8",
    ) -> Self:
        """Construct a UTF-8 text response."""
        return cls(status=status, body=body.encode(), content_type=content_type)

    @classmethod
    def json(cls, value: object, *, status: int = 200) -> Self:
        """Construct a compact UTF-8 JSON response."""
        return cls(
            status=status,
            body=json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":"),
                default=_json_default,
            ).encode(),
        )


@dataclass(slots=True)
class HttpQueryServerConfig:
    """Configuration shared by :class:`HttpQueryApp` and its servers."""

    tool_name: str = "libxsql"
    help_text: str = ""
    bind_address: str = "127.0.0.1"
    port: int = 0
    auth_token: str | None = None
    query_handler: QueryHandler | None = None
    status_handler: StatusHandler | None = None
    extra_routes: Mapping[tuple[str, str], ExtraRouteHandler] = field(default_factory=_empty_routes)
    serialize_requests: bool = True
    use_queue: bool = False
    queue_admission_timeout: float | None = 60.0
    max_queue: int = 0
    max_body_bytes: int = 64 * 1024 * 1024
    allow_insecure_no_auth: bool = False
    status_requires_auth: bool = True
    streaming_executor: StreamingExecutor | None = None
    """Optional chunked executor, used when a request sends ``X-XSQL-Stream``.

    ``X-XSQL-Stream: 1`` (or ``true``) asks for the JSON envelope streamed row
    by row, ``X-XSQL-Stream: ndjson`` for one JSON object per line. Ignored in
    queue mode, and the header is ignored when this is unset.
    """
    keep_alive_max_count: int = 1000
    """Requests served on one kept-alive connection before it is closed."""
    cancel_handler: CancelHandler | None = None
    """Called by ``POST /cancel`` after the local cancellation, to forward it.

    It may be synchronous or asynchronous. If it raises, ``/cancel`` answers
    502 naming the failure instead of claiming the whole chain was cancelled.
    """

    def __post_init__(self) -> None:
        """Validate values that otherwise fail late inside an HTTP server."""
        if not self.tool_name:
            msg = "tool_name must not be empty"
            raise ConfigurationError(msg)
        if not 0 <= self.port <= _MAX_PORT:
            msg = "port must be between 0 and 65535"
            raise ConfigurationError(msg)
        if self.max_queue < 0:
            msg = "max_queue must not be negative"
            raise ConfigurationError(msg)
        if self.max_body_bytes <= 0:
            msg = "max_body_bytes must be positive"
            raise ConfigurationError(msg)
        if self.keep_alive_max_count <= 0:
            msg = "keep_alive_max_count must be positive"
            raise ConfigurationError(msg)
        _ensure_secure_bind(
            self.bind_address,
            self.auth_token,
            allow_insecure=self.allow_insecure_no_auth,
        )


# Concise spelling retained for callers that do not need to distinguish the app
# from the convenience server.
HttpQueryConfig = HttpQueryServerConfig


class StreamWriteCoalescer:
    """Batch a streaming executor's per-row writes into larger physical writes.

    A streaming executor writes once per row; sending each of those as its own
    chunked frame costs several times more than the bytes themselves. This
    buffers them and calls ``real_write`` once ``flush_threshold_bytes`` have
    accumulated, keeping the bytes on the wire identical. Once ``real_write``
    returns ``False`` (a disconnected client), every later :meth:`write` and
    :meth:`flush` returns or does nothing without calling it again.
    """

    def __init__(
        self,
        real_write: Callable[[bytes], bool],
        flush_threshold_bytes: int = _STREAM_FLUSH_BYTES,
    ) -> None:
        """Wrap ``real_write`` with a ``flush_threshold_bytes`` buffer."""
        if flush_threshold_bytes <= 0:
            msg = "flush_threshold_bytes must be positive"
            raise ConfigurationError(msg)
        self._real_write = real_write
        self._threshold = flush_threshold_bytes
        self._buffer = bytearray()
        self._failed = False
        self._real_write_count = 0

    def write(self, data: str | bytes) -> bool:
        """Buffer ``data``, flushing at the threshold; ``False`` once a write failed."""
        if self._failed:
            return False
        self._buffer += data.encode() if isinstance(data, str) else data
        if len(self._buffer) >= self._threshold:
            self.flush()
        return not self._failed

    def flush(self) -> None:
        """Send whatever is buffered. Safe to call repeatedly or after a failure."""
        if not self._buffer or self._failed:
            return
        chunk = bytes(self._buffer)
        self._buffer.clear()
        self._real_write_count += 1
        if not self._real_write(chunk):
            self._failed = True

    @property
    def real_write_count(self) -> int:
        """How many times ``real_write`` was called (for tests of the batching)."""
        return self._real_write_count


class _AsyncStreamWriteCoalescer:
    """:class:`StreamWriteCoalescer` for a coroutine ``real_write``."""

    def __init__(
        self,
        real_write: Callable[[bytes], Awaitable[bool]],
        flush_threshold_bytes: int = _STREAM_FLUSH_BYTES,
    ) -> None:
        self._real_write = real_write
        self._threshold = flush_threshold_bytes
        self._buffer = bytearray()
        self._failed = False

    async def write(self, data: str | bytes) -> bool:
        if self._failed:
            return False
        self._buffer += data.encode() if isinstance(data, str) else data
        if len(self._buffer) >= self._threshold:
            await self.flush()
        return not self._failed

    async def flush(self) -> None:
        if not self._buffer or self._failed:
            return
        chunk = bytes(self._buffer)
        self._buffer.clear()
        if not await self._real_write(chunk):
            self._failed = True


class _ResponseChannel:
    """Sends one ASGI response, whole or streamed, gzip'd when the client accepts it."""

    def __init__(self, send: AsgiSend, receive: AsgiReceive, *, accept_encoding: str) -> None:
        self._send = send
        self._receive = receive
        self._accept_gzip = "gzip" in accept_encoding
        self._compressor: _Compressor | None = None
        self.started = False
        self.disconnected = False

    def _gzip_for(self, content_type: str) -> bool:
        return self._accept_gzip and _can_compress(content_type)

    async def send_response(self, response: HttpResponse) -> None:
        body = response.body
        headers = _standard_headers(response.content_type)
        if self._gzip_for(response.content_type):
            compressor = _gzip_compressor()
            body = compressor.compress(body) + compressor.flush()
            headers.append((b"content-encoding", b"gzip"))
        headers.append((b"content-length", str(len(body)).encode("ascii")))
        headers.extend(
            (name.lower().encode("latin-1"), value.encode("latin-1"))
            for name, value in response.headers.items()
        )
        await self._send(
            {"type": "http.response.start", "status": response.status, "headers": headers}
        )
        await self._send({"type": "http.response.body", "body": body})

    async def watch_disconnect(self) -> None:
        """Record the client's departure while a stream is being produced."""
        while True:
            message = await self._receive()
            if message.get("type") == "http.disconnect":
                self.disconnected = True
                return

    async def stream_write(self, content_type: str, data: bytes) -> bool:
        """Start the stream on first use and send ``data``; ``False`` once the client left."""
        if self.disconnected:
            return False
        if not self.started:
            headers = _standard_headers(content_type)
            if self._gzip_for(content_type):
                self._compressor = _gzip_compressor()
                headers.append((b"content-encoding", b"gzip"))
            await self._send({"type": "http.response.start", "status": 200, "headers": headers})
            self.started = True
        if self._compressor is not None:
            data = self._compressor.compress(data)
        if data:
            try:
                await self._send({"type": "http.response.body", "body": data, "more_body": True})
            except OSError:
                self.disconnected = True
                return False
        return not self.disconnected

    async def finish_stream(self, content_type: str) -> None:
        """End the stream, first starting it if nothing was ever written."""
        if not self.started:
            await self.stream_write(content_type, b"")
        tail = self._compressor.flush() if self._compressor is not None else b""
        with contextlib.suppress(OSError):
            await self._send({"type": "http.response.body", "body": tail, "more_body": False})


@dataclass(slots=True)
class _QueuedQuery:
    sql: str
    options: ScriptOptions
    output_format: str
    admitted: threading.Event = field(default_factory=threading.Event)
    finished: threading.Event = field(default_factory=threading.Event)
    result: str | None = None
    error: BaseException | None = None
    error_status: int = 500
    cancelled: bool = False
    completed: bool = False
    processing: bool = False
    state_lock: threading.Lock = field(default_factory=threading.Lock)


class HttpQueryApp:
    """Framework-neutral ASGI application implementing the xsql HTTP contract."""

    def __init__(self, config: HttpQueryServerConfig) -> None:
        """Create an application with validated server configuration."""
        self.config = config
        self.bound_port = config.port
        self._serialize_lock: anyio.Lock | None = None
        self._waiters = 0
        self._waiters_lock = threading.Lock()
        self._queued: queue.Queue[_QueuedQuery] = queue.Queue()
        self._queue_state_lock = threading.Lock()
        self._processing_commands = 0
        self._accepting_queued_commands = True
        self._shutdown_callback: Callable[[], None] | None = None
        self._active_cancellations: set[threading.Event] = set()
        self._active_cancellations_lock = threading.Lock()

    @classmethod
    def from_connection(
        cls,
        connection: Connection | AsyncConnection,
        *,
        config: HttpQueryServerConfig | None = None,
    ) -> Self:
        """Create an app that executes scripts against ``connection``."""
        from .connection import AsyncConnection  # noqa: PLC0415 - avoids an import cycle
        from .script import (  # noqa: PLC0415 - avoids an import cycle
            run_database_script,
            run_database_script_async,
        )

        resolved = config or HttpQueryServerConfig()
        if resolved.query_handler is not None:
            msg = "config.query_handler must be empty when using from_connection()"
            raise ConfigurationError(msg)

        if isinstance(connection, AsyncConnection):

            async def async_query_handler(sql: str, options: ScriptOptions) -> ScriptResult:
                return await run_database_script_async(connection, sql, options)

            resolved.query_handler = async_query_handler
        else:

            def sync_query_handler(sql: str, options: ScriptOptions) -> ScriptResult:
                return run_database_script(connection, sql, options)

            # A synchronous Connection is creation-thread affine.  Queue mode
            # lets HttpQueryServer.run_until_stopped() execute requests on that
            # owner thread instead of a Hypercorn worker.
            resolved.use_queue = True
            resolved.query_handler = sync_query_handler

        return cls(resolved)

    async def __call__(
        self,
        scope: AsgiScope,
        receive: AsgiReceive,
        send: AsgiSend,
    ) -> None:
        """Handle one ASGI HTTP or lifespan scope."""
        scope_type = scope.get("type")
        if scope_type == "lifespan":
            await self._lifespan(receive, send)
            return
        if scope_type != "http":
            msg = f"unsupported ASGI scope type: {scope_type!r}"
            raise ProtocolError(msg)

        request_or_response = await self._read_request(scope, receive)
        if isinstance(request_or_response, HttpResponse):
            channel = _ResponseChannel(send, receive, accept_encoding="")
            response: HttpResponse | None = request_or_response
        else:
            channel = _ResponseChannel(
                send,
                receive,
                accept_encoding=request_or_response.headers.get("accept-encoding", ""),
            )
            response = await self._dispatch(request_or_response, channel)
        if response is None:
            return  # streamed
        await channel.send_response(response)
        if response.request_shutdown and self._shutdown_callback is not None:
            self._shutdown_callback()

    async def _lifespan(self, receive: AsgiReceive, send: AsgiSend) -> None:
        while True:
            message = await receive()
            message_type = message.get("type")
            if message_type == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message_type == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return

    async def _read_request(
        self,
        scope: AsgiScope,
        receive: AsgiReceive,
    ) -> HttpRequest | HttpResponse:
        headers = {
            bytes(key).decode("latin-1").lower(): bytes(value).decode("latin-1")
            for key, value in scope.get("headers", ())
        }
        content_length = headers.get("content-length")
        if content_length is not None:
            try:
                declared = int(content_length)
            except ValueError:
                return _error_response("Invalid Content-Length", status=400)
            if declared > self.config.max_body_bytes:
                return _error_response("Request body too large", status=413)

        chunks: list[bytes] = []
        body_size = 0
        while True:
            message = await receive()
            if message.get("type") == "http.disconnect":
                return _error_response("Client disconnected", status=499)
            chunk = bytes(message.get("body", b""))
            body_size += len(chunk)
            if body_size > self.config.max_body_bytes:
                return _error_response("Request body too large", status=413)
            chunks.append(chunk)
            if not message.get("more_body", False):
                break

        raw_query = bytes(scope.get("query_string", b"")).decode("latin-1")
        parsed_query = {
            key: tuple(values)
            for key, values in parse_qs(
                raw_query,
                keep_blank_values=True,
                strict_parsing=False,
            ).items()
        }
        return HttpRequest(
            method=str(scope.get("method", "GET")).upper(),
            path=str(scope.get("path", "/")),
            query=parsed_query,
            headers=headers,
            body=b"".join(chunks),
        )

    async def _dispatch(  # noqa: PLR0911 - each route terminates independently
        self,
        request: HttpRequest,
        channel: _ResponseChannel,
    ) -> HttpResponse | None:
        if not self._authorized(request):
            return _error_response("Unauthorized", status=401)

        route = (request.method, request.path)
        custom = self.config.extra_routes.get(route)
        if custom is not None:
            try:
                return await _coerce_response(await _invoke(custom, request))
            except Exception as exc:  # noqa: BLE001 - callback boundary
                return _error_response(str(exc), status=500)

        if route == ("GET", "/"):
            return HttpResponse.text(
                _root_welcome(self.config.tool_name, self.bound_port),
            )
        if route == ("GET", "/help"):
            return HttpResponse.text(self.config.help_text or _default_help())
        if route == ("GET", "/status"):
            return await self._status()
        if route == ("POST", "/cancel"):
            return await self._cancel()
        if route == ("POST", "/query"):
            return await self._query(request, channel)
        if route == ("POST", "/shutdown"):
            return HttpResponse(
                status=200,
                body=_json_bytes({"success": True, "message": "Server shutting down"}),
                request_shutdown=True,
            )
        return _error_response("Not found", status=404)

    def _authorized(self, request: HttpRequest) -> bool:
        token = self.config.auth_token
        if not token:
            return True
        if request.method == "GET" and (
            request.path in {"/", "/help"}
            or (request.path == "/status" and not self.config.status_requires_auth)
        ):
            return True
        supplied = request.headers.get("x-xsql-token")
        bearer = request.headers.get("authorization", "")
        if bearer.startswith("Bearer "):
            supplied = bearer[7:]
        return supplied is not None and hmac.compare_digest(supplied, token)

    async def _status(self) -> HttpResponse:
        value: dict[str, object] = {
            "success": True,
            "status": "ok",
            "tool": self.config.tool_name,
        }
        if self.config.status_handler is not None:
            try:
                patch = await _invoke(self.config.status_handler)
                value = _merge_patch(value, patch)
            except Exception as exc:  # noqa: BLE001 - callback boundary
                return _error_response(str(exc), status=500)
        return HttpResponse.json(value)

    async def _cancel(self) -> HttpResponse:
        with self._active_cancellations_lock:
            for cancellation in self._active_cancellations:
                cancellation.set()
        if self.config.cancel_handler is not None:
            try:
                await _invoke(self.config.cancel_handler)
            except Exception as exc:  # noqa: BLE001 - downstream callback boundary
                detail = f": {exc}" if str(exc) else ""
                return _error_response(
                    f"local cancellation requested, but downstream propagation failed{detail}",
                    status=502,
                )
        return HttpResponse.json({"success": True, "message": "cancel requested"})

    async def _query(  # noqa: C901, PLR0911, PLR0912 - protocol decision tree
        self,
        request: HttpRequest,
        channel: _ResponseChannel,
    ) -> HttpResponse | None:
        stream_header = request.headers.get("x-xsql-stream", "")
        stream = (
            self.config.streaming_executor is not None
            and not self.config.use_queue
            and stream_header in {"1", "true", "ndjson"}
        )
        if self.config.query_handler is None and not stream:
            return _error_response("No query handler configured", status=503)
        try:
            sql, options = _decode_query(request)
        except (UnicodeDecodeError, ValueError, TypeError) as exc:
            return _error_response(str(exc), status=400)
        if not sql:
            return _error_response("Empty query", status=400)
        output_format = request.parameter("format", "json") or "json"
        if output_format not in {"json", "jsonl", "text", "csv", "tsv"}:
            return _error_response("Unsupported format", status=400)
        ndjson = stream_header == "ndjson"
        if stream and not (output_format == "json" or (output_format == "jsonl" and ndjson)):
            return _error_response(
                f"?format={output_format} is incompatible with X-XSQL-Stream: {stream_header} "
                "(streaming only supports json, or jsonl with X-XSQL-Stream: ndjson "
                "-- drop ?format= or X-XSQL-Stream to use the other)",
                status=400,
            )

        async def run() -> HttpResponse | None:
            if stream:
                return await self._stream_query(sql, options, ndjson=ndjson, channel=channel)
            return await self._direct_query(sql, options, output_format)

        if self.config.use_queue:
            return await self._queued_query(sql, options, output_format)
        if not self.config.serialize_requests:
            return await run()

        with self._waiters_lock:
            if self.config.max_queue and self._waiters >= self.config.max_queue:
                return _error_response(
                    "Queue full",
                    status=503,
                    hint="Reduce concurrency or increase max_queue",
                )
            self._waiters += 1
        try:
            if self._serialize_lock is None:
                self._serialize_lock = anyio.Lock()
            timeout = self.config.queue_admission_timeout
            try:
                if timeout is None:
                    await self._serialize_lock.acquire()
                else:
                    with anyio.fail_after(timeout):
                        await self._serialize_lock.acquire()
            except TimeoutError:
                return _error_response(
                    "Request timed out while waiting for serialization",
                    status=408,
                    hint="Reduce concurrency or increase queue_admission_timeout",
                )
            try:
                # Held for the whole call, streamed responses included.
                return await run()
            finally:
                self._serialize_lock.release()
        finally:
            with self._waiters_lock:
                self._waiters -= 1

    async def _stream_query(
        self,
        sql: str,
        options: ScriptOptions,
        *,
        ndjson: bool,
        channel: _ResponseChannel,
    ) -> HttpResponse | None:
        """Run the streaming executor, sending its chunks as they are produced.

        Returns an error response only if the executor failed before sending a
        byte; once the stream has started, a failure just ends it.
        """
        executor = cast("StreamingExecutor", self.config.streaming_executor)
        content_type = "application/x-ndjson" if ndjson else "application/json"

        async def physical_write(data: bytes) -> bool:
            return await channel.stream_write(content_type, data)

        with self._active_query_options(options) as active_options:
            async with anyio.create_task_group() as watchers:
                watchers.start_soon(channel.watch_disconnect)
                try:
                    if _is_async_callable(executor):
                        async_coalescer = _AsyncStreamWriteCoalescer(physical_write)
                        await cast(
                            "Awaitable[object]",
                            executor(sql, active_options, ndjson, async_coalescer.write),
                        )
                        await async_coalescer.flush()
                    else:

                        def blocking_write(data: bytes) -> bool:
                            return from_thread.run(physical_write, data)

                        coalescer = StreamWriteCoalescer(blocking_write)

                        def execute() -> None:
                            executor(sql, active_options, ndjson, coalescer.write)
                            coalescer.flush()

                        await to_thread.run_sync(execute)
                except Exception as exc:  # noqa: BLE001 - executor boundary
                    if not channel.started:
                        watchers.cancel_scope.cancel()
                        return _error_response(str(exc), status=500)
                finally:
                    watchers.cancel_scope.cancel()
        await channel.finish_stream(content_type)
        return None

    async def _direct_query(
        self,
        sql: str,
        options: ScriptOptions,
        output_format: str,
    ) -> HttpResponse:
        handler = self.config.query_handler
        if handler is None:
            return _error_response("No query handler configured", status=503)
        with self._active_query_options(options) as active_options:
            try:
                outcome = await _invoke(handler, sql, active_options)
                body = _render_query_output(
                    outcome,
                    output_format,
                    include_sql=active_options.include_sql,
                )
            except Exception as exc:  # noqa: BLE001 - query callback boundary
                return _error_response(str(exc), status=500)
        return HttpResponse.text(
            body,
            content_type=_content_type(output_format),
        )

    async def _queued_query(
        self,
        sql: str,
        options: ScriptOptions,
        output_format: str,
    ) -> HttpResponse:
        command = _QueuedQuery(sql=sql, options=options, output_format=output_format)
        with self._queue_state_lock:
            if not self._accepting_queued_commands:
                return _error_response("HTTP server stopped", status=503)
            outstanding = self._queued.qsize() + self._processing_commands
            if self.config.max_queue and outstanding >= self.config.max_queue:
                return _error_response(
                    "Queue full",
                    status=503,
                    hint="Reduce concurrency or increase max_queue",
                )
            self._queued.put(command)
        timeout = self.config.queue_admission_timeout
        admitted = await to_thread.run_sync(command.admitted.wait, timeout)
        if not admitted:
            with command.state_lock:
                if command.processing or command.completed:
                    admitted = True
                else:
                    command.cancelled = True
        if not admitted:
            return _error_response(
                "Request timed out while waiting for queue admission",
                status=408,
                hint="Process queued commands or increase queue_admission_timeout",
            )
        await to_thread.run_sync(command.finished.wait)
        if command.error is not None:
            return _error_response(str(command.error), status=command.error_status)
        return HttpResponse.text(
            command.result or "",
            content_type=_content_type(output_format),
        )

    def process_one_command(self) -> bool:
        """Execute one queued request on the calling thread."""
        with self._queue_state_lock:
            try:
                command = self._queued.get_nowait()
            except queue.Empty:
                return False
            self._processing_commands += 1
        try:
            with command.state_lock:
                if command.cancelled or command.completed:
                    return True
                command.processing = True
                command.admitted.set()
            self._execute_queued_command(command)
            return True
        finally:
            with command.state_lock:
                command.processing = False
            with self._queue_state_lock:
                self._processing_commands -= 1

    def _execute_queued_command(self, command: _QueuedQuery) -> None:
        """Execute one command already claimed by the owner thread."""
        handler = self.config.query_handler
        if handler is None:
            command.error = ConfigurationError("No query handler configured")
        else:
            with self._active_query_options(command.options) as active_options:
                try:
                    outcome = handler(command.sql, active_options)
                    if inspect.isawaitable(outcome):
                        if inspect.iscoroutine(outcome):
                            outcome.close()
                        msg = "async query handlers are incompatible with use_queue"
                        raise ConfigurationError(msg)  # noqa: TRY301
                    command.result = _render_query_output(
                        outcome,
                        command.output_format,
                        include_sql=active_options.include_sql,
                    )
                except BaseException as exc:  # noqa: BLE001 - cross-thread handoff
                    command.error = exc
        with command.state_lock:
            command.completed = True
        command.finished.set()

    @contextlib.contextmanager
    def _active_query_options(self, options: ScriptOptions) -> Generator[ScriptOptions, None, None]:
        """Give one active request an independent cooperative-cancel token."""
        cancellation = threading.Event()
        with self._active_cancellations_lock:
            self._active_cancellations.add(cancellation)
        try:
            yield replace(options, should_cancel=cancellation.is_set)
        finally:
            with self._active_cancellations_lock:
                self._active_cancellations.discard(cancellation)

    def drain_pending(self, message: str = "HTTP server stopped") -> None:
        """Fail all queued requests, normally during server shutdown."""
        pending: list[_QueuedQuery] = []
        with self._queue_state_lock:
            self._accepting_queued_commands = False
            while True:
                try:
                    pending.append(self._queued.get_nowait())
                except queue.Empty:
                    break
        for command in pending:
            with command.state_lock:
                command.cancelled = True
                command.completed = True
                command.error = ProtocolError(message)
                command.error_status = 503
                command.admitted.set()
            command.finished.set()

    def _resume_queue_admission(self) -> None:
        """Allow a newly started server to accept owner-thread commands."""
        with self._queue_state_lock:
            self._accepting_queued_commands = True


@dataclass(frozen=True, slots=True)
class ClientConfig:
    """HTTP client connection and authentication options."""

    host: str = "127.0.0.1"
    port: int = 5555
    timeout: float = 30.0
    auth_token: str | None = None
    scheme: str = "http"

    @property
    def base_url(self) -> str:
        """Return the normalized server base URL, bracketing an IPv6 host."""
        return f"{self.scheme}://{_format_url_host(self.host)}:{self.port}"


class ThinClient:
    """Blocking HTTP client for an xsql query server."""

    def __init__(self, config: ClientConfig | None = None) -> None:
        """Create a blocking HTTP client."""
        httpx = _require_httpx()
        self.config = config or ClientConfig()
        self._client = httpx.Client(
            base_url=self.config.base_url,
            timeout=self.config.timeout,
            headers=_auth_headers(self.config.auth_token),
        )

    def query(
        self,
        sql: str,
        *,
        output_format: str = "json",
        continue_on_error: bool = False,
        include_sql: bool = False,
    ) -> str:
        """Execute SQL and return the server response body."""
        response = self._client.post(
            "/query",
            params=_query_parameters(
                output_format,
                continue_on_error=continue_on_error,
                include_sql=include_sql,
            ),
            content=sql.encode(),
            headers={"content-type": "text/plain; charset=utf-8"},
        )
        _raise_for_response(response, "query")
        return str(response.text)

    def query_json(
        self,
        sql: str,
        *,
        continue_on_error: bool = False,
        include_sql: bool = False,
    ) -> object:
        """Execute SQL and decode its JSON response."""
        return json.loads(
            self.query(
                sql,
                continue_on_error=continue_on_error,
                include_sql=include_sql,
            ),
        )

    def status(self) -> Mapping[str, object]:
        """Return the decoded server status document."""
        response = self._client.get("/status")
        _raise_for_response(response, "status")
        decoded: object = response.json()
        if not isinstance(decoded, Mapping):
            msg = "status response is not a JSON object"
            raise ProtocolError(msg)
        value = cast("Mapping[object, object]", decoded)
        return {str(key): item for key, item in value.items()}

    def ping(self) -> bool:
        """Return whether the server responds successfully to ``/status``."""
        try:
            response = self._client.get("/status")
        except Exception:  # noqa: BLE001 - reachability probe
            return False
        return bool(response.status_code == _HTTP_OK)

    def shutdown(self) -> None:
        """Request graceful server shutdown."""
        try:
            self._client.post("/shutdown")
        except Exception:  # noqa: BLE001 - server may close before responding
            return

    def cancel(self) -> None:
        """Request cooperative cancellation of the in-flight query."""
        response = self._client.post("/cancel")
        _raise_for_response(response, "cancel")

    def close(self) -> None:
        """Release the underlying HTTP connection pool."""
        self._client.close()

    def __enter__(self) -> Self:
        """Return this open client."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the client when leaving its context."""
        self.close()


class AsyncThinClient:
    """AnyIO-compatible asynchronous HTTP client."""

    def __init__(self, config: ClientConfig | None = None) -> None:
        """Create an asynchronous HTTP client."""
        httpx = _require_httpx()
        self.config = config or ClientConfig()
        self._client = httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=self.config.timeout,
            headers=_auth_headers(self.config.auth_token),
        )

    async def query(
        self,
        sql: str,
        *,
        output_format: str = "json",
        continue_on_error: bool = False,
        include_sql: bool = False,
    ) -> str:
        """Execute SQL and return the server response body."""
        response = await self._client.post(
            "/query",
            params=_query_parameters(
                output_format,
                continue_on_error=continue_on_error,
                include_sql=include_sql,
            ),
            content=sql.encode(),
            headers={"content-type": "text/plain; charset=utf-8"},
        )
        _raise_for_response(response, "query")
        return str(response.text)

    async def query_json(
        self,
        sql: str,
        *,
        continue_on_error: bool = False,
        include_sql: bool = False,
    ) -> object:
        """Execute SQL and decode its JSON response."""
        return json.loads(
            await self.query(
                sql,
                continue_on_error=continue_on_error,
                include_sql=include_sql,
            ),
        )

    async def status(self) -> Mapping[str, object]:
        """Return the decoded server status document."""
        response = await self._client.get("/status")
        _raise_for_response(response, "status")
        decoded: object = response.json()
        if not isinstance(decoded, Mapping):
            msg = "status response is not a JSON object"
            raise ProtocolError(msg)
        value = cast("Mapping[object, object]", decoded)
        return {str(key): item for key, item in value.items()}

    async def ping(self) -> bool:
        """Return whether the server responds successfully to ``/status``."""
        try:
            response = await self._client.get("/status")
        except Exception:  # noqa: BLE001 - reachability probe
            return False
        return bool(response.status_code == _HTTP_OK)

    async def shutdown(self) -> None:
        """Request graceful server shutdown."""
        try:
            await self._client.post("/shutdown")
        except Exception:  # noqa: BLE001 - server may close before responding
            return

    async def cancel(self) -> None:
        """Request cooperative cancellation of the in-flight query."""
        response = await self._client.post("/cancel")
        _raise_for_response(response, "cancel")

    async def close(self) -> None:
        """Release the underlying HTTP connection pool."""
        await self._client.aclose()

    async def __aenter__(self) -> Self:
        """Return this open asynchronous client."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the client when leaving its asynchronous context."""
        await self.close()


class AsyncHttpQueryServer:
    """Hypercorn-backed query server for asyncio and Trio applications."""

    def __init__(
        self,
        app: HttpQueryApp | HttpQueryServerConfig,
    ) -> None:
        """Create a stopped server around an application or configuration."""
        self.app = app if isinstance(app, HttpQueryApp) else HttpQueryApp(app)
        self._task_group: TaskGroup | None = None
        self._task_group_cm: TaskGroup | None = None
        self._shutdown = threading.Event()
        self._running = False
        self._port = 0
        self._serve_error: BaseException | None = None

    @property
    def port(self) -> int:
        """Return the active port, or zero while stopped."""
        return self._port

    @property
    def url(self) -> str:
        """Return the listening base URL; see :func:`_format_url_host`."""
        return f"http://{_format_url_host(self.app.config.bind_address)}:{self._port}"

    @property
    def is_running(self) -> bool:
        """Return whether the server is accepting requests."""
        return self._running

    async def start(self) -> int:
        """Start serving and return the selected port."""
        if self._running:
            return self._port
        _require_hypercorn()
        self._shutdown.clear()
        self._serve_error = None
        # The same app may be stopped and started again.
        self.app._resume_queue_admission()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        self._port = _select_port(
            self.app.config.bind_address,
            self.app.config.port,
        )
        self.app.bound_port = self._port
        # The app and server form one adapter unit; this hook is intentionally private.
        self.app._shutdown_callback = self.request_stop  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        task_group_cm = anyio.create_task_group()
        self._task_group_cm = task_group_cm
        task_group = await task_group_cm.__aenter__()
        self._task_group = task_group
        task_group.start_soon(self._serve)
        try:
            with anyio.fail_after(10):
                while not await to_thread.run_sync(
                    _can_connect,
                    _connect_host(self.app.config.bind_address),
                    self._port,
                ):
                    if self._serve_error is not None:
                        raise self._serve_error  # noqa: TRY301
                    await anyio.sleep(0.01)
        except BaseException:
            await self.stop()
            raise
        self._running = True
        return self._port

    def request_stop(self) -> None:
        """Signal shutdown from any thread."""
        self._shutdown.set()

    async def stop(self) -> None:
        """Stop serving and wait for all request tasks to finish."""
        self.request_stop()
        self.app.drain_pending()
        if self._task_group is None or self._task_group_cm is None:
            self._running = False
            self._port = 0
            return
        task_group_cm = self._task_group_cm
        self._task_group = None
        self._task_group_cm = None
        await task_group_cm.__aexit__(None, None, None)
        self._running = False
        self._port = 0

    async def wait_stopped(self) -> None:
        """Wait until a local or HTTP shutdown request is received."""
        await to_thread.run_sync(self._shutdown.wait)

    async def _serve(self) -> None:
        try:
            config = _hypercorn_config(
                self.app.config.bind_address,
                self._port,
                self.app.config.keep_alive_max_count,
            )

            async def shutdown_trigger() -> None:
                await to_thread.run_sync(self._shutdown.wait)

            backend = _async_backend_name()
            module_name = "hypercorn.trio" if backend == "trio" else "hypercorn.asyncio"
            backend_module = importlib.import_module(module_name)
            serve = cast("_HypercornServe", backend_module.serve)
            await serve(self.app, config, shutdown_trigger=shutdown_trigger)
        except BaseException as exc:  # noqa: BLE001 - propagate through start()
            self._serve_error = exc
            self._shutdown.set()

    async def __aenter__(self) -> Self:
        """Start and return the server."""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Stop the server when leaving its asynchronous context."""
        await self.stop()


class HttpQueryServer:
    """Blocking background-thread wrapper around :class:`AsyncHttpQueryServer`."""

    def __init__(self, app: HttpQueryApp | HttpQueryServerConfig) -> None:
        """Create a stopped background-thread server."""
        self.app = app if isinstance(app, HttpQueryApp) else HttpQueryApp(app)
        self._thread: threading.Thread | None = None
        self._async_server: AsyncHttpQueryServer | None = None
        self._ready = threading.Event()
        self._stopped = threading.Event()
        self._error: BaseException | None = None
        self._port = 0

    @property
    def port(self) -> int:
        """Return the active port, or zero while stopped."""
        return self._port

    @property
    def url(self) -> str:
        """Return the listening base URL; see :func:`_format_url_host`."""
        return f"http://{_format_url_host(self.app.config.bind_address)}:{self._port}"

    @property
    def is_running(self) -> bool:
        """Return whether the background server is active."""
        return self._thread is not None and self._thread.is_alive() and self._port > 0

    def start(self) -> int:
        """Start the server in a background thread."""
        if self.is_running:
            return self._port
        self._ready.clear()
        self._stopped.clear()
        self._error = None
        self._thread = threading.Thread(
            target=self._thread_main,
            name="libxsql-http",
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(10):
            msg = "HTTP server did not start within 10 seconds"
            raise ProtocolError(msg)
        if self._error is not None:
            raise self._error
        return self._port

    def stop(self) -> None:
        """Request shutdown and join the background server thread."""
        server = self._async_server
        if server is not None:
            server.request_stop()
        self.app.drain_pending()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        self._thread = None
        self._async_server = None
        self._port = 0

    def process_one_command(self) -> bool:
        """Execute one queued query on this calling thread."""
        return self.app.process_one_command()

    def run_until_stopped(self) -> None:
        """Process queued commands until the HTTP server stops."""
        while self.is_running:
            if not self.process_one_command():
                time.sleep(0.01)

    def _thread_main(self) -> None:
        async def run() -> None:
            server = AsyncHttpQueryServer(self.app)
            self._async_server = server
            try:
                self._port = await server.start()
                self._ready.set()
                await server.wait_stopped()
                await server.stop()
            except BaseException as exc:  # noqa: BLE001 - thread handoff
                self._error = exc
                self._ready.set()
            finally:
                self._stopped.set()

        asyncio.run(run())

    def __enter__(self) -> Self:
        """Start and return the server."""
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Stop the server when leaving its context."""
        self.stop()


async def _invoke(callback: Callable[..., object], *args: object) -> object:
    is_async = _is_async_callable(callback)
    result = callback(*args) if is_async else await to_thread.run_sync(callback, *args)
    if inspect.isawaitable(result):
        return await result
    return result


def _is_async_callable(callback: Callable[..., object]) -> bool:
    return inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(
        type(callback).__call__,
    )


def _standard_headers(content_type: str) -> list[tuple[bytes, bytes]]:
    return [
        (b"content-type", content_type.encode("latin-1")),
        (b"cache-control", b"no-store"),
        (b"x-content-type-options", b"nosniff"),
    ]


def _can_compress(content_type: str) -> bool:
    """Whether gzip applies to a response, judged on its media type alone.

    ``text/event-stream`` is excluded even with parameters: a compressed SSE
    stream is held back until the compressor fills, so an event never arrives.
    """
    media_type = content_type.split(";", 1)[0].rstrip(" \t")
    if media_type in _COMPRESSIBLE_TYPES:
        return True
    return media_type.startswith("text/") and media_type != "text/event-stream"


def _gzip_compressor() -> _Compressor:
    # Level 1: most of level 6's ratio on query results at a fraction of the time.
    return zlib.compressobj(1, zlib.DEFLATED, 31)


async def _coerce_response(value: object) -> HttpResponse:
    if isinstance(value, HttpResponse):
        return value
    if isinstance(value, bytes):
        return HttpResponse(status=200, body=value, content_type="application/octet-stream")
    if isinstance(value, str):
        return HttpResponse.text(value)
    return HttpResponse.json(value)


def _decode_query(request: HttpRequest) -> tuple[str, ScriptOptions]:
    sql, options = _decode_query_body(request)
    timeout_ms = _timeout_header(request.headers.get("x-xsql-timeout"))
    if timeout_ms is not None:
        options = replace(options, timeout_ms=timeout_ms)
    return sql, options


def _timeout_header(value: str | None) -> int | None:
    """Parse ``X-XSQL-Timeout: <ms>`` the way ``std::stol`` does; invalid is ignored.

    Leading whitespace, a sign, and trailing text after the digits are accepted;
    a negative or unparsable value leaves the request's timeout unchanged.
    """
    if value is None:
        return None
    match = _LEADING_INTEGER.match(value)
    if match is None:
        return None
    digits = match.group(1)
    negative = digits.startswith("-")
    milliseconds = 0
    for digit in digits.lstrip("+-"):
        milliseconds = min(milliseconds * 10 + int(digit), 2**31 - 1)
    return milliseconds if not negative or milliseconds == 0 else None


def _decode_query_body(request: HttpRequest) -> tuple[str, ScriptOptions]:
    options = ScriptOptions(
        continue_on_error=_flag(request.parameter("continue_on_error")),
        include_sql=_flag(request.parameter("include_sql")),
    )
    content_type = request.headers.get("content-type", "")
    text = request.body.decode("utf-8")
    declared_json = "application/json" in content_type
    looks_json = declared_json or text.lstrip().startswith("{")
    if not looks_json:
        return text, options
    try:
        decoded: object = json.loads(text)
    except json.JSONDecodeError:
        if declared_json:
            msg = 'JSON request body must be an object with a string "sql"'
            raise ValueError(msg) from None
        return text, options
    if not isinstance(decoded, Mapping):
        msg = 'JSON request body must be an object with a string "sql"'
        raise TypeError(msg)
    value = cast("Mapping[str, object]", decoded)
    sql = value.get("sql")
    if not isinstance(sql, str):
        if declared_json:
            msg = 'JSON request body must be an object with a string "sql"'
            raise ValueError(msg)
        return text, options
    options = ScriptOptions(
        continue_on_error=options.continue_on_error or _flag(value.get("continue_on_error")),
        include_sql=options.include_sql or _flag(value.get("include_sql")),
    )
    return sql, options


def _render_query_output(  # noqa: PLR0911 - supports each public result representation
    outcome: object,
    output_format: str,
    *,
    include_sql: bool,
) -> str:
    if isinstance(outcome, bytes):
        return outcome.decode()
    if isinstance(outcome, str):
        return outcome
    if isinstance(outcome, ScriptResult):
        if output_format == "text":
            return script_result_to_text(outcome)
        if output_format == "csv":
            return script_result_to_csv(outcome)
        if output_format == "tsv":
            return script_result_to_tsv(outcome)
        if output_format == "jsonl":
            return script_result_to_jsonl(outcome)
        return script_result_to_json(outcome, include_sql=include_sql)
    if isinstance(outcome, QueryResult):
        document = {
            "success": True,
            "columns": list(outcome.columns),
            "rows": [list(row) for row in outcome.rows],
            "row_count": len(outcome.rows),
        }
        return json.dumps(
            document,
            ensure_ascii=False,
            separators=(",", ":"),
            default=_json_default,
        )
    return json.dumps(
        outcome,
        ensure_ascii=False,
        separators=(",", ":"),
        default=_json_default,
    )


def _merge_patch(
    target: dict[str, object],
    patch: object,
) -> dict[str, object]:
    if not isinstance(patch, Mapping):
        return {"value": patch}
    typed_patch = cast("Mapping[object, object]", patch)
    result = dict(target)
    for key, value in typed_patch.items():
        name = str(key)
        if value is None:
            result.pop(name, None)
        elif isinstance(value, Mapping) and isinstance(result.get(name), Mapping):
            existing = cast("Mapping[object, object]", result[name])
            nested = {str(key): item for key, item in existing.items()}
            result[name] = _merge_patch(nested, cast("object", value))
        else:
            result[name] = value
    return result


def _root_welcome(tool_name: str, port: int) -> str:
    return (
        f"{tool_name.upper()} HTTP Server\n\n"  # noqa: S608 - documentation example
        "Endpoints:\n"
        "  GET  /help     - API documentation\n"
        "  POST /query    - Execute SQL query\n"
        "  GET  /status   - Health check\n"
        "  POST /cancel   - Cancel the active query\n"
        "  POST /shutdown - Stop server\n\n"
        f"Example: curl -X POST http://localhost:{port}/query "
        "--data-binary \"SELECT name FROM sqlite_master WHERE type='table' LIMIT 10\"\n"
        # -d '' sends Content-Length: 0; some clients leave a bodyless POST
        # unframed, and the server then waits for a body instead of cancelling.
        f"Cancel:  curl -X POST -d '' http://localhost:{port}/cancel\n"
    )


def _default_help() -> str:
    return (
        "POST /query accepts UTF-8 SQL or an application/json object containing "
        '"sql". Query parameters: format=json|jsonl|text|csv|tsv, '
        "continue_on_error=1, include_sql=1.\n"
        "Headers: X-XSQL-Timeout: <ms> sets the query timeout; X-XSQL-Stream: 1 "
        "(or ndjson) streams the result when the server has a streaming executor.\n"
    )


def _error_response(
    message: str,
    *,
    status: int,
    hint: str | None = None,
) -> HttpResponse:
    document: dict[str, object] = {"success": False, "error": message}
    if hint:
        document["hint"] = hint
    return HttpResponse.json(document, status=status)


def _content_type(output_format: str) -> str:
    return {
        "json": "application/json",
        "jsonl": "application/x-ndjson",
        "text": "text/plain; charset=utf-8",
        "csv": "text/csv; charset=utf-8",
        "tsv": "text/tab-separated-values; charset=utf-8",
    }[output_format]


def _json_default(value: object) -> object:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, bytearray):
        return bytes(value).decode("utf-8", errors="replace")
    if isinstance(value, memoryview):
        return value.tobytes().decode("utf-8", errors="replace")
    msg = f"{type(value).__name__} is not JSON serializable"
    raise TypeError(msg)


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        default=_json_default,
    ).encode()


def _flag(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).lower() in {"1", "true"} if value is not None else False


def _ensure_secure_bind(
    address: str,
    token: str | None,
    *,
    allow_insecure: bool,
) -> None:
    if token or allow_insecure or _is_loopback(address):
        return
    msg = (
        "refusing an unauthenticated non-loopback HTTP bind; configure "
        "auth_token or explicitly set allow_insecure_no_auth=True"
    )
    raise ConfigurationError(msg)


def _is_loopback(address: str) -> bool:
    if address.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(address).is_loopback
    except ValueError:
        return False


def _format_url_host(host: str) -> str:
    """Render a bind or client host for a URL.

    An empty host is ``127.0.0.1`` and a bare IPv6 literal is bracketed
    (``::1`` -> ``[::1]``), so ``url`` reads ``http://[::1]:8100``. A wildcard
    bind is rendered as bound (``0.0.0.0``, ``[::]``); the address a client
    dials for it is :func:`_connect_host`. Mirrors C++/Rust ``format_url_host``.
    """
    host = host or "127.0.0.1"
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def _connect_host(bind_host: str) -> str:
    """Return the address a client dials to reach a server bound to ``bind_host``.

    A wildcard bind accepts connections but is not dialable (Windows refuses to
    connect to ``0.0.0.0`` or ``::``), so it maps to the matching loopback.
    Unbracketed, for :mod:`socket`; C++/Rust ``format_connect_url_host``.
    """
    host = (bind_host or "127.0.0.1").strip("[]")
    return {"0.0.0.0": "127.0.0.1", "::": "::1"}.get(host, host)  # noqa: S104


def _socket_family(address: str) -> socket.AddressFamily:
    return socket.AF_INET6 if ":" in address else socket.AF_INET


def _select_port(address: str, requested: int) -> int:
    if requested:
        if not _port_available(address, requested):
            msg = f"HTTP port {requested} is already in use"
            raise ProtocolError(msg)
        return requested
    candidates = list(range(8100, 9000))
    random.SystemRandom().shuffle(candidates)
    for candidate in candidates:
        if _port_available(address, candidate):
            return candidate
    with socket.socket(_socket_family(address), socket.SOCK_STREAM) as sock:
        sock.bind((address, 0))
        return int(sock.getsockname()[1])


def _port_available(address: str, port: int) -> bool:
    try:
        with socket.socket(_socket_family(address), socket.SOCK_STREAM) as sock:
            sock.bind((address, port))
    except OSError:
        return False
    return True


def _can_connect(address: str, port: int) -> bool:
    try:
        with socket.create_connection((address, port), timeout=0.1):
            return True
    except OSError:
        return False


def _require_httpx() -> Any:  # noqa: ANN401 - optional module adapter
    try:
        import httpx  # noqa: PLC0415 - optional thinclient extra
    except ImportError:
        msg = "HTTP clients require `pip install libxsql[thinclient]`"
        raise ImportError(msg) from None
    return httpx


def _require_hypercorn() -> None:
    try:
        importlib.import_module("hypercorn")
    except ImportError:
        msg = "HTTP servers require `pip install libxsql[thinclient]`"
        raise ImportError(msg) from None


def _hypercorn_config(
    address: str,
    port: int,
    keep_alive_max_requests: int,
) -> Any:  # noqa: ANN401 - optional adapter
    from hypercorn.config import Config  # noqa: PLC0415 - optional thinclient extra

    config = Config()
    config.bind = [f"{address}:{port}"]
    # A batch client issuing many small queries must not be forced to
    # reconnect every few requests.
    config.keep_alive_max_requests = keep_alive_max_requests
    config.accesslog = None
    config.errorlog = None
    config.use_reloader = False
    return config


def _async_backend_name() -> str:
    try:
        import sniffio  # noqa: PLC0415 - optional backend detector
    except ImportError:
        return "asyncio"
    try:
        return str(sniffio.current_async_library())
    except sniffio.AsyncLibraryNotFoundError:
        return "asyncio"


def _auth_headers(token: str | None) -> dict[str, str]:
    return {"authorization": f"Bearer {token}"} if token else {}


def _query_parameters(
    output_format: str,
    *,
    continue_on_error: bool,
    include_sql: bool,
) -> str:
    params: dict[str, str] = {"format": output_format}
    if continue_on_error:
        params["continue_on_error"] = "1"
    if include_sql:
        params["include_sql"] = "1"
    return urlencode(params)


def _raise_for_response(
    response: Any,  # noqa: ANN401 - sync or async HTTPX response
    operation: str,
) -> None:
    if response.status_code < _HTTP_ERROR:
        return
    try:
        document = response.json()
        detail = document.get("error", response.text)
    except (ValueError, AttributeError):
        detail = response.text
    msg = f"{operation} failed with HTTP {response.status_code}: {detail}"
    raise ProtocolError(msg)


__all__ = [
    "AsyncHttpQueryServer",
    "AsyncThinClient",
    "ClientConfig",
    "HttpQueryApp",
    "HttpQueryConfig",
    "HttpQueryServer",
    "HttpQueryServerConfig",
    "HttpRequest",
    "HttpResponse",
    "StreamWriteCoalescer",
    "ThinClient",
]
