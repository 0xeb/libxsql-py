# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Thread-safe shared runtime settings and PRAGMA parsing."""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import TYPE_CHECKING, cast

from .errors import ReadOnlyError

if TYPE_CHECKING:
    from .types import SQLiteValue
    from .vtable import TableDefinition

MAX_TIMEOUT_MS = 3_600_000
MAX_QUEUE_LIMIT = 10_000
_INTEGER_PATTERN = re.compile(r"[+-]?[0-9]+\Z")
_QUOTE_PAIR_LENGTH = 2
# Upper bound on a string setting's byte length, mirroring the C++
# ``kMaxStringSettingBytes``. A staged value lives in the per-connection overlay
# until COMMIT, so an unbounded blob is a per-connection memory hole.
_MAX_STRING_SETTING_BYTES = 4096
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


@dataclass(frozen=True, slots=True)
class RuntimeSettingsSnapshot:
    """Immutable copy of common runtime settings."""

    query_timeout_ms: int = 60_000
    queue_admission_timeout_ms: int = 120_000
    max_queue: int = 64
    hints_enabled: bool = True
    timeout_stack_depth: int = 0


@dataclass(frozen=True, slots=True)
class RuntimeSettingsCoreOptions:
    """Construction options for :class:`RuntimeSettingsCore`."""

    max_timeout_stack_depth: int = 64


class RuntimeSettingType(Enum):
    """Canonical type of one registered runtime setting."""

    BOOLEAN = "bool"
    INTEGER = "int"
    STRING = "string"


@dataclass(frozen=True, slots=True)
class RuntimeSettingSpec:
    """Registration metadata and validation policy for one setting."""

    key: str
    setting_type: RuntimeSettingType
    scope: str
    default_value: str
    writable: bool = True
    minimum: int = _INT64_MIN
    maximum: int = _INT64_MAX


@dataclass(frozen=True, slots=True)
class RuntimeSettingEntry:
    """One row in the canonical ``runtime_settings`` discovery table."""

    key: str
    value: str
    value_type: str
    scope: str

    @property
    def type(self) -> str:
        """Return the wire-compatible alias for :attr:`value_type`."""
        return self.value_type


@dataclass(frozen=True, slots=True)
class RuntimePragmaRequest:
    """A parsed ``PRAGMA <prefix>.<key>[=<value>]`` request."""

    matched: bool = False
    has_value: bool = False
    key: str = ""
    value: str = ""


@dataclass(frozen=True, slots=True)
class RuntimePragmaReply:
    """Result of dispatching a runtime PRAGMA request."""

    handled: bool = False
    success: bool = False
    name: str = ""
    value: str = ""
    error: str = ""


@dataclass(slots=True)
class _SettingRecord:
    spec: RuntimeSettingSpec
    value: str


@dataclass(slots=True)
class _RuntimeSettingsTableRow:
    entry: RuntimeSettingEntry
    connection_state: object

    @property
    def key(self) -> str:
        return self.entry.key

    @property
    def value(self) -> str:
        return self.entry.value

    @value.setter
    def value(self, value: str) -> None:
        self.entry = replace(self.entry, value=value)

    @property
    def value_type(self) -> str:
        return self.entry.value_type

    @property
    def scope(self) -> str:
        return self.entry.scope


class RuntimeSettings:
    """Thread-safe typed runtime-setting registry shared by SQL and transports."""

    def __init__(
        self,
        options: RuntimeSettingsCoreOptions | None = None,
        *,
        max_timeout_stack_depth: int | None = None,
    ) -> None:
        """Create settings with canonical defaults.

        Args:
            options: Typed core construction options.
            max_timeout_stack_depth: Maximum nested timeout overrides. Zero is
                explicitly unbounded. This keyword is the concise Python
                spelling and cannot be combined with ``options``.
        """
        if options is not None and max_timeout_stack_depth is not None:
            message = "options and max_timeout_stack_depth are mutually exclusive"
            raise ValueError(message)
        depth = (
            options.max_timeout_stack_depth
            if options is not None
            else 64
            if max_timeout_stack_depth is None
            else max_timeout_stack_depth
        )
        if depth < 0:
            message = "max_timeout_stack_depth must be non-negative"
            raise ValueError(message)
        self._lock = threading.RLock()
        self._order: list[str] = []
        self._settings: dict[str, _SettingRecord] = {}
        self._timeout_stack: list[int] = []
        self._max_timeout_stack_depth = depth
        self._staged_query_timeout_transactions = 0
        registrations = (
            self.register_integer_setting(
                "query_timeout_ms",
                60_000,
                0,
                MAX_TIMEOUT_MS,
                "common",
            ),
            self.register_integer_setting(
                "queue_admission_timeout_ms",
                120_000,
                0,
                MAX_TIMEOUT_MS,
                "common",
            ),
            self.register_integer_setting(
                "max_queue",
                64,
                0,
                MAX_QUEUE_LIMIT,
                "common",
            ),
            self.register_bool_setting(
                "hints_enabled",
                default_value=True,
                scope="common",
            ),
            self.register_integer_setting(
                "timeout_stack_depth",
                0,
                0,
                _INT64_MAX,
                "common",
                writable=False,
            ),
            self.register_integer_setting(
                "max_timeout_stack_depth",
                depth,
                0,
                _INT64_MAX,
                "common",
                writable=False,
            ),
            self.register_integer_setting(
                "timeout_push",
                60_000,
                0,
                MAX_TIMEOUT_MS,
                "action",
                writable=False,
            ),
            self.register_integer_setting(
                "timeout_pop",
                60_000,
                0,
                MAX_TIMEOUT_MS,
                "action",
                writable=False,
            ),
        )
        if not all(registrations):
            message = "could not initialize canonical runtime settings"
            raise RuntimeError(message)

    def register_bool_setting(
        self,
        key: str,
        default_value: bool,  # noqa: FBT001 - the value is the setting payload.
        scope: str,
        *,
        writable: bool = True,
    ) -> bool:
        """Register a boolean setting canonicalized to ``"1"`` or ``"0"``."""
        return self._register_setting(
            RuntimeSettingSpec(
                key,
                RuntimeSettingType.BOOLEAN,
                scope,
                "1" if default_value else "0",
                writable,
            ),
        )

    def register_integer_setting(  # noqa: PLR0913 - bounds are the public contract.
        self,
        key: str,
        default_value: int,
        minimum: int,
        maximum: int,
        scope: str,
        *,
        writable: bool = True,
    ) -> bool:
        """Register an integer setting with inclusive bounds."""
        if (
            isinstance(default_value, bool)
            or isinstance(minimum, bool)
            or isinstance(maximum, bool)
            or minimum > maximum
            or not minimum <= default_value <= maximum
        ):
            return False
        return self._register_setting(
            RuntimeSettingSpec(
                key,
                RuntimeSettingType.INTEGER,
                scope,
                str(default_value),
                writable,
                minimum,
                maximum,
            ),
        )

    def register_string_setting(
        self,
        key: str,
        default_value: str,
        scope: str,
        *,
        writable: bool = True,
    ) -> bool:
        """Register an uninterpreted UTF-8 string setting."""
        return self._register_setting(
            RuntimeSettingSpec(
                key,
                RuntimeSettingType.STRING,
                scope,
                default_value,
                writable,
            ),
        )

    def specs(self) -> tuple[RuntimeSettingSpec, ...]:
        """Return setting specifications in deterministic registration order."""
        with self._lock:
            return tuple(self._settings[key].spec for key in self._order)

    @property
    def query_timeout_ms(self) -> int:
        """Return the effective per-query timeout."""
        return self.integer_value("query_timeout_ms", 60_000)

    @property
    def queue_admission_timeout_ms(self) -> int:
        """Return the queue admission timeout."""
        return self.integer_value("queue_admission_timeout_ms", 120_000)

    @property
    def max_queue(self) -> int:
        """Return the maximum admitted queue length."""
        return self.integer_value("max_queue", 64)

    @property
    def hints_enabled(self) -> bool:
        """Whether planner/decompiler hints are enabled."""
        return self.bool_value("hints_enabled", fallback=True)

    @property
    def max_timeout_stack_depth(self) -> int:
        """Return the timeout-stack cap; zero means unbounded."""
        return self._max_timeout_stack_depth

    def snapshot(self) -> RuntimeSettingsSnapshot:
        """Return an atomic immutable snapshot."""
        with self._lock:
            return RuntimeSettingsSnapshot(
                query_timeout_ms=self._integer_locked("query_timeout_ms"),
                queue_admission_timeout_ms=self._integer_locked(
                    "queue_admission_timeout_ms",
                ),
                max_queue=self._integer_locked("max_queue"),
                hints_enabled=self._settings["hints_enabled"].value == "1",
                timeout_stack_depth=len(self._timeout_stack),
            )

    def set_query_timeout_ms(self, value: int) -> bool:
        """Set the query timeout, returning whether validation succeeded."""
        return self.apply("query_timeout_ms", str(value), "runtime").success

    def set_queue_admission_timeout_ms(self, value: int) -> bool:
        """Set the queue timeout, returning whether validation succeeded."""
        return self.apply("queue_admission_timeout_ms", str(value), "runtime").success

    def set_max_queue(self, value: int) -> bool:
        """Set the queue bound, returning whether validation succeeded."""
        return self.apply("max_queue", str(value), "runtime").success

    def set_hints_enabled(self, enabled: bool) -> None:  # noqa: FBT001
        """Enable or disable hints."""
        self.apply("hints_enabled", "1" if enabled else "0", "runtime")

    def timeout_push(self, timeout_ms: int) -> int | None:
        """Push a temporary query timeout and return its effective value."""
        if not self.is_valid_timeout(timeout_ms):
            return None
        with self._lock:
            if self._staged_query_timeout_transactions or (
                self._max_timeout_stack_depth
                and len(self._timeout_stack) >= self._max_timeout_stack_depth
            ):
                return None
            self._timeout_stack.append(self._integer_locked("query_timeout_ms"))
            self._settings["query_timeout_ms"].value = str(timeout_ms)
            return timeout_ms

    def timeout_pop(self) -> int | None:
        """Restore and return the previous query timeout."""
        with self._lock:
            if self._staged_query_timeout_transactions or not self._timeout_stack:
                return None
            restored = self._timeout_stack.pop()
            self._settings["query_timeout_ms"].value = str(restored)
            return restored

    def enumerate_common(self) -> tuple[RuntimeSettingEntry, ...]:
        """Enumerate common keys and imperative timeout verbs."""
        return tuple(row for row in self.enumerate() if row.scope in {"common", "action"})

    def enumerate(self) -> tuple[RuntimeSettingEntry, ...]:
        """Enumerate all runtime settings.

        Product-specific rows registered through the typed registry are included
        automatically.
        """
        with self._lock:
            return tuple(
                RuntimeSettingEntry(
                    key,
                    self._effective_value_locked(key),
                    self._settings[key].spec.setting_type.value,
                    self._settings[key].spec.scope,
                )
                for key in self._order
            )

    def normalize(
        self,
        key: str,
        value: str,
        product_prefix: str = "libxsql",
    ) -> RuntimePragmaReply:
        """Validate and canonicalize a setting without changing shared state."""
        with self._lock:
            return self._normalize_locked(key, value, product_prefix)

    def apply(
        self,
        key: str,
        value: str,
        product_prefix: str = "libxsql",
    ) -> RuntimePragmaReply:
        """Validate and immediately commit a direct, non-SQL write."""
        normalized_key = to_lower_copy(trim_copy(key))
        with self._lock:
            reply = self._normalize_locked(normalized_key, value, product_prefix)
            if reply.handled and reply.success:
                self._settings[normalized_key].value = reply.value
            return reply

    def value(self, key: str) -> str | None:
        """Return one committed effective value."""
        with self._lock:
            if key not in self._settings:
                return None
            return self._effective_value_locked(key)

    def bool_value(self, key: str, *, fallback: bool = False) -> bool:
        """Return a boolean setting or ``fallback``."""
        value = self.value(key)
        return fallback if value is None else value == "1"

    def integer_value(self, key: str, fallback: int = 0) -> int:
        """Return an integer setting or ``fallback``."""
        value = self.value(key)
        if value is None:
            return fallback
        try:
            return int(value)
        except ValueError:
            return fallback

    @property
    def timeout_stack_active(self) -> bool:
        """Whether an imperative timeout override is active."""
        with self._lock:
            return bool(self._timeout_stack)

    @property
    def staged_query_timeout_active(self) -> bool:
        """Whether a SQL transaction owns a staged query-timeout write."""
        with self._lock:
            return self._staged_query_timeout_transactions != 0

    @staticmethod
    def is_valid_timeout(value: int) -> bool:
        """Whether a timeout lies in the shared zero-to-one-hour range."""
        return 0 <= value <= MAX_TIMEOUT_MS

    def _register_setting(self, spec: RuntimeSettingSpec) -> bool:
        if not spec.key.strip() or not spec.scope.strip():
            return False
        # A string setting's default must satisfy the same bounds as a written
        # value: non-empty and within the byte cap (C++ validates the default at
        # registration too).
        if spec.setting_type is RuntimeSettingType.STRING and (
            not spec.default_value
            or len(spec.default_value.encode("utf-8")) > _MAX_STRING_SETTING_BYTES
        ):
            return False
        with self._lock:
            if spec.key in self._settings:
                return False
            self._settings[spec.key] = _SettingRecord(spec, spec.default_value)
            self._order.append(spec.key)
            return True

    def _normalize_locked(  # noqa: PLR0911 - one guarded return per setting type + bound.
        self,
        key: str,
        value: str,
        product_prefix: str,
    ) -> RuntimePragmaReply:
        record = self._settings.get(key)
        if record is None or record.spec.scope == "action":
            return RuntimePragmaReply()
        if not record.spec.writable:
            return _failure(f"runtime_settings: {key!r} is read-only")
        if record.spec.setting_type is RuntimeSettingType.BOOLEAN:
            parsed_bool = parse_bool_value(value)
            if parsed_bool is None:
                return _failure(f"Invalid {product_prefix}.{key} value")
            normalized = "1" if parsed_bool else "0"
        elif record.spec.setting_type is RuntimeSettingType.INTEGER:
            stripped = trim_copy(value)
            if not _INTEGER_PATTERN.fullmatch(stripped):
                return _failure(f"Invalid {product_prefix}.{key} value")
            parsed_int = int(stripped, 10)
            if not record.spec.minimum <= parsed_int <= record.spec.maximum:
                return _failure(f"Invalid {product_prefix}.{key} value")
            normalized = str(parsed_int)
        else:
            # String setting: reject empty (the writable table already rejects
            # NULL, and an empty string is the same "unset" shape) and bound size.
            if not value:
                return _failure(f"Invalid {product_prefix}.{key} value (may not be empty)")
            if len(value.encode("utf-8")) > _MAX_STRING_SETTING_BYTES:
                return _failure(
                    f"Invalid {product_prefix}.{key} value "
                    f"(exceeds {_MAX_STRING_SETTING_BYTES} bytes)"
                )
            normalized = value
        return RuntimePragmaReply(
            handled=True,
            success=True,
            name=key,
            value=normalized,
        )

    def _effective_value_locked(self, key: str) -> str:
        if key == "timeout_stack_depth":
            return str(len(self._timeout_stack))
        if key in {"timeout_push", "timeout_pop"}:
            return self._settings["query_timeout_ms"].value
        return self._settings[key].value

    def _integer_locked(self, key: str) -> int:
        return int(self._settings[key].value)

    def begin_staged_query_timeout(self) -> bool:
        """Reserve one SQL transaction's staged query-timeout ownership."""
        with self._lock:
            if self._timeout_stack:
                return False
            self._staged_query_timeout_transactions += 1
            return True

    def commit_staged(
        self,
        staged: dict[str, str],
        *,
        owns_timeout: bool,
    ) -> None:
        """Atomically publish a validated SQL transaction overlay."""
        with self._lock:
            for key, value in staged.items():
                record = self._settings.get(key)
                if record is not None and record.spec.writable:
                    record.value = value
            if owns_timeout and self._staged_query_timeout_transactions:
                self._staged_query_timeout_transactions -= 1

    def discard_staged(self, *, owns_timeout: bool) -> None:
        """Release timeout ownership for a discarded SQL overlay."""
        if not owns_timeout:
            return
        with self._lock:
            if self._staged_query_timeout_transactions:
                self._staged_query_timeout_transactions -= 1


RuntimeSettingsCore = RuntimeSettings
"""Compatibility alias matching the C++ and Rust core type name."""


def trim_copy(text: str) -> str:
    """Trim leading and trailing ASCII whitespace."""
    return text.strip(" \t\n\r\v\f")


def to_lower_copy(text: str) -> str:
    """Lowercase ASCII letters while leaving non-ASCII codepoints unchanged."""
    return "".join(
        chr(ord(character) + 32) if "A" <= character <= "Z" else character for character in text
    )


def strip_optional_quotes(text: str) -> str:
    """Strip one matching pair of surrounding single or double quotes."""
    if len(text) >= _QUOTE_PAIR_LENGTH and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def parse_int_value(text: str) -> int | None:
    """Parse a complete base-10 signed 32-bit integer."""
    stripped = trim_copy(text)
    if not _INTEGER_PATTERN.fullmatch(stripped):
        return None
    value = int(stripped, 10)
    return value if -(2**31) <= value < 2**31 else None


def parse_bool_value(text: str) -> bool | None:
    """Parse the shared case-insensitive boolean vocabulary."""
    normalized = to_lower_copy(trim_copy(text))
    if normalized in {"1", "on", "true", "yes"}:
        return True
    if normalized in {"0", "off", "false", "no"}:
        return False
    return None


def strip_sql_comments(text: str) -> str:
    """Replace every SQL comment with a single space, leaving literals intact.

    Mirrors the C++ one-pass ``strip_sql_comments``: a line comment (``--``) ends
    at a newline OR a lone carriage return, a block comment (``/* */``) runs to
    ``*/`` or end-of-input, and a doubled quote inside a string literal stays
    inside it. Stripping comments anywhere (not just at the ends) is what lets a
    ``;;`` or an interior comment normalize the same way the C++ scanner does.
    """
    out: list[str] = []
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char in ("'", '"'):
            quote = char
            out.append(char)
            index += 1
            while index < length:
                out.append(text[index])
                if text[index] == quote:
                    if index + 1 < length and text[index + 1] == quote:
                        out.append(text[index + 1])  # escaped quote: stay inside
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            continue
        if char == "-" and index + 1 < length and text[index + 1] == "-":
            index += 2
            while index < length and text[index] not in ("\n", "\r"):
                index += 1
            out.append(" ")
            continue
        if char == "/" and index + 1 < length and text[index + 1] == "*":
            end = text.find("*/", index + 2)
            index = length if end < 0 else end + 2
            out.append(" ")
            continue
        out.append(char)
        index += 1
    return "".join(out)


def parse_runtime_pragma(sql: str, product_prefix: str) -> RuntimePragmaRequest:
    """Parse a product-scoped runtime PRAGMA without executing it."""
    text = trim_copy(strip_sql_comments(sql))
    # Statement terminators (also ``;;`` — comment stripping can expose more than
    # one), each optionally followed by trailing whitespace.
    while text.endswith(";"):
        text = trim_copy(text[:-1])
    if not text:
        return RuntimePragmaRequest()
    keyword = "pragma"
    if not to_lower_copy(text).startswith(keyword):
        return RuntimePragmaRequest()
    # Word boundary: ``pragmatic_helper(...)`` starts with "pragma" but is not the
    # PRAGMA keyword — the next character must be ASCII whitespace.
    if len(text) <= len(keyword) or text[len(keyword)] not in " \t\n\r\v\f":
        return RuntimePragmaRequest()
    body = trim_copy(text[len(keyword) :])
    prefix = f"{to_lower_copy(product_prefix)}."
    if not to_lower_copy(body).startswith(prefix):
        return RuntimePragmaRequest()
    expression = trim_copy(body[len(prefix) :])
    key_expression, separator, value_expression = expression.partition("=")
    return RuntimePragmaRequest(
        matched=True,
        has_value=bool(separator),
        key=to_lower_copy(trim_copy(key_expression)),
        value=strip_optional_quotes(trim_copy(value_expression)) if separator else "",
    )


def unknown_runtime_pragma_error(product_prefix: str) -> str:
    """Return the canonical unknown-key diagnostic."""
    return f"Unknown {product_prefix} pragma key"


def _success(name: str, value: object) -> RuntimePragmaReply:
    return RuntimePragmaReply(
        handled=True,
        success=True,
        name=name,
        value=str(value),
    )


def _failure(message: str) -> RuntimePragmaReply:
    return RuntimePragmaReply(handled=True, error=message)


def handle_common_runtime_pragma(  # noqa: PLR0911 - two actions have exact diagnostics.
    request: RuntimePragmaRequest,
    product_prefix: str,
    settings: RuntimeSettings,
) -> RuntimePragmaReply:
    """Handle the imperative ``timeout_push`` and ``timeout_pop`` PRAGMAs."""
    if not request.matched:
        return RuntimePragmaReply()
    key = request.key
    value = request.value
    if key == "timeout_push":
        if not value:
            return _failure(f"{product_prefix}.timeout_push requires a timeout value")
        parsed = parse_int_value(value)
        if parsed is None or not RuntimeSettings.is_valid_timeout(parsed):
            return _failure(f"Invalid {product_prefix}.timeout_push value")
        effective = settings.timeout_push(parsed)
        if effective is None:
            if settings.staged_query_timeout_active:
                return _failure(
                    f"{product_prefix}.timeout_push is unavailable while "
                    "runtime_settings has a staged query_timeout_ms write",
                )
            return _failure(
                f"{product_prefix}.timeout_push stack full "
                f"(max {settings.max_timeout_stack_depth} entries)"
            )
        return _success("query_timeout_ms", effective)
    if key == "timeout_pop":
        effective = settings.timeout_pop()
        if effective is None:
            if settings.staged_query_timeout_active:
                return _failure(
                    f"{product_prefix}.timeout_pop is unavailable while "
                    "runtime_settings has a staged query_timeout_ms write",
                )
            return _failure(f"{product_prefix}.timeout_pop stack is empty")
        return _success("query_timeout_ms", effective)
    return RuntimePragmaReply()


def _empty_staged_values() -> dict[str, str]:
    return {}


def _empty_staged_savepoints() -> dict[int, dict[str, str]]:
    return {}


@dataclass(slots=True)
class _RuntimeSettingsTransaction:
    staged: dict[str, str] = field(default_factory=_empty_staged_values)
    savepoints: dict[int, dict[str, str]] = field(
        default_factory=_empty_staged_savepoints,
    )
    owns_staged_query_timeout: bool = False
    statement_baseline: dict[str, str] = field(default_factory=_empty_staged_values)
    statement_baseline_owns_timeout: bool = False

    def _libxsql_begin_statement(self) -> None:
        self.statement_baseline = dict(self.staged)
        self.statement_baseline_owns_timeout = self.owns_staged_query_timeout


def _settings_transaction(state: object) -> _RuntimeSettingsTransaction:
    if not isinstance(state, _RuntimeSettingsTransaction):
        message = "runtime_settings: invalid connection-local transaction state"
        raise TypeError(message)
    return state


def define_runtime_settings_table(  # noqa: C901, PLR0915 - one cohesive adapter.
    settings: RuntimeSettings,
    product_prefix: str = "libxsql",
) -> TableDefinition[RuntimeSettingEntry]:
    """Build the canonical transactional ``runtime_settings`` virtual table.

    Ordinary settings are writable through the ``value`` column. Discovery
    fields and imperative ``timeout_push``/``timeout_pop`` entries remain
    read-only. Each registration owns an isolated staged overlay with
    read-your-writes, commit, rollback, and savepoint behavior.
    """
    from .vtable import (  # noqa: PLC0415 - avoids a module import cycle
        TransactionHooks,
        cached_table,
    )

    def rows_for_state(state: object) -> tuple[_RuntimeSettingsTableRow, ...]:
        transaction = _settings_transaction(state)
        return tuple(
            _RuntimeSettingsTableRow(
                replace(
                    entry,
                    value=transaction.staged.get(entry.key, entry.value),
                ),
                state,
            )
            for entry in settings.enumerate()
        )

    def restore_statement(transaction: _RuntimeSettingsTransaction) -> None:
        target_has_timeout = transaction.statement_baseline_owns_timeout
        if transaction.owns_staged_query_timeout and not target_has_timeout:
            settings.discard_staged(owns_timeout=True)
        elif (
            not transaction.owns_staged_query_timeout
            and target_has_timeout
            and not settings.begin_staged_query_timeout()
        ):
            message = (
                "runtime_settings: cannot restore statement query_timeout_ms "
                "while the timeout stack is active"
            )
            raise RuntimeError(message)
        transaction.staged = dict(transaction.statement_baseline)
        transaction.owns_staged_query_timeout = target_has_timeout

    def stage_value(
        row: _RuntimeSettingsTableRow,
        value: SQLiteValue,
        transaction: _RuntimeSettingsTransaction,
    ) -> None:
        if row.scope == "action":
            message = f"runtime_settings: {row.key!r} is a PRAGMA verb, not a settable value"
            raise ReadOnlyError(message)
        if value is None:
            message = f"runtime_settings: {row.key!r} cannot be set to NULL"
            raise ValueError(message)
        reply = settings.normalize(row.key, str(value), product_prefix)
        if not reply.handled:
            message = f"runtime_settings: unknown key {row.key!r}"
            raise ReadOnlyError(message)
        if not reply.success:
            if "read-only" in reply.error:
                raise ReadOnlyError(reply.error)
            raise ValueError(reply.error)
        first_timeout = row.key == "query_timeout_ms" and row.key not in transaction.staged
        if first_timeout and not settings.begin_staged_query_timeout():
            message = (
                "runtime_settings: query_timeout_ms cannot be staged while "
                "the timeout stack is active"
            )
            raise ValueError(message)
        transaction.staged[row.key] = reply.value
        if first_timeout:
            transaction.owns_staged_query_timeout = True
        row.value = reply.value

    def set_value(row: _RuntimeSettingsTableRow, value: SQLiteValue) -> bool:
        transaction = _settings_transaction(row.connection_state)
        try:
            stage_value(row, value, transaction)
        except BaseException:
            restore_statement(transaction)
            raise
        return True

    def row_lookup(state: object, rowid: int) -> _RuntimeSettingsTableRow | None:
        rows = rows_for_state(state)
        return rows[rowid] if 0 <= rowid < len(rows) else None

    def commit(state: object) -> None:
        transaction = _settings_transaction(state)
        settings.commit_staged(
            transaction.staged,
            owns_timeout=transaction.owns_staged_query_timeout,
        )
        transaction.staged.clear()
        transaction.savepoints.clear()
        transaction.owns_staged_query_timeout = False
        transaction.statement_baseline.clear()
        transaction.statement_baseline_owns_timeout = False

    def rollback(state: object) -> None:
        transaction = _settings_transaction(state)
        settings.discard_staged(
            owns_timeout=transaction.owns_staged_query_timeout,
        )
        transaction.staged.clear()
        transaction.savepoints.clear()
        transaction.owns_staged_query_timeout = False
        transaction.statement_baseline.clear()
        transaction.statement_baseline_owns_timeout = False

    def savepoint(state: object, level: int) -> None:
        transaction = _settings_transaction(state)
        transaction.savepoints[level] = dict(transaction.staged)

    def release(state: object, level: int) -> None:
        transaction = _settings_transaction(state)
        transaction.savepoints = {
            saved: snapshot for saved, snapshot in transaction.savepoints.items() if saved < level
        }

    def rollback_to(state: object, level: int) -> None:
        transaction = _settings_transaction(state)
        target = dict(transaction.savepoints.get(level, {}))
        target_has_timeout = "query_timeout_ms" in target
        current_has_timeout = transaction.owns_staged_query_timeout
        if (
            not current_has_timeout
            and target_has_timeout
            and not settings.begin_staged_query_timeout()
        ):
            message = (
                "runtime_settings: cannot restore staged query_timeout_ms "
                "while the timeout stack is active"
            )
            raise ValueError(message)
        if current_has_timeout and not target_has_timeout:
            settings.discard_staged(owns_timeout=True)
        transaction.staged = target
        transaction.owns_staged_query_timeout = target_has_timeout
        transaction.statement_baseline = dict(target)
        transaction.statement_baseline_owns_timeout = target_has_timeout
        transaction.savepoints = {
            saved: snapshot for saved, snapshot in transaction.savepoints.items() if saved <= level
        }

    hooks = TransactionHooks(
        state_factory=_RuntimeSettingsTransaction,
        commit=commit,
        rollback=rollback,
        savepoint=savepoint,
        release=release,
        rollback_to=rollback_to,
    )

    definition = (
        cached_table("runtime_settings")
        .estimate_rows(lambda: len(settings.specs()))
        .stateful_cache_builder(rows_for_state)
        .stateful_row_lookup(row_lookup)
        .transaction_hooks(hooks)
        .column("key", str, attr="key")
        .column("value", str, attr="value", set=set_value, nullable=True)
        .column("type", str, attr="value_type")
        .column("scope", str, attr="scope")
        .build()
    )
    # The public row type is the immutable four-field discovery value. The
    # virtual-table adapter carries connection-local transaction state in a
    # private wrapper that never escapes through ``RuntimeSettings.enumerate``.
    return cast("TableDefinition[RuntimeSettingEntry]", definition)


__all__ = [
    "MAX_QUEUE_LIMIT",
    "MAX_TIMEOUT_MS",
    "RuntimePragmaReply",
    "RuntimePragmaRequest",
    "RuntimeSettingEntry",
    "RuntimeSettingSpec",
    "RuntimeSettingType",
    "RuntimeSettings",
    "RuntimeSettingsCore",
    "RuntimeSettingsCoreOptions",
    "RuntimeSettingsSnapshot",
    "define_runtime_settings_table",
    "handle_common_runtime_pragma",
    "parse_bool_value",
    "parse_int_value",
    "parse_runtime_pragma",
    "strip_optional_quotes",
    "strip_sql_comments",
    "to_lower_copy",
    "trim_copy",
    "unknown_runtime_pragma_error",
]
