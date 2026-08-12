# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Run the public examples against libxsql installed in this interpreter."""

from __future__ import annotations

import argparse
import importlib.metadata
import os
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict, cast

_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
_EXAMPLE_ROOT = _PACKAGE_ROOT / "examples"
_MANIFEST_PATH = _EXAMPLE_ROOT / "manifest.toml"
_TIMEOUT_SECONDS = 30


class _ExampleRecord(TypedDict):
    """One raw TOML example record."""

    name: str
    script: str
    summary: str
    extras: list[str]
    backends: list[str]
    reference: str


class _Manifest(TypedDict):
    """Typed public example manifest."""

    schema_version: int
    example: list[_ExampleRecord]


@dataclass(frozen=True, slots=True)
class ExampleCase:
    """One executable script/backend combination."""

    name: str
    script: Path
    backend: str

    @property
    def label(self) -> str:
        """Return the deterministic case label."""
        if self.backend == "sync":
            return self.name
        return f"{self.name}[{self.backend}]"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic libxsql examples with the current interpreter.",
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--all",
        action="store_true",
        help="run every example (the default)",
    )
    selection.add_argument(
        "--example",
        action="append",
        metavar="NAME",
        help="run one named example; repeat to select several",
    )
    return parser


def _records() -> tuple[_ExampleRecord, ...]:
    raw = cast("_Manifest", tomllib.loads(_MANIFEST_PATH.read_text(encoding="utf-8")))
    if raw["schema_version"] != 1:
        message = f"unsupported example manifest schema: {raw['schema_version']}"
        raise ValueError(message)
    return tuple(raw["example"])


def _cases(selected: frozenset[str] | None) -> tuple[ExampleCase, ...]:
    cases: list[ExampleCase] = []
    available: set[str] = set()
    for record in _records():
        name = record["name"]
        available.add(name)
        if selected is not None and name not in selected:
            continue
        cases.extend(
            (
                ExampleCase(
                    name=name,
                    script=_EXAMPLE_ROOT / record["script"],
                    backend=backend,
                )
                for backend in record["backends"]
            ),
        )
    if selected is not None and (unknown := selected - available):
        message = "unknown example(s): " + ", ".join(sorted(unknown))
        raise ValueError(message)
    return tuple(cases)


def _run(case: ExampleCase) -> None:
    environment = os.environ.copy()
    if case.backend != "sync":
        environment["LIBXSQL_EXAMPLE_BACKEND"] = case.backend
    else:
        environment.pop("LIBXSQL_EXAMPLE_BACKEND", None)
    with tempfile.TemporaryDirectory(prefix="libxsql-examples-") as temporary_directory:
        completed = subprocess.run(  # noqa: S603 - fixed interpreter and manifest path
            [sys.executable, str(case.script)],
            cwd=temporary_directory,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=_TIMEOUT_SECONDS,
        )
    if completed.returncode != 0 or completed.stderr:
        message = (
            f"{case.label} failed with exit code {completed.returncode}\n"
            f"stdout:\n{completed.stdout}"
            f"stderr:\n{completed.stderr}"
        )
        raise RuntimeError(message)


def main(arguments: list[str] | None = None) -> int:
    """Run selected examples and return a process exit status."""
    parsed = _parser().parse_args(arguments)
    selected = frozenset(cast("list[str]", parsed.example)) if parsed.example is not None else None
    try:
        importlib.metadata.distribution("libxsql")
        cases = _cases(selected)
        for case in cases:
            _run(case)
            print(f"PASS {case.label}")
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"FAIL {error}", file=sys.stderr)
        return 1
    print(f"{len(cases)}/{len(cases)} examples passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
