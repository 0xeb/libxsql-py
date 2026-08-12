# Copyright (c) 2024-2026 Elias Bachaalany
# SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
"""Rebuild a wheel from the sdist and compare the import-package payload."""

from __future__ import annotations

import argparse
import hashlib
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from zipfile import ZipFile

_TEXT_SUFFIXES = frozenset({".md", ".py", ".toml", ".txt", ".yaml", ".yml"})
_FORBIDDEN_PATH_PARTS = frozenset(
    {
        ".coverage",
        ".git",
        ".hypothesis",
        ".mypy_cache",
        ".pytest_cache",
        ".pyright",
        ".ruff_cache",
        ".venv",
        "__pycache__",
        "build",
        "dist",
        "site",
    }
)


def _ascii(hexadecimal: str) -> str:
    """Decode a scanner term without making this source match itself."""
    return bytes.fromhex(hexadecimal).decode("ascii")


_PRIVATE_KB_PATH = _ascii("6b622f")
_PRIVATE_TEST_PATH = _ascii("74657374732f6c69627873716c2d7079")
_PRIVATE_REMOTE = _ascii("737061726b2d676974")
_PRIVATE_MONOREPO = _ascii("7873716c206d6f6e6f7265706f")
_MACOS_USER_HOME = _ascii("2f55736572732f")
_PRIVATE_TEXT = re.compile(
    rf"(?i)(?:\b{re.escape(_PRIVATE_KB_PATH)}"
    rf"|{re.escape(_PRIVATE_TEST_PATH)}"
    rf"|{re.escape(_PRIVATE_REMOTE)}"
    rf"|{re.escape(_PRIVATE_MONOREPO)}"
    rf"|[a-z]:[/\\]Users[/\\]"
    rf"|{re.escape(_MACOS_USER_HOME)})"
)
_CREDENTIAL_TEXT = re.compile(
    r"(?:github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|"
    r"AKIA[0-9A-Z]{16}|-----BEGIN (?:OPENSSH |RSA |EC )?PRIVATE KEY-----)"
)


def _one_artifact(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern))
    if len(matches) != 1:
        message = f"expected exactly one {pattern!r} artifact in {directory}, found {len(matches)}"
        raise RuntimeError(message)
    return matches[0]


def _package_payload(wheel: Path) -> dict[str, str]:
    with ZipFile(wheel) as archive:
        return {
            name: hashlib.sha256(archive.read(name)).hexdigest()
            for name in sorted(archive.namelist())
            if name.startswith("libxsql/") and not name.endswith("/")
        }


def _wheel_members(wheel: Path) -> dict[str, bytes]:
    with ZipFile(wheel) as archive:
        return {name: archive.read(name) for name in archive.namelist() if not name.endswith("/")}


def _sdist_members(sdist: Path) -> dict[str, bytes]:
    members: dict[str, bytes] = {}
    with tarfile.open(sdist, "r:gz") as archive:
        for member in archive.getmembers():
            if not member.isfile():
                continue
            extracted = archive.extractfile(member)
            if extracted is None:
                msg = f"could not read sdist member {member.name!r}"
                raise RuntimeError(msg)
            members[member.name] = extracted.read()
    return members


def _require_suffix(members: dict[str, bytes], suffix: str, artifact: Path) -> str:
    matches = [name for name in members if name.endswith(suffix)]
    if len(matches) != 1:
        message = (
            f"{artifact.name}: expected exactly one member ending in {suffix!r}, "
            f"found {len(matches)}"
        )
        raise RuntimeError(message)
    return matches[0]


def _require_sdist_path(
    members: dict[str, bytes],
    relative_path: str,
    artifact: Path,
) -> str:
    expected_parts = Path(relative_path).parts
    matches = [
        name
        for name in members
        if len(Path(name).parts) > 1 and Path(name).parts[1:] == expected_parts
    ]
    if len(matches) != 1:
        message = (
            f"{artifact.name}: expected exactly one source member {relative_path!r}, "
            f"found {len(matches)}"
        )
        raise RuntimeError(message)
    return matches[0]


def _validate_member_names(members: dict[str, bytes], artifact: Path) -> None:
    for name in members:
        parts = Path(name).parts
        if name.endswith("AGENTS.md") or any(part in _FORBIDDEN_PATH_PARTS for part in parts):
            message = f"{artifact.name}: forbidden archive member {name!r}"
            raise RuntimeError(message)


def _validate_member_text(members: dict[str, bytes], artifact: Path) -> None:
    for name, content in members.items():
        if Path(name).suffix not in _TEXT_SUFFIXES and not name.endswith("METADATA"):
            continue
        text = content.decode("utf-8", errors="replace")
        if _PRIVATE_TEXT.search(text):
            message = f"{artifact.name}: private path or remote reference in {name!r}"
            raise RuntimeError(message)
        if _CREDENTIAL_TEXT.search(text):
            message = f"{artifact.name}: credential-like material in {name!r}"
            raise RuntimeError(message)


def _validate_archive_contents(wheel: Path, sdist: Path) -> None:
    wheel_members = _wheel_members(wheel)
    sdist_members = _sdist_members(sdist)
    for artifact, members in ((wheel, wheel_members), (sdist, sdist_members)):
        _validate_member_names(members, artifact)
        _validate_member_text(members, artifact)

    _require_suffix(wheel_members, "libxsql/py.typed", wheel)
    _require_suffix(wheel_members, ".dist-info/licenses/LICENSE", wheel)
    _require_suffix(wheel_members, ".dist-info/licenses/THIRD_PARTY_NOTICES", wheel)
    metadata_name = _require_suffix(wheel_members, ".dist-info/METADATA", wheel)
    metadata = wheel_members[metadata_name].decode("utf-8")
    if "Description-Content-Type: text/markdown" not in metadata:
        message = f"{wheel.name}: README content type is missing from core metadata"
        raise RuntimeError(message)
    if "# libxsql for Python" not in metadata:
        message = f"{wheel.name}: README is not embedded in core metadata"
        raise RuntimeError(message)

    for relative_path in (
        "README.md",
        "LICENSE",
        "THIRD_PARTY_NOTICES",
        "src/libxsql/py.typed",
        "examples/README.md",
        "examples/manifest.toml",
        "examples/store_showcase.py",
        "scripts/run_examples.py",
        "scripts/verify_artifacts.py",
    ):
        _require_sdist_path(sdist_members, relative_path, sdist)


def _compare_payloads(expected_wheel: Path, rebuilt_wheel: Path) -> None:
    expected = _package_payload(expected_wheel)
    rebuilt = _package_payload(rebuilt_wheel)
    if expected == rebuilt:
        return

    differing = sorted(
        name for name in expected.keys() | rebuilt.keys() if expected.get(name) != rebuilt.get(name)
    )
    message = "sdist-built wheel package payload differs: " + ", ".join(differing)
    raise RuntimeError(message)


def verify_artifacts(directory: Path) -> None:
    """Verify one wheel and sdist in ``directory``.

    Raises:
        RuntimeError: The artifact set or package payload is inconsistent.
        subprocess.CalledProcessError: Rebuilding the sdist fails.
    """
    direct_wheel = _one_artifact(directory, "libxsql-*.whl")
    sdist = _one_artifact(directory, "libxsql-*.tar.gz")
    _validate_archive_contents(direct_wheel, sdist)

    with tempfile.TemporaryDirectory(prefix="libxsql-artifacts-") as temporary:
        temporary_path = Path(temporary)
        unpacked = temporary_path / "source"
        wheel_output = temporary_path / "wheel"
        shutil.unpack_archive(sdist, unpacked)
        source_roots = [entry for entry in unpacked.iterdir() if entry.is_dir()]
        if len(source_roots) != 1:
            message = f"expected one sdist source root, found {len(source_roots)}"
            raise RuntimeError(message)

        subprocess.run(  # noqa: S603 - exact interpreter and build arguments
            [
                sys.executable,
                "-m",
                "build",
                "--wheel",
                "--outdir",
                str(wheel_output),
                str(source_roots[0]),
            ],
            check=True,
        )
        rebuilt_wheel = _one_artifact(wheel_output, "libxsql-*.whl")
        _compare_payloads(direct_wheel, rebuilt_wheel)
        rebuilt_name = rebuilt_wheel.name

    print(
        f"artifact parity passed: {direct_wheel.name} == "
        f"{sdist.name} -> {rebuilt_name} package payload"
    )


def main() -> int:
    """Run the artifact parity check."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "directory",
        nargs="?",
        type=Path,
        default=Path("dist"),
        help="directory containing exactly one libxsql wheel and sdist",
    )
    arguments = parser.parse_args()
    verify_artifacts(arguments.directory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
