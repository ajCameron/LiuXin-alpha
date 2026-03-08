"""
Build SquashFS archives from a JSON file manifest.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import tempfile

from typing import Any, Iterable, Optional


_SOURCE_KEYS = ("source", "src", "path", "file")
_TARGET_KEYS = ("archive_path", "internal_path", "dest", "target")


@dataclasses.dataclass(frozen=True)
class SquashfsManifestEntry:
    """One source-to-archive mapping entry loaded from a build manifest."""
    source_path: pathlib.Path
    archive_path: str


@dataclasses.dataclass(frozen=True)
class SquashfsBuildReport:
    """Summary metadata captured after a SquashFS build run."""
    manifest_path: str
    output_archive: str
    file_count: int
    total_input_bytes: int
    output_bytes: int
    compression: str
    deterministic: bool
    manifest_sha256: str
    output_sha256: str
    mksquashfs_executable: str
    mksquashfs_version: Optional[str]
    build_flags: tuple[str, ...]


def _normalize_archive_path(raw: str) -> str:
    text = str(raw).strip().replace("\\", "/")
    if not text:
        raise ValueError("archive_path cannot be empty.")
    if text.startswith("/"):
        raise ValueError("archive_path must be relative, got absolute path: {!r}".format(raw))

    parts: list[str] = []
    for part in text.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            raise ValueError("archive_path cannot contain '..': {!r}".format(raw))
        parts.append(part)
    if not parts:
        raise ValueError("archive_path resolves to empty path: {!r}".format(raw))
    return "/".join(parts)


def _pick_key(mapping: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _sha256_file(path: pathlib.Path, *, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _detect_mksquashfs_version(executable: str) -> Optional[str]:
    try:
        proc = subprocess.run(
            [executable, "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except Exception:
        return None
    text = (proc.stdout + b"\n" + proc.stderr).decode("utf-8", "replace")
    for raw in text.splitlines():
        line = raw.strip()
        if line:
            return line
    return None


def load_manifest_entries(
    manifest_path: pathlib.Path,
    *,
    manifest_base_dir: pathlib.Path | None = None,
) -> list[SquashfsManifestEntry]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        rows = payload.get("files")
    else:
        rows = payload
    if not isinstance(rows, list):
        raise TypeError("Manifest must be a list, or an object with a 'files' list.")

    base_dir = (manifest_base_dir or manifest_path.parent).expanduser().resolve()
    entries: list[SquashfsManifestEntry] = []
    seen_targets: set[str] = set()

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            raise TypeError("Manifest entry {} must be an object.".format(idx))

        src_raw = _pick_key(row, _SOURCE_KEYS)
        if src_raw is None:
            raise ValueError("Manifest entry {} has no source path key.".format(idx))

        src_path = pathlib.Path(str(src_raw)).expanduser()
        if not src_path.is_absolute():
            src_path = base_dir / src_path
        src_path = src_path.resolve()
        if not src_path.exists() or not src_path.is_file():
            raise FileNotFoundError("Manifest entry {} source file not found: {!r}".format(idx, str(src_path)))

        target_raw = _pick_key(row, _TARGET_KEYS)
        if target_raw is None:
            target_raw = src_path.name
        archive_path = _normalize_archive_path(str(target_raw))
        if archive_path in seen_targets:
            raise ValueError("Duplicate archive_path in manifest: {!r}".format(archive_path))
        seen_targets.add(archive_path)

        entries.append(SquashfsManifestEntry(source_path=src_path, archive_path=archive_path))

    if not entries:
        raise ValueError("Manifest is empty; no files to pack.")
    return entries


def build_squashfs_from_manifest(
    manifest_path: pathlib.Path | str,
    output_archive: pathlib.Path | str,
    *,
    manifest_base_dir: pathlib.Path | str | None = None,
    compression: str = "zstd",
    deterministic: bool = False,
    force: bool = False,
    quiet: bool = True,
    mksquashfs_exe: str = "mksquashfs",
) -> SquashfsBuildReport:
    manifest_path = pathlib.Path(manifest_path).expanduser().resolve()
    if not manifest_path.exists():
        raise FileNotFoundError("Manifest not found: {!r}".format(str(manifest_path)))

    if manifest_base_dir is None:
        resolved_base = None
    else:
        resolved_base = pathlib.Path(manifest_base_dir).expanduser().resolve()

    entries = load_manifest_entries(manifest_path, manifest_base_dir=resolved_base)
    manifest_sha256 = _sha256_file(manifest_path)

    output_archive = pathlib.Path(output_archive).expanduser().resolve()
    output_archive.parent.mkdir(parents=True, exist_ok=True)
    if output_archive.exists():
        if not force:
            raise FileExistsError(
                "Output archive already exists (use force=True to overwrite): {!r}".format(str(output_archive))
            )
        output_archive.unlink()

    total_input_bytes = sum(int(entry.source_path.stat().st_size) for entry in entries)

    mksquashfs = shutil.which(mksquashfs_exe) or mksquashfs_exe
    mksquashfs_version = _detect_mksquashfs_version(mksquashfs)
    build_flags: list[str] = ["-noappend", "-comp", str(compression)]
    if deterministic:
        build_flags.extend(["-all-root", "-no-xattrs", "-all-time", "0", "-mkfs-time", "0"])
    if quiet:
        build_flags.append("-quiet")

    with tempfile.TemporaryDirectory(prefix="liuxin-squashfs-pack-") as tmp_dir:
        staging_root = pathlib.Path(tmp_dir) / "root"
        staging_root.mkdir(parents=True, exist_ok=True)

        for entry in entries:
            target = staging_root.joinpath(*entry.archive_path.split("/"))
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(entry.source_path, target)
            except OSError:
                shutil.copy2(entry.source_path, target)

        cmd = [
            mksquashfs,
            str(staging_root),
            str(output_archive),
            "-noappend",
            "-comp",
            str(compression),
        ]
        if deterministic:
            cmd.extend(["-all-root", "-no-xattrs", "-all-time", "0", "-mkfs-time", "0"])
        if quiet:
            cmd.append("-quiet")

        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if proc.returncode != 0:
            raise RuntimeError(
                "mksquashfs failed (rc={}): {}".format(
                    proc.returncode,
                    proc.stderr.decode("utf-8", "replace").strip(),
                )
            )

    output_sha256 = _sha256_file(output_archive)
    return SquashfsBuildReport(
        manifest_path=str(manifest_path),
        output_archive=str(output_archive),
        file_count=len(entries),
        total_input_bytes=total_input_bytes,
        output_bytes=int(output_archive.stat().st_size),
        compression=str(compression),
        deterministic=bool(deterministic),
        manifest_sha256=manifest_sha256,
        output_sha256=output_sha256,
        mksquashfs_executable=str(mksquashfs),
        mksquashfs_version=mksquashfs_version,
        build_flags=tuple(build_flags),
    )
