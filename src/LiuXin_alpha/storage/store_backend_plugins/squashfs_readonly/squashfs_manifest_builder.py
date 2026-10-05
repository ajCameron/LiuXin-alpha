"""
Load JSON source mappings and build a SquashFS image through temporary staging.

This standalone helper writes the output path directly and can unlink an existing
image before a forced build. Sources may be hard-linked into staging. Its report
records hashes, sizes, and tool settings rather than transactional publication
or independently validated archive contents.
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
    """
    Retain one source pathname and normalized archive key without constructor validation.

    Example:
        >>> SquashfsManifestEntry(pathlib.Path("source.epub"), "books/a.epub").archive_path
        'books/a.epub'


    :ivar source_path: Local regular source file when produced by load_manifest_entries.
    :ivar archive_path: Normalized relative destination key when produced by the loader.
    """
    source_path: pathlib.Path
    archive_path: str


@dataclasses.dataclass(frozen=True)
class SquashfsBuildReport:
    """
    Retain observations and invocation settings from a completed manifest build.

    The frozen record performs no validation. Hashes and sizes are collected at separate times;
    deterministic records the requested flag set, not a cross-tool reproducibility guarantee.

    Example:
        >>> report = build_squashfs_from_manifest("manifest.json", "library.sqsh")  # doctest: +SKIP
        >>> report.file_count  # doctest: +SKIP
        2


    :ivar manifest_path: Resolved manifest pathname.
    :ivar output_archive: Resolved output image pathname.
    :ivar file_count: Number of manifest entries.
    :ivar total_input_bytes: Sum of source sizes observed before staging.
    :ivar output_bytes: Image size observed after output hashing.
    :ivar compression: Compression argument passed to mksquashfs.
    :ivar deterministic: Whether fixed owner/time and no-xattr flags were requested.
    :ivar manifest_sha256: SHA-256 of manifest bytes read after loading entries.
    :ivar output_sha256: SHA-256 of completed output bytes.
    :ivar mksquashfs_executable: Resolved executable or configured fallback name.
    :ivar mksquashfs_version: First nonempty version-output line, or None when unavailable.
    :ivar build_flags: Recorded option arguments, excluding executable and path arguments.
    """
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
    """
    Replace backslashes with slashes and remove empty or dot components from a relative target.

    Absolute paths, parent components, and an empty normalized result raise ValueError. Significant
    spaces and Unicode spelling are preserved. This helper does not enforce the reader's full key,
    NUL, depth, or byte-limit policy.

    Example:
        >>> _normalize_archive_path("books//./ leading.epub ")
        'books/ leading.epub '


    :param raw: Target text stringified before separator normalization.
    :return: Nonempty slash-separated relative key.
    """
    text = str(raw).replace("\\", "/")
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
    """
    Return the value for the first present key in the supplied priority order.

    Presence wins even when its value is None; later aliases are not then considered.

    Example:
        >>> _pick_key({"src": "book", "source": None}, ("source", "src")) is None
        True


    :param mapping: Entry dictionary holding alternative field spellings.
    :param keys: Ordered candidate keys, consumed only until the first match.
    :return: First present value, or None if no candidate key exists.
    """
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _sha256_file(path: pathlib.Path, *, chunk_size: int = 1024 * 1024) -> str:
    """
    Hash a locally opened binary file through repeated read requests and close it on exit.

    Chunk size is trusted: zero hashes no input, and a negative value can request the whole
    remaining file. No before/after signature comparison detects concurrent changes.

    Example:
        >>> _sha256_file(pathlib.Path("source.epub"))  # doctest: +SKIP


    :param path: Local pathname to open in binary read mode.
    :param chunk_size: Bytes requested per read; callers normally use the positive 1 MiB default.
    :return: Lowercase SHA-256 hexadecimal string; file/read errors propagate.
    """
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _detect_mksquashfs_version(executable: str) -> Optional[str]:
    """
    Run the version command and return the first nonempty decoded output line.

    Stdout precedes stderr in the combined text and UTF-8 errors are replaced. Command-start/run
    Exceptions produce None; exit status is ignored. Both output streams are captured without a size
    limit or timeout.

    Example:
        >>> _detect_mksquashfs_version("mksquashfs")  # doctest: +SKIP


    :param executable: Executable name or path passed directly to subprocess.run.
    :return: First stripped nonempty line, or None after a run Exception or empty output.
    """
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
    """
    Load a nonempty JSON entry list, resolve sources, and reject duplicate normalized targets.

    The top-level value is a list or an object containing a files list. Source aliases are tried in
    source/src/path/file order, target aliases in archive_path/internal_path/dest/target order. A
    missing or None target uses the resolved source basename. Source symlinks are resolved and may
    lead outside the manifest directory.

    Entries retain manifest order. Invalid JSON, row shapes, absent/non-regular sources, malformed
    targets, or duplicates raise before returning. The loader does not hash sources, reject
    ancestor/file target conflicts, or apply raw-reader expansion limits.

    Example:
        >>> entries = load_manifest_entries(pathlib.Path("manifest.json"))  # doctest: +SKIP
        >>> entries[0].archive_path  # doctest: +SKIP
        'books/a.epub'


    :param manifest_path: Path to UTF-8 JSON read directly by this function.
    :param manifest_base_dir: Base for relative sources, expanded and resolved; None uses the manifest parent.
    :return: Nonempty list of source/key records with resolved regular source paths.
    """
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
    """
    Stage manifest mappings and run mksquashfs directly against the destination path.

    Load entries and hash the manifest, then create the output parent. If force is enabled, an
    existing output is unlinked before source stats, staging, or tool execution; a later failure
    does not restore it. The helper has no candidate-publication transaction or reader-based content
    validation.

    Each staged source is hard-linked where possible, falling back to copy2 on OSError. Hard links
    share source bytes and do not freeze concurrent edits. Temporary staging is cleaned on context
    exit. Version and build commands capture output without timeout or size limits; nonzero build
    status raises RuntimeError and may leave partial output. Success hashes and stats the output
    without fsync or a concurrent-change check.

    Example:
        >>> report = build_squashfs_from_manifest("manifest.json", "library.sqsh", deterministic=True)  # doctest: +SKIP


    :param manifest_path: Manifest path expanded and resolved before loading.
    :param output_archive: Destination image pathname expanded and resolved before parent creation.
    :param manifest_base_dir: Optional base directory for relative sources; None uses the manifest parent.
    :param compression: Codec argument stringified for mksquashfs without local codec validation.
    :param deterministic: Request root ownership, no xattrs, and zero file/filesystem timestamps.
    :param force: Allow an existing destination to be removed before the build begins.
    :param quiet: Add -quiet; both process streams are captured in either mode.
    :param mksquashfs_exe: Executable name or path resolved with which or used as configured.
    :return: SquashfsBuildReport with separately observed hashes, sizes, and invocation settings.
    """
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
