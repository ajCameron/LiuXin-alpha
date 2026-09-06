"""Storage CLI ingest preflight ownership."""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
from pathlib import Path

from LiuXin_alpha.surfaces.cli.storage_commands.filesystem import (
    _nearest_existing_parent,
)


def _preflight_checks(
    args: argparse.Namespace,
    source_root: Path,
    recognized_formats: tuple[tuple[str, int], ...],
) -> list[dict[str, object]]:
    formats = dict(recognized_formats)
    checks: list[dict[str, object]] = []

    def add(
        name: str,
        ok: bool,
        message: str,
        *,
        severity: str = "error",
        **details: object,
    ) -> None:
        checks.append(
            {
                "name": name,
                "ok": bool(ok),
                "severity": severity,
                "message": message,
                **details,
            }
        )

    add(
        "source_readable",
        os.access(source_root, os.R_OK | os.X_OK),
        "source root is readable/searchable"
        if os.access(source_root, os.R_OK | os.X_OK)
        else "source root is not readable/searchable",
        path=str(source_root),
    )
    database_path = Path(args.database).expanduser().resolve(strict=False)
    database_parent = _nearest_existing_parent(database_path.parent)
    database_ok = (
        os.access(database_path, os.R_OK | os.W_OK)
        if database_path.exists()
        else os.access(database_parent, os.W_OK | os.X_OK)
    )
    add(
        "database_writable",
        database_ok,
        "existing catalogue is readable/writable"
        if database_path.exists() and database_ok
        else (
            "catalogue parent can create the database"
            if database_ok
            else "catalogue path is not writable"
        ),
        path=str(database_path),
        exists=database_path.exists(),
        free_bytes=shutil.disk_usage(database_parent).free,
    )
    if args.materialization_root:
        materialization = (
            Path(args.materialization_root).expanduser().resolve(strict=False)
        )
        materialization_parent = _nearest_existing_parent(materialization)
        writable = os.access(materialization_parent, os.W_OK | os.X_OK)
        add(
            "materialization_writable",
            writable,
            "materialization path is writable"
            if writable
            else "materialization path is not writable",
            path=str(materialization),
            free_bytes=shutil.disk_usage(materialization_parent).free,
        )
    elif not bool(args.no_nested_containers):
        add(
            "materialization_configured",
            False,
            "no cache is configured; nested containers will be catalogued but not opened",
            severity="warning",
        )

    if formats.get("squashfs", 0):
        executable = shutil.which(str(args.unsquashfs_exe))
        add(
            "squashfs_reader",
            executable is not None,
            f"unsquashfs available at {executable}"
            if executable
            else f"unsquashfs executable not found: {args.unsquashfs_exe}",
            executable=executable,
        )
    if formats.get("7z", 0):
        available = importlib.util.find_spec("py7zr") is not None
        add(
            "sevenzip_reader",
            available,
            "py7zr is installed"
            if available
            else "install LiuXin's archives extra for py7zr",
        )
    if formats.get("rar", 0):
        module_available = importlib.util.find_spec("rarfile") is not None
        extractor = (
            shutil.which(str(args.rar_extractor_exe))
            if args.rar_extractor_exe
            else shutil.which("unrar") or shutil.which("rar")
        )
        add(
            "rar_extended_readers",
            module_available or extractor is not None,
            "RAR optional reader/extractor is available"
            if module_available or extractor
            else "stored RAR 3/4 members remain available; RAR 5/compressed members may fail",
            severity="warning",
            rarfile_available=module_available,
            extractor=extractor,
        )
    if formats.get("iso", 0):
        udf_available = importlib.util.find_spec("pycdlib") is not None
        add(
            "udf_bridge_reader",
            udf_available,
            "pycdlib is installed for UDF bridge namespaces"
            if udf_available
            else "ISO 9660 remains available; install the archives extra for UDF bridge support",
            severity="warning",
        )
    return checks
