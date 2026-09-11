"""
Observe ingest path access, available space, and format-specific reader availability.

Checks do not open/validate a catalogue schema, extract a container, reserve space,
or compare free bytes with the requested budget. Module specifications and executable
discovery are availability hints, not proof a dependency can process a given file.
The caller combines error-severity checks with discovery health; warnings remain advisory.
"""

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
    """
    Build ordered access/capacity/dependency observations after source-format discovery.

    Repeated recognized-format names collapse through dict conversion. Check source
    read/traverse access, existing-database read/write or parent create access, and
    optional cache-parent write/traverse access. Space is reported, not thresholded.
    Missing cache with nested traversal enabled is a warning. Recognized SquashFS/7z
    require unsquashfs/py7zr; extended RAR and ISO-UDF dependencies are warnings.

    Paths and access may be observed more than once, so fields can differ under
    concurrent change. Disk-usage/import/executable lookup errors propagate. No
    directory, catalogue, or cache is created by this function.

    Example:
        >>> checks = _preflight_checks(args, source, (("squashfs", 1),))  # doctest: +SKIP


    :param args: Required database, optional materialization_root, nested-traversal flag,
        and configured SquashFS/RAR executable selectors.
    :param source_root: Prepared source directory used for read/traverse observations.
    :param recognized_formats: Ordered format/count pairs; truthy counts activate dependency checks.
    :return: Ordered check dictionaries with name, ok, severity, message, and supporting details.
    """
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
        """
        Append one preflight observation with boolean status and supporting detail fields.

        Base-field names are formal parameters, so Python argument binding handles
        duplicate inputs before this body. Extra detail values are retained without
        JSON-serializability checks or recursive copying.

        Example:
            >>> add("source_readable", True, "Readable", path="books")  # doctest: +SKIP


        :param name: Check identifier stored before detail expansion.
        :param ok: Value converted to bool for the base observation.
        :param message: Human-readable diagnostic explanation.
        :param severity: Base severity, defaulting to error rather than warning.
        :param details: Additional fields merged last into the appended dictionary.
        :return: None; mutate the enclosing ordered checks list.
        """
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
