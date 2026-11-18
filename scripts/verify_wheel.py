#!/usr/bin/env python3
"""
Provide verify wheel utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise verify wheel through a consuming regression::

        python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
"""

from __future__ import annotations

import argparse
import zipfile
from collections.abc import Sequence
from pathlib import Path

REQUIRED_MEMBERS = {
    "LiuXin_alpha/caches/predicates.py",
    "LiuXin_alpha/utils/calibre_compat/caches/search/adapter.py",
    "LiuXin_alpha/utils/calibre_compat/caches/search/upstream.py",
    "LiuXin_alpha/utils/calibre_compat/caches/search/upstream_parser.py",
    "LiuXin_alpha/utils/calibre_compat/caches/search/upstream_formatter.py",
    "LiuXin_alpha/utils/calibre_compat/caches/search/upstream_functions.py",
    "LiuXin_alpha/utils/calibre_compat/caches/search/provenance.json",
    "LiuXin_alpha/caches/field_metadata.py",
    "LiuXin_alpha/utils/calibre_compat/caches/write/__init__.py",
    "LiuXin_alpha/utils/calibre_compat/caches/base/base_cache.py",
    "LiuXin_alpha/utils/calibre_compat/caches/calibre/cache.py",
    "LiuXin_alpha/utils/calibre_compat/caches/search/__init__.py",
    "LiuXin_alpha/utils/calibre_compat/caches/categories.py",
    "LiuXin_alpha/utils/calibre_compat/caches/lazy.py",
    "LiuXin_alpha/utils/calibre_compat/metadata/field_metadata.py",
    "LiuXin_alpha/utils/calibre_compat/metadata/field_metadata_api.py",
    "LiuXin_alpha/utils/calibre_compat/metadata/field_adaptors.py",
    "LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/aggregate_tables.toml",
    "LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/3-digital-assets.sql",
    "LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/trigger_sql/storage/3-digital-assets_triggers.sql",
    "LiuXin_alpha/file_formats/epub/cfi/epubcfi.ebnf",
    "LiuXin_alpha/file_formats/unihandecode/pykakasi/kakasidict.utf8",
    "LiuXin_alpha/startup_scripts/lx_folders.json",
    "LiuXin_alpha/utils/libraries/iso639/ISO-639-2_utf-8.txt",
    "LiuXin_alpha/utils/libraries/liuxin_dateutil/zoneinfo/zoneinfo-2010g.tar.gz",
}
REQUIRED_ENTRY_POINTS = {"liuxin = LiuXin_alpha.surfaces.cli.app:main"}
RETIRED_CACHE_PATHS = (
    "LiuXin_alpha/caches/write/",
    "LiuXin_alpha/plugins/cache/",
    "LiuXin_alpha/library/caches/",
    "LiuXin_alpha/library/lazy.py",
    "LiuXin_alpha/library/tag_classes.py",
    "LiuXin_alpha/catalog/field_metadata.py",
    "LiuXin_alpha/catalog/api/field_metadata_api.py",
    "LiuXin_alpha/catalog/search/field_searches/",
    "LiuXin_alpha/metadata/field_adaptors.py",
    "LiuXin_alpha/surfaces/categories.py",
)


def verify_wheel(wheel_path: Path) -> None:
    """
    Validate package boundaries, console scripts, and representative data.

    Example:
        Exercise verify wheel through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param wheel_path: Value supplied for wheel path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with zipfile.ZipFile(wheel_path) as archive:
        members = set(archive.namelist())
        retired_prefixes = ("LiuXin_alpha/customize/", "LiuXin_alpha/plugin_system/")
        if any(name.startswith(retired_prefixes) for name in members):
            raise ValueError("wheel contains a retired plugin package")
        if any(name.startswith(RETIRED_CACHE_PATHS) for name in members):
            raise ValueError("wheel contains a retired cache compatibility path")
        missing_members = sorted(REQUIRED_MEMBERS - members)
        if missing_members:
            raise ValueError("wheel is missing runtime files: {}".format(", ".join(missing_members)))

        leaked_tests = sorted(name for name in members if name.startswith("LiuXin_tests/"))
        if leaked_tests:
            raise ValueError("wheel contains the excluded LiuXin_tests package")

        entry_point_files = sorted(name for name in members if name.endswith(".dist-info/entry_points.txt"))
        if len(entry_point_files) != 1:
            raise ValueError("wheel must contain exactly one entry_points.txt")
        entry_points = archive.read(entry_point_files[0]).decode("utf-8")
        missing_entry_points = sorted(line for line in REQUIRED_ENTRY_POINTS if line not in entry_points)
        if missing_entry_points:
            raise ValueError("wheel is missing console scripts: {}".format(", ".join(missing_entry_points)))


def main(argv: Sequence[str] | None = None) -> int:
    """
    Perform the main operation under explicit file-format and conversion rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param argv: Value supplied for argv under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path, help="Wheel archive to inspect.")
    args = parser.parse_args(argv)
    verify_wheel(args.wheel)
    print(f"Verified installable wheel: {args.wheel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
