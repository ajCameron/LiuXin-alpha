#!/usr/bin/env python3
"""Verify LiuXin wheel ownership and exercise an installed first-run system."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PurePosixPath("LiuXin_alpha")
SCHEMA_PACKAGE = PACKAGE_ROOT.joinpath(
    "databases",
    "database_driver_plugins",
    "SQL",
    "database_generator_frbr",
)
SCHEMA_SOURCE = REPO_ROOT.joinpath(
    "src",
    "LiuXin_alpha",
    "databases",
    "database_driver_plugins",
    "SQL",
    "database_generator_frbr",
)
REQUIRED_MODULE_ASSETS = (
    PACKAGE_ROOT / "catalog" / "py.typed",
    PACKAGE_ROOT / "startup_scripts" / "LX_folders",
    PACKAGE_ROOT / "startup_scripts" / "lx_folders.json",
    PACKAGE_ROOT
    / "utils"
    / "libraries"
    / "iso639"
    / "ISO-639-2_utf-8.txt",
    PACKAGE_ROOT
    / "utils"
    / "libraries"
    / "liuxin_dateutil"
    / "zoneinfo"
    / "zoneinfo-2010g.tar.gz",
)
REQUIRED_SCHEMA_SPECS = (
    "aggregate_tables.toml",
    "interlink_table_requests.toml",
    "intralink_table_requests.toml",
)
EXCLUDED_PACKAGE_PREFIXES = (
    "LiuXin_tests/",
    "LiuXin_alpha/working-memory/",
    "LiuXin_alpha/file_formats/lrf/html/demo/",
    "LiuXin_alpha/file_formats/oeb/display/test-cfi/",
    "LiuXin_alpha/file_formats/oeb/polish/tests/",
    "LiuXin_alpha/utils/decompression/rarfile/test/",
)


class WheelVerificationError(RuntimeError):
    """Report a wheel-content or installed-runtime packaging regression."""


def expected_runtime_assets(repo_root: Path = REPO_ROOT) -> frozenset[str]:
    """Return package-relative files which the maintained runtime opens directly."""

    schema_source = repo_root / SCHEMA_SOURCE.relative_to(REPO_ROOT)
    schema_package = SCHEMA_PACKAGE
    expected = {path.as_posix() for path in REQUIRED_MODULE_ASSETS}
    expected.update(
        (schema_package / filename).as_posix()
        for filename in REQUIRED_SCHEMA_SPECS
    )
    for folder in ("aggregate_sql", "table_sql", "trigger_sql"):
        for source_path in sorted((schema_source / folder).rglob("*.sql")):
            expected.add(
                (schema_package / source_path.relative_to(schema_source)).as_posix()
            )
    for package in (
        PurePosixPath("LiuXin_alpha/file_formats/oeb/display"),
        PurePosixPath("LiuXin_alpha/file_formats/oeb/polish"),
    ):
        source = repo_root / "src" / Path(package.as_posix())
        expected.update(
            (package / source_path.name).as_posix()
            for source_path in sorted(source.glob("*.coffee"))
        )
    return frozenset(expected)


def inspect_wheel(wheel: Path, repo_root: Path = REPO_ROOT) -> dict[str, object]:
    """Check discovery boundaries and all source-controlled runtime assets."""

    try:
        with zipfile.ZipFile(wheel) as archive:
            names = frozenset(archive.namelist())
    except (OSError, zipfile.BadZipFile) as exc:
        raise WheelVerificationError(f"Cannot read wheel {wheel}: {exc}") from exc

    unwanted = sorted(
        name
        for name in names
        if name.startswith(EXCLUDED_PACKAGE_PREFIXES)
    )
    if unwanted:
        raise WheelVerificationError(
            "Development-only test or demo modules leaked into the wheel: "
            + ", ".join(unwanted[:5])
        )

    missing = sorted(expected_runtime_assets(repo_root) - names)
    if missing:
        raise WheelVerificationError(
            "Wheel is missing runtime package assets:\n  - " + "\n  - ".join(missing)
        )

    compatibility_files = {"past/__init__.py", "past/builtins.py"}
    missing_compatibility = sorted(compatibility_files - names)
    if missing_compatibility:
        raise WheelVerificationError(
            "Wheel is missing the local past compatibility shim: "
            + ", ".join(missing_compatibility)
        )

    python_files = sum(name.endswith(".py") for name in names)
    return {
        "wheel": str(wheel),
        "entries": len(names),
        "python_files": python_files,
        "runtime_assets": len(expected_runtime_assets(repo_root)),
    }


def _run_installed_init(wheel: Path) -> dict[str, object]:
    """Install one wheel into a clean target and create then reopen a catalogue."""

    with tempfile.TemporaryDirectory(prefix="liuxin-wheel-check-") as temporary:
        temporary_root = Path(temporary)
        install_root = temporary_root / "installed"
        system_root = temporary_root / "system"
        state_root = temporary_root / "state"
        install = subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--no-deps",
                "--target",
                str(install_root),
                str(wheel.resolve()),
            ],
            cwd=temporary_root,
            capture_output=True,
            text=True,
        )
        if install.returncode:
            raise WheelVerificationError(
                "pip could not install the wheel:\n" + install.stderr.strip()
            )

        environment = os.environ.copy()
        environment.update(
            {
                "LIUXIN_BASE_DIR": str(state_root),
                "LIUXIN_CONFIG_DIR": str(state_root / "config"),
                "LIUXIN_PREFS_DIR": str(state_root / "preferences"),
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONNOUSERSITE": "1",
                "PYTHONPATH": str(install_root),
            }
        )
        origin_check = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from pathlib import Path; import LiuXin_alpha; "
                    "origin=Path(LiuXin_alpha.__file__).resolve(); "
                    f"root=Path({str(install_root)!r}).resolve(); "
                    "assert origin.is_relative_to(root), (origin, root); print(origin)"
                ),
            ],
            cwd=temporary_root,
            env=environment,
            capture_output=True,
            text=True,
        )
        if origin_check.returncode:
            raise WheelVerificationError(
                "The smoke process did not import the target-installed wheel:\n"
                + origin_check.stderr.strip()
            )

        results: list[dict[str, object]] = []
        for _ in range(2):
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "LiuXin_alpha.surfaces.cli",
                    "init",
                    str(system_root),
                    "--compact",
                ],
                cwd=temporary_root,
                env=environment,
                capture_output=True,
                text=True,
            )
            if completed.returncode:
                detail = completed.stderr.strip() or completed.stdout.strip()
                raise WheelVerificationError(
                    f"Installed `liuxin init` failed with {completed.returncode}:\n{detail}"
                )
            try:
                results.append(json.loads(completed.stdout))
            except json.JSONDecodeError as exc:
                raise WheelVerificationError(
                    "Installed `liuxin init` did not emit its JSON receipt:\n"
                    + completed.stdout.strip()
                ) from exc

        if results[0].get("database_created") is not True:
            raise WheelVerificationError("First installed init did not create a catalogue")
        if results[1].get("database_created") is not False:
            raise WheelVerificationError("Second installed init did not reopen the catalogue")
        database = system_root / "catalogue.sqlite"
        if not database.is_file():
            raise WheelVerificationError(f"Installed init did not create {database}")
        return {
            "package_origin": origin_check.stdout.strip(),
            "database_created": True,
            "database_reopened": True,
            "store_count": results[1].get("store_count"),
        }


def verify_wheel(wheel: Path, repo_root: Path = REPO_ROOT) -> dict[str, object]:
    """Verify wheel contents and its installed first-run SQLite workflow."""

    content = inspect_wheel(wheel, repo_root)
    return {**content, "installed_smoke": _run_installed_init(wheel)}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify a LiuXin wheel and its installed `liuxin init` workflow."
    )
    parser.add_argument("wheel", type=Path, help="Path to one built .whl file.")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the wheel verification command."""

    args = _build_parser().parse_args(argv)
    try:
        result = verify_wheel(args.wheel)
    except WheelVerificationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
