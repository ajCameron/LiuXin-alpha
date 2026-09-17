"""Check lightweight package namespaces and canonical database constants."""

from __future__ import annotations

import pytest


class TestDatabasesInitConstants:
    def test_custom_data_types_exported(self) -> None:
        from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES

        assert isinstance(CUSTOM_DATA_TYPES, frozenset)
        assert None not in CUSTOM_DATA_TYPES

    def test_valid_data_types_exported(self) -> None:
        from LiuXin_alpha.databases.constants import VALID_DATA_TYPES

        assert isinstance(VALID_DATA_TYPES, frozenset)
        assert None in VALID_DATA_TYPES

    def test_custom_data_types_is_subset_of_valid(self) -> None:
        from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES, VALID_DATA_TYPES

        assert CUSTOM_DATA_TYPES < VALID_DATA_TYPES


@pytest.mark.parametrize(
    "package",
    [
        "LiuXin_alpha.databases",
        "LiuXin_alpha.databases.database_driver_plugins",
        "LiuXin_alpha.ingest",
        "LiuXin_alpha.surfaces",
        "LiuXin_alpha.surfaces.renderers",
        "LiuXin_alpha.surfaces.terminal",
        "LiuXin_alpha.surfaces.cli",
    ],
)
def test_namespace_import_does_not_load_implementations(package: str) -> None:
    """Import each namespace in isolation without loading children or export hooks."""
    import subprocess
    import sys
    from pathlib import Path

    source = str(Path(__file__).resolve().parents[2] / "src")
    code = (
        "import importlib, sys\n"
        f"sys.path.insert(0, {source!r})\n"
        f"package = importlib.import_module({package!r})\n"
        "assert '__getattr__' not in vars(package)\n"
        "assert '__all__' not in vars(package)\n"
        "assert not any(name.startswith(package.__name__ + '.') for name in sys.modules)\n"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
