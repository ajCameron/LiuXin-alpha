"""
Check canonical datatype constants and lightweight namespace imports.

Constant tests import the owning constants module. Namespace tests start isolated
Python subprocesses for seven parameterized packages and prohibit child imports or
dynamic export hooks.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_databases_init.py
"""

from __future__ import annotations

import pytest


class TestDatabasesInitConstants:
    """
    Check canonical constant containers and their strict subset relationship.

    Example:
        >>> TestDatabasesInitConstants().test_custom_data_types_is_subset_of_valid()
    """
    def test_custom_data_types_exported(self) -> None:
        """
        Require a frozen custom datatype set that excludes None.

        Reads constants from their canonical module; this does not test a package-level
        re-export.

        Example:
            >>> TestDatabasesInitConstants().test_custom_data_types_exported()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES

        assert isinstance(CUSTOM_DATA_TYPES, frozenset)
        assert None not in CUSTOM_DATA_TYPES

    def test_valid_data_types_exported(self) -> None:
        """
        Require a frozen valid datatype set that includes None.

        Reads constants from their canonical module; this does not test a package-level
        re-export.

        Example:
            >>> TestDatabasesInitConstants().test_valid_data_types_exported()


        :return: None; failed expectations raise AssertionError.
        """
        from LiuXin_alpha.databases.constants import VALID_DATA_TYPES

        assert isinstance(VALID_DATA_TYPES, frozenset)
        assert None in VALID_DATA_TYPES

    def test_custom_data_types_is_subset_of_valid(self) -> None:
        """
        Require custom datatypes to form a strict subset of all valid datatypes.

        Example:
            >>> TestDatabasesInitConstants().test_custom_data_types_is_subset_of_valid()


        :return: None; failed expectations raise AssertionError.
        """
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
    """
    Import one namespace in an isolated child interpreter without loading implementations.

    Starts the current Python with -I, explicitly inserts this checkout's src directory,
    and requires no __getattr__, __all__ or imported child modules. Captures child
    output for assertion diagnostics and enforces a 60-second timeout. A nonzero child
    status fails the test; launch and timeout errors propagate.

    Example:
        Run one namespace case with pytest::

            python -m pytest -q tests/databases/test_databases_init.py -k namespace


    :param package: Dotted namespace package selected by the parameter table.
    :return: None; asserts that the isolated import probe exits successfully.
    """
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
