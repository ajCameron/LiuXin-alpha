# tests/databases/driver_contract/test_contract_driver_wrapper_abstractness.py
"""
Check DriverWrapper concreteness, its macros property, and construction with a minimal driver stub.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_driver_wrapper_abstractness.py
"""
from __future__ import annotations

import inspect
from pathlib import Path


def test_driver_wrapper_imports_from_repo_src_and_is_concrete() -> None:
    """
    Require a concrete DriverWrapper class with a macros property.

    Also compare its resolved module path with the derived source-checkout candidate
    when that candidate exists; an absent candidate bypasses the path assertion.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_driver_wrapper_abstractness.py::test_driver_wrapper_imports_from_repo_src_and_is_concrete


    :return: None; failed expectations raise AssertionError.
    """
    import LiuXin_alpha.databases.driver_wrapper as m
    from LiuXin_alpha.databases.driver_wrapper import DriverWrapper

    got_file = Path(m.__file__).resolve()
    repo_root = Path(__file__).resolve().parents[3]  # .../tests/databases/driver_contract -> repo root
    expected = (repo_root / "src" / "LiuXin_alpha" / "databases" / "driver_wrapper" / "__init__.py").resolve()

    # If we're in a source checkout, insist we import the in-repo module (catches site-packages shadowing).
    if expected.exists():
        assert got_file == expected, (
            "DriverWrapper is being imported from an unexpected location.\n"
            f"Expected: {expected}\n"
            f"Got:      {got_file}\n"
            "This usually means your interpreter / PYTHONPATH is picking up an older installed LiuXin_alpha."
        )

    # Concrete + implements macros
    abstract_methods = getattr(DriverWrapper, "__abstractmethods__", set())
    assert not inspect.isabstract(DriverWrapper), (
        "DriverWrapper is still abstract, so instantiation will fail.\n"
        f"Imported from: {got_file}\n"
        f"__abstractmethods__: {sorted(abstract_methods)}\n"
        "Most common cause: DatabaseDriverWrapperAPI declares abstract 'macros', but DriverWrapper "
        "does not implement a concrete @property macros."
    )

    macros_attr = getattr(DriverWrapper, "macros", None)
    assert isinstance(macros_attr, property), (
        "DriverWrapper.macros is not a @property on the class.\n"
        f"Imported from: {got_file}\n"
        f"type(DriverWrapper.macros) = {type(macros_attr)}\n"
        "If DatabaseDriverWrapperAPI defines abstract @property macros, DriverWrapper must implement it."
    )


def test_driver_wrapper_can_instantiate_with_minimal_driver_stub() -> None:
    """
    Construct a wrapper around a local dummy driver, require non-None macros, and close the wrapper.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_driver_wrapper_abstractness.py::test_driver_wrapper_can_instantiate_with_minimal_driver_stub


    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.driver_wrapper import DriverWrapper

    class _DummyLock:
        """
        Supply no-op commit and close methods for wrapper construction and teardown.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_driver_wrapper_abstractness.py::test_driver_wrapper_can_instantiate_with_minimal_driver_stub
        """
        def commit(self) -> None:
            """
            Accept a wrapper commit without persisting anything.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_driver_wrapper_abstractness.py::test_driver_wrapper_can_instantiate_with_minimal_driver_stub


            :return: None.
            """
            pass

        def close(self) -> None:
            """
            Accept wrapper cleanup without changing state or releasing a real resource.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_driver_wrapper_abstractness.py::test_driver_wrapper_can_instantiate_with_minimal_driver_stub


            :return: None.
            """
            pass

    class _DummyDriver:
        """
        Supply a macros sentinel and fresh dummy connection objects to DriverWrapper.

        Example:
            Run the owning tests with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_driver_wrapper_abstractness.py::test_driver_wrapper_can_instantiate_with_minimal_driver_stub
        """
        def __init__(self) -> None:
            """
            Attach a fresh object as the non-None macros sentinel.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_driver_wrapper_abstractness.py::test_driver_wrapper_can_instantiate_with_minimal_driver_stub


            :return: None.
            """
            self.macros = object()

        def get_connection(self):
            """
            Create a new dummy connection for each wrapper request.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_driver_wrapper_abstractness.py::test_driver_wrapper_can_instantiate_with_minimal_driver_stub


            :return: Fresh _DummyLock instance; no database is opened.
            """
            return _DummyLock()

    w = DriverWrapper(_DummyDriver())
    assert w.macros is not None
    w.close()
