# tests/databases/driver_contract/test_contract_driver_wrapper_abstractness.py
from __future__ import annotations

import inspect
from pathlib import Path


def test_driver_wrapper_imports_from_repo_src_and_is_concrete() -> None:
    """
    Fail early (without constructing a Database) if we are importing the wrong DriverWrapper
    or if it is still abstract due to a missing @property macros implementation.
    """
    import LiuXin_alpha.databases.driver_wrapper as m
    from LiuXin_alpha.databases.driver_wrapper import DriverWrapper

    got_file = Path(m.__file__).resolve()
    repo_root = Path(__file__).resolve().parents[3]  # .../tests/databases/driver_contract -> repo root
    expected = (repo_root / "src" / "LiuXin_alpha" / "databases" / "database_driver_plugins" / "driver_wrapper.py").resolve()

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
    Reproduce the instantiation path without needing the heavy Database fixture.
    This catches the exact TypeError you're seeing ('abstract method macros') in a tight unit test.
    """
    from LiuXin_alpha.databases.driver_wrapper import DriverWrapper

    class _DummyLock:
        def commit(self) -> None:
            pass

        def close(self) -> None:
            pass

    class _DummyDriver:
        def __init__(self) -> None:
            self.macros = object()

        def get_connection(self):
            return _DummyLock()

    w = DriverWrapper(_DummyDriver())
    assert w.macros is not None
    w.close()
