"""
Provide test config tools utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test config tools through a consuming regression::

        python -m pytest -q tests/utils/config/test_config_tools.py
"""
from __future__ import annotations

import importlib
import json
import os
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest


def _install_clint_stubs() -> None:
    """
    Provide minimal `clint` so `LiuXin_alpha.utils.terminal` can import.

    Example:
        Exercise  install clint stubs through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_tools.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    clint = types.ModuleType("clint")
    textui = types.ModuleType("clint.textui")

    def puts(s: str) -> None:
        """
        Perform the puts utility operation under explicit compatibility rules.

        Example:
            Exercise  install clint stubs.puts through a consuming regression::

                python -m pytest -q tests/utils/config/test_config_tools.py


        :param s: Value supplied for s under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        sys.stdout.write(str(s) + "\n")

    textui.puts = puts  # type: ignore[attr-defined]
    textui.colored = types.SimpleNamespace(green=lambda s: s)  # type: ignore[attr-defined]

    packages = types.ModuleType("clint.packages")
    six = types.ModuleType("clint.packages.six")
    six.text_type = str  # type: ignore[attr-defined]

    sys.modules.setdefault("clint", clint)
    sys.modules.setdefault("clint.textui", textui)
    sys.modules.setdefault("clint.textui.colored", textui)
    sys.modules.setdefault("clint.packages", packages)
    sys.modules.setdefault("clint.packages.six", six)


def _install_liuxin_dateutil_stubs() -> None:
    """
    Provide a minimal top-level `liuxin_dateutil` for LiuXin_alpha.utils.date.

    Example:
        Exercise  install liuxin dateutil stubs through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_tools.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """

    try:
        from dateutil.parser import parse  # type: ignore
    except Exception:
        return
    pkg = types.ModuleType('liuxin_dateutil')
    parser_mod = types.ModuleType('liuxin_dateutil.parser')
    parser_mod.parse = parse  # type: ignore[attr-defined]

    sys.modules.setdefault('liuxin_dateutil', pkg)
    sys.modules.setdefault('liuxin_dateutil.parser', parser_mod)


def _set_isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Perform the set isolated env utility operation under explicit compatibility rules.

    Example:
        Exercise  set isolated env through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_tools.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    base = tmp_path / "liuxin_base"
    prefs = base / "LiuXin_prefs"
    cfg = prefs / "calibre_config"

    monkeypatch.setenv("LIUXIN_BASE_DIR", str(base))
    monkeypatch.setenv("LIUXIN_PREFS_DIR", str(prefs))
    monkeypatch.setenv("LIUXIN_CONFIG_DIR", str(cfg))

    resources_dir = base / "LiuXin_data" / "calibre_resources"
    resources_dir.mkdir(parents=True, exist_ok=True)
    (resources_dir / "default_tweaks.py").write_bytes(
        b"# minimal defaults for tests\nexample_tweak = 1\n"
    )


def _clear_modules(*names: str) -> None:
    """
    Perform the clear modules utility operation under explicit compatibility rules.

    Example:
        Exercise  clear modules through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_tools.py


    :param names: Value supplied for names under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for n in names:
        sys.modules.pop(n, None)


def _import_config_tools(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    Perform the import config tools utility operation under explicit compatibility rules.

    Example:
        Exercise  import config tools through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_tools.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    _install_clint_stubs()
    _install_liuxin_dateutil_stubs()
    _set_isolated_env(tmp_path, monkeypatch)

    _clear_modules(
        "LiuXin_alpha.constants.paths",
        "LiuXin_alpha.utils.resources",
        "LiuXin_alpha.utils.config",
        "LiuXin_alpha.utils.config.config_base",
        "LiuXin_alpha.utils.config.config_tools",
    )

    import LiuXin_alpha.constants.paths as paths

    importlib.reload(paths)

    import LiuXin_alpha.utils.config.config_tools as ct

    return importlib.reload(ct)


@pytest.fixture()
def ct(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    Perform the ct utility operation under explicit compatibility rules.

    Example:
        Exercise ct through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_tools.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """
    mod = _import_config_tools(tmp_path, monkeypatch)
    try:
        yield mod
    finally:
        _clear_modules(
            "LiuXin_alpha.constants.paths",
            "LiuXin_alpha.utils.resources",
            "LiuXin_alpha.utils.config",
            "LiuXin_alpha.utils.config.config_base",
            "LiuXin_alpha.utils.config.config_tools",
        )


def test_to_json_datetime_roundtrip(ct) -> None:
    """
    Perform the test to json datetime roundtrip utility operation under explicit compatibility rules.

    Example:
        Exercise test to json datetime roundtrip through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_tools.py


    :param ct: Value supplied for ct under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    dt = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    raw = json.dumps({"dt": dt}, default=ct.to_json)
    obj = json.loads(raw, object_hook=ct.from_json)

    assert isinstance(obj["dt"], datetime)
    assert obj["dt"].replace(tzinfo=timezone.utc) == dt


def test_to_json_bytearray_roundtrip(ct) -> None:
    """
    Perform the test to json bytearray roundtrip utility operation under explicit compatibility rules.

    Example:
        Exercise test to json bytearray roundtrip through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_tools.py


    :param ct: Value supplied for ct under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    b = bytearray(b"abc")
    raw = json.dumps({"b": b}, default=ct.to_json)
    obj = json.loads(raw, object_hook=ct.from_json)
    assert obj["b"] == b


def test_device_prefs_overrides(ct) -> None:
    # prefs is imported from config_base; should act mapping-like.
    """
    Perform the test device prefs overrides utility operation under explicit compatibility rules.

    Example:
        Exercise test device prefs overrides through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_tools.py


    :param ct: Value supplied for ct under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    gp = ct.prefs
    ct.device_prefs.set_overrides(network_timeout=321)
    assert ct.device_prefs["network_timeout"] == 321
    # missing override should fall back to global prefs
    assert ct.device_prefs["swap_author_names"] == gp["swap_author_names"]


def test_dynamic_config_roundtrip(ct) -> None:
    """
    Perform the test dynamic config roundtrip utility operation under explicit compatibility rules.

    Example:
        Exercise test dynamic config roundtrip through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_tools.py


    :param ct: Value supplied for ct under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    d = ct.DynamicConfig(name="unit_dynamic")
    d.defaults["missing"] = "fallback"

    assert d["missing"] == "fallback"

    d["x"] = 12
    assert d["x"] == 12

    # Ensure persisted to disk and reload works.
    file_path = Path(d.file_path)
    assert file_path.exists()

    d2 = ct.DynamicConfig(name="unit_dynamic")
    assert d2["x"] == 12

def test_json_config_roundtrip(ct) -> None:
    """
    Perform the test json config roundtrip utility operation under explicit compatibility rules.

    Example:
        Exercise test json config roundtrip through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_tools.py


    :param ct: Value supplied for ct under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    j = ct.JSONConfig("unit_json")
    j.defaults["a"] = 1
    j["a"] = 2
    assert j["a"] == 2

    # reload from disk
    j2 = ct.JSONConfig("unit_json")
    j2.defaults["a"] = 1
    assert j2["a"] == 2


def test_xml_config_roundtrip(ct) -> None:
    """
    Perform the test xml config roundtrip utility operation under explicit compatibility rules.

    Example:
        Exercise test xml config roundtrip through a consuming regression::

            python -m pytest -q tests/utils/config/test_config_tools.py


    :param ct: Value supplied for ct under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    x = ct.XMLConfig("unit_xml")
    x.defaults["a"] = 1
    x["a"] = 2
    assert x["a"] == 2

    x2 = ct.XMLConfig("unit_xml")
    x2.defaults["a"] = 1
    assert x2["a"] == 2
