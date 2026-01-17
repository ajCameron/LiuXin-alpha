from __future__ import annotations

import importlib
import os
import sys
import types
from pathlib import Path

import pytest


def _install_clint_stubs() -> None:
    """Provide minimal `clint` so `LiuXin_alpha.utils.terminal` can import."""

    clint = types.ModuleType("clint")
    textui = types.ModuleType("clint.textui")

    def puts(s: str) -> None:
        # Behaves like clint.textui.puts: print with a newline.
        sys.stdout.write(str(s) + "\n")

    # Only the bits terminal.py imports.
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


def _set_isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point LiuXin path constants at a temp tree.

    constants.paths reads env vars at import time, so this must run before
    importing LiuXin_alpha.constants.paths (and anything that imports from it).
    """

    base = tmp_path / "liuxin_base"
    prefs = base / "LiuXin_prefs"
    cfg = prefs / "calibre_config"

    monkeypatch.setenv("LIUXIN_BASE_DIR", str(base))
    monkeypatch.setenv("LIUXIN_PREFS_DIR", str(prefs))
    monkeypatch.setenv("LIUXIN_CONFIG_DIR", str(cfg))

    # Provide a minimal resources tree so `P('default_tweaks.py', data=True)` works.
    resources_dir = base / "LiuXin_data" / "calibre_resources"
    resources_dir.mkdir(parents=True, exist_ok=True)
    (resources_dir / "default_tweaks.py").write_bytes(
        b"# minimal defaults for tests\n"
        b"use_series_auto_increment_tweak_when_importing = True\n"
        b"example_tweak = 1\n"
    )


def _clear_modules(*names: str) -> None:
    for n in names:
        sys.modules.pop(n, None)


def _import_config_base(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _install_clint_stubs()
    _set_isolated_env(tmp_path, monkeypatch)

    # Ensure env overrides are respected.
    _clear_modules(
        "LiuXin_alpha.constants.paths",
        "LiuXin_alpha.utils.resources",
        "LiuXin_alpha.utils.config",
        "LiuXin_alpha.utils.config.config_base",
    )

    import LiuXin_alpha.constants.paths as paths

    importlib.reload(paths)

    import LiuXin_alpha.utils.config.config_base as cb

    return importlib.reload(cb)


@pytest.fixture()
def cb(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    return _import_config_base(tmp_path, monkeypatch)


def test_optionset_has_get_update_and_smart_update(cb) -> None:
    os1 = cb.OptionSet(description="one")
    os1.add_opt("a", default=1)
    os1.add_opt("b", default=2)

    assert os1.has_option("a")
    assert os1.get_option("a").default == 1

    os2 = cb.OptionSet(description="two")
    os2.add_opt("b", default=200)
    os2.add_opt("c", default=3)

    # update should replace the existing Option("b") and add "c"
    os1.update(os2)
    assert os1.has_option("c")
    assert os1.get_option("b").default == 200

    opts1 = os1.parse_string("")
    opts2 = os1.parse_string('{"b": 999}')
    os1.smart_update(opts1, opts2)
    assert opts1.b == 999
    assert opts1.c == 3


def test_option_parser_parses_grouped_switches(cb) -> None:
    s = cb.OptionSet(description="parser")
    addg = s.add_group("group", "A group")
    addg("value", switches=["--value"], default=5, help="the value")

    parser = s.option_parser(user_defaults=s.parse_string(""), usage="%prog")
    opts, args = parser.parse_args(["--value", "7"])
    assert args == []
    assert opts.value == 7


def test_option_parser_parses_simple_switches(cb) -> None:
    s = cb.OptionSet(description="parser")
    s.add_opt("value", switches=["--value"], default=5, help="the value")
    parser = s.option_parser(user_defaults=s.parse_string(""), usage="%prog")
    opts, args = parser.parse_args(["--value", "7"])
    assert args == []
    assert opts.value == 7


def test_config_set_parse_and_parse(cb) -> None:
    c = cb.Config("unit_test_config", description="unit test config")
    c.add_opt("answer", default=42)

    c.set("answer", 7)

    p = Path(c.config_file_path)
    assert p.name.endswith(".py.json")
    text = p.read_text(encoding="utf-8")
    assert '"answer"' in text
    assert "7" in text

    parsed = c.parse()
    assert parsed.answer == 7


def test_config_refuses_legacy_py_content(cb) -> None:
    c = cb.Config("legacy", description="legacy")
    c.add_opt("x", default=1)
    Path(c.config_file_path).write_text("# legacy\nx = 99\n", encoding="utf-8")
    with pytest.raises(cb.LegacyConfigError):
        _ = c.parse()


def test_config_proxy_caches_until_refresh(cb) -> None:
    c = cb.Config("proxy_config", description="proxy")
    c.add_opt("x", default=1)
    p = cb.ConfigProxy(c)

    assert p.get("x") == 1

    # Update the file out-of-band; proxy should not see until refresh.
    Path(c.config_file_path).write_text('{"x": 99}', encoding="utf-8")
    assert p.get("x") == 1

    p.refresh()
    assert p.get("x") == 99


def test_global_prefs_installation_uuid_written(cb) -> None:
    # Import-time side effect should ensure UUID exists.
    uuid = cb.prefs["installation_uuid"]
    assert isinstance(uuid, str)
    assert len(uuid) >= 8

    # And it should be persisted to the config file.
    global_cfg = Path(cb.config_dir) / "global.py.json"
    assert global_cfg.exists()
    assert "installation_uuid" in global_cfg.read_text(encoding="utf-8")


def test_tweaks_roundtrip_and_context_manager(cb) -> None:
    # Ensure default tweak came from our resource.
    assert cb.tweaks.get("example_tweak") == 1

    raw = b"# user tweaks\nexample_tweak = 10\nnew_tweak = 'yep'\n"
    cb.write_tweaks(raw)

    d = cb.read_tweaks()
    assert d["example_tweak"] == 10
    assert d["new_tweak"] == "yep"

    # Global dict should be tweakable via context manager.
    before = cb.tweaks["example_tweak"]
    with cb.Tweak("example_tweak", 123):
        assert cb.tweaks["example_tweak"] == 123
    assert cb.tweaks["example_tweak"] == before

    cb.reset_tweaks_to_default()
    assert cb.tweaks["example_tweak"] == 1
