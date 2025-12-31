from __future__ import annotations

from pathlib import Path

import pytest


def test_load_names_returns_defaults_when_dir_missing(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.utils.plugins import name_loader

    # Force the module's data dir to a non-existent folder.
    fake = tmp_path / "no_such_folder"
    monkeypatch.setattr(name_loader, "NAME_DATA_DIR", str(fake))

    first, last = name_loader.load_names()
    assert isinstance(first, set) and isinstance(last, set)
    # Defaults should be non-empty
    assert first and last


@pytest.mark.xfail(reason="name_loader.open uses deprecated/invalid 'rU' mode on Python 3")
def test_load_names_can_read_files_when_present(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.utils.plugins import name_loader

    d = tmp_path / "names"
    d.mkdir()
    (d / "FIRST_NAMES.txt").write_text("Alice\nBob\n", encoding="utf-8")
    (d / "LAST_NAMES.txt").write_text("Smith\n", encoding="utf-8")

    monkeypatch.setattr(name_loader, "NAME_DATA_DIR", str(d))
    first, last = name_loader.load_names()
    assert "Alice" in first
    assert "Smith" in last


@pytest.mark.xfail(reason="add_name() treats first_name+last_name as first_name only due to if/elif ordering")
def test_add_name_can_add_to_both_files(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.utils.plugins import name_loader

    d = tmp_path / "names"
    d.mkdir()
    fn = d / "FIRST_NAMES.txt"
    ln = d / "LAST_NAMES.txt"
    fn.write_text("", encoding="utf-8")
    ln.write_text("", encoding="utf-8")

    monkeypatch.setattr(name_loader, "FIRST_NAMES", str(fn))
    monkeypatch.setattr(name_loader, "LAST_NAMES", str(ln))

    ok = name_loader.add_name("Alex", first_name=True, last_name=True)
    assert ok is True
    assert "Alex" in fn.read_text(encoding="utf-8")
    assert "Alex" in ln.read_text(encoding="utf-8")