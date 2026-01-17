from __future__ import annotations

import mimetypes
from pathlib import Path

import pytest


def test_guess_type_inits_once_and_can_use_custom_mime_types(monkeypatch, tmp_path: Path) -> None:
    from LiuXin_alpha.utils import mine_types

    # Make a minimal mime.types file so mimetypes can read from it.
    mt = tmp_path / "mime.types"
    mt.write_text("text/x-foo foo\napplication/x-bar bar\n", encoding="utf-8")

    calls: list[tuple] = []

    def fake_init(files=()):
        calls.append(tuple(files))
        # Also call real init so guess_type works for common types.
        return mimetypes.init(files)

    monkeypatch.setattr(mine_types, "_mt_inited", False)
    monkeypatch.setattr(mine_types, "_mt_init", fake_init)
    monkeypatch.setattr(mine_types, "resource_to_path", lambda *a, **k: str(mt))

    # First call: triggers init
    assert mine_types.guess_type("thing.foo")[0] == "text/x-foo"
    # Second call: should not re-init
    assert mine_types.guess_type("thing.bar")[0] == "application/x-bar"
    assert len(calls) == 1


def test_guess_type_wrapper_returns_tuple() -> None:
    from LiuXin_alpha.utils.mine_types import guess_type

    t = guess_type("file.txt")
    assert isinstance(t, tuple)
    assert len(t) == 2



import importlib
import mimetypes
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _reset_global_mimetypes() -> None:
    """mine_types works by mutating the stdlib mimetypes module; keep tests isolated."""
    yield
    importlib.reload(mimetypes)


def test_guess_type_inits_once_and_can_use_custom_mime_types(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from LiuXin_alpha.utils import mine_types

    # Minimal mime.types file so mimetypes can read from it.
    mt = tmp_path / "mime.types"
    mt.write_text("text/x-foo foo\napplication/x-bar bar\n", encoding="utf-8")

    calls: list[tuple[str, ...]] = []
    real_init = mimetypes.init

    def recording_init(files=()):
        calls.append(tuple(str(f) for f in files or ()))
        return real_init(files)

    monkeypatch.setattr(mine_types, "_mt_inited", False)
    monkeypatch.setattr(mine_types, "resource_to_path", lambda *a, **k: str(mt))
    monkeypatch.setattr(mimetypes, "init", recording_init)

    # First call: triggers init
    assert mine_types.guess_type("thing.foo")[0] == "text/x-foo"
    # Second call: should not re-init
    assert mine_types.guess_type("thing.bar")[0] == "application/x-bar"
    assert len(calls) == 1


def test_guess_extension_has_palmreader_special_case(monkeypatch: pytest.MonkeyPatch) -> None:
    from LiuXin_alpha.utils import mine_types

    monkeypatch.setattr(mine_types, "_mt_inited", False)

    assert mine_types.guess_extension("application/x-palmreader") == ".pdb"


def test_mine_types_integration_uses_calibre_mime_types(monkeypatch: pytest.MonkeyPatch) -> None:
    """Integration check: the shipped calibre mime.types drives the stdlib mappings."""

    from LiuXin_alpha.utils import mine_types

    monkeypatch.setattr(mine_types, "_mt_inited", False)

    # These are present in calibre's bundled mime.types.
    assert mine_types.guess_type("book.epub")[0] == "application/epub+zip"
    assert mine_types.guess_type("book.pobi")[0] == "application/x-mobipocket-subscription"


def test_guess_type_wrapper_returns_tuple(monkeypatch: pytest.MonkeyPatch) -> None:
    from LiuXin_alpha.utils import mine_types

    monkeypatch.setattr(mine_types, "_mt_inited", False)

    t = mine_types.guess_type("file.txt")
    assert isinstance(t, tuple)
    assert len(t) == 2
