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
