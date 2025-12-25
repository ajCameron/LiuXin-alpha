from __future__ import annotations

import pathlib

import pytest


class TestRenameReplaceMore:
    def test_rename_to_store_relative_nested_path_creates_parents(self, loc_cls, store) -> None:
        # Create a file at a nested location.
        src = loc_cls("dir", "src.txt", store=store)
        src.parent.mkdir(parents=True, exist_ok=True)
        src.write_text("hello")

        # Rename to a store-root-relative nested path.
        dst = src.rename("newdir/subdir/dst.txt")
        assert dst.parts == ("newdir", "subdir", "dst.txt")
        assert dst.exists() is True
        assert dst.read_text() == "hello"
        assert src.exists() is False

    def test_replace_overwrites_and_returns_new_location(self, loc_cls, store) -> None:
        a = loc_cls("a.txt", store=store)
        b = loc_cls("b.txt", store=store)
        a.write_text("A")
        b.write_text("B")

        out = a.replace("b.txt")
        assert out.parts == ("b.txt",)
        assert out.exists() is True
        assert out.read_text() == "A"

    def test_rename_refuses_absolute_outside_store(self, loc_cls, store, tmp_path) -> None:
        src = loc_cls("x.txt", store=store)
        src.write_text("x")

        outside = tmp_path.parent / (tmp_path.name + "_outside")
        outside.mkdir(parents=True, exist_ok=True)
        target = outside / "moved.txt"

        with pytest.raises(ValueError):
            _ = src.rename(target)

    def test_rename_allows_absolute_inside_store(self, loc_cls, store) -> None:
        src = loc_cls("x.txt", store=store)
        src.write_text("x")

        store_root = pathlib.Path(store.url).resolve()
        abs_target = store_root / "absdir" / "moved.txt"

        out = src.rename(abs_target)
        assert out.parts == ("absdir", "moved.txt")
        assert out.exists() is True
        assert out.read_text() == "x"

    def test_rename_refuses_dotdot_in_relative_target(self, loc_cls, store) -> None:
        src = loc_cls("x.txt", store=store)
        src.write_text("x")

        with pytest.raises(ValueError):
            _ = src.rename("../escape.txt")

    def test_replace_refuses_absolute_outside_store(self, loc_cls, store, tmp_path) -> None:
        src = loc_cls("x.txt", store=store)
        src.write_text("x")

        outside = tmp_path.parent / (tmp_path.name + "_outside")
        outside.mkdir(parents=True, exist_ok=True)
        target = outside / "moved.txt"

        with pytest.raises(ValueError):
            _ = src.replace(target)
