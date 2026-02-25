from __future__ import annotations

from pathlib import Path

import pytest

import LiuXin_alpha.databases.adding as adding_module
from LiuXin_alpha.databases.adding import find_books_in_directory, listdir, splitext


class TestAddingAPI:
    """Legacy-port smoke + behavior tests for databases.adding."""

    def test_imports(self) -> None:
        assert listdir is not None

    def test_splitext_normalizes_extension_to_lowercase(self) -> None:
        stem, ext = splitext("this/is/a/test.epUB")
        assert stem == "this/is/a/test"
        assert ext == "epub"

    def test_find_books_in_directory_groups_multiformat_books(self, tmp_path: Path) -> None:
        # Two logical books and one metadata-only OPF that should be ignored.
        (tmp_path / "Book One.EPUB").write_text("epub", encoding="utf-8")
        (tmp_path / "Book One.MOBI").write_text("mobi", encoding="utf-8")
        (tmp_path / "Book Two.EPUB").write_text("epub", encoding="utf-8")
        (tmp_path / "Book Two.OPF").write_text("opf", encoding="utf-8")
        (tmp_path / "lonely.OPF").write_text("opf", encoding="utf-8")

        books = list(
            find_books_in_directory(
                dirpath=tmp_path,
                single_book_per_directory=False,
                compiled_rules=(),
                listdir_impl=listdir,
                single_fmt=False,
            )
        )

        # One list per discovered logical book.
        assert len(books) == 2

        flattened_names: set[str] = set()
        for grouped_formats in books:
            for paths_for_one_format in grouped_formats:
                for item in paths_for_one_format:
                    flattened_names.add(Path(item).name.lower())

        assert "book one.epub" in flattened_names
        assert "book one.mobi" in flattened_names
        assert "book two.epub" in flattened_names
        assert "book two.opf" in flattened_names
        assert "lonely.opf" not in flattened_names

    def test_single_book_per_directory_keeps_all_same_format_files(self, tmp_path: Path) -> None:
        # Regression guard: one folder can legitimately contain many EPUB files.
        expected: set[str] = set()
        for idx in range(30):
            name = f"series_volume_{idx:02d}.epub"
            expected.add(name)
            (tmp_path / name).write_text(f"book-{idx}", encoding="utf-8")

        books = list(
            find_books_in_directory(
                dirpath=tmp_path,
                single_book_per_directory=True,
                compiled_rules=(),
                listdir_impl=listdir,
                single_fmt=False,
            )
        )

        # Single-book mode yields one grouped entry for the directory.
        assert len(books) == 1

        flattened_names: set[str] = set()
        for grouped_formats in books:
            for paths_for_one_format in grouped_formats:
                for item in paths_for_one_format:
                    flattened_names.add(Path(item).name.lower())

        assert flattened_names == expected

    def test_multi_book_mode_keeps_all_epubs_as_distinct_candidates(self, tmp_path: Path) -> None:
        # Thirty unique stems should produce thirty candidate books.
        expected: set[str] = set()
        for idx in range(30):
            name = f"series_volume_{idx:02d}.epub"
            expected.add(name)
            (tmp_path / name).write_text(f"book-{idx}", encoding="utf-8")

        books = list(
            find_books_in_directory(
                dirpath=tmp_path,
                single_book_per_directory=False,
                compiled_rules=(),
                listdir_impl=listdir,
                single_fmt=False,
            )
        )
        assert len(books) == 30

        flattened_names: set[str] = set()
        for grouped_formats in books:
            for paths_for_one_format in grouped_formats:
                for item in paths_for_one_format:
                    flattened_names.add(Path(item).name.lower())

        assert flattened_names == expected

    def test_find_books_in_directory_default_mode_is_multi_book(self, tmp_path: Path) -> None:
        # Default discovery mode should preserve per-stem candidates.
        (tmp_path / "a.epub").write_text("a", encoding="utf-8")
        (tmp_path / "b.epub").write_text("b", encoding="utf-8")
        (tmp_path / "c.epub").write_text("c", encoding="utf-8")

        books = list(find_books_in_directory(dirpath=tmp_path))
        assert len(books) == 3

    def test_recursive_import_defaults_to_multi_book_mode(self, monkeypatch, tmp_path: Path) -> None:
        single_calls: list[Path] = []
        multi_calls: list[Path] = []

        def _single(*args, **kwargs):
            single_calls.append(Path(kwargs.get("dirpath", args[1])))
            return []

        def _multi(*args, **kwargs):
            multi_calls.append(Path(kwargs.get("dirpath", args[1])))
            return []

        monkeypatch.setattr(adding_module, "import_book_directory", _single)
        monkeypatch.setattr(adding_module, "import_book_directory_multiple", _multi)

        duplicates = adding_module.recursive_import(db=object(), root=tmp_path)
        assert duplicates == []
        assert not single_calls
        assert multi_calls

    def test_single_fmt_true_is_deprecated_and_ignored(self, tmp_path: Path) -> None:
        for idx in range(3):
            (tmp_path / f"series_{idx}.epub").write_text(str(idx), encoding="utf-8")

        with pytest.warns(DeprecationWarning, match="single_fmt=True"):
            books = list(
                find_books_in_directory(
                    dirpath=tmp_path,
                    single_book_per_directory=True,
                    single_fmt=True,
                )
            )

        flattened_names: set[str] = set()
        for grouped_formats in books:
            for paths_for_one_format in grouped_formats:
                for item in paths_for_one_format:
                    flattened_names.add(Path(item).name.lower())

        assert flattened_names == {"series_0.epub", "series_1.epub", "series_2.epub"}
