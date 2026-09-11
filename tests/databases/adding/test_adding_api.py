"""
Characterize legacy filename discovery/grouping without importing real book metadata.

Temporary files contain arbitrary text under ebook suffixes. Tests establish
group shape, retained paths, defaults, and deprecation behavior; they do not
validate formats or run the legacy database import adapters. Recursive routing
is checked with patched child importers.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import LiuXin_alpha.ingest.adding as adding_module
from LiuXin_alpha.ingest.adding import find_books_in_directory, listdir, splitext


class TestAddingAPI:
    """
    Preserve grouping behavior and compatibility flags in ingest.adding.

    The historical test location predates the ingest package ownership. Real
    filesystem discovery is exercised; database writes and metadata reads are not.

    Example:
        >>> TestAddingAPI().test_splitext_normalizes_extension_to_lowercase()
    """

    def test_imports(self) -> None:
        """
        Check the discovery iterator is available after importing the relocated module.

        Example:
            >>> TestAddingAPI().test_imports()


        :return: None; assert the imported listdir binding exists, without invoking it.
        """
        assert listdir is not None

    def test_splitext_normalizes_extension_to_lowercase(self) -> None:
        """
        Split a mixed-case ebook suffix while preserving the path stem.

        Example:
            >>> TestAddingAPI().test_splitext_normalizes_extension_to_lowercase()


        :return: None; assert the exact unchanged stem and lowercase dotless epub suffix.
        """
        stem, ext = splitext("this/is/a/test.epUB")
        assert stem == "this/is/a/test"
        assert ext == "epub"

    def test_find_books_in_directory_groups_multiformat_books(self, tmp_path: Path) -> None:
        """
        Group two stems, retain their companion formats, and discard an OPF-only stem.

        Example:
            >>> TestAddingAPI().test_find_books_in_directory_groups_multiformat_books(tmp_path)  # doctest: +SKIP


        :param tmp_path: Directory receiving named EPUB/MOBI/OPF text fixtures.
        :return: None; assert two nested groups and the expected retained filename set.
        """
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
        """
        Retain all thirty EPUB paths when one directory is treated as a single book.

        Example:
            >>> TestAddingAPI().test_single_book_per_directory_keeps_all_same_format_files(tmp_path)  # doctest: +SKIP


        :param tmp_path: Directory populated with thirty differently named EPUB fixtures.
        :return: None; assert one candidate group containing every original filename.
        """
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
        """
        Produce thirty candidates from thirty distinct stems in explicit multi-book mode.

        Example:
            >>> TestAddingAPI().test_multi_book_mode_keeps_all_epubs_as_distinct_candidates(tmp_path)  # doctest: +SKIP


        :param tmp_path: Directory holding one arbitrary-text EPUB fixture per stem.
        :return: None; assert candidate count and complete filename retention.
        """
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
        """
        Keep separate stems as separate candidates when grouping options are omitted.

        Example:
            >>> TestAddingAPI().test_find_books_in_directory_default_mode_is_multi_book(tmp_path)  # doctest: +SKIP


        :param tmp_path: Directory containing three uniquely named EPUB fixtures.
        :return: None; assert the default call yields three candidate groups.
        """
        # Default discovery mode should preserve per-stem candidates.
        (tmp_path / "a.epub").write_text("a", encoding="utf-8")
        (tmp_path / "b.epub").write_text("b", encoding="utf-8")
        (tmp_path / "c.epub").write_text("c", encoding="utf-8")

        books = list(find_books_in_directory(dirpath=tmp_path))
        assert len(books) == 3

    def test_recursive_import_defaults_to_multi_book_mode(self, monkeypatch, tmp_path: Path) -> None:
        """
        Route default recursive discovery to the multi-book adapter using recording doubles.

        No legacy metadata module or database write is required. The test checks
        which child importer was invoked, not cancellation or traversal ordering.

        Example:
            >>> TestAddingAPI().test_recursive_import_defaults_to_multi_book_mode(monkeypatch, tmp_path)  # doctest: +SKIP


        :param monkeypatch: Replace both directory import adapters for this test.
        :param tmp_path: Existing root visited by the real os.walk call.
        :return: None; assert empty duplicate output, no single-book calls, and multi-book routing.
        """
        single_calls: list[Path] = []
        multi_calls: list[Path] = []

        def _single(*args, **kwargs):
            """
            Record an unexpected single-book child call without importing anything.

            The default expression eagerly reads args[1] even if dirpath is
            present in kwargs; this double expects the caller's positional shape.

            Example:
                >>> _single(database, directory)  # doctest: +SKIP
                []


            :param args: Child importer arguments with directory in position one.
            :param kwargs: Optional directory override and other ignored import options.
            :return: Empty duplicate list after appending the selected Path.
            """
            single_calls.append(Path(kwargs.get("dirpath", args[1])))
            return []

        def _multi(*args, **kwargs):
            """
            Record the expected multi-book child call without metadata or database work.

            The positional directory is evaluated as the default even when a
            keyword override exists, matching the actual caller shape in this test.

            Example:
                >>> _multi(database, directory)  # doctest: +SKIP
                []


            :param args: Child importer arguments with directory in position one.
            :param kwargs: Optional directory override and ignored callback/rule arguments.
            :return: Empty duplicate list after recording the selected directory.
            """
            multi_calls.append(Path(kwargs.get("dirpath", args[1])))
            return []

        monkeypatch.setattr(adding_module, "import_book_directory", _single)
        monkeypatch.setattr(adding_module, "import_book_directory_multiple", _multi)

        duplicates = adding_module.recursive_import(db=object(), root=tmp_path)
        assert duplicates == []
        assert not single_calls
        assert multi_calls

    def test_single_fmt_true_is_deprecated_and_ignored(self, tmp_path: Path) -> None:
        """
        Warn on single_fmt=True while retaining every same-format path in directory mode.

        Example:
            >>> TestAddingAPI().test_single_fmt_true_is_deprecated_and_ignored(tmp_path)  # doctest: +SKIP


        :param tmp_path: Directory populated with three EPUB-named text fixtures.
        :return: None; assert the deprecation warning and all three retained filenames.
        """
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
