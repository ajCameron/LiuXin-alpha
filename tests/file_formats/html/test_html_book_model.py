"""
Check the HTMLBook data model against the structural invariants of its spec.

The invariants S1 to S6 are those of ``dev-docs/file_formats/html-book-spec.md``.
None of them needs the filesystem, so no test here creates a file; several
use a root folder that does not exist to show that construction never looks.
"""

from __future__ import annotations

import dataclasses
import doctest
from pathlib import Path, PurePosixPath

import pytest

from LiuXin_alpha.file_formats.html import book as book_module
from LiuXin_alpha.file_formats.html.book import (
    HTMLBook,
    HTMLBookError,
    HTMLBookPathError,
    HTMLBookStructureError,
    HTMLPage,
    HTMLTocEntry,
    checked_member_path,
    member_path,
)

ROOT = Path(__file__).resolve().parent / "not-a-real-folder"
INDEX = PurePosixPath("index.html")
CHAPTER = PurePosixPath("text/chapter1.html")
COVER = PurePosixPath("images/cover.jpg")


def page(path: PurePosixPath, title: str | None = None) -> HTMLPage:
    """
    Build a UTF-8 page for a test.

    Example:
        >>> page(INDEX, "Contents").title
        'Contents'


    :param path: The page's member path.
    :param title: The page's title, if any.
    :return: The page.
    """
    return HTMLPage(path, "utf-8", title)


def book(**changes: object) -> HTMLBook:
    """
    Build a valid two-page book, with any field replaced.

    Example:
        >>> book().entry
        PurePosixPath('index.html')


    :param changes: Fields to set instead of the defaults.
    :return: The book.
    """
    fields: dict[str, object] = {
        "root": ROOT,
        "entry": INDEX,
        "pages": (page(INDEX, "A Book"), page(CHAPTER)),
        "resources": (COVER, PurePosixPath("style.css")),
        "cover": COVER,
    }
    fields.update(changes)
    return HTMLBook(**fields)


def test_a_valid_book_describes_its_folder_without_reading_it() -> None:
    """
    Build a book whose root does not exist and read its derived values.

    Example:
        >>> test_a_valid_book_describes_its_folder_without_reading_it()


    :return: None after the title, page paths and member paths are read.
    """
    assert not ROOT.exists()
    built = book(metadata=PurePosixPath("metadata.opf"))
    assert built.title == "A Book"
    assert built.page_paths == (INDEX, CHAPTER)
    assert [str(path) for path in built.member_paths] == [
        "index.html",
        "text/chapter1.html",
        "images/cover.jpg",
        "style.css",
        "metadata.opf",
    ]
    assert built.toc_is_derived is True


def test_book_values_are_immutable() -> None:
    """
    Refuse assignment to a book, a page or a table-of-contents entry.

    Example:
        >>> test_book_values_are_immutable()


    :return: None after each assignment is refused.
    """
    built = book()
    with pytest.raises(dataclasses.FrozenInstanceError):
        built.entry = CHAPTER
    with pytest.raises(dataclasses.FrozenInstanceError):
        built.pages[0].title = "Other"


def test_every_error_is_a_value_error() -> None:
    """
    Let callers catch every model error as one family.

    Example:
        >>> test_every_error_is_a_value_error()


    :return: None after the error hierarchy is checked.
    """
    assert issubclass(HTMLBookError, ValueError)
    assert issubclass(HTMLBookPathError, HTMLBookError)
    assert issubclass(HTMLBookStructureError, HTMLBookError)


@pytest.mark.parametrize(
    "text",
    ["index.html", "text/chapter 1.html", "名前/ページ.xhtml", "a..b/c...html"],
)
def test_member_path_accepts_relative_forward_slash_paths(text: str) -> None:
    """
    Accept ordinary relative paths, including dots inside names.

    Example:
        >>> test_member_path_accepts_relative_forward_slash_paths("index.html")


    :param text: A path that is a valid member path.
    :return: None after the path is accepted unchanged.
    """
    assert str(member_path(text)) == text


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("", "must not be empty"),
        ("/etc/passwd", "is not relative"),
        ("text//page.html", "empty segment"),
        ("text/", "empty segment"),
        ("./index.html", "'.' segment"),
        ("text/./page.html", "'.' segment"),
        ("../secret.txt", "'..' segment"),
        ("text/../../secret.txt", "'..' segment"),
        ("text\\page.html", "backslash"),
        ("..\\secret.txt", "backslash"),
        ("C:/Windows/win.ini", "has a drive"),
        ("C:win.ini", "has a drive"),
        ("page\x00.html", "NUL character"),
    ],
)
def test_member_path_rejects_anything_that_could_escape(
    text: str, message: str
) -> None:
    """
    Refuse each form of S2 violation in text from a file or archive.

    Example:
        >>> test_member_path_rejects_anything_that_could_escape("", "empty")


    :param text: A path that is not a member path.
    :param message: Part of the expected error message.
    :return: None after the path is refused with that message.
    """
    with pytest.raises(HTMLBookPathError, match=message):
        member_path(text)


@pytest.mark.parametrize(
    "value",
    [
        "index.html",
        Path("index.html"),
        PurePosixPath(""),
        PurePosixPath("."),
        PurePosixPath("/index.html"),
        PurePosixPath("../index.html"),
        PurePosixPath("text\\page.html"),
        PurePosixPath("D:/page.html"),
    ],
)
def test_held_paths_are_checked_too(value: object) -> None:
    """
    Refuse a value already held as a path when it is not a member path.

    Example:
        >>> test_held_paths_are_checked_too("index.html")


    :param value: A value that is not a member path.
    :return: None after the value is refused.
    """
    with pytest.raises(HTMLBookPathError):
        checked_member_path(value, "member")


def test_s1_root_must_be_absolute() -> None:
    """
    Refuse a relative or textual root.

    Example:
        >>> test_s1_root_must_be_absolute()


    :return: None after both roots are refused.
    """
    with pytest.raises(HTMLBookStructureError, match="absolute Path"):
        book(root=Path("relative/folder"))
    with pytest.raises(HTMLBookStructureError, match="absolute Path"):
        book(root=str(ROOT))


def test_s2_every_member_path_is_checked() -> None:
    """
    Refuse an escaping path in each field that holds one.

    Example:
        >>> test_s2_every_member_path_is_checked()


    :return: None after each field is given an escaping path.
    """
    escape = PurePosixPath("../outside.html")
    with pytest.raises(HTMLBookPathError, match="entry"):
        book(entry=escape)
    with pytest.raises(HTMLBookPathError, match="page path"):
        book(pages=(page(INDEX), page(escape)))
    with pytest.raises(HTMLBookPathError, match="resource"):
        book(resources=(COVER, escape))
    with pytest.raises(HTMLBookPathError, match="metadata"):
        book(metadata=escape)
    with pytest.raises(HTMLBookPathError, match="table-of-contents page"):
        HTMLTocEntry("Outside", escape)


def test_s3_pages_start_with_the_entry() -> None:
    """
    Refuse a book with no pages or whose first page is not the entry.

    Example:
        >>> test_s3_pages_start_with_the_entry()


    :return: None after both shapes are refused.
    """
    with pytest.raises(HTMLBookStructureError, match="at least one page"):
        book(pages=())
    with pytest.raises(HTMLBookStructureError, match="first page must be the entry"):
        book(pages=(page(CHAPTER), page(INDEX)))


def test_s4_members_are_distinct_and_in_the_right_place() -> None:
    """
    Refuse duplicates, a cover that is not a resource and a sidecar member.

    Example:
        >>> test_s4_members_are_distinct_and_in_the_right_place()


    :return: None after each misplacement is refused.
    """
    with pytest.raises(HTMLBookStructureError, match="more than once"):
        book(pages=(page(INDEX), page(CHAPTER), page(CHAPTER)))
    with pytest.raises(HTMLBookStructureError, match="more than once"):
        book(resources=(COVER, CHAPTER))
    with pytest.raises(HTMLBookStructureError, match="more than once"):
        book(resources=(COVER, COVER))
    with pytest.raises(HTMLBookStructureError, match="one of the resources"):
        book(cover=PurePosixPath("images/missing.jpg"))
    with pytest.raises(HTMLBookStructureError, match="one of the resources"):
        book(cover=INDEX)
    with pytest.raises(HTMLBookStructureError, match="must not also be"):
        book(metadata=COVER)
    with pytest.raises(HTMLBookStructureError, match="must not also be"):
        book(metadata=CHAPTER)


def test_s4_collections_are_tuples() -> None:
    """
    Refuse lists, so a book cannot change after it is checked.

    Example:
        >>> test_s4_collections_are_tuples()


    :return: None after list-valued fields are refused.
    """
    with pytest.raises(HTMLBookStructureError, match="pages must be a tuple"):
        book(pages=[page(INDEX)])
    with pytest.raises(HTMLBookStructureError, match="resources must be a tuple"):
        book(resources=[COVER])
    with pytest.raises(HTMLBookStructureError, match="toc must be a tuple"):
        book(toc=[HTMLTocEntry("Start", INDEX)])
    with pytest.raises(HTMLBookStructureError, match="only HTMLPage"):
        book(pages=(INDEX,))


@pytest.mark.parametrize(
    ("given", "stored"),
    [("utf-8", "utf-8"), ("cp1252", "cp1252"), ("iso8859-1", "iso8859-1")],
)
def test_s5_registered_codec_names_are_accepted(given: str, stored: str) -> None:
    """
    Accept an encoding written under its codec registry name.

    Example:
        >>> test_s5_registered_codec_names_are_accepted("utf-8", "utf-8")


    :param given: The encoding passed in.
    :param stored: The encoding the page holds.
    :return: None after the page is built.
    """
    assert HTMLPage(INDEX, given).encoding == stored


@pytest.mark.parametrize(
    ("given", "message"),
    [
        ("UTF8", "must be written as 'utf-8'"),
        ("latin-1", "must be written as 'iso8859-1'"),
        ("windows-1252", "must be written as 'cp1252'"),
        ("no-such-codec", "is not a known codec"),
    ],
)
def test_s5_other_spellings_and_unknown_codecs_are_refused(
    given: str, message: str
) -> None:
    """
    Refuse an alias, so equal encodings always compare equal, and refuse unknowns.

    Example:
        >>> test_s5_other_spellings_and_unknown_codecs_are_refused("UTF8", "utf-8")


    :param given: The encoding passed in.
    :param message: Part of the expected error message.
    :return: None after the page is refused.
    """
    with pytest.raises(HTMLBookStructureError, match=message):
        HTMLPage(INDEX, given)


@pytest.mark.parametrize("label", ["", "   ", "\t\n", "\u200b", "\u00a0"])
def test_s6_labels_must_show_something(label: str) -> None:
    """
    Refuse a table-of-contents label with no visible character.

    Example:
        >>> test_s6_labels_must_show_something("")


    :param label: A label that shows nothing.
    :return: None after the entry is refused.
    """
    with pytest.raises(HTMLBookStructureError, match="visible character"):
        HTMLTocEntry(label, INDEX)


def test_s6_entries_point_at_pages_at_any_depth() -> None:
    """
    Accept a nested table of contents and refuse a nested entry off the pages.

    Example:
        >>> test_s6_entries_point_at_pages_at_any_depth()


    :return: None after a valid and an invalid nested table are checked.
    """
    nested = HTMLTocEntry(
        "Part One",
        INDEX,
        children=(
            HTMLTocEntry("Chapter 1", CHAPTER),
            HTMLTocEntry("Section 1.1", CHAPTER, "s1"),
        ),
    )
    built = book(toc=(nested,), toc_is_derived=False)
    assert [entry.label for entry in built.toc[0].walk()] == [
        "Part One",
        "Chapter 1",
        "Section 1.1",
    ]

    stray = HTMLTocEntry("Part One", INDEX, children=(HTMLTocEntry("Cover", COVER),))
    with pytest.raises(HTMLBookStructureError, match="'Cover' points at"):
        book(toc=(stray,))


def test_toc_children_must_be_entries() -> None:
    """
    Refuse children that are not a tuple of entries.

    Example:
        >>> test_toc_children_must_be_entries()


    :return: None after list and mixed children are refused.
    """
    with pytest.raises(HTMLBookStructureError, match="must be a tuple"):
        HTMLTocEntry("Start", INDEX, children=[])
    with pytest.raises(HTMLBookStructureError, match="only HTMLTocEntry"):
        HTMLTocEntry("Start", INDEX, children=("Chapter 1",))


def test_title_comes_from_the_entry_page_only() -> None:
    """
    Take the book's title from the entry page, even when it has none.

    Example:
        >>> test_title_comes_from_the_entry_page_only()


    :return: None after an untitled entry page gives no book title.
    """
    untitled = book(pages=(page(INDEX), page(CHAPTER, "Chapter 1")))
    assert untitled.title is None


def test_examples_in_the_model_module_run() -> None:
    """
    Run the docstring examples of the model module as doctests.

    Example:
        >>> test_examples_in_the_model_module_run()


    :return: None after every example in the module passes.
    """
    result = doctest.testmod(book_module, verbose=False, report=False)
    assert result.attempted > 0
    assert result.failed == 0
