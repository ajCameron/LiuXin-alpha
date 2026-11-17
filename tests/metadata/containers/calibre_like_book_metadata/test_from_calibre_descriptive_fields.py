"""
Check that the LiuXin container imports the descriptive fields to_calibre exports.

``from_calibre`` used to import only the title, authors, sort, dates, identifiers,
languages, producer and cover, so every reader that converts through it dropped the
publisher, tags, comments, series, rating, title sort and rights it had parsed.
"""

from __future__ import annotations

from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)


def described_book() -> calibreMetadata:
    """
    Build calibre metadata with every descriptive field set.

    Example:
        >>> described_book().publisher
        'Pub Co'


    :return: The calibre metadata.
    """
    book = calibreMetadata("Example", ["Writer"])
    book.publisher = "Pub Co"
    book.tags = ["Sea; Ships", "Fiction"]
    book.comments = "<p>A blurb.</p>"
    book.series = "Saga"
    book.series_index = 3.5
    book.rating = 8
    book.title_sort = "Example, The"
    book.rights = "CC-BY"
    return book


def test_from_calibre_imports_the_descriptive_fields() -> None:
    """
    Import each descriptive field, keeping a tag that contains a semicolon whole.

    Example:
        >>> test_from_calibre_imports_the_descriptive_fields()


    :return: None; the assertions are the check.
    """
    md = CalibreLikeLiuXinBookMetaData.from_calibre(described_book())

    assert list(md.publisher) == ["Pub Co"]
    assert list(md.tags) == ["Sea; Ships", "Fiction"]
    assert list(md.comments) == ["<p>A blurb.</p>"]
    assert list(md.series) == ["Saga"]
    assert dict(md.series_index) == {"Saga": 3.5}
    assert md.ratings["calibre"] == 8
    assert md.title_sort == "Example, The"
    assert md.rights == "CC-BY"


def test_from_calibre_skips_null_descriptive_fields() -> None:
    """
    Add no empty entries for fields the calibre metadata leaves unset.

    Example:
        >>> test_from_calibre_skips_null_descriptive_fields()


    :return: None; the assertions are the check.
    """
    md = CalibreLikeLiuXinBookMetaData.from_calibre(
        calibreMetadata("Example", ["Writer"])
    )

    assert not md.publisher
    assert not md.tags
    assert not md.comments
    assert not md.series
    assert not md.series_index
    assert "calibre" not in md.ratings
    assert md.rights is None


def test_descriptive_fields_survive_a_round_trip() -> None:
    """
    Export the imported fields again, rights included.

    Example:
        >>> test_descriptive_fields_survive_a_round_trip()


    :return: None; the assertions are the check.
    """
    book = CalibreLikeLiuXinBookMetaData.from_calibre(described_book()).to_calibre()

    assert book.publisher == "Pub Co"
    assert book.tags == ["Sea; Ships", "Fiction"]
    assert book.comments == "<p>A blurb.</p>"
    assert (book.series, book.series_index) == ("Saga", 3.5)
    assert book.rating == 8
    assert book.title_sort == "Example, The"
    assert book.rights == "CC-BY"


def test_a_scalar_series_index_attaches_to_the_only_series() -> None:
    """
    Index the single series set, rather than failing on Python 2 key indexing.

    Example:
        >>> test_a_scalar_series_index_attaches_to_the_only_series()


    :return: None; the assertions are the check.
    """
    md = CalibreLikeLiuXinBookMetaData("Example", ["Writer"])
    md.series = "Saga"

    md.series_index = 2.0

    assert dict(md.series_index) == {"Saga": 2.0}
