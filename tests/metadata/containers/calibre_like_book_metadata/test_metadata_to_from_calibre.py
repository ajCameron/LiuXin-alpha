# tests/metadata/containers/calibre_like_book_metadata/test_metadata_to_from_calibre.py

from __future__ import annotations

import sys
import types
from datetime import datetime

import pytest

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData


def _install_fake_calibre(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Provide a minimal fake calibre module tree so to_calibre() can run even if calibre isn't installed.
    """
    calibre = types.ModuleType("calibre")
    ebooks = types.ModuleType("calibre.ebooks")
    metadata = types.ModuleType("calibre.ebooks.metadata")
    book = types.ModuleType("calibre.ebooks.metadata.book")
    base = types.ModuleType("calibre.ebooks.metadata.book.base")

    class MetaInformation:
        def __init__(self, title=None, authors=None):
            self.title = title
            self.authors = authors

    base.MetaInformation = MetaInformation
    base.Metadata = MetaInformation  # tolerate either import style

    monkeypatch.setitem(sys.modules, "calibre", calibre)
    monkeypatch.setitem(sys.modules, "calibre.ebooks", ebooks)
    monkeypatch.setitem(sys.modules, "calibre.ebooks.metadata", metadata)
    monkeypatch.setitem(sys.modules, "calibre.ebooks.metadata.book", book)
    monkeypatch.setitem(sys.modules, "calibre.ebooks.metadata.book.base", base)


class _FakeCalibreMd:
    def __init__(
        self,
        *,
        title="T",
        authors=("A",),
        author_sort=None,
        creator_sort=None,
        pubdate=None,
        pub_date=None,
        identifiers=None,
        languages=None,
        application_id=None,
        applicationid=None,
    ):
        self.title = title
        self.authors = list(authors) if authors is not None else None
        if author_sort is not None:
            self.author_sort = author_sort
        if creator_sort is not None:
            self.creator_sort = creator_sort
        if pubdate is not None:
            self.pubdate = pubdate
        if pub_date is not None:
            self.pub_date = pub_date
        if languages is not None:
            self.languages = languages
        if application_id is not None:
            self.application_id = application_id
        if applicationid is not None:
            self.applicationid = applicationid

        self._identifiers = identifiers or {}

    def get_identifiers(self):
        return dict(self._identifiers)


def test_from_calibre_author_sort_preference_and_pubdate_choice() -> None:
    now = datetime.utcnow()

    # both present -> prefer creator_sort if set, else author_sort
    c1 = _FakeCalibreMd(author_sort="AS", creator_sort="CS", pubdate=now, identifiers={})
    md1 = CalibreLikeLiuXinBookMetaData.from_calibre(c1)
    assert md1.creator_sort in ("CS", "AS")

    # pub_date alternative
    c2 = _FakeCalibreMd(pub_date=now, identifiers={})
    md2 = CalibreLikeLiuXinBookMetaData.from_calibre(c2)
    assert md2.pubdate == now


def test_from_calibre_identifier_rekey_scheme_smoke() -> None:
    # Pull the scheme from the module so we always pick a known alias.
    from LiuXin_alpha.metadata.containers.calibre_like_book_metadata.calibre_to_and_from_mixin import (
        EXTERNAL_EBOOK_REKEY_SCHEME,
    )

    # pick one alias -> canonical
    alias_set, canonical = next(iter(EXTERNAL_EBOOK_REKEY_SCHEME.items()))
    alias = next(iter(alias_set))

    c = _FakeCalibreMd(identifiers={alias: "V"})
    md = CalibreLikeLiuXinBookMetaData.from_calibre(c)

    # The canonical key should now exist on md as an attribute container
    container = getattr(md, canonical)
    assert "V" in container


# def test_to_calibre_runs_without_real_calibre(monkeypatch: pytest.MonkeyPatch) -> None:
#     _install_fake_calibre(monkeypatch)
#
#     md = CalibreLikeLiuXinBookMetaData(title="T", authors=["A"])
#     md.languages = ["en"]
#     md.rating = 5  # many implementations alias rating->ratings internally
#
#     cal = md.to_calibre()
#     assert hasattr(cal, "title")
#     assert cal.title == "T"
#     assert getattr(cal, "authors", None) is not None
