"""Verify the persistence-free metadata workflow facade."""

from __future__ import annotations

from LiuXin_alpha.metadata import facade


def test_metadata_facade_excludes_persistence_owned_workflows() -> None:
    """Database/cache hydration is available only from Catalog retrieval."""
    assert "metadata_from_database" not in facade.__all__
    assert "DatabaseMetadataReadSource" not in facade.__all__
    assert not hasattr(facade, "metadata_from_database")


def test_metadata_facade_formats_series_indexes_through_lazy_adapter() -> None:
    """The retained formatting convenience preserves its original arguments."""
    assert facade.fmt_sidx(2.5) == "2.50"
    assert facade.fmt_sidx(4, use_roman=True) == "IV"
