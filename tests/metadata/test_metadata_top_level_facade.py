from __future__ import annotations

import pytest

from LiuXin_alpha import metadata
from LiuXin_alpha.caches import CacheState
from LiuXin_alpha.metadata.containers import (
    LazyLiuXinWEMIMetadata,
    LiuXinWEMIMetadata,
)
from tests.metadata.containers.test_item_metadata_hydrator import (
    FakeCacheFacade,
    FakeStorageCache,
    _build_fake_database,
)


class _InitiallyEmptyCacheFacade(FakeCacheFacade):
    def __init__(self, storage: FakeStorageCache) -> None:
        super().__init__(storage)
        self.load_calls = 0
        self._state = CacheState.EMPTY

    @property
    def state(self) -> CacheState:
        return self._state

    def load(self) -> None:
        self.load_calls += 1
        self._state = CacheState.READY


def test_metadata_top_level_exports_workflow_facade_without_leaf_containers() -> None:
    expected = {
        "LiuXinWEMIMetadata",
        "LazyLiuXinWEMIMetadata",
        "DatabaseMetadataReadSource",
        "CacheMetadataReadSource",
        "metadata_from_database",
        "lazy_metadata_from_database",
        "cache_metadata_from_database",
        "metadata_read_source_from",
        "metadata_from_opf",
        "metadata_to_opf_bytes",
        "metadata_to_opf_file",
        "update_opf_bytes",
        "update_opf_file",
        "fmt_sidx",
    }

    assert expected <= set(metadata.__all__)
    for name in expected:
        assert hasattr(metadata, name)

    assert not hasattr(metadata, "WorkTitle")
    assert not hasattr(metadata, "WorkLanguagesContainer")
    assert not hasattr(metadata, "WorkAgentCredit")


def test_metadata_from_database_hydrates_wemi_and_kind_views() -> None:
    db = _build_fake_database()

    wemi_metadata = metadata.metadata_from_database(db, item_id=1)
    calibre_metadata = metadata.metadata_from_database(
        db,
        item_id=1,
        kind="calibre",
    )

    assert isinstance(wemi_metadata, LiuXinWEMIMetadata)
    assert wemi_metadata.title == "Permutation City"
    assert wemi_metadata.database_ids["work_id"] == 30
    assert calibre_metadata.title == "Permutation City"
    assert list(calibre_metadata.tags) == ["Space Opera"]


def test_metadata_from_database_can_read_from_explicit_cache() -> None:
    db = _build_fake_database()

    wemi_metadata = metadata.cache_metadata_from_database(
        db,
        item_id=1,
        cache=FakeCacheFacade(FakeStorageCache(db)),
        allow_database_fallback=False,
    )

    assert isinstance(wemi_metadata, LiuXinWEMIMetadata)
    assert wemi_metadata.title == "Permutation City"
    assert list(wemi_metadata.tags.keys()) == ["Space Opera"]


def test_metadata_from_database_loads_an_initially_empty_cache_facade() -> None:
    db = _build_fake_database()
    cache = _InitiallyEmptyCacheFacade(FakeStorageCache(db))

    wemi_metadata = metadata.cache_metadata_from_database(
        db,
        item_id=1,
        cache=cache,
        allow_database_fallback=False,
    )

    assert wemi_metadata.title == "Permutation City"
    assert cache.load_calls == 1


def test_lazy_metadata_from_database_defers_and_optionally_forces_fields() -> None:
    db = _build_fake_database()

    lazy_metadata = metadata.lazy_metadata_from_database(db, item_id=1)
    forced_metadata = metadata.lazy_metadata_from_database(
        db,
        item_id=1,
        force_hydrate=("tags", "labels"),
    )

    assert isinstance(lazy_metadata, LazyLiuXinWEMIMetadata)
    assert lazy_metadata.is_lazy_field_loaded("tags") is False
    assert lazy_metadata.title == "Permutation City"
    assert forced_metadata.is_lazy_field_loaded("tags") is True
    assert forced_metadata.is_lazy_field_loaded("labels") is True
    assert list(forced_metadata.tags.keys()) == ["Space Opera"]


def test_lazy_metadata_can_read_from_explicit_cache_source() -> None:
    db = _build_fake_database()

    lazy_metadata = metadata.cache_metadata_from_database(
        db,
        item_id=1,
        cache=FakeCacheFacade(FakeStorageCache(db)),
        allow_database_fallback=False,
        lazy=True,
        force_hydrate=("tags", "labels", "identifiers"),
    )

    assert isinstance(lazy_metadata, LazyLiuXinWEMIMetadata)
    assert list(lazy_metadata.tags.keys()) == ["Space Opera"]
    assert list(lazy_metadata.labels.keys()) == ["Science Fiction"]
    assert set(lazy_metadata.get_identifiers()["isbn"]) == {"9780000000001"}


def test_metadata_from_database_rejects_unknown_source_or_kind() -> None:
    db = _build_fake_database()

    with pytest.raises(ValueError, match="Expected 'database' or 'cache'"):
        metadata.metadata_from_database(db, item_id=1, source="memory")

    with pytest.raises(ValueError, match="Expected 'wemi', 'liuxin', or 'calibre'"):
        metadata.metadata_from_database(db, item_id=1, kind="marc")
