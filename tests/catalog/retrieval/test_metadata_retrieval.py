"""
Verify the explicit metadata facade exports stable container and helper identities.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test metadata top level facade through its owning regression module::

        python -m pytest -q tests/catalog/retrieval/test_metadata_retrieval.py
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.caches.api import CacheState
from LiuXin_alpha.catalog.retrieval import metadata
from LiuXin_alpha.catalog.retrieval.metadata import MetadataRetriever
from LiuXin_alpha.metadata.containers import (
    LazyLiuXinWEMIMetadata,
    LiuXinWEMIMetadata,
)
from tests.catalog.retrieval.test_item_metadata_hydrator import (
    FakeCacheFacade,
    FakeStorageCache,
    _build_fake_database,
)


class _InitiallyEmptyCacheFacade(FakeCacheFacade):
    """
    Provide the InitiallyEmptyCacheFacade test fixture or double with explicit deterministic behavior.

    Example:
        Exercise InitiallyEmptyCacheFacade through its owning regression module::

            python -m pytest -q tests/catalog/retrieval/test_metadata_retrieval.py
    """

    def __init__(self, storage: FakeStorageCache) -> None:
        """
        Initialize the InitiallyEmptyCacheFacade test double.

        Example:
            Exercise InitiallyEmptyCacheFacade.init through its owning regression module::

                python -m pytest -q tests/catalog/retrieval/test_metadata_retrieval.py


        :param storage: Value supplied for storage in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        super().__init__(storage)
        self.load_calls = 0
        self._state = CacheState.EMPTY

    @property
    def state(self) -> CacheState:
        """
        Return the cache lifecycle state exposed to the adapter.

        Example:
            Exercise InitiallyEmptyCacheFacade.state through its owning regression module::

                python -m pytest -q tests/catalog/retrieval/test_metadata_retrieval.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._state

    def load(self) -> None:
        """
        Load deterministic cache state for adapter tests.

        Example:
            Exercise InitiallyEmptyCacheFacade.load through its owning regression module::

                python -m pytest -q tests/catalog/retrieval/test_metadata_retrieval.py


        :return: None; the function records state or raises through its assertions.
        """
        self.load_calls += 1
        self._state = CacheState.READY


def test_explicit_metadata_facade_exports_workflows_without_leaf_containers() -> None:
    """
    Verify the explicit metadata facade exports workflows without leaf containers.

    Example:
        Exercise the explicit metadata facade exports through its owning regression module::

            python -m pytest -q tests/catalog/retrieval/test_metadata_retrieval.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify metadata from database hydrates wemi and kind views.

    Example:
        Exercise test metadata from database hydrates wemi and kind views through its owning regression module::

            python -m pytest -q tests/catalog/retrieval/test_metadata_retrieval.py


    :return: None; the function records state or raises through its assertions.
    """
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


def test_metadata_retriever_binds_database_owned_read_workflows() -> None:
    """The Catalog retrieval service supplies its bound database to hydration calls."""
    db = _build_fake_database()

    hydrated = MetadataRetriever(db).from_database(item_id=1)

    assert isinstance(hydrated, LiuXinWEMIMetadata)
    assert hydrated.database_ids["item_id"] == 1


def test_metadata_from_database_can_read_from_explicit_cache() -> None:
    """
    Verify metadata from database can read from explicit cache.

    Example:
        Exercise test metadata from database can read from explicit cache through its owning regression module::

            python -m pytest -q tests/catalog/retrieval/test_metadata_retrieval.py


    :return: None; the function records state or raises through its assertions.
    """
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


def test_metadata_from_database_composes_read_cache_when_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Automatic cache reads use the Catalog-independent read-cache factory."""
    db = _build_fake_database()
    cache = FakeCacheFacade(FakeStorageCache(db))
    calls: list[tuple[object, str, dict[str, object]]] = []

    def create_read_cache(
        database: object,
        storage_type: str,
        **storage_kwargs: object,
    ) -> FakeCacheFacade:
        calls.append((database, storage_type, storage_kwargs))
        return cache

    monkeypatch.setattr(metadata, "create_read_cache", create_read_cache)

    hydrated = metadata.cache_metadata_from_database(
        db,
        item_id=1,
        cache_type="schema_backed",
        cache_kwargs={"marker": 7},
        allow_database_fallback=False,
    )

    assert hydrated.title == "Permutation City"
    assert calls == [(db, "schema_backed", {"marker": 7})]


def test_metadata_from_database_loads_an_initially_empty_cache_facade() -> None:
    """
    Verify metadata from database loads an initially empty cache facade.

    Example:
        Exercise test metadata from database loads an initially empty cache facade through its owning regression module::

            python -m pytest -q tests/catalog/retrieval/test_metadata_retrieval.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify lazy metadata from database defers and optionally forces fields.

    Example:
        Exercise test lazy metadata from database defers and optionally forces fields through its owning regression module::

            python -m pytest -q tests/catalog/retrieval/test_metadata_retrieval.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify lazy metadata can read from explicit cache source.

    Example:
        Exercise test lazy metadata can read from explicit cache source through its owning regression module::

            python -m pytest -q tests/catalog/retrieval/test_metadata_retrieval.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify metadata from database rejects unknown source or kind.

    Example:
        Exercise test metadata from database rejects unknown source or kind through its owning regression module::

            python -m pytest -q tests/catalog/retrieval/test_metadata_retrieval.py


    :return: None; the function records state or raises through its assertions.
    """
    db = _build_fake_database()

    with pytest.raises(ValueError, match="Expected 'database' or 'cache'"):
        metadata.metadata_from_database(db, item_id=1, source="memory")

    with pytest.raises(ValueError, match="Expected 'wemi', 'liuxin', or 'calibre'"):
        metadata.metadata_from_database(db, item_id=1, kind="marc")
