"""Core field commands select one writer and preserve its persistence semantics."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from LiuXin_alpha.caches.api import CacheAPI, CacheReconciliationError, CacheState
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.core.application_api import CoreApplicationAPI
from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.services import CoreServices
from tests.databases.caches.test_metadata_api import MODES
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


def write_field(services, single, work, title):
    api = CoreApplicationAPI()
    payload = {"src_table": "works", "dst_column": "work_title"}
    if single:
        payload.update(src_id=work, dst_value=title)
        handler = api.catalog_field_write_one
    else:
        payload["args"] = [{work: title}]
        handler = api.catalog_field_write
    return handler(SimpleNamespace(services=services), CoreCommand("field", payload))


@pytest.mark.parametrize("metadata_db", ("SQLite", "SQLite_apsw"), indirect=True)
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("single", (False, True))
def test_field_command_reconciles_native_cache(metadata_db, mode, single, monkeypatch):
    catalog = Catalog(metadata_db)
    work = catalog.works.create({"title": "Before"})
    apply = Mock(wraps=catalog._apply_column_update)
    monkeypatch.setattr(catalog, "_apply_column_update", apply)
    services = CoreServices(
        library=SimpleNamespace(database=metadata_db), catalog=catalog, cache_type=mode
    )
    try:
        result = write_field(services, single, work, "After")
        assert result == {
            "result": {work: "After"},
            "cache": {"configured": mode is not None, "reconciled": mode is not None},
        }
        apply.assert_called_once()
        assert catalog.works.require(work)["work_title"] == "After"
        if mode is not None:
            assert services.field_writer is services.metadata
            assert services.cache.get("works", work).value["work_title"] == "After"
            assert not hasattr(services.cache, "write_one")
        else:
            assert services.field_writer is catalog
    finally:
        services.close()
    with pytest.raises(RuntimeError, match="closed"):
        write_field(services, single, work, "Must not persist")


@pytest.mark.parametrize("single", (False, True))
def test_field_command_preserves_refresh_failure_receipt(
    metadata_db, single, monkeypatch
):
    catalog = Catalog(metadata_db)
    work = catalog.works.create({"title": "Before"})
    services = CoreServices(
        library=SimpleNamespace(database=metadata_db),
        catalog=catalog,
        cache_type="schema_backed",
    )
    apply = Mock(wraps=catalog._apply_column_update)
    monkeypatch.setattr(catalog, "_apply_column_update", apply)
    try:
        with monkeypatch.context() as broken:
            broken.setattr(
                services.cache.storage,
                "reload_main_table",
                Mock(side_effect=RuntimeError("refresh failed")),
            )
            with pytest.raises(CacheReconciliationError) as caught:
                write_field(services, single, work, "Saved once")
        assert caught.value.receipt == {work: "Saved once"}
        assert services.cache.state == CacheState.DIRTY
        apply.assert_called_once()
        assert catalog.works.require(work)["work_title"] == "Saved once"
        assert services.cache.get("works", work).value["work_title"] == "Saved once"
    finally:
        services.close()


@pytest.mark.parametrize("single", (False, True))
@pytest.mark.parametrize("cached", (False, True))
def test_field_command_keeps_injected_writer_errors(single, cached):
    database = object()
    catalog = Mock(database=database)
    cache = (
        Mock(spec=CacheAPI, database=database, state=CacheState.READY)
        if cached
        else None
    )
    target = cache if cached else catalog
    method = "write_one" if single else "write"
    writer = Mock(side_effect=ValueError("selected writer failed"))
    setattr(target, method, writer)
    services = CoreServices(
        library=SimpleNamespace(database=database), catalog=catalog, cache=cache
    )
    try:
        with pytest.raises(ValueError, match="selected writer failed"):
            write_field(services, single, 1, "After")
        writer.assert_called_once()
        if cached:
            catalog.write.assert_not_called()
            catalog.write_one.assert_not_called()
    finally:
        services.close()
