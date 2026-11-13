"""Cache-owned native metadata publication across transaction boundaries."""

import pytest

from LiuXin_alpha.caches.custom_fields import CustomFieldCache
from LiuXin_alpha.catalog.field_metadata import FieldMetadata
from tests.databases.test_native_custom_fields import native as native
from tests.databases.test_row_write_safety import db as db
from tests.databases.test_row_write_safety import template as template


@pytest.fixture
def projection(native):
    service, _ = native
    return CustomFieldCache(service, FieldMetadata())


def test_native_writes_leave_metadata_to_explicit_cache_owner(projection):
    service = projection.service
    assert not hasattr(service, "field_metadata")
    field = service.create_field("works", "direct")
    assert field.key not in projection.field_metadata
    projection.refresh()
    assert projection.field_metadata[field.key]["revision"] == field.revision


def test_cache_nested_scopes_publish_final_state_and_discard_savepoints(projection):
    metadata = projection.field_metadata
    service = projection.service
    with projection.write():
        first = service.create_field("works", "first")
        with projection.write():
            second = service.create_field("works", "second")
        with pytest.raises(RuntimeError, match="abort"):
            with projection.write():
                service.create_field("works", "aborted")
                raise RuntimeError("abort")
        last = service.create_field("works", "last")
        assert first.key not in metadata
    assert {d.key for d in service.list_fields()} == {first.key, second.key, last.key}
    assert all(d.key in metadata for d in service.list_fields())
    assert "#works.aborted" not in metadata


def test_cache_publication_waits_for_outer_commit_and_rolls_back(projection):
    service = projection.service
    with pytest.raises(RuntimeError, match="abort"):
        with service.write():
            with projection.write():
                field = service.create_field("works", "pending")
            projection.refresh()
            assert field.key not in projection.field_metadata
            raise RuntimeError("abort")
    assert field.key not in projection.field_metadata
    assert service.list_fields() == ()
    with service.write():
        with projection.write():
            field = service.create_field("works", "committed")
        assert field.key not in projection.field_metadata
    assert field.key in projection.field_metadata


def test_cache_commit_failure_preserves_previous_projection(projection, monkeypatch):
    service = projection.service
    with projection.write():
        existing = service.create_field("works", "existing")
    before = dict(projection.field_metadata[existing.key])
    with monkeypatch.context() as patch:
        with pytest.raises(RuntimeError, match="commit failure"):
            with projection.write():
                service.retire_field(existing.id)
                service.create_field("works", "lost")

                def fail():
                    raise RuntimeError("commit failure")

                patch.setattr(
                    service.db.macros.current_transaction_connection(), "commit", fail
                )
    assert projection.field_metadata[existing.key] == before
    assert "#works.lost" not in projection.field_metadata
    assert service.list_fields() == (existing,)


def test_cache_publication_failure_reports_committed_write(projection, monkeypatch):
    service = projection.service

    def fail():
        raise RuntimeError("cache delivery failed")

    with monkeypatch.context() as patch:
        patch.setattr(projection, "prepare", lambda: fail)
        with pytest.raises(ExceptionGroup, match="Committed transaction"):
            with projection.write():
                field = service.create_field("works", "durable")
    assert service.get_field(field.id) == field
    projection.refresh()
    assert field.key in projection.field_metadata


def test_cache_projects_revisions_series_migration_and_retirement(projection, native):
    service, (owner, _) = native
    metadata = projection.field_metadata
    with projection.write():
        field = service.create_field("works", "series", datatype="series", mode="list")
    assert field.key + "_index" not in metadata
    with projection.write():
        service.set_value(owner, field.id, ["Saga"])
    assert metadata[field.key]["revision"] == service.get_field(field.id).revision
    with projection.write():
        service.apply_field_migration(
            service.plan_field_migration(field.id, {"mode": "scalar"}, reduce="first")
        )
    assert metadata[field.key]["collection_mode"] == "scalar"
    assert field.key + "_index" not in metadata
    with projection.write():
        service.retire_field(field.id)
    assert field.key not in metadata and field.key + "_index" not in metadata


def test_failed_receipt_listener_does_not_skip_cache_publication(projection):
    def fail(receipt):
        raise RuntimeError("listener failure")

    projection.service.subscribe(fail)
    with pytest.raises(ExceptionGroup, match="Committed transaction"):
        with projection.write() as service:
            field = service.create_field("works", "published")
    assert field.key in projection.field_metadata
