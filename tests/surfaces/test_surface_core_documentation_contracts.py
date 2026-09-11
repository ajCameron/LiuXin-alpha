"""
Check documented surface/Core adapter contracts using recording clients and factory doubles.

These tests exercise the real model, session, and compatibility views without
opening databases, making HTTP requests, or starting runtime services. They cover
shallow values, cache boundaries, conversion failures, ownership, and legacy write
return conventions rather than claiming a live transport integration run.
"""

from __future__ import annotations

import argparse
import binascii
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any
from unittest.mock import Mock, call

import pytest

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces import core as adapter
from LiuXin_alpha.surfaces.core import (
    CoreDatabaseView,
    CoreDriverView,
    CoreRow,
    CoreRowPage,
    CoreSurfaceModel,
    SurfaceCoreSession,
)


def _cached_model(*schemas: dict[str, Any]) -> tuple[CoreSurfaceModel, Mock]:
    """
    Load an explicit schema inventory into a real model, then clear the client's recorded setup call.

    Example:
        >>> model, client = _cached_model({"name": "works", "columns": ["work_id"]})
        >>> model.id_column("works")
        'work_id'
        >>> client.query.call_count
        0


    :param schemas: Ordered table summaries returned by the one setup schema.tables query.
    :return: Pair of schema-loaded model and its recording CoreClientAPI-spec mock.
    """
    client = Mock(spec=CoreClientAPI)
    client.query.return_value = {"tables": schemas}
    model = CoreSurfaceModel(client)
    model.table_names()
    client.reset_mock()
    return model, client


def test_row_values_and_pages_remain_shallow_unvalidated_records() -> None:
    """
    Preserve mutable value aliases, mapping-only truthiness, string-key collisions, and unverified page metadata.

    Example:
        >>> test_row_values_and_pages_remain_shallow_unvalidated_records()


    :return: None after row attribute freezing, shallow-copy identity, and passive page assertions.
    """
    nested: list[str] = []
    values = {"title": "雪", "nested": nested, "nullable": None}
    row = CoreRow("works", 7, values, ("tags",))
    copy = row.row_dict
    assert copy is not values and copy["nested"] is nested
    copy["title"] = "copy only"
    values["title"] = "updated"
    assert row["title"] == "updated" and row.get("nullable", "fallback") is None
    assert row.get("absent", nested) is nested
    assert list(row) == ["title", "nested", "nullable"] and len(row) == 3
    assert "row_id" not in row and "table" not in row
    with pytest.raises(KeyError):
        row["absent"]
    with pytest.raises(FrozenInstanceError):
        row.row_id = 9
    empty = CoreRow("works", 7, {})
    assert not empty
    page = CoreRowPage((row,), -2, -3, 0, False, "custom")
    assert (page.total_count, page.offset, page.limit) == (-2, -3, 0)
    converted = adapter._mapping({1: nested, "1": "last"}, label="record")
    assert converted == {"1": "last"}
    with pytest.raises(TypeError, match="record must be a mapping"):
        adapter._mapping([("key", "value")], label="record")


def test_session_open_routes_and_normalizes_only_the_selected_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Separate remote configuration from local construction and preserve server connection strings and forwarded policies.

    Example:
        >>> with pytest.MonkeyPatch.context() as patch:
        ...     test_session_open_routes_and_normalizes_only_the_selected_source(patch)


    :param monkeypatch: Fixture replacing the module's Core factory and client selector with recording mocks.
    :return: None after selection exclusivity, path handling, ownership, and option-forwarding assertions.
    """
    runtime = Mock()
    client = Mock(spec=CoreClientAPI)
    factory = Mock(return_value=runtime)
    selector = Mock(return_value=client)
    monkeypatch.setattr(adapter, "create_core", factory)
    monkeypatch.setattr(adapter, "core_client", selector)
    for kwargs in ({}, {"database_path": "", "endpoint": ""}):
        with pytest.raises(ValueError, match="exactly one"):
            SurfaceCoreSession.open(**kwargs)
    factory.assert_not_called()
    selector.assert_not_called()
    remote = SurfaceCoreSession.open(endpoint="", timeout_seconds="2.5", create=True)
    selector.assert_called_once_with(endpoint="", timeout_seconds=2.5)
    factory.assert_not_called()
    assert (
        remote.client is client and remote.runtime is None and not remote.owns_runtime
    )

    metadata = {"service": "catalogue"}
    for driver, path, expected in (
        ("SQLite", "~/catalogue.sqlite", Path("~/catalogue.sqlite").expanduser()),
        ("SQLite", "relative.sqlite", Path("relative.sqlite")),
        (" PG ", "service=~catalogue", "service=~catalogue"),
    ):
        local = SurfaceCoreSession.open(
            database_path=path,
            db_type=driver,
            database_metadata=metadata,
            create=1,
            backup=0,
            cache_type="test-cache",
            cache_allow_database_fallback=0,
            enable_storage_manager=0,
            strict_storage_manager_bootstrap=1,
            storage_startup_on_add=1,
            enable_maintenance=1,
            repair_bootstrap_rows=0,
            timeout_seconds=object(),
        )
        factory.assert_called_with(
            database_path=expected,
            db_type=driver,
            database_metadata=metadata,
            create=True,
            backup=False,
            cache_type="test-cache",
            cache_allow_database_fallback=False,
            enable_storage_manager=False,
            strict_storage_manager_bootstrap=True,
            storage_startup_on_add=True,
            enable_maintenance=True,
            repair_bootstrap_rows=False,
        )
        selector.assert_called_with(runtime=runtime)
        assert (
            local.client is client and local.runtime is runtime and local.owns_runtime
        )


def test_legacy_coercion_keeps_borrowed_services_and_direct_client_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Leave existing Core clients alone and pass other objects through the legacy factory with explicit ownership policy.

    Example:
        >>> with pytest.MonkeyPatch.context() as patch:
        ...     test_legacy_coercion_keeps_borrowed_services_and_direct_client_identity(patch)


    :param monkeypatch: Fixture installing recording factory/client-selector doubles.
    :return: None after identity, ignored direct-client options, and borrowed-service forwarding checks.
    """
    client = Mock(spec=CoreClientAPI)
    runtime = Mock()
    factory = Mock(return_value=runtime)
    monkeypatch.setattr(adapter, "create_core", factory)
    monkeypatch.setattr(adapter, "core_client", Mock(return_value=client))
    assert adapter.coerce_surface_core(client, read_source=object()) == (client, None)
    factory.assert_not_called()
    database, manager, source, fallback = object(), object(), object(), object()
    normalized, session = adapter.coerce_surface_core(
        database,
        job_manager=manager,
        read_source=source,
        cache_type="test-cache",
        cache_allow_database_fallback=fallback,
    )
    factory.assert_called_once_with(
        database=database,
        job_manager=manager,
        close_job_manager_on_shutdown=False,
        read_source=source,
        cache_type="test-cache",
        cache_allow_database_fallback=fallback,
        enable_maintenance=False,
        repair_bootstrap_rows=False,
    )
    assert normalized is client and session is not None and session.owns_runtime
    session.close()
    runtime.shutdown.assert_called_once_with()


def test_session_close_records_failure_before_shutdown_and_does_not_retry() -> None:
    """
    Preserve at-most-once shutdown attempts, non-owning close behavior, and unsuppressed context exceptions.

    Example:
        >>> test_session_close_records_failure_before_shutdown_and_does_not_retry()


    :return: None after owned-failure identity, closed-session entry, and borrowed-client assertions.
    """
    client = Mock(spec=CoreClientAPI)
    failure = RuntimeError("shutdown failed")
    runtime = Mock()
    runtime.shutdown.side_effect = failure
    session = SurfaceCoreSession(client, runtime, owns_runtime=True)
    with pytest.raises(RuntimeError) as raised:
        session.close()
    assert raised.value is failure and session.closed
    with session as same:
        assert same is session and same.closed
    session.close()
    runtime.shutdown.assert_called_once_with()
    borrowed = SurfaceCoreSession.from_client(client)
    body_error = LookupError("context body")
    with pytest.raises(LookupError) as raised:
        with borrowed:
            raise body_error
    assert raised.value is body_error and borrowed.closed
    client.shutdown.assert_not_called()
    unowned = SurfaceCoreSession(client, runtime, owns_runtime=False)
    unowned.close()
    runtime.shutdown.assert_called_once_with()


def test_schema_cache_commits_only_complete_conversion_and_reuses_empty_results() -> (
    None
):
    """
    Keep failed schema loads retryable, drop falsey names, and retain successful empty inventories.

    Example:
        >>> test_schema_cache_commits_only_complete_conversion_and_reuses_empty_results()


    :return: None after malformed-entry retry, duplicate-name replacement, and empty-cache call-count checks.
    """
    client = Mock(spec=CoreClientAPI)
    client.query.side_effect = [
        {"tables": [{"name": "discarded"}, None]},
        {
            "tables": [
                {"name": "z"},
                {"name": 0},
                {"name": "A"},
                {"name": "z", "columns": ["new"]},
            ]
        },
        {"tables": []},
    ]
    model = CoreSurfaceModel(client)
    with pytest.raises(TypeError, match="table schema"):
        model.table_names()
    assert model._schema is None
    assert model.table_names() == ("A", "z")
    assert model.columns("z") == ("new",)
    assert model._table_summary("z") is model._schema_map()["z"]
    assert not model.table_exists(" Z ") and client.query.call_count == 2
    model.invalidate_schema()
    assert model.table_names() == model.table_names() == ()
    assert client.query.call_count == 3


def test_detail_cache_requires_relations_flag_and_exposes_only_shallow_copies() -> None:
    """
    Requery unflagged details, share nested schema values, and leave the cache intact during source refresh.

    Example:
        >>> test_detail_cache_requires_relations_flag_and_exposes_only_shallow_copies()


    :return: None after detail-query precedence, cache-key, aliasing, and refresh-result checks.
    """
    model, client = _cached_model({"name": "works", "columns": ["work_id"]})
    nested = ["tags"]
    client.query.side_effect = [
        {"name": "different", "related_tables": nested},
        {"name": "different", "related_tables": nested, "relations_included": True},
    ]
    first = model.table_schema("works")
    first["local"] = True
    assert "local" not in model._schema_map()["works"]
    second = model.table_schema("works")
    assert first["related_tables"] is second["related_tables"] is nested
    nested.append("subjects")
    assert model.related_tables("works") == ("tags", "subjects")
    assert model.table_names() == ("works",) and client.query.call_count == 2
    client.command.side_effect = [
        {"refreshed": False, "reloaded": True},
        {"reloaded": True},
        {},
        None,
    ]
    assert [model.refresh() for _ in range(4)] == [False, True, True, False]
    assert model.table_names() == ("works",) and client.query.call_count == 2


@pytest.mark.parametrize("bad", [None, "text", b"bytes", {"name": "works"}])
def test_schema_and_page_collections_reject_non_array_shapes(bad: Any) -> None:
    """
    Reject malformed schema/page collections rather than converting them into successful empty results.

    Example:
        >>> test_schema_and_page_collections_reject_non_array_shapes("text")


    :param bad: Non-Sequence or explicitly excluded text/bytes collection supplied by the mock Core.
    :return: None after both array checks raise TypeError and schema remains unloaded.
    """
    client = Mock(spec=CoreClientAPI)
    client.query.side_effect = [{"tables": bad}, {"records": bad}]
    model = CoreSurfaceModel(client)
    with pytest.raises(TypeError, match="must be an array"):
        model.table_names()
    assert model._schema is None
    with pytest.raises(TypeError, match="must be an array"):
        model.query_rows("works")


def test_page_request_coercions_do_not_recompute_backend_metadata_or_exhaust_rows() -> (
    None
):
    """
    Clamp request bounds while preserving response bounds/counts and single-query enumeration semantics.

    Example:
        >>> test_page_request_coercions_do_not_recompute_backend_metadata_or_exhaust_rows()


    :return: None after exact request shape, shallow predicate identity, row conversion, and call-count assertions.
    """
    client = Mock(spec=CoreClientAPI)
    model = CoreSurfaceModel(client)
    value: list[str] = ["雪"]
    predicate = {"field": "title", "operator": "eq", "value": value}
    client.query.return_value = {
        "records": [{"row_id": "7", "values": {1: value}}],
        "total_count": "99",
        "offset": -8,
        "limit": -2,
        "complete": False,
        "source": "custom",
    }
    page = model.query_rows(
        "works",
        predicates=(predicate,),
        relation={},
        text=" 雪 ",
        text_fields=(1,),
        sort=({"field": "title"}, 2),
        projection=(3,),
        offset=-7,
        limit=-4,
    )
    client.query.assert_called_once_with(
        "rows.query",
        {
            "table": "works",
            "predicates": [predicate],
            "relation": {},
            "text": " 雪 ",
            "text_fields": ["1"],
            "sort": [{"field": "title"}, "2"],
            "projection": ["3"],
            "offset": 0,
            "limit": 0,
        },
    )
    sent = client.query.call_args.args[1]["predicates"][0]
    assert sent is not predicate and sent["value"] is value
    assert (page.total_count, page.offset, page.limit, page.complete) == (
        99,
        -8,
        -2,
        False,
    )
    assert page.records[0].row_id == 7 and page.records[0]["1"] is value
    assert model.rows("works") == page.records
    assert (
        client.query.call_count == 2 and client.query.call_args.args[1]["limit"] is None
    )
    assert model.record_count("works") == 99
    assert client.query.call_count == 3 and client.query.call_args.args[1]["limit"] == 0


def test_row_conversion_uses_schema_relations_and_preserves_failure_identity() -> None:
    """
    Derive row relationships from cached schema, keep None distinct from malformed rows, and propagate read errors.

    Example:
        >>> test_row_conversion_uses_schema_relations_and_preserves_failure_identity()


    :return: None after identity/relationship conversion, unknown-table, malformed-value, and failure assertions.
    """
    model, client = _cached_model(
        {
            "name": "works",
            "relations_included": True,
            "related_tables": ["tags"],
        }
    )
    row = model.row_from_record(
        {
            "table": "works",
            "row_id": 7.9,
            "values": {},
            "linkable_tables": ["ignored"],
        }
    )
    assert row.row_id == 7 and row.linkable_tables == ("tags",)
    assert model.row_from_record({"table": "unknown"}).linkable_tables == ()
    client.query.assert_not_called()
    with pytest.raises(TypeError, match="Core row values"):
        model.row_from_record({"values": None})
    client.query.return_value = {"record": None}
    assert model.row("works", 7) is None
    failure = OSError("read failed")
    client.query.side_effect = failure
    with pytest.raises(OSError) as raised:
        model.row("works", 7)
    assert raised.value is failure


def test_relations_skip_unidentified_rows_and_forward_explicit_empty_type() -> None:
    """
    Avoid relation calls for None IDs and keep returned entity/link collections independent and ordered.

    Example:
        >>> test_relations_skip_unidentified_rows_and_forward_explicit_empty_type()


    :return: None after no-call behavior, exact relation payload, and unequal tuple-length assertions.
    """
    model, client = _cached_model()
    assert model.related(CoreRow("works", None, {}), "tags") == ((), ())
    client.query.assert_not_called()
    client.query.return_value = {
        "records": [{"row_id": 1}, {"row_id": 2}],
        "link_records": [{"row_id": 3}],
    }
    records, links = model.related(
        CoreRow("works", 7, {}), "tags", type_filter="", include_link_rows=True
    )
    client.query.assert_called_once_with(
        "relations.list",
        {
            "table": "works",
            "row_id": 7,
            "related_table": "tags",
            "type_filter": "",
            "include_link_rows": True,
        },
    )
    assert [row.row_id for row in records] == [1, 2] and links[0].row_id == 3


def test_driver_column_heuristics_preserve_priority_order_and_ambiguity() -> None:
    """
    Retain ID fallback order, priority-before-type prefixes, timestamp ties, and ambiguous link-column failures.

    Example:
        >>> test_driver_column_heuristics_preserve_priority_order_and_ambiguity()


    :return: None after real model/driver schema heuristics and capability override assertions.
    """
    model, client = _cached_model(
        {"name": "works", "columns": ["earlier_id", "id", "x_modified", "y_modified"]},
        {
            "name": "links",
            "columns": ["first_type", "later_priority", "a_work_id", "b_work_id"],
        },
        {"name": "view", "columns": [], "is_view": True},
        {"name": "dated", "columns": ["x_modified", "datestamp"]},
    )
    driver = CoreDriverView(model)
    assert driver.get_id_column("works") == "earlier_id"
    assert driver.get_column_base("links") == "later"
    assert driver.get_datestamp_column("works") == "x_modified"
    assert driver.get_datestamp_column("dated") == "datestamp"
    assert driver.get_datestamp_column("view") is None
    assert driver.get_blank_row("view") == {} and driver.get_id_column("view") == "id"
    assert driver.get_tables(False) == ["dated", "links", "works"]
    assert driver.identify_table_from_column("earlier_id") == "works"
    assert driver.identify_table_from_column("absent", error=False) is None
    client.query.return_value = {
        "capabilities": {"link_table": "links", "priority_column": "declared"}
    }
    assert driver.get_link_column("works", "tags", "priority") == "declared"
    assert driver.get_link_column("works", "tags", "first_type") == "first_type"
    assert driver.get_link_column("works", "tags", "type") == "first_type"
    with pytest.raises(ValueError, match="Cannot identify"):
        driver.get_link_column("works", "tags", "work_id")
    client.query.return_value = {"capabilities": {"link_table": ""}}
    assert driver.get_link_table_name("works", "tags") is None


def test_database_info_cache_and_eager_iteration_use_their_separate_clients() -> None:
    """
    Keep metadata caching distinct from fresh type queries and allow an injected model to use another client.

    Example:
        >>> test_database_info_cache_and_eager_iteration_use_their_separate_clients()


    :return: None after info-cache aliasing, type precedence, and eager default-mode assertions on both views.
    """
    info_client = Mock(spec=CoreClientAPI)
    model, row_client = _cached_model()
    nested: list[str] = []
    info_client.query.side_effect = [
        {"metadata": {"nested": nested}},
        {"database_type": "PostgreSQL", "type": "ignored"},
        {"database_type": "", "type": "APSW"},
        {},
    ]
    view = CoreDatabaseView(info_client, model=model)
    first, second = view.metadata, view.metadata
    assert first is not second and first["nested"] is second["nested"] is nested
    assert info_client.query.call_count == 1
    assert [view.type for _ in range(3)] == ["PostgreSQL", "APSW", "SQLite"]
    row_client.query.return_value = {"records": [{"row_id": 7}], "complete": False}
    iterator = view.get_all_rows("works")
    row_client.query.assert_called_once()
    assert iter(iterator) is iterator and next(iterator).row_id == 7
    rows = view.driver_wrapper.get_all_rows("works")
    assert isinstance(rows, list) and rows[0].row_id == 7
    assert info_client.query.call_count == 4 and row_client.query.call_count == 2


def test_legacy_source_truthiness_and_positional_link_readback_are_not_strengthened() -> (
    None
):
    """
    Preserve falsey primary-row fallback and zip truncation during legacy link lookup.

    Example:
        >>> test_legacy_source_truthiness_and_positional_link_readback_are_not_strengthened()


    :return: None after source precedence and first/all positional link-match assertions.
    """
    empty = CoreRow("works", 7, {})
    fallback = CoreRow("works", 8, {"id": 8})
    with pytest.raises(ValueError, match="source row"):
        CoreDatabaseView._source_row(primary_row=empty)
    assert (
        CoreDatabaseView._source_row(primary_row=empty, target_row=fallback) is fallback
    )
    assert CoreDatabaseView._source_row(target_row=empty) is empty
    model = Mock(spec=CoreSurfaceModel)
    view = CoreDatabaseView(Mock(spec=CoreClientAPI), model=model)
    target = CoreRow("tags", 9, {})
    link = CoreRow("links", 12, {})
    model.related.return_value = ((target, target), (link,))
    assert view.get_interlink_row(primary_row=empty, secondary_row=target) is link
    assert view.get_interlink_row(
        primary_row=empty, secondary_row=target, onelink=False
    ) == [link]
    model.related.assert_called_with(empty, "tags", include_link_rows=True)


@pytest.mark.parametrize("priority", [None, "", "not_set", "highest", "4"])
def test_legacy_writes_keep_priority_sentinels_and_unverified_success(
    priority: Any,
) -> None:
    """
    Omit exact legacy priority sentinels, read back without a type filter, and ignore write receipts.

    Example:
        >>> test_legacy_writes_keep_priority_sentinels_and_unverified_success("highest")


    :param priority: Legacy omission sentinel or numeric string supplied to relation creation.
    :return: None after relation payload/readback and normal-return deletion/unlink checks.
    """
    model = Mock(spec=CoreSurfaceModel)
    view = CoreDatabaseView(Mock(spec=CoreClientAPI), model=model)
    primary, secondary = CoreRow("works", 7, {}), CoreRow("tags", 9, {})
    model.related.return_value = ((), ())
    assert (
        view.interlink_rows(
            primary_row=primary,
            secondary_row=secondary,
            priority=priority,
            type="subject",
            note="extra",
        )
        is None
    )
    model.link.assert_called_once_with(
        primary,
        secondary,
        priority=4 if priority == "4" else None,
        link_type="subject",
        values={"note": "extra"},
    )
    model.related.assert_called_once_with(primary, "tags", include_link_rows=True)
    model.unlink.return_value = {"removed": False}
    model.delete_row.return_value = {"deleted": False}
    assert view.unlink_interlink(primary, secondary) is True
    assert view.delete(primary) is True
    with pytest.raises(ValueError, match="without an identifier"):
        view.delete(CoreRow("works", None, {}))
    failure = OSError("readback failed")
    model.related.side_effect = failure
    with pytest.raises(OSError) as raised:
        view.interlink_rows(primary_row=primary, secondary_row=secondary)
    assert raised.value is failure and model.link.call_count == 2


def test_administrative_payloads_and_post_dispatch_receipt_validation() -> None:
    """
    Check actual mutation routing and confirm malformed receipts fail only after the command is sent.

    Example:
        >>> test_administrative_payloads_and_post_dispatch_receipt_validation()


    :return: None after all row/relation write payloads, missing-ID preflight, and receipt-error assertions.
    """
    model, client = _cached_model()
    client.command.return_value = {"ok": True}
    assert model.create_row("works", {"title": "雪"}) == {"ok": True}
    model.update_row("works", "7", {"title": "changed"})
    model.delete_row("works", "7")
    primary, secondary = CoreRow("works", 7, {}), CoreRow("tags", 9, {})
    model.link(primary, secondary, priority="2", link_type="", values={"note": "extra"})
    model.unlink(primary, secondary, link_type="")
    assert client.command.call_args_list == [
        call("admin.row.create", {"table": "works", "values": {"title": "雪"}}),
        call(
            "admin.row.update",
            {"table": "works", "row_id": 7, "updates": {"title": "changed"}},
        ),
        call("admin.row.delete", {"table": "works", "row_id": 7}),
        call(
            "admin.relation.link",
            {
                "table": "works",
                "row_id": 7,
                "related_table": "tags",
                "related_row_id": 9,
                "extra": {"note": "extra"},
                "priority": 2,
                "type": "",
            },
        ),
        call(
            "admin.relation.unlink",
            {
                "table": "works",
                "row_id": 7,
                "related_table": "tags",
                "related_row_id": 9,
                "type": "",
            },
        ),
    ]
    for method in (model.link, model.unlink):
        with pytest.raises(ValueError, match="identifiers"):
            method(CoreRow("works", None, {}), secondary)
    assert client.command.call_count == 5
    client.command.return_value = None
    with pytest.raises(TypeError, match="admin.row.create result"):
        model.create_row("works", {})
    assert client.command.call_count == 6 and model._schema_map() == {}


def test_acquisition_content_uses_native_bytes_or_nonstrict_base64() -> None:
    """
    Retain byte identity and permissive tagged decoding while exposing unsupported content and padding failures.

    Example:
        >>> test_acquisition_content_uses_native_bytes_or_nonstrict_base64()


    :return: None after native, tagged, missing-data, wrong-content, and invalid-padding acquisition checks.
    """
    model, client = _cached_model()
    payload = b"snow"
    client.query.return_value = {"resource": {"id": 7}, "content": payload}
    resource, content = model.acquisition_read("file", "7")
    assert resource == {"id": 7} and content is payload
    client.query.assert_called_once_with("acquisition.read", {"kind": "file", "id": 7})
    client.query.return_value = {"content": {"$type": "bytes", "base64": "c25v!!dw=="}}
    assert model.acquisition_read("file", 7) == ({}, b"snow")
    client.query.return_value = {"content": {"$type": "bytes"}}
    assert model.acquisition_read("file", 7) == ({}, b"")
    for invalid in (None, bytearray(payload), memoryview(payload), {"$type": "other"}):
        client.query.return_value = {"content": invalid}
        with pytest.raises(TypeError, match="not bytes"):
            model.acquisition_read("file", 7)
    client.query.return_value = {"content": {"$type": "bytes", "base64": "a"}}
    with pytest.raises(binascii.Error):
        model.acquisition_read("file", 7)


def test_connection_argument_registration_leaves_required_selection_to_opening(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Keep optional parser selection, independent float timeouts, and profile mutation before session argument extraction.

    Example:
        >>> with pytest.MonkeyPatch.context() as patch:
        ...     test_connection_argument_registration_leaves_required_selection_to_opening(patch)


    :param monkeypatch: Fixture replacing profile application and session opening without touching environment profiles.
    :return: None after parser defaults/exclusivity and resolved namespace forwarding assertions.
    """
    from LiuXin_alpha.surfaces import system_profile

    parser = argparse.ArgumentParser()
    assert (
        adapter.add_core_client_arguments(parser, database_help="Catalogue path")
        is parser
    )
    args = parser.parse_args(["--core-timeout", "-2"])
    assert (
        args.database is None
        and args.core_endpoint is None
        and args.core_timeout == -2.0
    )
    with pytest.raises(SystemExit):
        parser.parse_args(["--database", "one", "--profile", "two"])

    def apply_profile(namespace: argparse.Namespace) -> None:
        """
        Emulate profile resolution by mutating connection fields before the caller extracts them.

        Example:
            >>> apply_profile(args)  # doctest: +SKIP


        :param namespace: Parsed arguments receiving a remote endpoint and resolved-manifest marker.
        :return: None after recording resolution and selecting the test endpoint.
        """
        namespace.core_endpoint = "http://profile.invalid:8080"
        namespace.resolved_system_manifest = "test-profile.json"

    profile = Mock(side_effect=apply_profile)
    opened = Mock(spec=SurfaceCoreSession)
    opener = Mock(return_value=opened)
    monkeypatch.setattr(system_profile, "apply_system_profile", profile)
    monkeypatch.setattr(SurfaceCoreSession, "open", opener)
    assert (
        adapter.open_surface_core_from_args(args, create=True, cache_type="test-cache")
        is opened
    )
    profile.assert_called_once_with(args)
    assert args.resolved_system_manifest == "test-profile.json"
    assert opener.call_args.kwargs["endpoint"] == "http://profile.invalid:8080"
    assert opener.call_args.kwargs["timeout_seconds"] == -2.0
    assert opener.call_args.kwargs["create"] is True
    assert opener.call_args.kwargs["cache_type"] == "test-cache"
