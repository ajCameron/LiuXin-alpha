"""Keep category counts, membership and work tags identical across Core transports and surfaces."""

from contextlib import nullcontext
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from LiuXin_alpha.catalog.retrieval.hydration import DatabaseMetadataReadSource
from LiuXin_alpha.core import CoreHttpDaemon, CoreRuntime, RemoteCoreClient
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.surfaces.api_readonly.app import ApiReadOnlyApplication
from LiuXin_alpha.surfaces.cli import catalogue
from LiuXin_alpha.surfaces.core import CoreSurfaceModel
from LiuXin_alpha.surfaces.opds_readonly.app import OpdsReadOnlyApplication
from LiuXin_alpha.surfaces.web_calibre_readonly.app import CalibreReadOnlyWebApplication
from tests.surfaces.test_core_surface_acceptance import _call_wsgi
from tests.surfaces.test_read_model_api import (
    _insert_label_row,
    _insert_tag_row,
    _insert_work_row,
)


@pytest.mark.parametrize(
    "available,tag_count,label_count,selected",
    [
        ({"tags"}, 2, 0, "tags"),
        ({"labels"}, 0, 3, "labels"),
        ({"tags", "labels"}, 2, 3, "tags"),
        ({"tags", "labels"}, 0, 3, "labels"),
        ({"tags"}, 0, 0, "tags"),
        ({"tags", "labels"}, 0, 0, "labels"),
        (set(), 2, 3, None),
    ],
    ids=[
        "tags-only",
        "labels-only",
        "both-populated",
        "empty-tags",
        "empty-tags-only",
        "both-empty",
        "neither-advertised",
    ],
)
def test_category_sources_agree_across_surfaces_and_transports(
    driver_spec,
    tmp_path,
    monkeypatch,
    available,
    tag_count,
    label_count,
    selected,
):
    """Exercise real links and colliding tag/label IDs, including a label-only Work.

    Optional tables are masked at the read-source boundary, without modifying a
    real schema. Both native backends and loopback HTTP use the same seeded rows.
    """
    with Database(
        metadata={"database_path": str(tmp_path / "categories.sqlite")},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        first = _insert_work_row(db, title="Tag Work")
        second = _insert_work_row(db, title="Label Work")
        tags = [_insert_tag_row(db, text=f"Tag {i}") for i in range(tag_count)]
        labels = [_insert_label_row(db, text=f"Label {i}") for i in range(label_count)]
        for table, ids, work in (("tags", tags, first), ("labels", labels, second)):
            for row_id in ids:
                db.interlink_rows(
                    primary_row=db.get_row_from_id("works", work),
                    secondary_row=db.get_row_from_id(table, row_id),
                )
        source = DatabaseMetadataReadSource(db)
        tables = set(db.get_tables()) - {"tags", "labels"} | available
        monkeypatch.setattr(
            source, "get_tables", lambda force_refresh=False: sorted(tables)
        )
        runtime = CoreRuntime(library=SimpleNamespace(database=db), read_source=source)
        daemon = CoreHttpDaemon(runtime, endpoint_namespace="category-ownership")
        try:
            daemon.start()
            expected_count = (
                tag_count if selected == "tags" else label_count if selected else 0
            )
            expected_names = [
                f"{'Tag' if selected == 'tags' else 'Label'} {i}"
                for i in range(expected_count)
            ]
            for client in (runtime, RemoteCoreClient(endpoint=daemon.base_url)):
                summary = client.query("browse.categories", {"category": "tags"})[
                    "categories"
                ][0]
                assert summary["count"] == expected_count
                assert summary["tables"] == ([selected] if selected else [])
                entries = client.query("browse.category.items", {"category": "tags"})
                assert entries["total_count"] == expected_count
                assert [
                    entry["label"] for entry in entries["records"]
                ] == expected_names
                assert all(
                    entry["table"] == selected and entry["work_count"] == 1
                    for entry in entries["records"]
                )

                # The CLI publishes the same semantic receipt, including selection.
                emitted = []
                monkeypatch.setattr(
                    catalogue, "open_cli_core", lambda *a, **kw: nullcontext(client)
                )
                monkeypatch.setattr(
                    catalogue, "emit_json", lambda result, args: emitted.append(result)
                )
                assert catalogue.cmd_browse_categories(SimpleNamespace()) == 0
                assert (
                    next(
                        row
                        for row in emitted[0]["categories"]
                        if row["category"] == "tags"
                    )
                    == summary
                )

                api = ApiReadOnlyApplication(client)
                status, _, body = _call_wsgi(api, "/api/categories")
                assert status == "200 OK"
                assert (
                    next(
                        row
                        for row in json.loads(body)["items"]
                        if row["category"] == "tags"
                    )["count"]
                    == expected_count
                )
                status, _, body = _call_wsgi(api, "/api/tags")
                assert status == "200 OK"
                assert [
                    row["name"] for row in json.loads(body)["items"]
                ] == expected_names
                web = CalibreReadOnlyWebApplication(client)
                opds = OpdsReadOnlyApplication(client)
                for backend in (api.read_model, web.read_model, opds.read_model):
                    assert backend.tag_category_table() == selected
                    assert backend.browse_count("tags") == expected_count
                    assert [
                        row["label"] for row in backend.category_rows("tags")
                    ] == expected_names
                core_works = client.query("browse.works", {"category": "all"})[
                    "records"
                ]
                for work_id in (first, second):
                    work = api.read_model.row_by_id("works", work_id)
                    names = (
                        expected_names
                        if (selected == "tags" and work_id == first)
                        or (selected == "labels" and work_id == second)
                        else []
                    )
                    metadata_tags = api.read_model.work_metadata_payload(work)["tags"]
                    assert sorted(metadata_tags) == names
                    assert [
                        tag["name"]
                        for tag in next(
                            row for row in core_works if row["work_id"] == work_id
                        )["tags"]
                    ] == metadata_tags
                if selected is None:
                    status, _, body = _call_wsgi(api, f"/api/tags/{tags[0]}")
                    assert status == "404 Not Found"
                    status, _, body = _call_wsgi(api, f"/api/tags/{tags[0]}/works")
                    assert status == "200 OK" and json.loads(body)["items"] == []
                    assert opds.opds_rows_for_category_item("tags", str(tags[0])) == []
                    status, _, body = _call_wsgi(web, f"/tag/{tags[0]}")
                    assert b"Tag Work" not in body
                if expected_count:
                    row_id = entries["records"][0]["id"]
                    members = client.query(
                        "browse.works", {"category": "tags", "category_id": row_id}
                    )
                    assert [row["work_id"] for row in members["records"]] == [
                        first if selected == "tags" else second
                    ]
                    assert [
                        row["work_id"]
                        for row in opds.opds_rows_for_category_item("tags", str(row_id))
                    ] == [first if selected == "tags" else second]
                # An ID present only in the unselected family must never leak in.
                if tags and labels:
                    assert (
                        client.query(
                            "browse.works",
                            {"category": "tags", "category_id": labels[-1]},
                        )["records"]
                        == []
                    )
        finally:
            daemon.stop()
            runtime.shutdown()


def test_category_summary_never_materializes_rows_or_relationships():
    """Summary counts must stay cheap even when both tag-like tables exist."""
    source = SimpleNamespace(
        get_tables=lambda **kw: ["works", "tags", "labels"],
        get_record_count=Mock(
            side_effect={"works": 7, "tags": 2, "labels": 9}.__getitem__
        ),
        get_all_rows=Mock(side_effect=AssertionError("summary materialized rows")),
        get_interlinked_rows=Mock(
            side_effect=AssertionError("summary traversed links")
        ),
    )
    runtime = CoreRuntime(
        library=SimpleNamespace(database=SimpleNamespace()), read_source=source
    )
    try:
        backend = ApiReadOnlyApplication(runtime).read_model
        summary = backend.category_summary_payload()
        assert [row["count"] for row in summary] == [7, 7, 0, 2, 0]
        assert source.get_record_count.call_args_list.count((("works",),)) == 1
    finally:
        runtime.shutdown()


def test_entity_pages_use_one_bounded_semantic_query_and_keep_core_order():
    """Surface page rendering must not fetch or sort the complete entity table."""
    client = Mock()
    client.query.return_value = {
        "records": [
            {
                "table": "tags",
                "id": 3,
                "label": "Z",
                "work_count": 4,
                "record": {"tag_id": 3},
            },
            {
                "table": "tags",
                "id": 2,
                "label": "A",
                "work_count": 2,
                "record": {"tag_id": 2},
            },
        ],
        "total_count": 5000,
        "offset": 10,
        "complete": False,
    }
    model = CoreSurfaceModel(client)
    from LiuXin_alpha.surfaces.read_model import ReadModelBackend

    backend = ReadModelBackend(
        SimpleNamespace(core=client, _row_href=lambda *a: "/tag"), model=model
    )
    page = backend.category_items_payload(
        "tags", num=2, offset=10, sort="popularity", sort_order="desc"
    )
    assert [row["label"] for row in page["items"]] == ["Z", "A"]
    assert page["total_num"] == 5000
    client.query.assert_called_once_with(
        "browse.category.items",
        {
            "category": "tags",
            "limit": 2,
            "offset": 10,
            "sort": "popularity",
            "ascending": False,
        },
    )


@pytest.mark.parametrize(
    "failure",
    [
        "count",
        "entities",
        "relations",
        "incomplete-count",
        "incomplete-entities",
        "unsupported",
    ],
)
def test_category_failures_survive_direct_and_http_transport(failure):
    """Never turn failed/incomplete source reads into empty categories or labels fallback."""
    from LiuXin_alpha.caches.api import (
        CacheQueryResult,
        CacheRecord,
        UnsupportedCacheQueryError,
    )
    from LiuXin_alpha.core.errors import CoreError, CoreHandlerError
    from LiuXin_alpha.core.proxies.remote import RemoteProxyError

    def query_cache(query):
        is_count = query.limit == 0
        if failure == "unsupported":
            raise UnsupportedCacheQueryError("tags")
        if failure == ("count" if is_count else "entities"):
            raise CoreError(
                "category read failed",
                code="category_test_error",
                details={"phase": failure},
            )
        records = (
            () if is_count else (CacheRecord("tags", 1, {"tag_id": 1, "tag": "Tag"}),)
        )
        complete = failure != (
            "incomplete-count" if is_count else "incomplete-entities"
        )
        return CacheQueryResult(records, 1, 0, query.limit, complete, 0)

    def related(**kwargs):
        raise CoreError(
            "link read failed",
            code="category_test_error",
            details={"phase": "relations"},
        )

    source = SimpleNamespace(
        get_tables=lambda **kw: ["works", "tags", "labels"],
        query_cache=query_cache,
        get_row_from_id=lambda *a: {"tag_id": 1, "tag": "Tag"},
        get_interlinked_rows=related,
    )
    runtime = CoreRuntime(
        library=SimpleNamespace(database=SimpleNamespace()), read_source=source
    )
    daemon = CoreHttpDaemon(runtime, endpoint_namespace="category-errors")
    try:
        daemon.start()
        for client, error_type in (
            (runtime, CoreHandlerError),
            (RemoteCoreClient(endpoint=daemon.base_url), RemoteProxyError),
        ):
            backend = ApiReadOnlyApplication(client).read_model
            operation = (
                backend.tag_category_table
                if failure in {"count", "incomplete-count", "unsupported"}
                else lambda: backend.category_rows("tags")
            )
            with pytest.raises(error_type) as caught:
                operation()
            if failure.startswith("incomplete") or failure == "unsupported":
                assert caught.value.code == "read_query_unavailable"
                assert caught.value.details["table"] == "tags"
            else:
                assert caught.value.code == "category_test_error"
                assert caught.value.details == {"phase": failure}
    finally:
        daemon.stop()
        runtime.shutdown()


def test_large_category_windows_are_assembled_from_bounded_pages():
    """A configured page above 1,000 entries must not skip records in navigation."""
    client = Mock()

    def query(name, payload):
        assert name == "browse.category.items"
        assert payload["limit"] <= 1000
        end = min(payload["offset"] + payload["limit"], 2000)
        return {
            "records": [
                {
                    "table": "tags",
                    "id": i,
                    "label": str(i),
                    "work_count": 0,
                    "record": {"tag_id": i},
                }
                for i in range(payload["offset"], end)
            ],
            "total_count": 2000,
            "offset": payload["offset"],
            "complete": end == 2000,
        }

    client.query.side_effect = query
    from LiuXin_alpha.surfaces.read_model import ReadModelBackend

    backend = ReadModelBackend(
        SimpleNamespace(core=client, _row_href=lambda *a: "/tag")
    )
    page = backend.category_items_payload(
        "tags", num=1500, offset=10, sort="name", sort_order="asc"
    )
    assert page["offset"] == 10 and page["num"] == 1500 and page["total_num"] == 2000
    assert [row["id"] for row in page["items"]] == list(range(10, 1510))
    assert client.query.call_count == 2
    client.query.reset_mock()
    assert [row["id"] for row in backend.category_rows("tags")] == list(range(2000))
    assert client.query.call_count == 2


def test_category_source_is_reselected_after_population_changes():
    """A retained surface model must observe the transition from labels to tags."""
    counts = {"tags": 0, "labels": 3}
    source = SimpleNamespace(
        get_tables=lambda **kw: tuple(counts),
        get_record_count=lambda table: counts[table],
    )
    runtime = CoreRuntime(
        library=SimpleNamespace(database=SimpleNamespace()), read_source=source
    )
    try:
        backend = ApiReadOnlyApplication(runtime).read_model
        assert backend.tag_category_table() == "labels"
        counts["tags"] = 2
        assert backend.tag_category_table() == "tags"
        assert backend.browse_count("tags") == 2
        counts["tags"] = 0
        assert backend.tag_category_table() == "labels"
        assert backend.browse_count("tags") == 3
    finally:
        runtime.shutdown()
