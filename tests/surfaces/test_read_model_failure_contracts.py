"""Read failures must not become empty, missing, or silently retried results."""

import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, create_autospec

import pytest

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.core.proxies.remote import RemoteProxyError
from LiuXin_alpha.surfaces.core import CoreRow, CoreRowPage, CoreSurfaceModel
from LiuXin_alpha.surfaces.presentation import row_value
from LiuXin_alpha.surfaces.read_model import ReadModelBackend

WORK = CoreRow("works", 7, {"work_id": 7, "work_title": "雪"}, ("tags",))
FAILURES = (
    RuntimeError,
    OSError,
    KeyError,
    ValueError,
    TypeError,
    AttributeError,
    CoreHandlerError,
    RemoteProxyError,
)


def _page(records=(), *, complete=True) -> CoreRowPage:
    return CoreRowPage(tuple(records), len(records), 0, None, complete, "test")


@pytest.fixture
def backend() -> ReadModelBackend:
    model = create_autospec(CoreSurfaceModel, instance=True, spec_set=True)
    model.table_exists.return_value = True
    model.table_names.return_value = ("works", "tags", "labels")
    model.columns.return_value = ("work_id", "work_title")
    model.id_column.return_value = "work_id"
    model.rows.return_value = ()
    model.row.return_value = None
    model.search.return_value = ()
    model.record_count.return_value = 0
    model.related.return_value = ((), ())
    model.related_tables.return_value = ("works",)
    model.query_rows.return_value = _page()
    host = SimpleNamespace(
        core=create_autospec(CoreClientAPI, instance=True, spec_set=True),
        _table_exists=Mock(side_effect=AssertionError("must not retry through host")),
        _id_column=lambda _table: "work_id",
        _preferred_summary_fields=lambda _table: ("work_title",),
        _row_primary_text=lambda _table, row: row["work_title"],
        _ordered_related_tables=lambda _row: ("works",),
        _public_search_tables=lambda: ("works",),
        _search_candidate_columns=lambda _table: ("work_title",),
        _global_search_entry=lambda table, row, needle: (
            {"table": table, "row": row, "sort_key": row["work_title"]}
            if needle in row["work_title"]
            else None
        ),
    )
    return ReadModelBackend(host, model=model)


@pytest.mark.parametrize("failure", FAILURES)
@pytest.mark.parametrize(
    ("method", "args", "kwargs", "failing_method"),
    [
        ("rows_for_table", ("works",), {}, "rows"),
        ("row_by_id", ("works", 7), {}, "row"),
        ("table_record_count", ("works",), {}, "record_count"),
        ("search_rows", ("works", "work_title", "雪"), {}, "search"),
        ("interlinked_rows", (WORK, "tags"), {}, "related"),
        ("work_rows", (), {"sorted_by": "title"}, "query_rows"),
        (
            "work_page",
            (),
            {"sorted_by": "recent", "limit": 10, "offset": 0},
            "query_rows",
        ),
        ("search_entries", ("雪",), {}, "query_rows"),
        ("works_for_linked_entity", ("tags", "7"), {}, "row"),
        ("author_tables", (), {}, "table_exists"),
        ("tag_category_table", (), {}, "record_count"),
        ("work_rows", (), {"sorted_by": "title"}, "columns"),
    ],
)
def test_query_failures_propagate_unchanged(
    backend, failure, method, args, kwargs, failing_method
) -> None:
    error = failure("read failed")
    getattr(backend.model, failing_method).side_effect = error
    with pytest.raises(failure) as raised:
        getattr(backend, method)(*args, **kwargs)
    assert raised.value is error
    backend.host._table_exists.assert_not_called()
    if failing_method == "query_rows":
        backend.model.rows.assert_not_called()
        assert backend.model.query_rows.call_count == 1


@pytest.mark.parametrize("method", ("rows_for_table", "search_rows"))
def test_iteration_failure_does_not_publish_a_partial_result(backend, method) -> None:
    error = OSError("iteration failed after one row")

    def records():
        yield WORK
        raise error

    if method == "rows_for_table":
        backend.model.rows.return_value = records()
        args = ("works",)
    else:
        backend.model.search.return_value = records()
        args = ("works", "work_title", "雪")
    with pytest.raises(OSError) as raised:
        getattr(backend, method)(*args)
    assert raised.value is error


def test_known_empty_results_and_missing_records_remain_normal(backend) -> None:
    assert backend.rows_for_table("works") == []
    assert backend.row_by_id("works", 7) is None
    assert backend.table_record_count("works") == 0
    assert backend.search_rows("works", "work_title", "雪") == []
    assert backend.interlinked_rows(WORK, "tags") == []
    assert backend.work_rows(sorted_by="title") == []
    assert backend.work_page(sorted_by="title", limit=10, offset=0) == ([], 0)
    assert backend.search_entries("雪") == []
    assert backend.works_for_linked_entity("tags", "7") == []


def test_absent_optional_tables_are_checked_without_querying_them(backend) -> None:
    backend.model.table_exists.return_value = False
    assert backend.rows_for_table("missing") == []
    assert backend.row_by_id("missing", 7) is None
    assert backend.table_record_count("missing") == 0
    assert backend.search_rows("missing", "name", "雪") == []
    assert backend.interlinked_rows(WORK, "missing") == []
    assert backend.work_rows(sorted_by="title") == []
    assert backend.work_page(sorted_by="title", limit=10, offset=0) == ([], 0)
    for method in ("rows", "row", "record_count", "search", "related", "query_rows"):
        getattr(backend.model, method).assert_not_called()


def test_invalid_linked_entity_id_is_not_a_lookup_failure(backend) -> None:
    assert backend.works_for_linked_entity("tags", "not-an-id") == []
    backend.model.row.assert_not_called()


def test_unsaved_row_has_no_relationships_without_consulting_the_source(
    backend,
) -> None:
    row = CoreRow("works", None, {"work_title": "unsaved"})
    assert backend.interlinked_rows(row, "tags") == []
    backend.model.table_exists.assert_not_called()
    backend.model.related.assert_not_called()


def test_incomplete_optimized_queries_keep_the_explicit_fallback(backend) -> None:
    earlier = CoreRow("works", 2, {"work_id": 2, "work_title": "A 雪"})
    backend.model.query_rows.return_value = _page((), complete=False)
    backend.model.rows.return_value = (WORK, earlier)
    assert backend.work_rows(sorted_by="title") == [earlier, WORK]
    assert backend.work_page(sorted_by="recent", limit=1, offset=1) == ([earlier], 2)
    assert [entry["row"] for entry in backend.search_entries("雪")] == [earlier, WORK]


def test_no_sortable_column_uses_the_normal_materialized_path(backend) -> None:
    backend.model.columns.return_value = ("work_id",)
    backend.model.rows.return_value = (WORK,)
    assert backend.work_page(sorted_by="title", limit=1, offset=0) == ([WORK], 1)
    backend.model.query_rows.assert_not_called()


def test_failure_during_explicit_fallback_still_propagates(backend) -> None:
    error = OSError("fallback failed")
    backend.model.query_rows.return_value = _page((), complete=False)
    backend.model.rows.side_effect = error
    with pytest.raises(OSError) as raised:
        backend.work_page(sorted_by="title", limit=10, offset=0)
    assert raised.value is error


@pytest.mark.parametrize("failure", (RuntimeError, OSError, TypeError, AttributeError))
def test_row_accessor_only_handles_missing_columns(failure) -> None:
    error = failure("row access failed")

    class BrokenRow:
        def __getitem__(self, key):
            raise error

    with pytest.raises(failure) as raised:
        row_value(BrokenRow(), "title")
    assert raised.value is error
    assert row_value({}, "missing") is None


@pytest.mark.parametrize("method", ("work_file_rows", "file_summary_payload"))
def test_numeric_metadata_fallback_does_not_catch_arbitrary_failures(
    backend, method
) -> None:
    class BrokenNumber:
        def __int__(self):
            raise RuntimeError("numeric provider failed")

    if method == "work_file_rows":
        argument = {"files": [{"file_id": BrokenNumber()}]}
    else:
        backend.host._file_capabilities = lambda _row: {}
        backend.host._download_name_for_file_row = lambda _row: "book.epub"
        argument = {"file_id": 7, "file_size_bytes": BrokenNumber()}
    with pytest.raises(RuntimeError, match="numeric provider failed"):
        getattr(backend, method)(argument)


def test_malformed_core_payload_does_not_look_like_an_empty_catalogue(backend) -> None:
    client = backend.host.core
    client.query.side_effect = lambda name, _payload=None: (
        {"tables": [{"name": "works", "columns": ["work_id", "work_title"]}]}
        if name == "schema.tables"
        else {"records": "not an array"}
    )
    backend.model = CoreSurfaceModel(client)
    with pytest.raises(TypeError, match="must be an array"):
        backend.rows_for_table("works")


@pytest.mark.parametrize("relative", ("read_model/api.py", "images/api.py"))
def test_read_backends_do_not_reintroduce_catch_all_handlers(relative: str) -> None:
    path = Path(__file__).resolve().parents[2] / "src/LiuXin_alpha/surfaces" / relative
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        assert node.type is not None, (relative, node.lineno)
        caught = {name.id for name in ast.walk(node.type) if isinstance(name, ast.Name)}
        assert not caught.intersection({"Exception", "BaseException"}), (
            relative,
            node.lineno,
        )
