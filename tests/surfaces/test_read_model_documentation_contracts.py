"""
Exercise the read-model's documented identity, ordering, payload, and delegation edge cases.

Recording collaborators isolate policy from real storage and transport. Tests
preserve current behavior, including differing pagination paths, shallow sharing,
duplicate formats, repeated metadata reads, and noncanonical ordering metadata.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces.core import CoreRow, CoreRowPage, CoreSurfaceModel
from LiuXin_alpha.surfaces.images import ImageBackend
from LiuXin_alpha.surfaces.read_model import ReadModelBackend


def _subject() -> ReadModelBackend:
    """
    Construct a read-model with recording collaborators and deterministic display/schema defaults.

    Example:
        >>> _subject().read_source.table_names()
        ('works', 'agents', 'tags', 'labels')


    :return: Fresh backend whose model, image adapter, Core client, and selected host hooks can be overridden per test.
    """
    identifiers = {
        "works": "work_id",
        "agents": "agent_id",
        "human_agents": "human_agent_id",
        "org_agents": "org_agent_id",
        "tags": "tag_id",
        "labels": "label_id",
        "series": "series_id",
    }
    model = Mock(spec=CoreSurfaceModel)
    model.table_names.return_value = ("works", "agents", "tags", "labels")
    model.table_exists.return_value = True
    model.id_column.side_effect = identifiers.__getitem__
    model.columns.return_value = ("work_id", "work_title")
    model.related_tables.return_value = ()
    model.record_count.return_value = 0
    model.rows.return_value = ()
    model.row.return_value = None
    model.related.return_value = ((), ())
    model.query_rows.return_value = CoreRowPage((), 0, 0, None, True, "test")
    host = SimpleNamespace(
        core=Mock(spec=CoreClientAPI),
        _id_column=model.id_column,
        _preferred_summary_fields=Mock(return_value=("work_title",)),
        _row_primary_text=lambda _table, row: str(
            row.get("title") or row.get("name") or row.get("work_title") or ""
        ),
        _row_label=lambda table, _row: table,
        _row_href=lambda table, _row: "/" + table,
        _row_dict=lambda _table, row: dict(row),
        _related_rows_by_table=Mock(return_value={}),
        _download_name_for_file_row=lambda row: row["name"],
        _file_capabilities=Mock(
            return_value={"downloadable": False, "preview_kind": ""}
        ),
        _stringify_detail_value=str,
    )
    return ReadModelBackend(host, images=Mock(spec=ImageBackend), model=model)


def test_construction_borrows_collaborators_and_refresh_keeps_cached_schema() -> None:
    """
    Keep construction query-free and preserve cached schema after a false refresh receipt.

    Example:
        >>> test_construction_borrows_collaborators_and_refresh_keeps_cached_schema()


    :return: None after collaborator identity, refresh command, receipt precedence, and no-schema-reload checks.
    """
    client = Mock(spec=CoreClientAPI)
    client.query.return_value = {
        "tables": [{"name": "works", "id_column": "work_id", "columns": ["work_id"]}]
    }
    host = SimpleNamespace(core=client)
    backend = ReadModelBackend(host)
    assert backend.images.host is host and backend.read_source is backend.model
    client.query.assert_not_called()
    assert backend.model.table_exists("works")
    client.command.return_value = {"refreshed": False, "reloaded": True}
    assert backend.refresh_read_source() is False
    client.command.assert_called_once_with("read-source.refresh")
    assert backend.model.table_exists("works") and client.query.call_count == 1
    other = ReadModelBackend(host, images=backend.images, model=backend.model)
    assert other.images is backend.images and other.model is backend.model


def test_row_conversion_prefers_attributes_and_stops_at_first_mapping_identity() -> (
    None
):
    """
    Preserve direct CoreRow identity and first-match lookup without trying later hints after a missing record.

    Example:
        >>> test_row_conversion_prefers_attributes_and_stops_at_first_mapping_identity()


    :return: None after unchanged CoreRow, attributed lookup, mapping precedence, and query-count assertions.
    """
    backend = _subject()
    unsaved = CoreRow("works", None, {"work_title": "new"})
    assert backend._as_core_row(unsaved) is unsaved
    backend.model.row.assert_not_called()
    assert backend._as_core_row(SimpleNamespace(table="tags", row_id=" 7 ")) is None
    backend.model.row.assert_called_once_with("tags", 7)
    backend.model.row.reset_mock()
    assert backend._as_core_row({"work_id": 1, "tag_id": 2}) is None
    backend.model.row.assert_called_once_with("works", 1)


def test_work_page_clamps_only_optimized_arguments_and_requeries_on_fallback() -> None:
    """
    Distinguish clamped Core pagination from raw fallback slicing and its additional work-order query.

    Example:
        >>> test_work_page_clamps_only_optimized_arguments_and_requeries_on_fallback()


    :return: None after optimized arguments, negative fallback slice, and exact query-count checks.
    """
    backend = _subject()
    backend.work_page(sorted_by="recent", limit=-1, offset=-2)
    assert backend.model.query_rows.call_args.kwargs["limit"] == 0
    assert backend.model.query_rows.call_args.kwargs["offset"] == 0
    backend.model.query_rows.reset_mock()
    rows = tuple({"work_id": n, "title": str(n)} for n in range(1, 6))
    backend.model.query_rows.return_value = CoreRowPage((), 0, 0, None, False, "test")
    backend.model.rows.return_value = rows
    page, total = backend.work_page(sorted_by="recent", limit=1, offset=-3)
    assert [row["work_id"] for row in page] == [3] and total == 5
    assert backend.model.query_rows.call_count == 2
    assert backend.model.query_rows.call_args_list[0].kwargs["offset"] == 0
    assert "offset" not in backend.model.query_rows.call_args_list[1].kwargs


def test_credit_projection_orders_roles_then_priorities_without_mutating_receipts() -> (
    None
):
    """
    Retain raw link values and shared nested fields while sorting mixed numeric/fallback priorities within roles.

    Example:
        >>> test_credit_projection_orders_roles_then_priorities_without_mutating_receipts()


    :return: None after credit order, copied top-level values, raw priorities, and absent-table handling are checked.
    """
    backend = _subject()
    nested: list[str] = []
    agents = [
        {"agent_id": 1, "name": "B", "_catalog_link": {"type": "B", "priority": 99}},
        {
            "agent_id": 2,
            "name": "A slow",
            "_catalog_link": {"type": "A", "priority": 1},
        },
        {
            "agent_id": 3,
            "name": "A fast",
            "nested": nested,
            "_catalog_link": {"type": "A", "priority": "9"},
        },
        {
            "agent_id": 4,
            "name": "A fallback",
            "_catalog_link": {"type": "A", "priority": "bad"},
        },
        "ignored nonmapping",
    ]
    backend.host.core.query.return_value = {"agents": agents}
    work = CoreRow("works", 7, {"work_id": 7})
    entries = backend.work_credit_entries(work)
    assert [entry["row"].row_id for entry in entries] == [3, 2, 4, 1]
    assert entries[0]["priority"] == "9" and entries[2]["priority"] == "bad"
    assert entries[0]["row"]["nested"] is nested
    assert "_catalog_link" in agents[2] and "_catalog_link" not in entries[0]["row"]
    backend.model.table_exists.return_value = False
    assert backend.work_credit_entries(work) == []


def test_grouping_and_tag_selection_keep_fallback_identity_and_global_precedence() -> (
    None
):
    """
    Return host grouping unchanged and choose populated tags globally even when only labels are linked to this work.

    Example:
        >>> test_grouping_and_tag_selection_keep_fallback_identity_and_global_precedence()


    :return: None after fallback identity, tag/label precedence, and unconditional author-table fallback checks.
    """
    backend = _subject()
    related = {"labels": [{"label_id": 1}], "empty": []}
    backend.host._related_rows_by_table.return_value = related
    assert backend.related_rows_by_table(object()) is related
    assert backend.tag_category_table() == "labels"
    table, rows = backend.work_tag_rows(related)
    assert (
        table == "labels"
        and rows == related["labels"]
        and rows is not related["labels"]
    )
    backend.model.record_count.return_value = 1
    assert backend.work_tag_rows(related) == ("tags", [])
    backend.model.table_exists.return_value = False
    assert backend.author_tables() == ["agents"]


def test_category_paging_keeps_rating_placeholder_sharing_and_normalized_direction() -> (
    None
):
    """
    Sort rating requests by label, preserve nested row identity, and canonicalize unknown direction to ascending.

    Example:
        >>> test_category_paging_keeps_rating_placeholder_sharing_and_normalized_direction()


    :return: None after rating/name order, normalized metadata, and shallow-copy assertions.
    """
    backend = _subject()
    original = [
        {"label": "B", "count": 99, "row": {}},
        {"label": "A", "count": 0, "row": {}},
    ]
    backend.category_rows = Mock(return_value=original)
    result = backend.category_items_payload(
        " TAGS ", num=1, offset=0, sort=" RATING ", sort_order="unknown"
    )
    assert result["category"] == "tags" and result["sort"] == "rating"
    assert result["sort_order"] == "asc" and result["total_num"] == 2
    assert result["items"][0]["label"] == "A"
    assert result["items"][0] is not original[1]
    assert result["items"][0]["row"] is original[1]["row"]


def test_file_and_metadata_payloads_preserve_zero_fallback_and_duplicate_formats() -> (
    None
):
    """
    Keep truthy size fallback, duplicate format tokens, last-format metadata, and capability-independent format download routes.

    Example:
        >>> test_file_and_metadata_payloads_preserve_zero_fallback_and_duplicate_formats()


    :return: None after file discovery, summary/format route differences, supplied-group reuse, and repeated subtitle checks.
    """
    backend = _subject()
    first = {"file_id": 1, "name": "a.epub", "file_size_bytes": 0, "file_size": 12}
    last = {"file_id": "2", "name": "z.EPUB", "file_size_bytes": "bad"}
    replacement = {**first, "name": "replacement.epub"}
    assert backend.work_file_rows(
        {"files": [first, last, replacement, {"file_id": "bad"}]}
    ) == [replacement, last]
    summary = backend.file_summary_payload(first)
    assert summary["size"] == 12 and summary["download_url"] == ""
    related = {"files": [last, first]}
    backend._work_credit_entries = Mock(return_value=[])
    backend.work_subtitle = Mock(side_effect=["first read", "second read"])
    work = {"work_id": "7", "title": "Title"}
    result = backend.work_metadata_payload(work, related_rows_by_table=related)
    assert result["formats"] == ["EPUB", "EPUB"] and result["main_format"] == "EPUB"
    assert result["format_metadata"]["EPUB"] == {
        "path": "/files/2/download",
        "name": "z.EPUB",
        "size": None,
        "preview": "",
    }
    assert result["formats_detail"][0]["download_url"] == "/files/1/download"
    assert result["comments"] == "first read" and result["summary"] == "second read"
    assert result["id"] == 7 and result["rating"] is None
    backend.host._related_rows_by_table.assert_not_called()
    assert related["files"] == [last, first]


def test_work_list_counts_ids_separately_and_metadata_mapping_keeps_last_duplicate() -> (
    None
):
    """
    Keep visible missing-ID rows, echo unknown ordering tokens, and replace duplicate projected metadata keys in place.

    Example:
        >>> test_work_list_counts_ids_separately_and_metadata_mapping_keeps_last_duplicate()


    :return: None after row/ID count differences, numeric fallback sorting, and last-value mapping semantics are verified.
    """
    backend = _subject()
    rows = [{"work_id": 2}, {}, {"work_id": 1}]
    result = backend.work_list_payload(
        rows, num=3, offset=0, sort="unknown", sort_order="SIDEWAYS"
    )
    assert result["book_ids"] == [1, 2] and result["num"] == 2
    assert len(result["rows"]) == 3 and result["rows"][0] is rows[1]
    assert result["sort_order"] == "sideways" and result["sort"] == "unknown"
    assert rows[0]["work_id"] == 2
    first, last = {"id": None, "title": "first"}, {"id": None, "title": "last"}
    backend.work_metadata_payload = Mock(side_effect=[first, last])
    payload = backend.books_metadata_payload(rows[:2])
    assert list(payload) == ["None"] and payload["None"] is last


def test_search_unknown_table_falls_back_and_groups_count_all_ranked_entries() -> None:
    """
    Fall back from an unknown table to public search scope and count groups before slicing while echoing original metadata.

    Example:
        >>> test_search_unknown_table_falls_back_and_groups_count_all_ranked_entries()


    :return: None after normalized query arguments, full group counts, selected IDs, and original request echo checks.
    """
    backend = _subject()
    rows = ({"work_id": 1, "title": "A"}, {"work_id": 2, "title": "B"})
    backend.model.table_exists.side_effect = lambda table: table == "works"
    backend.model.query_rows.return_value = CoreRowPage(rows, 2, 0, None, True, "test")
    backend.host._public_search_tables = Mock(return_value=("works",))
    backend.host._search_candidate_columns = Mock(return_value=("work_title",))
    backend.host._global_search_entry = lambda table, row, needle: {
        "table": table,
        "row": row,
        "sort_key": row["title"],
        "snippet": needle,
    }
    result = backend.search_results_payload(
        query_text="  snow  ", table_filter="unknown", limit=1, offset=1
    )
    assert result["query"] == "  snow  " and result["table_filter"] == "unknown"
    assert result["group_counts"] == {"works": 2} and result["total"] == 2
    assert result["results"][0]["id"] == 2 and result["results"][0]["snippet"] == "snow"
    backend.model.query_rows.assert_called_once_with(
        "works", text="snow", text_fields=("work_title",)
    )
    backend.model.query_rows.reset_mock()
    assert backend.search_entries(" ") == []
    backend.model.query_rows.assert_not_called()


def test_image_wrappers_forward_identity_but_initials_use_the_static_backend() -> None:
    """
    Retain image delegate arguments/results without copying while static initials bypass an injected collaborator.

    Example:
        >>> test_image_wrappers_forward_identity_but_initials_use_the_static_backend()


    :return: None after all image wrapper identities, keyword dimensions, and static initial selection are checked.
    """
    backend = _subject()
    row, related, sentinel = {}, {"images": []}, object()
    for name, argument in (
        ("work_image_rows", related),
        ("image_download_name", row),
        ("image_content_type", row),
        ("image_storage_lookup_metadata", row),
        ("resolve_storage_image", row),
        ("resolve_image_target", row),
        ("work_image_row", row),
    ):
        delegate = getattr(backend.images, name)
        delegate.return_value = sentinel
        assert getattr(backend, name)(argument) is sentinel
        assert delegate.call_args.args[0] is argument
    backend.images.placeholder_cover_svg.return_value = sentinel
    assert backend.placeholder_cover_svg(row, width=-1, height=0) is sentinel
    backend.images.placeholder_cover_svg.assert_called_once_with(
        row, width=-1, height=0
    )
    assert backend.thumbnail_text("ß") == "SS"
    backend.images.thumbnail_text.assert_not_called()
