"""
Verify documented catalogue mutation, acquisition fallback, and OPDS token/pagination edge cases.

All providers and HTTP responses are in-memory doubles. XML is parsed only for
assertions; no redirect is followed, no publication is downloaded, and no server
or real storage path is opened.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock
from xml.etree import ElementTree

import pytest

from LiuXin_alpha.surfaces.acquisition.api import (
    AcquisitionCompatApi,
    _coerce_payload_bytes,
    _cover_dimensions,
)
from LiuXin_alpha.surfaces.catalog import CalibreCatalogBackend
from LiuXin_alpha.surfaces.core import CoreSurfaceModel
from LiuXin_alpha.surfaces.images import ImageBackend
from LiuXin_alpha.surfaces.opds.api import (
    OpdsApi,
    decode_compat_token,
    decode_opds_category_token,
    decode_opds_item_token,
    encode_compat_token,
    normalized_category_key,
    opds_category_groups,
    opds_category_token,
    opds_feed,
    opds_item_token,
    opds_nav_token,
    opds_pager_hrefs,
    opds_with_offset,
)
from LiuXin_alpha.surfaces.read_model import ReadModelBackend
from tests.surfaces.test_acquisition_api import _DummyHost as AcquisitionHost
from tests.surfaces.test_opds_api import _DummyHost as OpdsHost


def _catalog() -> CalibreCatalogBackend:
    """
    Supply a catalogue adapter with recording read/image collaborators and empty relationship defaults.

    Example:
        >>> _catalog().ajax_setup_payload()["library_id"]
        'main'


    :return: Fresh catalogue backend whose host and model hooks can be overridden without constructing Core.
    """
    model = Mock(spec=ReadModelBackend)
    model.images = Mock(spec=ImageBackend)
    model.work_tag_rows.return_value = (None, [])
    host = SimpleNamespace(
        config=SimpleNamespace(title="Test", default_page_size=10),
        _related_rows_by_table=Mock(return_value={}),
        _work_credit_entries=Mock(return_value=[]),
        _id_column=lambda table: {
            "works": "work_id",
            "agents": "agent_id",
            "series": "series_id",
        }[table],
    )
    return CalibreCatalogBackend(host, read_model=model)


def test_tokens_preserve_falsey_hex_and_delimiter_ambiguities() -> None:
    """
    Retain falsey encoding, raw-hex reinterpretation, repeated decoding, and first-colon item parsing.

    Example:
        >>> test_tokens_preserve_falsey_hex_and_delimiter_ambiguities()


    :return: None after exact Unicode, invalid-UTF-8, zero-ID, and ambiguous token results are checked.
    """
    assert encode_compat_token(0) == encode_compat_token(False) == ""
    assert decode_compat_token(" e9-9b-aa ") == "雪"
    assert decode_compat_token("ff") == "ff" and decode_compat_token("41") == "A"
    assert normalized_category_key("3431") == "41"
    assert decode_opds_category_token("3431") == "a"
    assert decode_opds_item_token(
        opds_item_token(category="tags", item_id=0), "authors"
    ) == ("0", "tags")
    token = opds_item_token(category="tags", item_id="x:y")
    assert decode_opds_item_token(token, "authors") == ("x", "y:tags")


def test_pager_links_clamp_without_replacing_queries_or_clamping_feed_slices() -> None:
    """
    Keep literal URL appending and expose the distinction between clamped navigation links and an overlarge empty page.

    Example:
        >>> test_pager_links_clamp_without_replacing_queries_or_clamping_feed_slices()


    :return: None after duplicate query offsets, fragment placement, nonaligned pager offsets, and empty feed assertions.
    """
    assert opds_with_offset("/opds?offset=3#tail", 2) == "/opds?offset=3#tail&offset=2"
    assert opds_with_offset("/opds?offset=3", 0) == "/opds?offset=3"
    hrefs = opds_pager_hrefs(path="/opds", up_href="/", offset=2, total=10, page_size=3)
    assert (
        hrefs["self_href"] == "/opds?offset=2"
        and hrefs["next_href"] == "/opds?offset=5"
    )
    path = "/opds/navcatalog/" + opds_nav_token("titles")
    response = OpdsApi(OpdsHost()).serve(path, {"offset": ["99"]})
    root = ElementTree.fromstring(b"".join(response.body))
    ns = {"a": "http://www.w3.org/2005/Atom"}
    assert root.findall("a:entry", ns) == []
    assert root.find("a:link[@rel='self']", ns).attrib["href"] == path


def test_grouping_counts_items_and_expanding_initials_do_not_round_trip() -> None:
    """
    Count category rows rather than linked books and expose repeated initial-normalization failure for sharp-s groups.

    Example:
        >>> test_grouping_counts_items_and_expanding_initials_do_not_round_trip()


    :return: None after item-count grouping, threshold selection, and the current expanding-initial group-route 404.
    """
    rows = [{"id": 1, "label": "ßeta", "count": 99}]
    assert opds_category_groups(rows) == [{"label": "SS", "count": 1}]
    host = OpdsHost()
    host.config = SimpleNamespace(
        title="Groups",
        default_page_size=10,
        max_page_size=25,
        opds_max_ungrouped_items=1,
    )
    host.opds_category_rows = Mock(
        return_value=rows + [{"id": 2, "label": "ßother", "count": 99}]
    )
    api = OpdsApi(host)
    feed = b"".join(api.serve("/opds/navcatalog/" + opds_nav_token("authors"), {}).body)
    assert b"2 items" in feed and b"<title>SS</title>" in feed
    path = "/opds/categorygroup/{}/{}".format(
        opds_category_token("authors"), encode_compat_token("SS")
    )
    assert api.serve(path, {}).status == "404 Not Found"


def test_feed_escapes_scalars_but_retains_trusted_entries_and_unvalidated_sizes() -> (
    None
):
    """
    Preserve scalar escaping, verbatim entry fragments, fixed image MIME, Unknown authors, and nonnumeric acquisition lengths.

    Example:
        >>> test_feed_escapes_scalars_but_retains_trusted_entries_and_unvalidated_sizes()


    :return: None after parsed XML values and intentionally unvalidated fragment/length behavior are checked.
    """
    assert "<trusted/>" in opds_feed(
        title="<title>", feed_id="id", entries=["<trusted/>"]
    )
    assert "&lt;title&gt;" in opds_feed(title="<title>", feed_id="id", entries=[])
    host = OpdsHost()
    metadata = host.opds_work_metadata_payload(host.work_row)
    metadata.update(authors=[], summary="", tags=["A&B"])
    metadata["format_metadata"]["EPUB"]["size"] = "not-a-number"
    host.opds_work_metadata_payload = Mock(return_value=metadata)
    root = ElementTree.fromstring(OpdsApi(host).work_entry(host.work_row))
    assert root.find("author/name").text == "Unknown"
    assert root.find("summary").text == "A&B"
    assert len(root.findall("link[@type='image/png']")) == 4
    assert (
        root.find("link[@rel='http://opds-spec.org/acquisition']").attrib["length"]
        == "not-a-number"
    )


def test_acquisition_helpers_keep_list_only_receipts_and_dimension_precedence() -> None:
    """
    Retain record identity in list receipts, reject other sequences, and keep suffix/invalid-size precedence for covers.

    Example:
        >>> test_acquisition_helpers_keep_list_only_receipts_and_dimension_precedence()


    :return: None after record filtering, byte-helper type limits, and dimension edge cases are verified.
    """
    record = {"id": 1}
    assert AcquisitionCompatApi._records({"covers": (record,)}, "covers") == []
    assert (
        AcquisitionCompatApi._records({"covers": [None, record]}, "covers")[0] is record
    )
    assert _coerce_payload_bytes(memoryview(b"x")) == b"x"
    with pytest.raises(TypeError):
        _coerce_payload_bytes([1, 2])
    assert _cover_dimensions(suffix="-2_0", query={}, thumb=True) == (-2, 0)
    assert _cover_dimensions(
        suffix="10_20_30", query={"sz": ["badxsize"]}, thumb=False
    ) == (10, 20)
    assert _cover_dimensions(suffix="10_20", query={"sz": ["7"]}, thumb=False) == (
        240,
        320,
    )


def test_cover_read_and_response_failures_can_fall_back_to_the_same_redirect() -> None:
    """
    Exercise actual readable-cover failures inside both read and byte-response construction before redirect fallback.

    Example:
        >>> test_cover_read_and_response_failures_can_fall_back_to_the_same_redirect()


    :return: None after both injected error locations trigger a read attempt and then the advertised redirect.
    """
    for failing_stage in ("read", "response"):
        host = AcquisitionHost()
        resolution = {
            "readable": True,
            "delivery": "redirect",
            "location": "https://example.invalid/cover",
        }
        host.core.query = Mock(
            side_effect=[
                {"work": host.work_row},
                {"covers": [{"id": 1, "resolution": resolution}]},
            ]
        )
        api = AcquisitionCompatApi(host)
        api.model = Mock(spec=CoreSurfaceModel)
        api.model.acquisition_read.return_value = ({}, b"cover")
        if failing_stage == "read":
            api.model.acquisition_read.side_effect = RuntimeError("read failed")
        else:
            host.acquisition_bytes_response = Mock(
                side_effect=RuntimeError("response failed")
            )
        response = api.serve_cover_or_thumb("1", query={}, environ={}, thumb=True)
        assert response.status == "302 Found"
        assert dict(response.headers)["Location"] == resolution["location"]
        api.model.acquisition_read.assert_called_once_with("image", 1)


def test_format_read_and_response_failures_propagate_instead_of_redirecting() -> None:
    """
    Preserve the exact readable-format failure even when that same record advertises a redirect fallback.

    Example:
        >>> test_format_read_and_response_failures_propagate_instead_of_redirecting()


    :return: None after read/response exception identity and untouched redirect-factory assertions.
    """
    for failing_stage in ("read", "response"):
        host = AcquisitionHost()
        record = {
            "extension": "epub",
            "id": 7,
            "kind": "legacy-file",
            "resolution": {
                "readable": True,
                "delivery": "redirect",
                "location": "/fallback",
            },
        }
        host.core.query = Mock(
            side_effect=[{"work": host.work_row}, {"formats": [record]}]
        )
        host.acquisition_redirect_response = Mock()
        api = AcquisitionCompatApi(host)
        api.model = Mock(spec=CoreSurfaceModel)
        api.model.acquisition_read.return_value = ({}, b"file")
        error = RuntimeError(failing_stage + " failed")
        if failing_stage == "read":
            api.model.acquisition_read.side_effect = error
        else:
            host.acquisition_bytes_response = Mock(side_effect=error)
        with pytest.raises(RuntimeError) as raised:
            api.serve_compat_get("epub", "1", {}, {})
        assert raised.value is error
        host.acquisition_redirect_response.assert_not_called()


def test_format_adapter_filters_malformed_records_and_does_not_strip_record_extensions() -> (
    None
):
    """
    Test adapter-side filtering directly rather than relying on the original fixture producer to remove invalid files.

    Example:
        >>> test_format_adapter_filters_malformed_records_and_does_not_strip_record_extensions()


    :return: None after only the final usable extension/identity/kind/resolution record reaches acquisition_read.
    """
    host = AcquisitionHost()
    usable = {
        "extension": "epub",
        "id": 9,
        "kind": "legacy-file",
        "resolution": {"readable": True},
    }
    records = [
        None,
        {**usable, "extension": " epub "},
        {**usable, "id": ""},
        {**usable, "kind": ""},
        {**usable, "resolution": []},
        usable,
    ]
    host.core.query = Mock(side_effect=[{"work": host.work_row}, {"formats": records}])
    api = AcquisitionCompatApi(host)
    api.model = Mock(spec=CoreSurfaceModel)
    api.model.acquisition_read.return_value = ({}, b"chosen")
    response = api.serve_compat_get(" .EPUB ", "1", {}, {})
    assert b"".join(response.body) == b"chosen"
    api.model.acquisition_read.assert_called_once_with("legacy-file", 9)


def test_catalog_reuses_collaborators_mutates_urls_and_preserves_author_id_spelling() -> (
    None
):
    """
    Preserve default image sharing, explicit override isolation, in-place URL changes, and original author-ID spelling.

    Example:
        >>> test_catalog_reuses_collaborators_mutates_urls_and_preserves_author_id_spelling()


    :return: None after collaborator/list identity, escaped route, lookup order, and zero-token distinctions are checked.
    """
    backend = _catalog()
    assert backend.images is backend.read_model.images
    override = Mock(spec=ImageBackend)
    other = CalibreCatalogBackend(
        backend.host, read_model=backend.read_model, images=override
    )
    assert other.images is override and backend.read_model.images is not override
    rows = [{"table": "agents extra", "id": "a/b", "url": "old"}]
    backend.read_model.category_rows.return_value = rows
    assert backend.category_rows("authors") is rows
    assert rows[0]["url"] == "/author/agents%20extra/a%2Fb"
    backend.read_model.author_tables.return_value = ["agents", "human_agents"]
    backend.read_model.row_by_id.side_effect = [None, {}]
    assert backend.category_route_target("authors", "007") == "/author/human_agents/007"
    assert backend.read_model.row_by_id.call_args.args == ("human_agents", 7)
    assert backend.split_compat_book_token(0) == (None, "")
    assert backend.split_compat_book_token("0_suffix") == (0, "suffix")


def test_catalog_author_selection_and_visible_metadata_keep_order_and_raw_id_types() -> (
    None
):
    """
    Stop author lookup at the first nonempty result and filter visible metadata in input order without ID coercion.

    Example:
        >>> test_catalog_author_selection_and_visible_metadata_keep_order_and_raw_id_types()


    :return: None after first-table result identity, no merged author results, and raw membership/duplicate retention checks.
    """
    backend = _catalog()
    chosen = [{"work_id": 7}]
    backend.read_model.author_tables.return_value = [
        "agents",
        "human_agents",
        "org_agents",
    ]
    backend.read_model.works_for_linked_entity.side_effect = [[], chosen]
    assert backend.work_rows_for_category_item("author", "7") is chosen
    assert backend.read_model.works_for_linked_entity.call_count == 2
    rows = [{"work_id": 1}, {"work_id": "2"}, {"work_id": 2}, {"work_id": 1}]
    visible = backend.metadata_rows_for_search_result(rows, {"book_ids": [2, 1]})
    assert visible == [rows[0], rows[2], rows[3]] and visible[0] is rows[0]


def test_catalog_metadata_augmentation_mutates_only_the_augmented_projection() -> None:
    """
    Add category URLs in place with unfiltered missing IDs while bulk work_rows_payload bypasses augmentation.

    Example:
        >>> test_catalog_metadata_augmentation_mutates_only_the_augmented_projection()


    :return: None after mapping identity, encoded None ID, and unaugmented bulk metadata behavior are checked.
    """
    backend = _catalog()
    original, plain = {"id": 7}, {"id": 8}
    backend.read_model.work_metadata_payload.side_effect = [original, plain]
    backend.host._work_credit_entries.return_value = [
        {"table": "agents", "row": {"agent_id": None}}
    ]
    assert backend.work_metadata_payload({"work_id": 7}) is original
    assert original["category_urls"]["authors"] == [
        "/ajax/books_in/{}/{}/main".format(
            encode_compat_token("authors"), encode_compat_token("None")
        )
    ]
    result = backend.work_rows_payload([{"work_id": 8}])
    assert result[0] is plain and "category_urls" not in plain
