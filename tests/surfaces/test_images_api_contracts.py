"""
Verify image discovery, Core resolution, naming, and SVG fallback using deterministic in-memory host doubles.

The doubles record Core queries and simulate relationship/read failures without
opening databases, requesting network resources, or rendering raster images.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.surfaces.images.api import ImageBackend


class _DiscoverySource:
    """
    Supply deterministic manifestation/item/image lookup data with token-selected failure paths.

    Example:
        >>> _DiscoverySource()._manifestations({"mode": "empty"})
        []
    """

    def _manifestations(
        self, expression_row: dict[str, object]
    ) -> list[dict[str, object]]:
        """
        Return no manifestations, raise an interlink failure, or emit a missing-ID row followed by the selected ID.

        Example:
            >>> _DiscoverySource()._manifestations({"mode": 2})
            [{'manifestation_id': None}, {'manifestation_id': 2}]


        :param expression_row: Mapping whose mode chooses empty/failing behavior or the returned identifier.
        :return: Fixture manifestation list, defaulting the usable identifier to ten for a falsey mode.
        :raises RuntimeError: If mode is interlink-error.
        """
        mode = expression_row.get("mode")
        if mode == "interlink-error":
            raise RuntimeError("interlink failed")
        if mode == "empty":
            return []
        return [
            {"manifestation_id": None},
            {"manifestation_id": mode or 10},
        ]

    def _search(
        self,
        table: str,
        column: str,
        value: object,
    ) -> list[dict[str, object]]:
        """
        Emit item/image fixture rows or raise the corresponding lookup failure for a selected mode token.

        Example:
            >>> _DiscoverySource()._search("items", "item_manifestation_id", 2)
            [{'item_id': ''}, {'item_id': 2}]


        :param table: Expected items or images lookup table.
        :param column: Recorded query context used only in unexpected-table diagnostics.
        :param value: Returned fixture identifier or item-error/image-error failure selector.
        :return: Two fixture rows, including one deliberately unusable identifier.
        :raises RuntimeError: For the matching lookup-error selector.
        :raises AssertionError: If table is neither items nor images.
        """
        if table == "items":
            if value == "item-error":
                raise RuntimeError("item lookup failed")
            return [
                {"item_id": ""},
                {"item_id": value},
            ]
        if table == "images":
            if value == "image-error":
                raise RuntimeError("image lookup failed")
            return [
                {"image_id": value, "image_name": f"{value}.jpg"},
                {"image_id": "not-an-integer"},
            ]
        raise AssertionError((table, column, value))


class _ReadModel(_DiscoverySource):
    """
    Expose discovery fixtures through the read-model methods consumed by ImageBackend.

    Example:
        >>> _ReadModel().interlinked_rows({"mode": "empty"}, "manifestations")
        []
    """

    def interlinked_rows(
        self,
        expression_row: dict[str, object],
        table: str,
    ) -> list[dict[str, object]]:
        """
        Require the manifestations relationship and forward expression lookup to its fixture provider.

        Example:
            >>> _ReadModel().interlinked_rows({"mode": "empty"}, "manifestations")
            []


        :param expression_row: Fixture expression mapping carrying its lookup mode.
        :param table: Relationship target, required to be manifestations.
        :return: Provider manifestation list, with provider failures left unchanged.
        """
        assert table == "manifestations"
        return self._manifestations(expression_row)

    def search_rows(
        self,
        table: str,
        column: str,
        value: object,
    ) -> list[dict[str, object]]:
        """
        Forward item/image searches unchanged to the deterministic discovery fixture.

        Example:
            >>> rows = _ReadModel().search_rows("items", "item_manifestation_id", 2)
            >>> rows[-1]["item_id"]
            2


        :param table: Requested discovery table passed to _search.
        :param column: Requested matching column passed to _search.
        :param value: Matching identifier or failure-mode token passed to _search.
        :return: Fixture rows, or the unchanged provider exception.
        """
        return self._search(table, column, value)


class _Core:
    """
    Record acquisition queries and answer them from supplied resolution/content mappings.

    Missing resolutions are explicitly unreadable; injected Exception values are
    raised unchanged. This is a test double, not a Core runtime or HTTP client.

    Example:
        >>> _Core().query("acquisition.resolve", {"id": 7})
        {'delivery': 'unavailable', 'readable': False}
    """

    def __init__(
        self,
        *,
        resolutions: dict[int, object] | None = None,
        contents: dict[int, bytes] | None = None,
    ) -> None:
        """
        Retain truthy response mappings or substitute fresh empty dicts, and initialize the request log.

        Example:
            >>> _Core().queries
            []


        :param resolutions: Resource-ID resolution mappings or exception instances, defaulting empty when falsey.
        :param contents: Resource-ID byte payload mapping, defaulting empty when falsey.
        :return: None after storing fixture responses and an empty query log.
        """
        self.resolutions = resolutions or {}
        self.contents = contents or {}
        self.queries: list[tuple[str, dict[str, object]]] = []

    def query(self, name: str, payload=None):
        """
        Record a copied request, select its integer ID, and return resolution or resource/content fixture data.

        Example:
            >>> core = _Core()
            >>> result = core.query("acquisition.resolve", {"id": "7"})
            >>> core.queries
            [('acquisition.resolve', {'id': '7'})]


        :param name: Expected acquisition.resolve or acquisition.read query name.
        :param payload: Request mapping shallow-copied into the log; must supply an int-convertible id.
        :return: Copied resolution or resource/content mapping selected by the request.
        :raises AssertionError: If the requested query name is unsupported after ID/outcome processing.
        """
        request = dict(payload or {})
        self.queries.append((name, request))
        resource_id = int(request["id"])
        outcome = self.resolutions.get(
            resource_id,
            {"delivery": "unavailable", "readable": False},
        )
        if isinstance(outcome, Exception):
            raise outcome
        if name == "acquisition.resolve":
            return dict(outcome)
        if name == "acquisition.read":
            return {
                "resource": dict(outcome),
                "content": self.contents[resource_id],
            }
        raise AssertionError(f"Unexpected Core query: {name}")


class _Host:
    """
    Provide mutable related groups, a title, and optional read-model/Core doubles to the image backend.

    Example:
        >>> _Host(title="雪")._row_primary_text("works", {})
        '雪'
    """

    def __init__(
        self,
        *,
        read_model: _ReadModel | None = None,
        core: _Core | None = None,
        title: str = "Example",
    ) -> None:
        """
        Store the requested discovery source/title and use a supplied truthy Core double or a fresh default.

        Example:
            >>> _Host().related
            {}


        :param read_model: Optional expression/item discovery provider retained unchanged.
        :param core: Optional acquisition provider, replaced by a new _Core when falsey.
        :param title: Text returned by the primary-display hook.
        :return: None after initializing borrowed providers, title, and an empty relationship mapping.
        """
        self.read_model = read_model
        self.core = core or _Core()
        self.title = title
        self.related: dict[str, list[object]] = {}

    @staticmethod
    def _row_dict(_table: str, row: object) -> dict[str, object]:
        """
        Shallow-copy a dict-convertible row without applying table-specific visibility rules.

        Example:
            >>> _Host._row_dict("images", {"image_id": 7})
            {'image_id': 7}


        :param _table: Schema context intentionally ignored by this projection double.
        :param row: Fixture row passed directly to dict construction.
        :return: New shallow row dict; invalid conversion errors propagate.
        """
        return dict(row)  # type: ignore[arg-type]

    def _related_rows_by_table(self, _work_row: object) -> dict[str, list[object]]:
        """
        Return the host's configured relationship mapping unchanged for any work row.

        Example:
            >>> host = _Host()
            >>> host._related_rows_by_table({}) is host.related
            True


        :param _work_row: Work context ignored by this fixed-data hook.
        :return: The mutable related mapping retained by this host.
        """
        return self.related

    def _row_primary_text(self, _table: str, _work_row: object) -> str:
        """
        Return the configured fixture title without reading the supplied row.

        Example:
            >>> _Host(title="雪")._row_primary_text("works", {})
            '雪'


        :param _table: Table context intentionally unused by this fixture.
        :param _work_row: Row context intentionally unused by this fixture.
        :return: The host's title attribute unchanged.
        """
        return self.title


def _image_ids(rows: list[object]) -> list[int]:
    """
    Extract and numerically sort image IDs from subscriptable test rows for order-independent membership assertions.

    Example:
        >>> _image_ids([{"image_id": "2"}, {"image_id": 1}])
        [1, 2]


    :param rows: Fixture rows expected to expose int-convertible image_id values.
    :return: Ascending list of converted image IDs without deduplication.
    """
    return sorted(int(row["image_id"]) for row in rows)  # type: ignore[index]


@pytest.mark.parametrize("use_read_model", (True, False))
def test_image_discovery_walks_expressions_and_ignores_bad_rows(
    use_read_model: bool,
) -> None:
    """
    Discover direct images alone or add expression-linked images while skipping unusable IDs.

    Example:
        >>> test_image_discovery_walks_expressions_and_ignores_bad_rows(True)


    :param use_read_model: Whether the host exposes the manifestation/item discovery provider.
    :return: None after expected image membership and discovered-row metadata are checked.
    """
    host = _Host(read_model=_ReadModel() if use_read_model else None)
    backend = ImageBackend(host)
    related = {
        "images": [
            {"image_id": 1, "image_name": "direct.jpg"},
            {"image_id": None},
            {"image_id": "not-an-integer"},
        ],
        "expressions": [
            {"mode": 2},
            {"mode": "empty"},
        ],
    }

    rows = backend.work_image_rows(related)

    assert _image_ids(rows) == ([1, 2] if use_read_model else [1])
    if use_read_model:
        assert (
            next(row for row in rows if row["image_id"] == 2)["image_name"] == "2.jpg"
        )


def test_duplicate_image_ids_are_deduplicated_by_the_latest_row() -> None:
    """
    Replace a direct image with the later discovered row when both convert to the same image ID.

    Example:
        >>> test_duplicate_image_ids_are_deduplicated_by_the_latest_row()


    :return: None after single-row cardinality and later-row filename assertions.
    """
    host = _Host(read_model=_ReadModel())
    backend = ImageBackend(host)
    related = {
        "images": [{"image_id": 2, "image_name": "direct.jpg"}],
        "expressions": [{"mode": 2}],
    }

    rows = backend.work_image_rows(related)

    assert len(rows) == 1
    assert rows[0]["image_name"] == "2.jpg"  # type: ignore[index]


def test_image_names_content_types_and_storage_aliases() -> None:
    """
    Preserve filename/MIME precedence and shallow legacy image-to-file alias projection.

    Example:
        >>> test_image_names_content_types_and_storage_aliases()


    :return: None after declared/fallback naming, MIME inference, eligible aliases, and nested-copy assertions.
    """
    backend = ImageBackend(_Host())

    named = {
        "image_name": "cover.jpg",
        "image_mime_type": " image/custom ",
        "image_store_id": 7,
        "image_storage_key": "covers/cover.jpg",
        "image_original_name": "",
        "image_original_path": "/source/cover.jpg",
        "image_source": None,
    }
    original = {"image_original_name": "scan.png"}
    stored = {"image_storage_key": "opaque.data"}
    empty: dict[str, object] = {}

    assert backend.image_download_name(named) == "cover.jpg"
    assert backend.image_download_name(original) == "scan.png"
    assert backend.image_download_name(stored) == "opaque.data"
    assert backend.image_download_name(empty) == "cover.bin"
    assert backend.image_content_type(named) == "image/custom"
    assert backend.image_content_type(original) == "image/png"
    assert backend.image_content_type(stored) == "application/octet-stream"

    metadata = backend.image_storage_lookup_metadata(named)
    assert metadata["file_store_id"] == 7
    assert metadata["file_storage_key"] == "covers/cover.jpg"
    assert metadata["file_name"] == "cover.jpg"
    assert metadata["file_original_path"] == "/source/cover.jpg"
    assert "file_original_name" not in metadata
    assert "file_source" not in metadata
    assert metadata["image_row"] == named
    assert metadata["image_row"] is not named


def test_storage_resolution_returns_a_core_backed_file() -> None:
    """
    Resolve a readable image first and fetch content only when the returned stored-resource adapter is read.

    Example:
        >>> test_storage_resolution_returns_a_core_backed_file()


    :return: None after byte-content and exact ordered resolve/read query assertions.
    """
    core = _Core(
        resolutions={7: {"delivery": "core", "readable": True}},
        contents={7: b"cover-bytes"},
    )
    stored = ImageBackend(_Host(core=core)).resolve_storage_image(
        {"image_id": 7, "image_name": "cover.jpg"}
    )

    assert stored is not None
    assert stored.read_bytes() == b"cover-bytes"
    assert core.queries == [
        ("acquisition.resolve", {"kind": "image", "id": 7}),
        ("acquisition.read", {"kind": "image", "id": 7}),
    ]


@pytest.mark.parametrize(
    "image_row",
    ({}, {"image_id": None}, {"image_id": "not-an-integer"}),
)
def test_storage_resolution_rejects_images_without_a_valid_id(
    image_row: dict[str, object],
) -> None:
    """
    Return no stored-resource reader for missing, None, or malformed IDs without issuing a Core query.

    Example:
        >>> test_storage_resolution_rejects_images_without_a_valid_id({"image_id": "bad"})


    :param image_row: Parametrized fixture row lacking a usable image ID.
    :return: None after unavailable-result and empty-query-log assertions.
    """
    core = _Core()

    assert ImageBackend(_Host(core=core)).resolve_storage_image(image_row) is None
    assert core.queries == []


def test_storage_resolution_rejects_explicitly_unreadable_core_results() -> None:
    """
    Treat a false readable flag as unavailable for a byte reader even when a redirect delivery is advertised.

    Example:
        >>> test_storage_resolution_rejects_explicitly_unreadable_core_results()


    :return: None after the readable gate rejects the fixture resolution.
    """
    core = _Core(resolutions={7: {"delivery": "redirect", "readable": False}})

    assert ImageBackend(_Host(core=core)).resolve_storage_image({"image_id": 7}) is None


def test_redirect_targets_come_from_core_resolution() -> None:
    """
    Construct a redirect target from Core location metadata while falling back to the row's filename.

    Example:
        >>> test_redirect_targets_come_from_core_resolution()


    :return: None after redirect mode, URL, and download-name assertions.
    """
    core = _Core(
        resolutions={
            7: {
                "delivery": "redirect",
                "readable": False,
                "location": "https://cdn.example/covers/cover.jpg",
            }
        }
    )

    target = ImageBackend(_Host(core=core)).resolve_image_target(
        {"image_id": 7, "image_name": "cover.jpg"}
    )

    assert target is not None
    assert target.mode == "redirect"
    assert target.location == "https://cdn.example/covers/cover.jpg"
    assert target.download_name == "cover.jpg"


@pytest.mark.parametrize(
    ("image_row", "resolution"),
    (
        ({}, None),
        ({"image_id": "bad"}, None),
        ({"image_id": 7}, {"delivery": "core", "readable": True}),
    ),
)
def test_non_redirect_core_results_are_not_redirect_targets(
    image_row: dict[str, object],
    resolution: object | None,
) -> None:
    """
    Return no redirect for missing/invalid IDs or a readable non-redirect Core delivery.

    Example:
        >>> test_non_redirect_core_results_are_not_redirect_targets({}, None)


    :param image_row: Parametrized invalid row or the valid row used for a non-redirect resolution.
    :param resolution: Optional Core outcome associated with fixture image seven.
    :return: None after confirming the redirect adapter returns None.
    """
    core = _Core(resolutions={} if resolution is None else {7: resolution})

    assert ImageBackend(_Host(core=core)).resolve_image_target(image_row) is None


def test_work_image_row_returns_the_first_image_or_none() -> None:
    """
    Select the first discovered image for a work and distinguish an empty related-image list.

    Example:
        >>> test_work_image_row_returns_the_first_image_or_none()


    :return: None after first-row and empty-discovery assertions on the same host.
    """
    host = _Host()
    backend = ImageBackend(host)

    host.related = {"images": [{"image_id": 1}, {"image_id": 2}]}
    assert backend.work_image_row({"work_id": 1}) == {"image_id": 1}

    host.related = {}
    assert backend.work_image_row({"work_id": 1}) is None


@pytest.mark.parametrize("mode", ("interlink-error", "item-error", "image-error"))
def test_image_discovery_propagates_read_model_failures(mode: str) -> None:
    """
    Propagate failures at manifestation, item, and image discovery stages rather than returning partial results.

    Example:
        >>> test_image_discovery_propagates_read_model_failures("item-error")


    :param mode: Parametrized provider stage selected to raise a RuntimeError.
    :return: None after the expected discovery failure remains visible to the caller.
    """
    backend = ImageBackend(_Host(read_model=_ReadModel()))
    with pytest.raises(RuntimeError, match="failed"):
        backend.work_image_rows({"expressions": [{"mode": mode}]})


@pytest.mark.parametrize("method", ("resolve_storage_image", "resolve_image_target"))
def test_image_resolution_propagates_failed_core_queries(method: str) -> None:
    """
    Keep the exact Core exception from both stored-reader and redirect resolution paths.

    Example:
        >>> test_image_resolution_propagates_failed_core_queries("resolve_storage_image")


    :param method: Parametrized ImageBackend resolution method invoked with a valid image ID.
    :return: None after exception identity is verified.
    """
    error = RuntimeError("Core unavailable")
    backend = ImageBackend(_Host(core=_Core(resolutions={7: error})))
    with pytest.raises(RuntimeError) as raised:
        getattr(backend, method)({"image_id": 7})
    assert raised.value is error


@pytest.mark.parametrize(
    ("text", "expected"),
    (
        ("  élan", "É"),
        ("-- 9 lives", "9"),
        ("!?", "?"),
        ("", "?"),
    ),
)
def test_thumbnail_text_uses_the_first_alphanumeric_character(
    text: str,
    expected: str,
) -> None:
    """
    Preserve Unicode letter/numeric initial selection and question-mark fallback for empty or punctuation-only titles.

    Example:
        >>> test_thumbnail_text_uses_the_first_alphanumeric_character("-- 9 lives", "9")


    :param text: Parametrized title-like input supplied to thumbnail_text.
    :param expected: Exact initial or fallback string required for that input.
    :return: None after the rendered text matches its expected value.
    """
    assert ImageBackend.thumbnail_text(text) == expected


@pytest.mark.parametrize(
    ("width", "expected_font_size"),
    ((10, b"font-size='18'"), (200, b"font-size='48'")),
)
def test_placeholder_cover_svg_clamps_font_size_and_escapes_text(
    width: int,
    expected_font_size: bytes,
) -> None:
    """
    Clamp the initial's font at both width extremes while escaping title markup in the SVG output.

    Example:
        >>> test_placeholder_cover_svg_clamps_font_size_and_escapes_text(10, b"font-size='18'")


    :param width: Parametrized width that drives the lower or upper font-size clamp.
    :param expected_font_size: Byte fragment required in the generated SVG for that width.
    :return: None after SVG prefix, font, escaped subtitle, and selected initial are checked.
    """
    host = _Host(title="<A & very long title>")
    svg = ImageBackend(host).placeholder_cover_svg(
        {"work_id": 1},
        width=width,
        height=20,
    )

    assert svg.startswith(b"<svg")
    assert expected_font_size in svg
    assert b"&lt;A &amp; very long title&gt;" in svg
    assert b">A</text>" in svg
