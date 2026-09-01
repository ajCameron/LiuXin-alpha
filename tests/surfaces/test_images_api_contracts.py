"""Behavioral edge contracts for the shared surface image backend."""

from __future__ import annotations

import pytest

from LiuXin_alpha.surfaces.images.api import ImageBackend


class _DiscoverySource:
    def _manifestations(self, expression_row: dict[str, object]) -> list[dict[str, object]]:
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
    def interlinked_rows(
        self,
        expression_row: dict[str, object],
        table: str,
    ) -> list[dict[str, object]]:
        assert table == "manifestations"
        return self._manifestations(expression_row)

    def search_rows(
        self,
        table: str,
        column: str,
        value: object,
    ) -> list[dict[str, object]]:
        return self._search(table, column, value)

class _Core:
    def __init__(
        self,
        *,
        resolutions: dict[int, object] | None = None,
        contents: dict[int, bytes] | None = None,
    ) -> None:
        self.resolutions = resolutions or {}
        self.contents = contents or {}
        self.queries: list[tuple[str, dict[str, object]]] = []

    def query(self, name: str, payload=None):
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
    def __init__(
        self,
        *,
        read_model: _ReadModel | None = None,
        core: _Core | None = None,
        title: str = "Example",
    ) -> None:
        self.read_model = read_model
        self.core = core or _Core()
        self.title = title
        self.related: dict[str, list[object]] = {}

    @staticmethod
    def _row_dict(_table: str, row: object) -> dict[str, object]:
        return dict(row)  # type: ignore[arg-type]

    def _related_rows_by_table(self, _work_row: object) -> dict[str, list[object]]:
        return self.related

    def _row_primary_text(self, _table: str, _work_row: object) -> str:
        return self.title


def _image_ids(rows: list[object]) -> list[int]:
    return sorted(int(row["image_id"]) for row in rows)  # type: ignore[index]


@pytest.mark.parametrize("use_read_model", (True, False))
def test_image_discovery_walks_expressions_and_ignores_bad_rows(
    use_read_model: bool,
) -> None:
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
            {"mode": "interlink-error"},
            {"mode": "item-error"},
            {"mode": "image-error"},
            {"mode": "empty"},
        ],
    }

    rows = backend.work_image_rows(related)

    assert _image_ids(rows) == ([1, 2] if use_read_model else [1])
    if use_read_model:
        assert next(row for row in rows if row["image_id"] == 2)[
            "image_name"
        ] == "2.jpg"


def test_duplicate_image_ids_are_deduplicated_by_the_latest_row() -> None:
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
    core = _Core()

    assert ImageBackend(_Host(core=core)).resolve_storage_image(image_row) is None
    assert core.queries == []


@pytest.mark.parametrize(
    "resolution",
    (
        {"delivery": "redirect", "readable": False},
        RuntimeError("Core unavailable"),
    ),
)
def test_storage_resolution_rejects_unreadable_or_failed_core_results(
    resolution: object,
) -> None:
    core = _Core(resolutions={7: resolution})

    assert ImageBackend(_Host(core=core)).resolve_storage_image({"image_id": 7}) is None


def test_redirect_targets_come_from_core_resolution() -> None:
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
        ({"image_id": 7}, RuntimeError("Core unavailable")),
    ),
)
def test_non_redirect_core_results_are_not_redirect_targets(
    image_row: dict[str, object],
    resolution: object | None,
) -> None:
    core = _Core(resolutions={} if resolution is None else {7: resolution})

    assert ImageBackend(_Host(core=core)).resolve_image_target(image_row) is None


def test_work_image_row_returns_the_first_image_or_none() -> None:
    host = _Host()
    backend = ImageBackend(host)

    host.related = {"images": [{"image_id": 1}, {"image_id": 2}]}
    assert backend.work_image_row({"work_id": 1}) == {"image_id": 1}

    host.related = {}
    assert backend.work_image_row({"work_id": 1}) is None


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
    assert ImageBackend.thumbnail_text(text) == expected


@pytest.mark.parametrize(
    ("width", "expected_font_size"),
    ((10, b"font-size='18'"), (200, b"font-size='48'")),
)
def test_placeholder_cover_svg_clamps_font_size_and_escapes_text(
    width: int,
    expected_font_size: bytes,
) -> None:
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
