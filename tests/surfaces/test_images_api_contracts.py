"""Behavioral edge contracts for the shared surface image backend."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.surfaces.images.api import ImageBackend


class _Storage:
    def __init__(self, *outcomes: object) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, object]] = []

    def locate_file(self, *, metadata: dict[str, object]):
        self.calls.append(metadata)
        outcome = self.outcomes.pop(0) if self.outcomes else None
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


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
    def __init__(self, store_rows: dict[int, object] | None = None) -> None:
        self.store_rows = store_rows or {}

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

    def row_by_id(self, table: str, row_id: int):
        assert table == "stores"
        value = self.store_rows.get(row_id)
        if isinstance(value, Exception):
            raise value
        return value


class _Db(_DiscoverySource):
    def __init__(
        self,
        *,
        storage: object | None = None,
        store_rows: dict[int, object] | None = None,
    ) -> None:
        self.storage = storage
        self.store_rows = store_rows or {}

    def get_interlinked_rows(
        self,
        *,
        target_row: dict[str, object],
        secondary_table: str,
    ) -> list[dict[str, object]]:
        assert secondary_table == "manifestations"
        return self._manifestations(target_row)

    def search(
        self,
        table: str,
        column: str,
        value: object,
    ) -> list[dict[str, object]]:
        return self._search(table, column, value)

    def get_row_from_id(self, table: str, row_id: int):
        assert table == "stores"
        value = self.store_rows.get(row_id)
        if isinstance(value, Exception):
            raise value
        return value


class _Host:
    def __init__(
        self,
        *,
        db: _Db | None = None,
        read_model: _ReadModel | None = None,
        title: str = "Example",
    ) -> None:
        self.db = db or _Db()
        self.read_model = read_model
        self.title = title
        self.related: dict[str, list[object]] = {}
        self.refresh_actions: list[tuple[bool, object | None]] = []
        self.refresh_count = 0

    @staticmethod
    def _row_dict(_table: str, row: object) -> dict[str, object]:
        return dict(row)  # type: ignore[arg-type]

    def _refresh_storage_manager(self) -> bool:
        self.refresh_count += 1
        if not self.refresh_actions:
            return False
        result, storage = self.refresh_actions.pop(0)
        self.db.storage = storage
        return result

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

    assert _image_ids(rows) == [1, 2]
    assert next(row for row in rows if row["image_id"] == 2)["image_name"] == "2.jpg"


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


def test_storage_resolution_uses_an_existing_storage_manager() -> None:
    located = object()
    storage = _Storage(located)
    host = _Host(db=_Db(storage=storage))
    backend = ImageBackend(host)

    assert backend.resolve_storage_image({"image_name": "cover.jpg"}) is located
    assert host.refresh_count == 0
    assert storage.calls[0]["image_name"] == "cover.jpg"


def test_storage_resolution_can_install_or_refresh_the_manager() -> None:
    installed_result = object()
    installed = _Storage(installed_result)
    install_host = _Host()
    install_host.refresh_actions = [(True, installed)]

    assert (
        ImageBackend(install_host).resolve_storage_image(
            {"image_store_id": 7, "image_name": "cover.jpg"}
        )
        is installed_result
    )
    assert install_host.refresh_count == 1

    refreshed_result = object()
    stale = _Storage(RuntimeError("stale"))
    refreshed = _Storage(refreshed_result)
    refresh_host = _Host(db=_Db(storage=stale))
    refresh_host.refresh_actions = [(True, refreshed)]

    assert (
        ImageBackend(refresh_host).resolve_storage_image(
            {"image_store_id": 7, "image_name": "cover.jpg"}
        )
        is refreshed_result
    )
    assert refresh_host.refresh_count == 1
    assert len(stale.calls) == 1
    assert len(refreshed.calls) == 1


def test_storage_resolution_uses_manager_attached_by_a_false_refresh() -> None:
    located = object()
    storage = _Storage(located)
    host = _Host()
    host.refresh_actions = [(False, storage)]

    assert ImageBackend(host).resolve_storage_image({}) is located
    assert host.refresh_count == 1


def test_storage_resolution_returns_none_when_refresh_cannot_supply_storage() -> None:
    host = _Host()
    host.refresh_actions = [(False, None), (True, None)]
    backend = ImageBackend(host)

    assert backend.resolve_storage_image({"image_store_id": 7}) is None
    assert host.refresh_count == 2

    failing = _Storage(RuntimeError("unavailable"))
    no_retry_host = _Host(db=_Db(storage=failing))
    assert ImageBackend(no_retry_host).resolve_storage_image({}) is None
    assert no_retry_host.refresh_count == 0


def test_direct_local_and_remote_image_targets_are_resolved(tmp_path: Path) -> None:
    cover = tmp_path / "cover.png"
    cover.write_bytes(b"png")
    backend = ImageBackend(_Host())

    local = backend.resolve_image_target(
        {
            "image_original_path": "",
            "image_path": str(cover),
            "image_name": "cover.png",
        }
    )
    remote = backend.resolve_image_target(
        {
            "image_source": "",
            "image_original_path": "https://images.example/cover.jpg",
            "image_name": "cover.jpg",
        }
    )

    assert local is not None
    assert local.mode == "local"
    assert Path(local.location) == cover
    assert remote is not None
    assert remote.mode == "redirect"
    assert remote.location == "https://images.example/cover.jpg"


@pytest.mark.parametrize(
    ("store_row", "expected"),
    (
        (
            {
                "store_root_uri": "https://cdn.example/books",
                "store_access_protocol": "https",
            },
            "https://cdn.example/books/covers/cover.jpg",
        ),
        (
            {
                "store_root_uri": "https://cdn.example/books/",
                "store_access_protocol": "https",
            },
            "https://cdn.example/books/covers/cover.jpg",
        ),
    ),
)
def test_remote_store_roots_resolve_to_redirects(
    store_row: dict[str, object],
    expected: str,
) -> None:
    read_model = _ReadModel({7: store_row})
    backend = ImageBackend(_Host(read_model=read_model))

    target = backend.resolve_image_target(
        {
            "image_store_id": 7,
            "image_storage_key": "covers/cover.jpg",
            "image_name": "cover.jpg",
        }
    )

    assert target is not None
    assert target.mode == "redirect"
    assert target.location == expected


@pytest.mark.parametrize(
    "root_factory",
    (
        lambda root: f"file://{root}",
        lambda root: str(root),
    ),
    ids=("file-uri", "absolute-path"),
)
def test_local_store_roots_resolve_existing_files(
    tmp_path: Path,
    root_factory: Callable[[Path], str],
) -> None:
    cover = tmp_path / "covers" / "cover.jpg"
    cover.parent.mkdir()
    cover.write_bytes(b"jpeg")
    store_row = {
        "store_root_uri": root_factory(tmp_path),
        "store_access_protocol": "custom",
    }
    backend = ImageBackend(_Host(db=_Db(store_rows={7: store_row})))

    target = backend.resolve_image_target(
        {
            "image_store_id": 7,
            "image_storage_key": "covers/cover.jpg",
            "image_name": "cover.jpg",
        }
    )

    assert target is not None
    assert target.mode == "local"
    assert Path(target.location) == cover


def test_relative_file_store_roots_are_supported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cover = tmp_path / "assets" / "cover.jpg"
    cover.parent.mkdir()
    cover.write_bytes(b"jpeg")
    monkeypatch.chdir(tmp_path)
    backend = ImageBackend(
        _Host(
            db=_Db(
                store_rows={
                    7: {
                        "store_root_uri": "assets",
                        "store_access_protocol": "file",
                    }
                }
            )
        )
    )

    target = backend.resolve_image_target(
        {
            "image_store_id": 7,
            "image_storage_key": "cover.jpg",
        }
    )

    assert target is not None
    assert Path(target.location) == Path("assets/cover.jpg")


@pytest.mark.parametrize(
    "image_row",
    (
        {},
        {"image_storage_key": "cover.jpg"},
        {"image_store_id": 7, "image_storage_key": ""},
        {"image_store_id": 7, "image_storage_key": "cover.jpg"},
    ),
)
def test_unresolvable_store_targets_return_none(image_row: dict[str, object]) -> None:
    backend = ImageBackend(
        _Host(
            read_model=_ReadModel({7: RuntimeError("store unavailable")}),
        )
    )

    assert backend.resolve_image_target(image_row) is None


def test_non_file_relative_roots_and_missing_files_are_not_targets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    rows = {
        1: {"store_root_uri": "", "store_access_protocol": "file"},
        2: {"store_root_uri": "relative", "store_access_protocol": "https"},
        3: {"store_root_uri": str(tmp_path), "store_access_protocol": "file"},
    }
    backend = ImageBackend(_Host(db=_Db(store_rows=rows)))

    for store_id in rows:
        assert (
            backend.resolve_image_target(
                {
                    "image_store_id": store_id,
                    "image_storage_key": "missing.jpg",
                }
            )
            is None
        )


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
