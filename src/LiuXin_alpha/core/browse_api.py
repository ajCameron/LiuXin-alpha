"""Display-neutral browse and acquisition projections hosted by Core."""

# pyright: reportImportCycles=false

from __future__ import annotations

import mimetypes

from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urljoin

from LiuXin_alpha.core.description import CorePayloadFieldDescription
from LiuXin_alpha.core.errors import CoreDispatchError

if TYPE_CHECKING:
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


_CATEGORY_TABLES: dict[str, tuple[str, ...]] = {
    "authors": ("agents", "human_agents", "org_agents"),
    "tags": ("tags", "labels"),
    "series": ("series",),
}


def _field(
    name: str,
    *,
    required: bool = False,
    field_type: str | None = None,
) -> CorePayloadFieldDescription:
    return CorePayloadFieldDescription(
        name=name,
        required=required,
        field_type=field_type,
    )


def _payload(envelope: Any) -> dict[str, Any]:
    raw = getattr(envelope, "payload", None)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise CoreDispatchError("Core payload must be an object.")
    return dict(raw)


def _required_int(payload: Mapping[str, Any], name: str) -> int:
    value = payload.get(name)
    if value is None or isinstance(value, bool):
        raise CoreDispatchError("`{}` must be an integer.".format(name))
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise CoreDispatchError(
            "`{}` must be an integer.".format(name)
        ) from exc


def _row_mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    values = getattr(row, "row_dict", None)
    if isinstance(values, Mapping):
        return dict(values)
    raise CoreDispatchError(
        "The read source returned a non-row value.",
        code="invalid_read_source_row",
    )


def _tables(runtime: "CoreRuntime") -> set[str]:
    source = runtime.services.read_source
    method = getattr(source, "get_tables", None)
    if not callable(method):
        return set()
    try:
        values = cast(Iterable[object], method(force_refresh=False))
    except TypeError:
        values = cast(Iterable[object], method())
    return {str(value) for value in values}


def _id_column(runtime: "CoreRuntime", table: str) -> str:
    wrapper = getattr(runtime.database, "driver_wrapper", None)
    method = getattr(wrapper, "get_id_column", None)
    if callable(method):
        try:
            return str(method(table))
        except Exception:
            pass
    return "{}_id".format(table.rstrip("s"))


def _all_rows(runtime: "CoreRuntime", table: str) -> list[dict[str, Any]]:
    source = runtime.services.read_source
    method = getattr(source, "get_all_rows", None)
    if not callable(method):
        return []
    try:
        rows = method(table, iterator_return=False)
    except TypeError:
        rows = method(table)
    return [
        _row_mapping(row)
        for row in cast(Iterable[object], rows)
    ]


def _interlinked_tables(
    runtime: "CoreRuntime",
    table: str,
) -> list[str]:
    wrapper = getattr(runtime.database, "driver_wrapper", None)
    method = getattr(wrapper, "get_interlinked_tables", None)
    if callable(method):
        try:
            values = cast(Iterable[object], method(table))
            return sorted(
                {
                    str(value)
                    for value in values
                    if str(value) != table
                }
            )
        except Exception:
            pass
    return [
        value
        for value in (
            "agents",
            "expressions",
            "files",
            "images",
            "items",
            "labels",
            "series",
            "tags",
        )
        if value in _tables(runtime)
    ]


def _get_row(
    runtime: "CoreRuntime",
    table: str,
    row_id: int,
) -> dict[str, Any] | None:
    method = getattr(runtime.services.read_source, "get_row_from_id", None)
    if not callable(method):
        return None
    row = method(table, row_id)
    return None if row is None else _row_mapping(row)


def _search_rows(
    runtime: "CoreRuntime",
    table: str,
    column: str,
    value: Any,
) -> list[dict[str, Any]]:
    method = getattr(runtime.services.read_source, "search", None)
    if callable(method):
        try:
            rows = cast(Iterable[object], method(table, column, value))
            return [_row_mapping(row) for row in rows]
        except Exception:
            pass
    return [
        row
        for row in _all_rows(runtime, table)
        if row.get(column) == value
    ]


def _related_rows(
    runtime: "CoreRuntime",
    row: Mapping[str, Any],
    table: str,
) -> list[dict[str, Any]]:
    method = getattr(
        runtime.services.read_source,
        "get_interlinked_rows",
        None,
    )
    if not callable(method):
        return []
    target: Any = row
    getter = getattr(runtime.database, "get_row_from_id", None)
    if callable(getter):
        ranked: list[tuple[int, str, Any]] = []
        heading_getter = getattr(
            runtime.services.read_source,
            "get_column_headings",
            None,
        )
        for candidate in _tables(runtime):
            try:
                headings = (
                    {
                        str(value)
                        for value in cast(
                            Iterable[object],
                            heading_getter(candidate),
                        )
                    }
                    if callable(heading_getter)
                    else set()
                )
            except Exception:
                headings = set()
            id_candidates = sorted(
                (
                    value
                    for value in headings
                    if value == "id" or value.endswith("_id")
                ),
                key=len,
            )
            if not id_candidates:
                continue
            id_column = id_candidates[0]
            row_id = row.get(id_column)
            if row_id is None:
                continue
            prefix = candidate.rstrip("s") + "_"
            rank = sum(1 for key in row if str(key).startswith(prefix))
            ranked.append((rank, candidate, row_id))
        for _rank, candidate, row_id in sorted(ranked, reverse=True):
            try:
                candidate_row = getter(candidate, int(row_id))
            except Exception:
                continue
            if candidate_row is not None:
                target = candidate_row
                break
    try:
        rows = method(target_row=target, secondary_table=table)
    except Exception:
        if target is row:
            return []
        try:
            rows = method(target_row=row, secondary_table=table)
        except Exception:
            return []
    return [
        _row_mapping(value)
        for value in cast(Iterable[object], rows)
    ]


def _primary_text(table: str, row: Mapping[str, Any]) -> str:
    stem = table.rstrip("s")
    preferred = (
        "{}_title".format(stem),
        "{}_name".format(stem),
        "{}_label".format(stem),
        "{}_value".format(stem),
        "title",
        "name",
        "label",
        "tag",
        "series",
    )
    for key in preferred:
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    for key, value in row.items():
        if key.endswith(("_title", "_name", "_label")) and value not in (None, ""):
            return str(value)
    return "{} {}".format(table.rstrip("s").replace("_", " ").title(), row.get("{}_id".format(stem), ""))


def _flatten(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Mapping):
        return " ".join(_flatten(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return " ".join(_flatten(item) for item in value)
    return str(value).casefold()


def _work_summary(
    runtime: "CoreRuntime",
    row: Mapping[str, Any],
) -> dict[str, Any]:
    work_id = row.get(_id_column(runtime, "works"))
    authors: list[dict[str, Any]] = []
    for table in _CATEGORY_TABLES["authors"]:
        for author_row in _related_rows(runtime, row, table):
            authors.append(
                {
                    "id": author_row.get(_id_column(runtime, table)),
                    "table": table,
                    "name": _primary_text(table, author_row),
                }
            )
    series = [
        {
            "id": value.get(_id_column(runtime, "series")),
            "name": _primary_text("series", value),
        }
        for value in _related_rows(runtime, row, "series")
    ]
    tag_rows: list[dict[str, Any]] = []
    for table in _CATEGORY_TABLES["tags"]:
        linked_tags = _related_rows(runtime, row, table)
        if linked_tags:
            tag_rows = [
                {
                    "id": value.get(_id_column(runtime, table)),
                    "table": table,
                    "name": _primary_text(table, value),
                }
                for value in linked_tags
            ]
            break
    return {
        "work_id": work_id,
        "title": _primary_text("works", row),
        "authors": authors,
        "series": series,
        "tags": tag_rows,
        "record": dict(row),
    }


def _work_item_ids(
    runtime: "CoreRuntime",
    work: Mapping[str, Any],
) -> list[int]:
    item_ids: set[int] = set()
    for expression in _related_rows(runtime, work, "expressions"):
        manifestations = _related_rows(
            runtime,
            expression,
            "manifestations",
        )
        for manifestation in manifestations:
            manifestation_id = manifestation.get(
                _id_column(runtime, "manifestations")
            )
            if manifestation_id is None:
                continue
            for item in _search_rows(
                runtime,
                "items",
                "item_manifestation_id",
                manifestation_id,
            ):
                item_id = item.get(_id_column(runtime, "items"))
                if item_id is not None:
                    item_ids.add(int(item_id))
    for item in _related_rows(runtime, work, "items"):
        item_id = item.get(_id_column(runtime, "items"))
        if item_id is not None:
            item_ids.add(int(item_id))
    return sorted(item_ids)


def _legacy_file_record(row: Mapping[str, Any]) -> dict[str, Any]:
    name = str(
        row.get("file_name")
        or row.get("file_original_name")
        or row.get("file_storage_key")
        or "download.bin"
    )
    extension = str(
        row.get("file_extension")
        or row.get("file_original_extension")
        or Path(name).suffix.lstrip(".")
        or ""
    ).lower()
    return {
        "kind": "legacy-file",
        "id": row.get("file_id"),
        "item_id": row.get("file_item_id"),
        "name": name,
        "extension": extension,
        "mime_type": (
            row.get("file_mime_type")
            or mimetypes.guess_type(name)[0]
            or "application/octet-stream"
        ),
        "size": row.get("file_size_bytes") or row.get("file_size"),
        "store_id": row.get("file_store_id"),
        "storage_key": row.get("file_storage_key"),
    }


def _asset_records(
    runtime: "CoreRuntime",
    item_ids: Iterable[int],
) -> list[dict[str, Any]]:
    macros = getattr(runtime.database, "macros", None)
    if macros is None or not callable(getattr(macros, "get_rows", None)):
        return []
    results: list[dict[str, Any]] = []
    seen_replicas: set[int] = set()

    def add_asset(
        asset_id: Any,
        *,
        item_id: int,
        composite_id: Any = None,
        member_sequence: Any = None,
    ) -> None:
        if asset_id is None:
            return
        asset = macros.get_row(
            "digital_assets",
            asset_id,
            id_column="digital_asset_id",
        )
        if asset is None:
            return
        replicas = macros.get_rows(
            "asset_replicas",
            where={"asset_replica_digital_asset_id": asset_id},
            order_by=("asset_replica_id",),
        )
        for replica in replicas:
            replica_id = int(replica["asset_replica_id"])
            if replica_id in seen_replicas:
                continue
            seen_replicas.add(replica_id)
            name = str(
                replica.get("asset_replica_name")
                or asset.get("digital_asset_name")
                or replica.get("asset_replica_storage_key")
                or "asset.bin"
            )
            extension = str(
                replica.get("asset_replica_extension")
                or asset.get("digital_asset_extension")
                or Path(name).suffix.lstrip(".")
                or ""
            ).lower()
            results.append(
                {
                    "kind": "replica",
                    "id": replica_id,
                    "asset_id": asset_id,
                    "composite_id": composite_id,
                    "member_sequence": member_sequence,
                    "item_id": item_id,
                    "name": name,
                    "extension": extension,
                    "mime_type": (
                        asset.get("digital_asset_mime_type")
                        or mimetypes.guess_type(name)[0]
                        or "application/octet-stream"
                    ),
                    "size": (
                        replica.get("asset_replica_observed_size_bytes")
                        or asset.get("digital_asset_size_bytes")
                    ),
                    "store_id": replica.get("asset_replica_store_id"),
                    "storage_key": replica.get(
                        "asset_replica_storage_key"
                    ),
                    "mode": replica.get("asset_replica_mode"),
                }
            )

    for item_id in item_ids:
        links = macros.get_rows(
            "digital_asset_item_links",
            where={"digital_asset_item_link_item_id": item_id},
            order_by=("digital_asset_item_link_id",),
        )
        for link in links:
            add_asset(
                link.get("digital_asset_item_link_digital_asset_id"),
                item_id=item_id,
            )
        composite_links = macros.get_rows(
            "composite_digital_asset_item_links",
            where={
                "composite_digital_asset_item_link_item_id": item_id,
            },
            order_by=("composite_digital_asset_item_link_id",),
        )
        for composite_link in composite_links:
            composite_id = composite_link.get(
                "composite_digital_asset_item_link_composite_digital_asset_id"
            )
            if composite_id is None:
                continue
            members = macros.get_rows(
                "composite_digital_asset_digital_asset_links",
                where={
                    "composite_digital_asset_digital_asset_link_composite_digital_asset_id": composite_id,
                },
                order_by=(
                    "composite_digital_asset_digital_asset_link_id",
                ),
            )
            for member in members:
                add_asset(
                    member.get(
                        "composite_digital_asset_digital_asset_link_digital_asset_id"
                    ),
                    item_id=item_id,
                    composite_id=composite_id,
                    member_sequence=member.get(
                        "composite_digital_asset_digital_asset_link_sequence_number"
                    ),
                )
    return results


def _work_files(
    runtime: "CoreRuntime",
    work: Mapping[str, Any],
) -> list[dict[str, Any]]:
    item_ids = _work_item_ids(runtime, work)
    legacy: list[dict[str, Any]] = []
    seen: set[int] = set()
    for row in _related_rows(runtime, work, "files"):
        file_id = row.get("file_id")
        if file_id is not None:
            seen.add(int(file_id))
        legacy.append(_legacy_file_record(row))
    for item_id in item_ids:
        for row in _search_rows(runtime, "files", "file_item_id", item_id):
            file_id = row.get("file_id")
            if file_id is not None and int(file_id) in seen:
                continue
            if file_id is not None:
                seen.add(int(file_id))
            legacy.append(_legacy_file_record(row))
    return legacy + _asset_records(runtime, item_ids)


def _store_row(
    runtime: "CoreRuntime",
    store_id: Any,
) -> dict[str, Any] | None:
    if store_id is None:
        return None
    macros = getattr(runtime.database, "macros", None)
    if macros is not None and callable(getattr(macros, "get_row", None)):
        row = macros.get_row("stores", store_id, id_column="store_id")
        if row is not None:
            return dict(row)
    return _get_row(runtime, "stores", int(store_id))


def _resolution(
    runtime: "CoreRuntime",
    *,
    kind: str,
    row: Mapping[str, Any],
) -> dict[str, Any]:
    local_paths: tuple[Any, ...]
    remote_urls: tuple[Any, ...]
    if kind == "legacy-file":
        file_id = row.get("file_id")
        name = _legacy_file_record(row)["name"]
        store_id = row.get("file_store_id")
        storage_key = row.get("file_storage_key")
        local_paths = (row.get("file_original_path"), row.get("file_path"))
        remote_urls = (row.get("file_source"), row.get("file_original_path"))
    elif kind == "replica":
        file_id = row.get("asset_replica_id")
        name = (
            row.get("asset_replica_name")
            or row.get("asset_replica_storage_key")
            or "asset.bin"
        )
        store_id = row.get("asset_replica_store_id")
        storage_key = row.get("asset_replica_storage_key")
        local_paths = ()
        remote_urls = ()
    elif kind == "image":
        file_id = row.get("image_id")
        name = (
            row.get("image_name")
            or row.get("image_original_name")
            or row.get("image_storage_key")
            or "cover.bin"
        )
        store_id = row.get("image_store_id")
        storage_key = row.get("image_storage_key")
        local_paths = (row.get("image_original_path"), row.get("image_path"))
        remote_urls = (row.get("image_source"), row.get("image_original_path"))
    else:
        raise CoreDispatchError("Unknown acquisition kind `{}`.".format(kind))

    for value in remote_urls:
        url = str(value or "").strip()
        if url.startswith(("http://", "https://")):
            return {
                "kind": kind,
                "id": file_id,
                "name": str(name),
                "delivery": "redirect",
                "location": url,
                "readable": False,
            }
    for value in local_paths:
        path = Path(str(value or "").strip())
        if str(value or "").strip() and path.is_file():
            return {
                "kind": kind,
                "id": file_id,
                "name": str(name),
                "delivery": "core",
                "readable": True,
            }
    store = _store_row(runtime, store_id)
    if store is not None and storage_key:
        root = str(store.get("store_root_uri") or store.get("store_url") or "").strip()
        if root.startswith(("http://", "https://")):
            base = root if root.endswith("/") else root + "/"
            return {
                "kind": kind,
                "id": file_id,
                "name": str(name),
                "delivery": "redirect",
                "location": urljoin(base, str(storage_key)),
                "readable": False,
            }
        return {
            "kind": kind,
            "id": file_id,
            "name": str(name),
            "delivery": "core",
            "readable": True,
            "store_id": store_id,
            "storage_key": storage_key,
        }
    return {
        "kind": kind,
        "id": file_id,
        "name": str(name),
        "delivery": "unavailable",
        "readable": False,
    }


def _resource_row(
    runtime: "CoreRuntime",
    *,
    kind: str,
    resource_id: int,
) -> dict[str, Any] | None:
    if kind == "legacy-file":
        return _get_row(runtime, "files", resource_id)
    if kind == "image":
        return _get_row(runtime, "images", resource_id)
    if kind == "replica":
        macros = getattr(runtime.database, "macros", None)
        if macros is None or not callable(getattr(macros, "get_row", None)):
            return None
        value = macros.get_row(
            "asset_replicas",
            resource_id,
            id_column="asset_replica_id",
        )
        return None if value is None else dict(value)
    raise CoreDispatchError("Unknown acquisition kind `{}`.".format(kind))


def _resource_bytes(
    runtime: "CoreRuntime",
    *,
    kind: str,
    row: Mapping[str, Any],
) -> bytes:
    metadata = dict(row)
    try:
        location = runtime.services.library.retrieve_file(metadata=metadata)
        for method_name in ("read_bytes", "as_bytes"):
            method = getattr(location, method_name, None)
            if callable(method):
                value = method()
                if isinstance(value, bytes):
                    return value
                if isinstance(value, str):
                    return value.encode("utf-8")
                if isinstance(value, (bytearray, memoryview)):
                    return bytes(value)
                raise TypeError("Storage byte reader returned a non-byte value.")
    except Exception:
        pass

    local_paths: tuple[Any, ...]
    if kind == "legacy-file":
        local_paths = (row.get("file_original_path"), row.get("file_path"))
        store_id = row.get("file_store_id")
        storage_key = row.get("file_storage_key")
    elif kind == "image":
        local_paths = (row.get("image_original_path"), row.get("image_path"))
        store_id = row.get("image_store_id")
        storage_key = row.get("image_storage_key")
    else:
        local_paths = ()
        store_id = row.get("asset_replica_store_id")
        storage_key = row.get("asset_replica_storage_key")
    for value in local_paths:
        path_text = str(value or "").strip()
        if path_text and Path(path_text).is_file():
            return Path(path_text).read_bytes()
    store = _store_row(runtime, store_id)
    if store is not None and storage_key:
        root = str(store.get("store_root_uri") or store.get("store_url") or "").strip()
        if root.startswith("file://"):
            root = root[7:]
        if root and not root.startswith(("http://", "https://")):
            path = Path(root) / str(storage_key)
            if path.is_file():
                return path.read_bytes()
    raise CoreDispatchError(
        "The acquisition resource is not readable by this Core.",
        code="acquisition_unavailable",
    )


class CoreBrowseAPI:
    """Install browse and acquisition operations on a Core runtime."""

    def install(self, runtime: "CoreRuntime") -> None:
        query = runtime.register_query_handler
        query(
            "browse.categories",
            self.categories,
            summary="List display-neutral top-level library browse categories.",
            tags=("browse", "catalog", "read"),
        )
        query(
            "browse.category.items",
            self.category_items,
            summary="List entities within one browse category.",
            payload_fields=(
                _field("category", required=True, field_type="string"),
                _field("limit", field_type="integer"),
                _field("offset", field_type="integer"),
                _field("sort", field_type="string"),
                _field("ascending", field_type="boolean"),
            ),
            tags=("browse", "catalog", "read"),
        )
        query(
            "browse.works",
            self.works,
            summary="Page works by category entity, search text, and sort order.",
            payload_fields=(
                _field("category", field_type="string"),
                _field("category_id", field_type="integer"),
                _field("text", field_type="string"),
                _field("limit", field_type="integer"),
                _field("offset", field_type="integer"),
                _field("sort", field_type="string"),
                _field("ascending", field_type="boolean"),
            ),
            tags=("browse", "catalog", "read"),
        )
        query(
            "browse.work",
            self.work,
            summary="Return one work projection with related entities and formats.",
            payload_fields=(_field("work_id", required=True, field_type="integer"),),
            tags=("browse", "catalog", "acquisition", "read"),
        )
        query(
            "acquisition.formats",
            self.acquisition_formats,
            summary="List downloadable legacy files and managed replicas for one work.",
            payload_fields=(_field("work_id", required=True, field_type="integer"),),
            tags=("acquisition", "storage", "read"),
        )
        query(
            "acquisition.resolve",
            self.acquisition_resolve,
            summary="Resolve one acquisition resource to Core delivery or redirect.",
            payload_fields=(
                _field("kind", required=True, field_type="string"),
                _field("id", required=True, field_type="integer"),
            ),
            tags=("acquisition", "storage", "read"),
        )
        query(
            "acquisition.read",
            self.acquisition_read,
            summary="Read one Core-accessible acquisition resource.",
            payload_fields=(
                _field("kind", required=True, field_type="string"),
                _field("id", required=True, field_type="integer"),
            ),
            tags=("acquisition", "storage", "read"),
        )
        query(
            "acquisition.cover",
            self.acquisition_cover,
            summary="Return cover candidates for one work.",
            payload_fields=(_field("work_id", required=True, field_type="integer"),),
            tags=("acquisition", "images", "read"),
        )

    @staticmethod
    def categories(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del query
        available = _tables(runtime)
        works_count = len(_all_rows(runtime, "works")) if "works" in available else 0
        records: list[dict[str, Any]] = [
            {
                "category": "all",
                "label": "All works",
                "count": works_count,
                "entity_category": False,
            },
            {
                "category": "newest",
                "label": "Newest",
                "count": works_count,
                "entity_category": False,
            },
        ]
        for category, candidates in _CATEGORY_TABLES.items():
            selected = [table for table in candidates if table in available]
            count = sum(len(_all_rows(runtime, table)) for table in selected)
            records.append(
                {
                    "category": category,
                    "label": category.title(),
                    "count": count,
                    "entity_category": True,
                    "tables": selected,
                }
            )
        return {"categories": records}

    @staticmethod
    def category_items(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        category = str(payload.get("category") or "").strip().lower()
        if category not in {*_CATEGORY_TABLES, "all", "newest"}:
            raise CoreDispatchError("Unknown browse category `{}`.".format(category))
        limit = max(0, min(int(payload.get("limit", 100)), 10_000))
        offset = max(0, int(payload.get("offset", 0)))
        ascending = bool(payload.get("ascending", True))
        sort = str(payload.get("sort") or "name").strip().lower()
        if category in {"all", "newest"}:
            works = _all_rows(runtime, "works")
            records = [_work_summary(runtime, row) for row in works]
            if category == "newest" or sort == "recent":
                records.sort(
                    key=lambda row: int(row.get("work_id") or 0),
                    reverse=ascending,
                )
            else:
                records.sort(
                    key=lambda row: str(row["title"]).casefold(),
                    reverse=not ascending,
                )
        else:
            available = _tables(runtime)
            records = []
            for table in _CATEGORY_TABLES[category]:
                if table not in available:
                    continue
                for row in _all_rows(runtime, table):
                    row_id = row.get(_id_column(runtime, table))
                    works = _related_rows(runtime, row, "works")
                    records.append(
                        {
                            "category": category,
                            "table": table,
                            "id": row_id,
                            "label": _primary_text(table, row),
                            "work_count": len(works),
                            "record": row,
                        }
                    )
            if sort == "popularity":
                records.sort(
                    key=lambda row: (
                        int(row.get("work_count") or 0),
                        str(row.get("label") or "").casefold(),
                    ),
                    reverse=not ascending,
                )
            else:
                records.sort(
                    key=lambda row: str(row.get("label") or "").casefold(),
                    reverse=not ascending,
                )
        visible = records[offset : offset + limit]
        return {
            "category": category,
            "records": visible,
            "total_count": len(records),
            "limit": limit,
            "offset": offset,
            "complete": offset + len(visible) >= len(records),
        }

    @staticmethod
    def works(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        category = str(payload.get("category") or "all").strip().lower()
        text = str(payload.get("text") or "").strip().casefold()
        limit = max(0, min(int(payload.get("limit", 100)), 10_000))
        offset = max(0, int(payload.get("offset", 0)))
        ascending = bool(payload.get("ascending", category != "newest"))
        sort = str(payload.get("sort") or ("recent" if category == "newest" else "title")).strip().lower()
        if category in _CATEGORY_TABLES:
            category_id = _required_int(payload, "category_id")
            table = next(
                (
                    value
                    for value in _CATEGORY_TABLES[category]
                    if _get_row(runtime, value, category_id) is not None
                ),
                None,
            )
            category_row = (
                None
                if table is None
                else _get_row(runtime, table, category_id)
            )
            rows = (
                []
                if category_row is None
                else _related_rows(runtime, category_row, "works")
            )
        elif category in {"all", "newest"}:
            rows = _all_rows(runtime, "works")
        else:
            raise CoreDispatchError("Unknown browse category `{}`.".format(category))
        if text:
            rows = [row for row in rows if text in _flatten(row)]
        records = [_work_summary(runtime, row) for row in rows]
        if sort == "recent":
            records.sort(
                key=lambda row: int(row.get("work_id") or 0),
                reverse=not ascending,
            )
        else:
            records.sort(
                key=lambda row: str(row["title"]).casefold(),
                reverse=not ascending,
            )
        visible = records[offset : offset + limit]
        return {
            "records": visible,
            "total_count": len(records),
            "limit": limit,
            "offset": offset,
            "complete": offset + len(visible) >= len(records),
        }

    @staticmethod
    def work(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        work_id = _required_int(_payload(query), "work_id")
        row = _get_row(runtime, "works", work_id)
        if row is None:
            return {"work": None}
        related: dict[str, list[dict[str, Any]]] = {}
        for table in _interlinked_tables(runtime, "works"):
            values = _related_rows(runtime, row, table)
            if values:
                related[table] = values
        return {
            "work": _work_summary(runtime, row),
            "related": related,
            "item_ids": _work_item_ids(runtime, row),
            "formats": _work_files(runtime, row),
        }

    @staticmethod
    def acquisition_formats(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        work_id = _required_int(_payload(query), "work_id")
        row = _get_row(runtime, "works", work_id)
        if row is None:
            return {"work_id": work_id, "formats": []}
        records = _work_files(runtime, row)
        for record in records:
            resource = _resource_row(
                runtime,
                kind=str(record["kind"]),
                resource_id=int(record["id"]),
            )
            record["resolution"] = (
                None
                if resource is None
                else _resolution(
                    runtime,
                    kind=str(record["kind"]),
                    row=resource,
                )
            )
        return {"work_id": work_id, "formats": records}

    @staticmethod
    def acquisition_resolve(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        kind = str(payload.get("kind") or "").strip().lower()
        resource_id = _required_int(payload, "id")
        row = _resource_row(runtime, kind=kind, resource_id=resource_id)
        if row is None:
            raise CoreDispatchError(
                "Unknown {} {}.".format(kind, resource_id),
                code="acquisition_not_found",
            )
        return _resolution(runtime, kind=kind, row=row)

    @staticmethod
    def acquisition_read(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        kind = str(payload.get("kind") or "").strip().lower()
        resource_id = _required_int(payload, "id")
        row = _resource_row(runtime, kind=kind, resource_id=resource_id)
        if row is None:
            raise CoreDispatchError(
                "Unknown {} {}.".format(kind, resource_id),
                code="acquisition_not_found",
            )
        resolved = _resolution(runtime, kind=kind, row=row)
        if not resolved["readable"]:
            raise CoreDispatchError(
                "The acquisition resource must be followed as a redirect.",
                code="acquisition_redirect",
                details=resolved,
            )
        return {
            "resource": resolved,
            "content": _resource_bytes(runtime, kind=kind, row=row),
        }

    @staticmethod
    def acquisition_cover(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        work_id = _required_int(_payload(query), "work_id")
        work = _get_row(runtime, "works", work_id)
        if work is None:
            return {"work_id": work_id, "covers": []}
        covers = []
        for image in _related_rows(runtime, work, "images"):
            image_id = image.get("image_id")
            if image_id is None:
                continue
            covers.append(
                {
                    "kind": "image",
                    "id": image_id,
                    "name": _primary_text("images", image),
                    "mime_type": (
                        image.get("image_mime_type")
                        or mimetypes.guess_type(
                            str(image.get("image_name") or "")
                        )[0]
                        or "application/octet-stream"
                    ),
                    "resolution": _resolution(
                        runtime,
                        kind="image",
                        row=image,
                    ),
                }
            )
        return {"work_id": work_id, "covers": covers}


def install_browse_api(runtime: "CoreRuntime") -> CoreBrowseAPI:
    """Register display-neutral browse and acquisition operations."""

    api = CoreBrowseAPI()
    api.install(runtime)
    return api


__all__ = ["CoreBrowseAPI", "install_browse_api"]
