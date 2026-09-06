"""Read-only catalogue facade used by web and protocol surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

from LiuXin_alpha.surfaces.api import ReadModelHostApi
from LiuXin_alpha.surfaces.core import CoreRow, CoreSurfaceModel
from LiuXin_alpha.surfaces.images import ImageBackend
from LiuXin_alpha.surfaces.acquisition_types import ResolvedFileTarget as _ResolvedFileTarget
from LiuXin_alpha.surfaces.presentation import escape as _escape, row_value as _row_value


@dataclass
class ReadModelBackend:
    """Project Core queries without disguising failures as missing catalogue data.

    Missing tables/records and explicitly incomplete optimized queries have
    normal empty/fallback paths. Query, transport, and programming failures
    propagate to the caller; fallback is never inferred from an exception.
    """

    host: ReadModelHostApi
    images: Optional[ImageBackend] = None
    model: CoreSurfaceModel | None = None

    def __post_init__(self) -> None:
        if self.images is None:
            self.images = ImageBackend(self.host)
        if self.model is None:
            self.model = CoreSurfaceModel(self.host.core)

    @property
    def read_source(self) -> CoreSurfaceModel:
        """Compatibility name for the Core-backed surface model."""

        assert self.model is not None
        return self.model

    def refresh_read_source(self) -> bool:
        return self.read_source.refresh()

    def _table_exists(self, table: str) -> bool:
        return self.read_source.table_exists(str(table))

    def _all_rows(self, table: str) -> list[object]:
        if not self._table_exists(table):
            return []
        return list(self.read_source.rows(str(table)))

    def _get_row_from_id(self, table: str, row_id: int) -> object | None:
        if not self._table_exists(table):
            return None
        return self.read_source.row(str(table), int(row_id))

    def _search_rows(self, table: str, column: str, value: object) -> list[object]:
        if not self._table_exists(table):
            return []
        return list(self.read_source.search(str(table), str(column), value))

    def row_by_id(self, table: str, row_id: int) -> object | None:
        """Return None for an absent table or record, never for a failed lookup."""
        return self._get_row_from_id(table, row_id)

    def rows_for_table(self, table: str) -> list[object]:
        """Materialize a table, propagating failures before returning any rows."""
        return self._all_rows(table)

    def table_record_count(self, table: str) -> int:
        """Count rows; only a confirmed absent or empty table has a zero count.

        Unsupported queries and other failures propagate. Optional callers may
        present Core's explicit ``read_query_unavailable`` code as unavailable.
        """
        if not self._table_exists(table):
            return 0
        return self.read_source.record_count(str(table))

    def search_rows(self, table: str, column: str, value: object) -> list[object]:
        return self._search_rows(table, column, value)

    def _cache_sort_field(self, table: str) -> Optional[str]:
        preferred = getattr(self.host, "_preferred_summary_fields", None)
        candidates = (
            tuple(preferred(table))
            if callable(preferred)
            else ()
        )
        columns = set(self.read_source.columns(table))
        return next(
            (str(field) for field in candidates if str(field) in columns),
            None,
        )

    def _interlinked_rows(self, row, secondary_table: str) -> list[object]:
        core_row = self._as_core_row(row)
        if (
            core_row is None
            or core_row.row_id is None
            or not self._table_exists(secondary_table)
        ):
            return []
        records, _links = self.read_source.related(core_row, str(secondary_table))
        return list(records)

    def _as_core_row(self, row: object) -> CoreRow | None:
        if isinstance(row, CoreRow):
            return row
        table = str(getattr(row, "table", "") or "")
        raw_id = getattr(row, "row_id", None)
        if table and raw_id not in (None, ""):
            return self.read_source.row(table, int(raw_id))
        if isinstance(row, Mapping):
            for candidate in self.read_source.table_names():
                id_column = self.read_source.id_column(candidate)
                if id_column in row and row[id_column] not in (None, ""):
                    return self.read_source.row(candidate, int(row[id_column]))
        return None

    def interlinked_rows(self, row, secondary_table: str) -> list[object]:
        """Return related rows; unsaved rows have none and failed reads propagate."""
        return self._interlinked_rows(row, secondary_table)

    def related_rows_by_table(self, row) -> dict[str, list[object]]:
        return self._related_rows_by_table(row)

    def _related_rows_by_table(self, row) -> dict[str, list[object]]:
        ordered_getter = getattr(self.host, "_ordered_related_tables", None)
        if not callable(ordered_getter):
            return self.host._related_rows_by_table(row)
        related: dict[str, list[object]] = {}
        for linked_table in ordered_getter(row):
            linked_rows = self._interlinked_rows(row, str(linked_table))
            if linked_rows:
                related[str(linked_table)] = linked_rows
        return related

    def _work_credit_entries(self, row) -> list[dict[str, object]]:
        entries: list[dict[str, object]] = []
        core_row = self._as_core_row(row)
        if core_row is None or core_row.table != "works" or core_row.row_id is None:
            return entries

        pretty_role = getattr(self.host, "_pretty_credit_role", None)
        result = self.host.core.query(
            "catalog.agents.list",
            {"level": "work", "entity_id": core_row.row_id},
        )
        raw_agents = (
            result.get("agents", ())
            if isinstance(result, Mapping)
            else ()
        )
        for position, raw_agent in enumerate(raw_agents):
            if not isinstance(raw_agent, Mapping):
                continue
            values = dict(raw_agent)
            raw_link = values.pop("_catalog_link", {})
            link = dict(raw_link) if isinstance(raw_link, Mapping) else {}
            linked_table = next(
                (
                    table
                    for table in ("agents", "human_agents", "org_agents")
                    if self.read_source.table_exists(table)
                    and self.read_source.id_column(table) in values
                ),
                "agents",
            )
            if not self.read_source.table_exists(linked_table):
                continue
            row_id = values.get(self.read_source.id_column(linked_table))
            linked_row = CoreRow(
                table=linked_table,
                row_id=None if row_id is None else int(row_id),
                values=values,
                linkable_tables=self.read_source.related_tables(linked_table),
            )
            role_raw = link.get("type")
            priority_value = link.get("priority")
            try:
                priority_sort = -int(priority_value)
            except (TypeError, ValueError, OverflowError):
                priority_sort = position
            role = (
                pretty_role(role_raw)
                if callable(pretty_role)
                else str(role_raw or "Contributors")
            )
            entries.append(
                {
                    "table": linked_table,
                    "row": linked_row,
                    "role": role,
                    "role_raw": role_raw,
                    "priority": priority_value,
                    "sort_key": (str(role), priority_sort, position),
                }
            )

        return sorted(entries, key=lambda item: item["sort_key"])

    def work_credit_entries(self, row) -> list[dict[str, object]]:
        return self._work_credit_entries(row)

    @staticmethod
    def category_display_name(category: str) -> str:
        mapping = {
            "allbooks": "All books",
            "newest": "Newest",
            "authors": "Authors",
            "tags": "Tags",
            "series": "Series",
            "titles": "Titles",
            "recent": "Recent",
        }
        return mapping.get(str(category or "").strip().lower(), str(category or "").replace("_", " ").title())

    def author_tables(self) -> list[str]:
        tables = []
        for table in ("agents", "human_agents", "org_agents"):
            if self._table_exists(table):
                tables.append(table)
        return tables or ["agents"]

    def tag_category_table(self) -> Optional[str]:
        available = set(self.read_source.table_names())
        if "tags" in available:
            if self.table_record_count("tags") > 0 or "labels" not in available:
                return "tags"
        if "labels" in available:
            return "labels"
        return "tags" if "tags" in available else None

    def work_tag_rows(self, related_rows_by_table: dict[str, list[object]]) -> tuple[Optional[str], list[object]]:
        tag_table = self.tag_category_table()
        if tag_table is None:
            return None, []
        return tag_table, list(related_rows_by_table.get(tag_table, []))

    def work_rows(self, *, sorted_by: str) -> list[object]:
        if not self._table_exists("works"):
            return []
        id_column = self.host._id_column("works") or "work_id"
        sort_field = (
            id_column
            if sorted_by == "recent"
            else self._cache_sort_field("works")
        )
        if sort_field is not None:
            result = self.read_source.query_rows(
                "works",
                sort=({"field": sort_field, "ascending": sorted_by != "recent"},),
            )
            if result.complete:
                return list(result.records)
        rows = self._all_rows("works")
        if sorted_by == "recent":
            return sorted(rows, key=lambda row: int(_row_value(row, id_column) or 0), reverse=True)
        return sorted(rows, key=lambda row: self.host._row_primary_text("works", row).lower())

    def work_page(
        self,
        *,
        sorted_by: str,
        limit: int,
        offset: int,
    ) -> tuple[list[object], int]:
        """Return a page and total, preferring a complete ordered Core query.

        An explicitly incomplete result or missing sort field selects the
        materialized fallback. Query failures propagate instead of retrying.
        """

        if not self._table_exists("works"):
            return [], 0
        id_column = self.host._id_column("works") or "work_id"
        sort_field = (
            id_column
            if sorted_by == "recent"
            else self._cache_sort_field("works")
        )
        if sort_field is not None:
            result = self.read_source.query_rows(
                "works",
                sort=({"field": sort_field, "ascending": sorted_by != "recent"},),
                offset=max(0, int(offset)),
                limit=max(0, int(limit)),
            )
            if result.complete:
                return list(result.records), int(result.total_count)
        rows = self.work_rows(sorted_by=sorted_by)
        return rows[offset : offset + limit], len(rows)

    def works_for_linked_entity(self, table: str, raw_row_id: str) -> list[object]:
        if not self._table_exists(table):
            return []
        try:
            row_id = int(str(raw_row_id).strip())
        except (TypeError, ValueError, OverflowError):
            return []
        row = self._get_row_from_id(table, row_id)
        if row is None:
            return []
        return list(self._related_rows_by_table(row).get("works", []))

    def category_rows(self, kind: str) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        if kind in {"allbooks", "titles", "recent", "newest"}:
            sorted_by = "recent" if kind in {"recent", "newest"} else "title"
            for row in self.work_rows(sorted_by=sorted_by):
                row_id = _row_value(row, self.host._id_column("works") or "work_id")
                rows.append(
                    {
                        "table": "works",
                        "row": row,
                        "id": row_id,
                        "label": self.host._row_primary_text("works", row),
                        "count": 0,
                        "url": self.host._row_href("works", row) or "",
                    }
                )
            return rows
        if kind == "authors":
            for table in self.author_tables():
                for row in sorted(
                    self._all_rows(table),
                    key=lambda one: self.host._row_primary_text(table, one).lower(),
                ):
                    row_id = _row_value(row, self.host._id_column(table) or "")
                    works = self.works_for_linked_entity(table, str(row_id))
                    rows.append(
                        {
                            "table": table,
                            "row": row,
                            "id": row_id,
                            "label": self.host._row_primary_text(table, row),
                            "count": len(works),
                            "url": self.host._row_href(table, row) or "",
                        }
                    )
            return rows
        if kind == "tags":
            tag_table = self.tag_category_table()
            if tag_table is None:
                return rows
            for row in sorted(
                self._all_rows(tag_table),
                key=lambda one: self.host._row_primary_text(tag_table, one).lower(),
            ):
                row_id = _row_value(row, self.host._id_column(tag_table) or "")
                works = self.works_for_linked_entity(tag_table, str(row_id))
                rows.append(
                    {
                        "table": tag_table,
                        "row": row,
                        "id": row_id,
                        "label": self.host._row_primary_text(tag_table, row),
                        "count": len(works),
                        "url": self.host._row_href(tag_table, row) or "",
                    }
                )
            return rows
        if kind == "series" and self._table_exists("series"):
            for row in sorted(
                self._all_rows("series"),
                key=lambda one: self.host._row_primary_text("series", one).lower(),
            ):
                row_id = _row_value(row, self.host._id_column("series") or "")
                works = self.works_for_linked_entity("series", str(row_id))
                rows.append(
                    {
                        "table": "series",
                        "row": row,
                        "id": row_id,
                        "label": self.host._row_primary_text("series", row),
                        "count": len(works),
                        "url": self.host._row_href("series", row) or "",
                    }
                )
            return rows
        return rows

    def browse_count(self, kind: str) -> int:
        if kind in {"authors", "tags", "series"}:
            return len(self.category_rows(kind))
        if kind in {"titles", "recent", "allbooks", "newest"}:
            sorted_by = "recent" if kind == "recent" else "title"
            return len(self.work_rows(sorted_by=sorted_by))
        return 0

    def category_summary_payload(self) -> list[dict[str, object]]:
        return [
            {
                "name": self.category_display_name(category),
                "count": self.browse_count(count_kind),
                "is_category": is_category,
                "category": category,
            }
            for category, count_kind, is_category in (
                ("allbooks", "titles", False),
                ("newest", "recent", False),
                ("authors", "authors", True),
                ("tags", "tags", True),
                ("series", "series", True),
            )
        ]

    def category_items_payload(self, category: str, *, num: int, offset: int, sort: str, sort_order: str) -> dict[str, object]:
        kind = str(category or "").strip().lower()
        raw_rows = self.category_rows(kind)
        sort_key = str(sort or "name").strip().lower()
        ascending = str(sort_order or "asc").strip().lower() != "desc"
        if sort_key == "popularity":
            key_fn = lambda item: (int(item.get("count") or 0), str(item.get("label") or "").lower())
        elif sort_key == "rating":
            key_fn = lambda item: (0, str(item.get("label") or "").lower())
        else:
            key_fn = lambda item: str(item.get("label") or "").lower()
        sorted_rows = sorted(raw_rows, key=key_fn, reverse=not ascending)
        visible = [dict(item) for item in sorted_rows[offset : offset + num]]
        return {
            "category": kind,
            "category_name": self.category_display_name(kind),
            "total_num": len(sorted_rows),
            "offset": offset,
            "num": len(visible),
            "sort": sort_key,
            "sort_order": "asc" if ascending else "desc",
            "items": visible,
        }

    def entity_summary_payload(self, table: str, row) -> dict[str, object]:
        row_id = _row_value(row, self.host._id_column(table) or "")
        return {
            "id": row_id,
            "table": table,
            "primary": self.host._row_primary_text(table, row),
            "label": self.host._row_label(table, row),
            "html_url": self.host._row_href(table, row),
        }

    def related_payload(self, row) -> dict[str, list[dict[str, object]]]:
        payload: dict[str, list[dict[str, object]]] = {}
        for table, rows in self._related_rows_by_table(row).items():
            payload[str(table)] = [self.entity_summary_payload(str(table), linked_row) for linked_row in rows]
        return payload

    def work_subtitle(self, row) -> str:
        parts: list[str] = []
        credit_entries = self._work_credit_entries(row)
        if credit_entries:
            names = [self.host._row_primary_text(str(entry["table"]), entry["row"]) for entry in credit_entries[:3]]
            if names:
                parts.append("by {}".format(", ".join(names)))
        related = self._related_rows_by_table(row)
        series_rows = related.get("series", [])
        if series_rows:
            parts.append("Series: {}".format(", ".join(self.host._row_primary_text("series", one) for one in series_rows[:2])))
        tag_table, tag_rows = self.work_tag_rows(related)
        if tag_table is not None and tag_rows:
            parts.append("Tags: {}".format(", ".join(self.host._row_primary_text(tag_table, one) for one in tag_rows[:3])))
        return " · ".join(parts)

    def work_sort_value(self, row, *, sort_key: str) -> object:
        lowered = str(sort_key or "date").strip().lower()
        if lowered == "title":
            return self.host._row_primary_text("works", row).lower()
        if lowered == "author":
            credit_entries = self._work_credit_entries(row)
            names = [self.host._row_primary_text(str(entry["table"]), entry["row"]) for entry in credit_entries[:4]]
            return " | ".join(names).lower()
        if lowered == "series":
            related = self._related_rows_by_table(row)
            names = [self.host._row_primary_text("series", one) for one in related.get("series", [])[:3]]
            return " | ".join(names).lower()
        if lowered == "tags":
            related = self._related_rows_by_table(row)
            tag_table, tag_rows = self.work_tag_rows(related)
            names = [self.host._row_primary_text(tag_table, one) for one in tag_rows[:5]] if tag_table is not None else []
            return " | ".join(names).lower()
        id_column = self.host._id_column("works") or "work_id"
        return int(_row_value(row, id_column) or 0)

    def work_file_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        file_rows_by_id: dict[int, object] = {}

        def add_file_row(file_row) -> None:
            file_id = _row_value(file_row, "file_id")
            if file_id in (None, ""):
                return
            try:
                file_rows_by_id[int(file_id)] = file_row
            except (TypeError, ValueError, OverflowError):
                return

        for file_row in related_rows_by_table.get("files", []):
            add_file_row(file_row)

        for expression_row in related_rows_by_table.get("expressions", []):
            manifestation_rows = self._interlinked_rows(expression_row, "manifestations")
            for manifestation_row in manifestation_rows:
                manifestation_id = _row_value(manifestation_row, "manifestation_id")
                if manifestation_id in (None, ""):
                    continue
                item_rows = self._search_rows("items", "item_manifestation_id", manifestation_id)
                for item_row in item_rows:
                    item_id = _row_value(item_row, "item_id")
                    if item_id in (None, ""):
                        continue
                    discovered_file_rows = self._search_rows("files", "file_item_id", item_id)
                    for file_row in discovered_file_rows:
                        add_file_row(file_row)
        return list(file_rows_by_id.values())

    def file_summary_payload(self, file_row) -> dict[str, object]:
        file_id = _row_value(file_row, "file_id")
        capabilities = self.host._file_capabilities(file_row)
        name = self.host._download_name_for_file_row(file_row)
        payload = {
            "id": file_id,
            "name": name,
            "store_id": _row_value(file_row, "file_store_id"),
            "media_category": _row_value(file_row, "file_media_category"),
            "role": _row_value(file_row, "file_role"),
            "source": _row_value(file_row, "file_source"),
            "downloadable": bool(capabilities.get("downloadable")),
            "preview_kind": capabilities.get("preview_kind") or "",
            "delivery": capabilities.get("delivery") or "",
            "download_url": "/files/{}/download".format(file_id) if capabilities.get("downloadable") else "",
            "preview_url": "/files/{}/preview".format(file_id) if capabilities.get("preview_kind") else "",
        }
        size_value = _row_value(file_row, "file_size_bytes") or _row_value(file_row, "file_size")
        try:
            payload["size"] = int(size_value) if size_value not in (None, "") else None
        except (TypeError, ValueError, OverflowError):
            payload["size"] = None
        return payload

    def file_detail_payload(self, file_row) -> dict[str, object]:
        payload = self.file_summary_payload(file_row)
        payload["related"] = self.related_payload(file_row)
        payload["file"] = {
            "item_id": _row_value(file_row, "file_item_id"),
            "storage_key": _row_value(file_row, "file_storage_key"),
            "extension": _row_value(file_row, "file_extension") or _row_value(file_row, "file_original_extension"),
            "mime_type": _row_value(file_row, "file_mime_type"),
            "store_id": _row_value(file_row, "file_store_id"),
        }
        return payload

    def work_metadata_payload(
        self,
        row,
        *,
        related_rows_by_table: Optional[dict[str, list[object]]] = None,
    ) -> dict[str, object]:
        row_id = _row_value(row, self.host._id_column("works") or "work_id")
        related = related_rows_by_table if related_rows_by_table is not None else self._related_rows_by_table(row)
        format_rows = self.work_file_rows(related)
        formats = []
        format_metadata: dict[str, dict[str, object]] = {}
        for file_row in sorted(
            format_rows,
            key=lambda one: self.host._download_name_for_file_row(one).lower(),
        ):
            file_id = _row_value(file_row, "file_id")
            if file_id in (None, ""):
                continue
            name = self.host._download_name_for_file_row(file_row)
            fmt = Path(name).suffix.lower().lstrip(".") or "file"
            download_url = "/files/{}/download".format(file_id)
            preview_url = (
                "/files/{}/preview".format(file_id)
                if self.host._file_capabilities(file_row).get("preview_kind")
                else ""
            )
            formats.append(
                {
                    "format": fmt.upper(),
                    "name": name,
                    "download_url": download_url,
                    "preview_url": preview_url,
                }
            )
            size_value = _row_value(file_row, "file_size_bytes") or _row_value(file_row, "file_size")
            try:
                size_int = int(size_value) if size_value not in (None, "") else None
            except (TypeError, ValueError, OverflowError):
                size_int = None
            format_metadata[fmt.upper()] = {
                "path": download_url,
                "name": name,
                "size": size_int,
                "preview": preview_url,
            }
        work_id_value = int(row_id) if row_id not in (None, "") else row_id
        title = self.host._row_primary_text("works", row)
        authors = [
            self.host._row_primary_text(str(entry["table"]), entry["row"])
            for entry in self._work_credit_entries(row)
        ]
        tag_table, tag_rows = self.work_tag_rows(related)
        tags = [self.host._row_primary_text(tag_table, one) for one in tag_rows] if tag_table is not None else []
        series_values = [self.host._row_primary_text("series", one) for one in related.get("series", [])]
        return {
            "id": work_id_value,
            "title": title,
            "sort": self.host._stringify_detail_value(_row_value(row, "work_sort_title") or title),
            "authors": authors,
            "author_sort": " & ".join(authors),
            "series": series_values[0] if series_values else "",
            "series_index": None,
            "tags": tags,
            "comments": self.work_subtitle(row),
            "formats": [item["format"] for item in formats],
            "format_metadata": format_metadata,
            "thumbnail": "/get/thumb/{}/main?sz=60x80".format(_escape(row_id)),
            "cover": "/get/cover/{}/main".format(_escape(row_id)),
            "main_format": formats[0]["format"] if formats else "",
            "rating": None,
            "pubdate": None,
            "timestamp": None,
            "last_modified": None,
            "uuid": "work-{}".format(work_id_value),
            "url": "/book/{}".format(_escape(row_id)),
            "formats_detail": formats,
            "summary": self.work_subtitle(row),
        }

    def work_detail_payload(self, row) -> dict[str, object]:
        metadata = self.work_metadata_payload(row)
        related_rows_by_table = self._related_rows_by_table(row)
        credits = []
        for entry in self._work_credit_entries(row):
            table = str(entry["table"])
            linked_row = entry["row"]
            linked_row_data = self.host._row_dict(table, linked_row)
            entity_type = linked_row_data.get("agent_type") or linked_row_data.get("human_agent_type") or linked_row_data.get("org_agent_type")
            credits.append(
                {
                    "role": str(entry["role"]),
                    "priority": entry.get("priority"),
                    "entity_type": str(entity_type or "").strip(),
                    "entity": self.entity_summary_payload(table, linked_row),
                }
            )
        files = [self.file_summary_payload(file_row) for file_row in self.work_file_rows(related_rows_by_table)]
        return {
            "work": metadata,
            "credits": credits,
            "files": files,
            "related": self.related_payload(row),
        }

    def sorted_work_rows(self, rows: list[object], *, sort: str, sort_order: str) -> list[object]:
        sort_key = str(sort or "title").strip().lower()
        sort_order_text = str(sort_order or "asc").strip().lower()
        ascending = sort_order_text != "desc"
        return sorted(rows, key=lambda row: self.work_sort_value(row, sort_key=sort_key), reverse=not ascending)

    def work_list_payload(self, rows: list[object], *, num: int, offset: int, sort: str, sort_order: str) -> dict[str, object]:
        sort_key = str(sort or "title").strip().lower()
        sort_order_text = str(sort_order or "asc").strip().lower()
        sorted_rows = self.sorted_work_rows(rows, sort=sort_key, sort_order=sort_order_text)
        visible_rows = sorted_rows[offset : offset + num]
        ids = [
            int(_row_value(row, self.host._id_column("works") or "work_id"))
            for row in visible_rows
            if _row_value(row, self.host._id_column("works") or "work_id") not in (None, "")
        ]
        return {
            "total_num": len(sorted_rows),
            "sort_order": sort_order_text,
            "offset": offset,
            "num": len(ids),
            "sort": sort_key,
            "book_ids": ids,
            "rows": visible_rows,
        }

    def books_metadata_payload(self, rows: list[object]) -> dict[str, dict[str, object]]:
        payload: dict[str, dict[str, object]] = {}
        for row in rows:
            metadata = self.work_metadata_payload(row)
            payload[str(metadata["id"])] = metadata
        return payload

    def search_entries(self, query_text: str, *, table_filter: str = "") -> list[dict[str, object]]:
        needle = str(query_text or "").strip()
        if not needle:
            return []

        if table_filter and self._table_exists(table_filter):
            tables = [table_filter]
        else:
            public_tables = getattr(self.host, "_public_search_tables", None)
            tables = public_tables() if callable(public_tables) else ["works"]

        entry_builder = getattr(self.host, "_global_search_entry", None)
        if not callable(entry_builder):
            return []

        results: list[dict[str, object]] = []
        for table in tables:
            search_columns_getter = getattr(
                self.host,
                "_search_candidate_columns",
                None,
            )
            text_fields = (
                tuple(str(value) for value in search_columns_getter(str(table)))
                if callable(search_columns_getter)
                else ()
            )
            queried = self.read_source.query_rows(
                str(table),
                text=needle,
                text_fields=text_fields,
            )
            candidate_rows = (
                list(queried.records)
                if queried.complete
                else self._all_rows(str(table))
            )
            for row in candidate_rows:
                entry = entry_builder(str(table), row, needle)
                if entry is not None:
                    results.append(entry)
        return sorted(results, key=lambda item: item["sort_key"])

    def search_results_payload(self, *, query_text: str, table_filter: str, limit: int, offset: int) -> dict[str, object]:
        entries = self.search_entries(query_text, table_filter=table_filter)
        visible = entries[offset : offset + limit]
        group_counts: dict[str, int] = {}
        for entry in entries:
            table = str(entry["table"])
            group_counts[table] = group_counts.get(table, 0) + 1
        results = []
        for entry in visible:
            table = str(entry["table"])
            row = entry["row"]
            row_id = _row_value(row, self.host._id_column(table) or "")
            results.append(
                {
                    "table": table,
                    "id": row_id,
                    "primary": self.host._row_primary_text(table, row),
                    "label": self.host._row_label(table, row),
                    "snippet": str(entry.get("snippet") or ""),
                    "match_column": str(entry.get("match_column") or ""),
                    "score": int(entry.get("score") or 0),
                    "html_url": self.host._row_href(table, row),
                }
            )
        return {
            "query": query_text,
            "table_filter": table_filter,
            "results": results,
            "group_counts": group_counts,
            "total": len(entries),
            "limit": limit,
            "offset": offset,
        }

    def work_image_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        return self.images.work_image_rows(related_rows_by_table)

    def image_download_name(self, image_row) -> str:
        return self.images.image_download_name(image_row)

    def image_content_type(self, image_row) -> str:
        return self.images.image_content_type(image_row)

    def image_storage_lookup_metadata(self, image_row) -> dict[str, object]:
        return self.images.image_storage_lookup_metadata(image_row)

    def resolve_storage_image(self, image_row):
        return self.images.resolve_storage_image(image_row)

    def resolve_image_target(self, image_row) -> Optional[_ResolvedFileTarget]:
        return self.images.resolve_image_target(image_row)

    def work_image_row(self, work_row) -> Optional[object]:
        return self.images.work_image_row(work_row)

    @staticmethod
    def thumbnail_text(text: str) -> str:
        return ImageBackend.thumbnail_text(text)

    def placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        return self.images.placeholder_cover_svg(work_row, width=width, height=height)
