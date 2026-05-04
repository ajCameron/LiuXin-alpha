"""Write item-centred LiuXin/WEMI metadata changes back to a database."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError
from LiuXin_alpha.metadata.standardization import make_tag_search_term


@dataclass(frozen=True)
class LegacyRelationFieldSpec:
    field: str
    relation: str
    table: str
    text_columns: tuple[str, ...]
    id_column: str
    norm_column: str | None = None
    norm_function: Any = None


@dataclass
class LiuXinWEMIMetadataWriteReport:
    """Summary of a metadata write-back attempt."""

    item_id: int | None
    target_level: str
    target_table: str | None = None
    target_id: int | None = None
    fields_checked: list[str] = field(default_factory=list)
    rows_added: list[dict[str, Any]] = field(default_factory=list)
    links_added: list[dict[str, Any]] = field(default_factory=list)
    links_removed: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return bool(self.rows_added or self.links_added or self.links_removed)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "target_level": self.target_level,
            "target_table": self.target_table,
            "target_id": self.target_id,
            "fields_checked": list(self.fields_checked),
            "rows_added": list(self.rows_added),
            "links_added": list(self.links_added),
            "links_removed": list(self.links_removed),
            "skipped": list(self.skipped),
            "changed": self.changed,
        }


class LiuXinWEMIMetadataWriter:
    """
    Apply supported metadata-container changes back to the database.

    This first writer slice handles legacy relation-backed fields by comparing
    the metadata object's desired value-to-id mappings with the current database
    links for one WEMI target row. It deliberately leaves core identity fields
    and file/storage rows alone.
    """

    _LEVEL_TABLES = {
        "work": "works",
        "expression": "expressions",
        "manifestation": "manifestations",
        "item": "items",
    }
    _LEVEL_ID_NAMES = {
        "work": "work_id",
        "expression": "expression_id",
        "manifestation": "manifestation_id",
        "item": "item_id",
    }
    _LEVEL_FALLBACKS = ("work", "expression", "manifestation", "item")
    _FIELD_SPECS = {
        "tags": LegacyRelationFieldSpec(
            field="tags",
            relation="tags",
            table="tags",
            text_columns=("tag",),
            id_column="tag_id",
            norm_column="tag_phash",
            norm_function=make_tag_search_term,
        ),
        "labels": LegacyRelationFieldSpec(
            field="labels",
            relation="labels",
            table="labels",
            text_columns=("label_text", "label"),
            id_column="label_id",
            norm_column="label_text_norm",
            norm_function=make_tag_search_term,
        ),
        "genre": LegacyRelationFieldSpec(
            field="genre",
            relation="genres",
            table="genres",
            text_columns=("genre_full", "genre"),
            id_column="genre_id",
        ),
        "subject": LegacyRelationFieldSpec(
            field="subject",
            relation="subjects",
            table="subjects",
            text_columns=("subject_full", "subject"),
            id_column="subject_id",
        ),
        "series": LegacyRelationFieldSpec(
            field="series",
            relation="series",
            table="series",
            text_columns=("series_full", "series"),
            id_column="series_id",
        ),
        "notes": LegacyRelationFieldSpec(
            field="notes",
            relation="notes",
            table="notes",
            text_columns=("note",),
            id_column="note_id",
        ),
        "comments": LegacyRelationFieldSpec(
            field="comments",
            relation="comments",
            table="comments",
            text_columns=("comment",),
            id_column="comment_id",
        ),
        "synopses": LegacyRelationFieldSpec(
            field="synopses",
            relation="synopses",
            table="synopses",
            text_columns=("synopsis",),
            id_column="synopsis_id",
        ),
    }
    _FIELD_ALIASES = {
        "tag": "tags",
        "tags": "tags",
        "label": "labels",
        "labels": "labels",
        "genre": "genre",
        "genres": "genre",
        "subject": "subject",
        "subjects": "subject",
        "series": "series",
        "note": "notes",
        "notes": "notes",
        "comment": "comments",
        "comments": "comments",
        "synopsis": "synopses",
        "synopses": "synopses",
    }

    def __init__(self, database: Any) -> None:
        if database is None:
            raise ValueError("LiuXinWEMIMetadataWriter requires a database instance.")
        self.db = database
        try:
            self._tables = set(self.db.get_tables(force_refresh=False))
        except Exception:
            self._tables = set()
        try:
            self._tables_and_columns = dict(self.db.get_tables_and_columns())
        except Exception:
            self._tables_and_columns = {}

    def write(
        self,
        metadata: Any,
        *,
        fields: Iterable[str] | None = None,
        target_level: str = "work",
        replace: bool = False,
        mark_dirty: bool = True,
    ) -> LiuXinWEMIMetadataWriteReport:
        target_level_key = self._normalize_level(target_level)
        resolved_target = self._resolve_target_row(metadata, target_level_key)
        actual_level = resolved_target[0] if resolved_target is not None else target_level_key
        source_row = resolved_target[1] if resolved_target is not None else None
        report = LiuXinWEMIMetadataWriteReport(
            item_id=self._metadata_database_id(metadata, "item_id"),
            target_level=actual_level,
            target_table=source_row.table if source_row is not None else None,
            target_id=int(source_row.row_id) if source_row is not None and source_row.row_id is not None else None,
        )
        if source_row is None:
            report.skipped.append(f"Could not resolve a database row for {target_level_key!r}.")
            return report

        for field_name in self._normalize_fields(fields):
            report.fields_checked.append(field_name)
            spec = self._FIELD_SPECS[field_name]
            if not self._has_table(spec.table):
                report.skipped.append(f"{field_name}: table {spec.table!r} is not present.")
                continue
            if not self._relation_supported(source_row.table, spec.table):
                report.skipped.append(
                    f"{field_name}: {source_row.table!r} cannot link to {spec.table!r}."
                )
                continue

            desired = self._desired_terms(metadata, field_name)
            existing = self._existing_terms(source_row, spec)
            for text, row_id in desired.items():
                key = self._term_key(text)
                if not key or key in existing:
                    continue
                target_row, created = self._ensure_relation_row(spec, text, row_id)
                if target_row is None:
                    report.skipped.append(f"{field_name}: could not create/find row for {text!r}.")
                    continue
                if self._link_rows(source_row, target_row):
                    report.links_added.append(
                        {
                            "field": field_name,
                            "source": self._row_ref(source_row),
                            "target": self._row_ref(target_row),
                        }
                    )
                if created:
                    report.rows_added.append(
                        {
                            "field": field_name,
                            "table": spec.table,
                            "text": text,
                            "row_id": int(target_row.row_id) if target_row.row_id is not None else None,
                        }
                    )
                existing[key] = target_row

            if replace:
                desired_keys = {self._term_key(text) for text in desired}
                for key, target_row in list(existing.items()):
                    if key in desired_keys:
                        continue
                    if self._unlink_rows(source_row, target_row):
                        report.links_removed.append(
                            {
                                "field": field_name,
                                "source": self._row_ref(source_row),
                                "target": self._row_ref(target_row),
                            }
                        )

        if mark_dirty and report.changed:
            self._mark_dirty(source_row, reason="metadata_write_back")
        return report

    def _normalize_level(self, level: str) -> str:
        level_key = str(level).strip().lower()
        aliases = {"w": "work", "e": "expression", "m": "manifestation", "i": "item"}
        level_key = aliases.get(level_key, level_key)
        if level_key not in self._LEVEL_TABLES:
            raise KeyError(f"Unknown WEMI level {level!r}.")
        return level_key

    def _normalize_fields(self, fields: Iterable[str] | None) -> tuple[str, ...]:
        if fields is None:
            return tuple(self._FIELD_SPECS)
        out: list[str] = []
        for field in fields:
            key = self._FIELD_ALIASES.get(str(field).strip().lower())
            if key is None:
                raise KeyError(f"Unsupported metadata write-back field {field!r}.")
            if key not in out:
                out.append(key)
        return tuple(out)

    def _resolve_target_row(self, metadata: Any, preferred_level: str) -> tuple[str, Row] | None:
        levels = (preferred_level,) + tuple(
            level for level in self._LEVEL_FALLBACKS if level != preferred_level
        )
        for level in levels:
            table = self._LEVEL_TABLES[level]
            row_id = self._metadata_database_id(metadata, self._LEVEL_ID_NAMES[level])
            if row_id is None:
                continue
            row = self._get_row(table, row_id)
            if row is not None:
                return level, row
        return None

    @staticmethod
    def _metadata_database_id(metadata: Any, name: str) -> int | None:
        getter = getattr(metadata, "get_database_id", None)
        value = getter(name) if callable(getter) else None
        if value in (None, ""):
            database_ids = getattr(metadata, "database_ids", {})
            if isinstance(database_ids, Mapping):
                value = database_ids.get(name)
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError, OverflowError):
            return None

    def _desired_terms(self, metadata: Any, field_name: str) -> OrderedDict[str, Any]:
        getter = getattr(metadata, "direct_get", None)
        value = getter(field_name) if callable(getter) else getattr(metadata, field_name)
        if value is None:
            return OrderedDict()
        if isinstance(value, Mapping):
            items = value.items()
        elif isinstance(value, (str, bytes)):
            items = [(value, None)]
        else:
            items = [(item, None) for item in value]

        out: OrderedDict[str, Any] = OrderedDict()
        for raw_text, row_id in items:
            text = str(raw_text or "").strip()
            if text:
                out[text] = row_id
        return out

    def _existing_terms(
        self,
        source_row: Row,
        spec: LegacyRelationFieldSpec,
    ) -> dict[str, Row]:
        out: dict[str, Row] = {}
        try:
            link_rows = list(
                self.db.get_interlink_rows(
                    primary_row=source_row,
                    secondary_table=spec.table,
                )
            )
        except Exception:
            return out

        for link_row in link_rows:
            target_row = self._target_row_from_link(source_row, spec, link_row)
            if target_row is None:
                continue
            text = self._row_text(target_row, spec)
            key = self._term_key(text)
            if key:
                out[key] = target_row
        return out

    def _target_row_from_link(
        self,
        source_row: Row,
        spec: LegacyRelationFieldSpec,
        link_row: Any,
    ) -> Row | None:
        link_map = link_row.row_dict if isinstance(link_row, Row) else dict(link_row)
        try:
            target_id_column = self.db.driver_wrapper.get_link_column(
                source_row.table,
                spec.table,
                spec.id_column,
            )
        except Exception:
            return None
        target_id = link_map.get(target_id_column)
        if target_id in (None, ""):
            return None
        return self._get_row(spec.table, target_id)

    def _ensure_relation_row(
        self,
        spec: LegacyRelationFieldSpec,
        text: str,
        row_id: Any,
    ) -> tuple[Row | None, bool]:
        existing_id = self._as_int(row_id)
        if existing_id is not None:
            row = self._get_row(spec.table, existing_id)
            if row is not None:
                return row, False

        norm_value = self._norm_value(spec, text)
        if spec.norm_column and norm_value and self._has_column(spec.table, spec.norm_column):
            rows = self._search(spec.table, spec.norm_column, norm_value)
            if rows:
                return rows[0], False

        for column in spec.text_columns:
            if not self._has_column(spec.table, column):
                continue
            rows = self._search(spec.table, column, text)
            if rows:
                return rows[0], False

        payload = self._new_row_payload(spec, text, norm_value)
        if not payload:
            return None, False
        try:
            row = Row.from_idless_row_dict(
                self.db,
                payload,
                table=spec.table,
                read_only=True,
            )
            return row, True
        except Exception:
            return None, False

    def _new_row_payload(
        self,
        spec: LegacyRelationFieldSpec,
        text: str,
        norm_value: str | None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for column in spec.text_columns:
            if self._has_column(spec.table, column):
                payload[column] = text
                break
        if spec.norm_column and norm_value and self._has_column(spec.table, spec.norm_column):
            payload[spec.norm_column] = norm_value
        return payload

    def _link_rows(self, source_row: Row, target_row: Row) -> bool:
        try:
            self.db.interlink_rows(primary_row=source_row, secondary_row=target_row)
            return True
        except DatabaseIntegrityError:
            return False
        except Exception:
            return False

    def _unlink_rows(self, source_row: Row, target_row: Row) -> bool:
        unlink = getattr(self.db, "unlink_interlink", None)
        if not callable(unlink):
            return False
        try:
            unlink(primary_row=source_row, secondary_row=target_row)
            return True
        except Exception:
            return False

    def _mark_dirty(self, row: Row, *, reason: str) -> None:
        dirty_record = getattr(self.db, "dirty_record", None)
        if not callable(dirty_record) or row.row_id is None:
            return
        try:
            dirty_record(str(row.table), int(row.row_id), reason=reason)
        except TypeError:
            try:
                dirty_record(str(row.table), int(row.row_id))
            except Exception:
                return
        except Exception:
            return

    def _get_row(self, table: str, row_id: Any) -> Row | None:
        if row_id in (None, "") or not self._has_table(table):
            return None
        try:
            return self.db.get_row_from_id(table, int(row_id))
        except Exception:
            return None

    def _search(self, table: str, column: str, value: Any) -> list[Row]:
        try:
            return list(self.db.search(table=table, column=column, search_term=value))
        except Exception:
            return []

    def _relation_supported(self, source_table: str, target_table: str) -> bool:
        try:
            return bool(self.db.driver_wrapper.get_link_table_name(source_table, target_table))
        except Exception:
            return False

    def _has_table(self, table: str) -> bool:
        return table in self._tables or table in self._tables_and_columns

    def _has_column(self, table: str, column: str) -> bool:
        return column in set(self._tables_and_columns.get(table, []))

    @staticmethod
    def _as_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError, OverflowError):
            return None

    @staticmethod
    def _term_key(text: Any) -> str:
        return str(text or "").strip().casefold()

    @staticmethod
    def _norm_value(spec: LegacyRelationFieldSpec, text: str) -> str | None:
        if spec.norm_function is None:
            return None
        try:
            return str(spec.norm_function(text))
        except Exception:
            return None

    @staticmethod
    def _row_text(row: Row, spec: LegacyRelationFieldSpec) -> str | None:
        for column in spec.text_columns:
            value = row.row_dict.get(column)
            if value not in (None, ""):
                return str(value)
        return None

    @staticmethod
    def _row_ref(row: Row) -> dict[str, Any]:
        return {
            "table": str(row.table),
            "row_id": int(row.row_id) if row.row_id is not None else None,
        }


__all__ = [
    "LegacyRelationFieldSpec",
    "LiuXinWEMIMetadataWriteReport",
    "LiuXinWEMIMetadataWriter",
]
