"""
Write supported WEMI relation terms and entity identifiers back to a database.

The writer resolves one target Row, compares desired values under column policies
and records attempted changes. Operations use the caller-owned database without
adding a transaction or rollback boundary.

Example:
    >>> report = LiuXinWEMIMetadataWriteReport(item_id=7, target_level="work")
    >>> report.changed
    False
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any
import unicodedata

from LiuXin_alpha.databases.column_metadata import (
    ColumnMetadata,
    ColumnNormalizationProfile,
)
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError
from LiuXin_alpha.metadata.api.containers_api.metadata_write_api import (
    MetadataWriteRecord,
    MetadataWriteReportMapping,
)
from LiuXin_alpha.metadata.standardization import (
    make_tag_search_term,
    make_title_search_term,
)


@dataclass(frozen=True)
class LegacyRelationFieldSpec:
    """
    Describe how a legacy field maps to a relation table and its value columns.

    field is the legacy key; relation names the WEMI bucket; table and id_column
    identify target rows. text_columns gives lookup/display precedence. Optional
    norm_column and norm_function supply legacy comparison values. Instances are frozen,
    while a supplied callable retains its own state.

    Example:
        >>> spec = LiuXinWEMIMetadataWriter._FIELD_SPECS["tags"]
        >>> spec.table, spec.id_column
        ('tags', 'tag_id')
    """
    field: str
    relation: str
    table: str
    text_columns: tuple[str, ...]
    id_column: str
    norm_column: str | None = None
    norm_function: Any = None


@dataclass
class LiuXinWEMIMetadataWriteReport:
    """
    Collect target identity, checked fields, successful changes, skips and errors.

    Each list starts independently empty. A report can contain both changes and
    failures; changed reflects recorded row/link changes and does not imply atomic
    success.

    Example:
        >>> report = LiuXinWEMIMetadataWriteReport(item_id=7, target_level="work")
        >>> report.errors.append("could not link")
        >>> report.changed
        False
    """

    item_id: int | None
    target_level: str
    target_table: str | None = None
    target_id: int | None = None
    fields_checked: list[str] = field(default_factory=list)
    rows_added: list[MetadataWriteRecord] = field(default_factory=list)
    rows_updated: list[MetadataWriteRecord] = field(default_factory=list)
    rows_removed: list[MetadataWriteRecord] = field(default_factory=list)
    links_added: list[MetadataWriteRecord] = field(default_factory=list)
    links_removed: list[MetadataWriteRecord] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        """
        Test whether any row or link change has been recorded.

        Checked fields, skips and errors alone do not make the report changed.

        Example:
            >>> report = LiuXinWEMIMetadataWriteReport(item_id=7, target_level="work")
            >>> report.rows_added.append({"row_id": 1})
            >>> report.changed
            True


        :return: True when a change list is nonempty.
        """
        return bool(
            self.rows_added
            or self.rows_updated
            or self.rows_removed
            or self.links_added
            or self.links_removed
        )

    def to_mapping(self) -> MetadataWriteReportMapping:
        """
        Snapshot report scalars and lists, including the derived changed flag.

        The lists are copied, but nested row/link record dictionaries remain shared.

        Example:
            >>> report = LiuXinWEMIMetadataWriteReport(item_id=7, target_level="work")
            >>> payload = report.to_mapping()
            >>> payload["fields_checked"] is report.fields_checked, payload["changed"]
            (False, False)


        :return: New report dictionary with shallow list copies.
        """
        return {
            "item_id": self.item_id,
            "target_level": self.target_level,
            "target_table": self.target_table,
            "target_id": self.target_id,
            "fields_checked": list(self.fields_checked),
            "rows_added": list(self.rows_added),
            "rows_updated": list(self.rows_updated),
            "rows_removed": list(self.rows_removed),
            "links_added": list(self.links_added),
            "links_removed": list(self.links_removed),
            "skipped": list(self.skipped),
            "errors": list(self.errors),
            "changed": self.changed,
        }


class LiuXinWEMIMetadataWriter:
    """
    Apply supported relation-backed fields and entity identifiers to one resolved Row.

    Append mode adds missing terms/links and identifiers and can mark an existing
    identifier primary. Replace mode also unlinks stale term links and deletes stale
    entity identifiers; shared vocabulary rows are retained. Core identity fields,
    item-observed identifiers and storage rows are outside this writer.

    Writes are incremental and are not rolled back by this class. A failed link can
    leave a newly created term row recorded in the report. The caller owns database
    lifetime and transaction policy.

    Example:
        Exercise this contract with pytest::

            python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py
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
    _LEVEL_IDENTITY_ATTRIBUTES = {
        "work": "work",
        "expression": "expression",
        "manifestation": "manifestation",
        "item": "item",
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
    _SPECIAL_FIELDS = ("identifiers",)
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
        "identifier": "identifiers",
        "identifiers": "identifiers",
    }

    def __init__(self, database: Any) -> None:
        """
        Retain a non-None database and cache schema snapshots and column-policy lookups.

        None raises ValueError. Failed schema discovery uses empty snapshots; the
        caller-owned database is never closed here.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param database: Caller-owned writable database used for schema, row and relation
            operations.
        :return: None.
        """
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
        self._column_case_sensitivity: dict[tuple[str, str], bool] = {}
        self._column_metadata: dict[tuple[str, str], ColumnMetadata | None] = {}

    def write(
        self,
        metadata: Any,
        *,
        fields: Iterable[str] | None = None,
        target_level: str | None = None,
        item_id: int | None = None,
        target_row: Row | Mapping[str, Any] | None = None,
        replace: bool = False,
        mark_dirty: bool = True,
    ) -> LiuXinWEMIMetadataWriteReport:
        """
        Resolve a target and apply normalized requested relation fields and identifiers.

        The preferred level comes from target_level, an individual bundle, or work.
        Resolution can fall back to another level. Missing schema and unsupported relations
        are reported as skipped; selected write failures are reported as errors. In replace
        mode the filtered desired values are authoritative, so omitted or rejected values
        can cause stale links or entity identifiers to be removed.

        A complete slice uses legacy values for replacement and includes matching WEMI
        values in append mode; individual bundles use their relation values in either mode.
        No transaction boundary is added. Dirty marking is best-effort after reported
        changes.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param fields: Supported field names or aliases, deduplicated in order; None selects
            defaults appropriate to the metadata shape.
        :param target_level: Optional preferred WEMI level or w/e/m/i alias; false values
            allow bundle/work inference.
        :param item_id: Optional fallback item id used after direct target and bundle
            resolution.
        :param target_row: Optional explicit Row, or mapping containing the preferred level
            id; a resolved explicit target wins.
        :param replace: Remove stale links and entity identifiers for selected fields when
            True.
        :param mark_dirty: Attempt dirty_record after any recorded row/link changes when
            True.
        :return: Report describing the actual target, changes, skips and errors.
        """
        bundle_level = self._metadata_bundle_level(metadata)
        target_level_key = self._normalize_level(target_level or bundle_level or "work")
        resolved_target = self._resolve_target_row(
            metadata,
            target_level_key,
            item_id=item_id,
            target_row=target_row,
        )
        actual_level = resolved_target[0] if resolved_target is not None else target_level_key
        source_row = resolved_target[1] if resolved_target is not None else None
        report = LiuXinWEMIMetadataWriteReport(
            item_id=(
                self._metadata_database_id(metadata, "item_id")
                or self._metadata_bundle_database_id(metadata, "item")
                or self._as_int(item_id)
            ),
            target_level=actual_level,
            target_table=source_row.table if source_row is not None else None,
            target_id=int(source_row.row_id) if source_row is not None and source_row.row_id is not None else None,
        )
        if source_row is None:
            report.skipped.append(f"Could not resolve a database row for {target_level_key!r}.")
            return report

        for field_name in self._normalize_fields(fields, metadata=metadata):
            report.fields_checked.append(field_name)
            if field_name == "identifiers":
                self._write_identifier_rows(
                    metadata,
                    source_row=source_row,
                    actual_level=actual_level,
                    replace=replace,
                    report=report,
                )
                continue

            spec = self._FIELD_SPECS[field_name]
            if self._metadata_bundle_level(metadata) is not None and not self._bundle_has_relation(
                metadata,
                spec.relation,
            ):
                report.skipped.append(
                    f"{field_name}: {metadata.__class__.__name__} does not expose {spec.relation!r}."
                )
                continue
            if not self._has_table(spec.table):
                report.skipped.append(f"{field_name}: table {spec.table!r} is not present.")
                continue
            if not self._relation_supported(source_row.table, spec.table):
                report.skipped.append(
                    f"{field_name}: {source_row.table!r} cannot link to {spec.table!r}."
                )
                continue

            desired = self._desired_terms(
                metadata,
                field_name,
                spec,
                actual_level,
                include_wemi_relations=not replace,
            )
            desired = self._filter_safe_terms(field_name, desired, report)
            existing = self._existing_terms(source_row, spec)
            desired_links = self._desired_relation_links(metadata, spec, actual_level)
            for text, row_id in desired.items():
                key = self._term_key(text, spec)
                if not key or key in existing:
                    continue
                target_row, created = self._ensure_relation_row(spec, text, row_id)
                if target_row is None:
                    report.skipped.append(f"{field_name}: could not create/find row for {text!r}.")
                    continue
                if self._link_rows(source_row, target_row, desired_links.get(key)):
                    report.links_added.append(
                        {
                            "field": field_name,
                            "source": self._row_ref(source_row),
                            "target": self._row_ref(target_row),
                        }
                    )
                else:
                    report.errors.append(
                        "{}: could not link {}:{} to {}:{}.".format(
                            field_name,
                            source_row.table,
                            source_row.row_id,
                            target_row.table,
                            target_row.row_id,
                        )
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
                desired_keys = {self._term_key(text, spec) for text in desired}
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
                    else:
                        report.errors.append(
                            "{}: could not remove link from {}:{} to {}:{}.".format(
                                field_name,
                                source_row.table,
                                source_row.row_id,
                                target_row.table,
                                target_row.row_id,
                            )
                        )

        if mark_dirty and report.changed:
            self._mark_dirty(source_row, reason="metadata_write_back")
        return report

    def _filter_safe_terms(
        self,
        field_name: str,
        desired: OrderedDict[str, Any],
        report: LiuXinWEMIMetadataWriteReport,
    ) -> OrderedDict[str, Any]:
        """
        Remove terms containing unsupported control characters or lone surrogates.

        Append one report error per rejected term; accepted row-id hints retain their
        values.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param field_name: Canonical supported legacy field name.
        :param desired: Ordered desired values produced by the corresponding collection
            helper.
        :param report: Mutable write report receiving change, skip or error entries.
        :return: New ordered mapping of accepted terms.
        """
        out: OrderedDict[str, Any] = OrderedDict()
        for text, row_id in desired.items():
            unsafe_reason = self._unsafe_text_reason(text)
            if unsafe_reason is not None:
                report.errors.append(
                    f"{field_name}: skipped unsafe text value ({unsafe_reason})."
                )
                continue
            out[text] = row_id
        return out

    def _normalize_level(self, level: str) -> str:
        """
        Strip and lowercase a WEMI level, expanding w/e/m/i aliases.

        Unknown names raise KeyError.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param level: Candidate level name or single-letter abbreviation.
        :return: Canonical WEMI level name.
        """
        level_key = str(level).strip().lower()
        aliases = {"w": "work", "e": "expression", "m": "manifestation", "i": "item"}
        level_key = aliases.get(level_key, level_key)
        if level_key not in self._LEVEL_TABLES:
            raise KeyError(f"Unknown WEMI level {level!r}.")
        return level_key

    def _normalize_fields(
        self,
        fields: Iterable[str] | None,
        *,
        metadata: Any | None = None,
    ) -> tuple[str, ...]:
        """
        Choose supported write fields and normalize aliases without duplicates.

        None selects all defaults for legacy/slice objects, or only relations exposed by an
        individual bundle. Unsupported explicit names raise KeyError.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param fields: Iterable of requested field names, or None for shape-dependent
            defaults.
        :param metadata: Optional metadata object used to restrict default fields for
            individual bundles.
        :return: Tuple of canonical field names in selection order.
        """
        if fields is None:
            if metadata is not None and self._metadata_bundle_level(metadata) is not None:
                exposed_relations = set(self._bundle_relation_names(metadata))
                relation_fields = tuple(
                    field
                    for field, spec in self._FIELD_SPECS.items()
                    if spec.relation in exposed_relations
                )
                if "identifiers" in exposed_relations:
                    return relation_fields + ("identifiers",)
                return relation_fields
            return tuple(self._FIELD_SPECS) + self._SPECIAL_FIELDS
        out: list[str] = []
        for field in fields:
            key = self._FIELD_ALIASES.get(str(field).strip().lower())
            if key is None:
                raise KeyError(f"Unsupported metadata write-back field {field!r}.")
            if key not in out:
                out.append(key)
        return tuple(out)

    def _resolve_target_row(
        self,
        metadata: Any,
        preferred_level: str,
        *,
        item_id: int | None,
        target_row: Row | Mapping[str, Any] | None,
    ) -> tuple[str, Row] | None:
        """
        Try an explicit target, stored ids, bundle relations and an item-derived chain.

        Stored-id fallback tries the preferred level then
        work/expression/manifestation/item, omitting direct item fallback unless item was
        preferred. Item-chain fallback may still choose an item when higher levels are
        unavailable.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param preferred_level: Canonical WEMI level to try before fallback levels.
        :param item_id: Optional fallback item id preferred within the item-chain resolution
            step.
        :param target_row: Optional explicit Row or mapping with a preferred-level identity
            key.
        :return: Resolved level/Row pair, or None.
        """
        explicit_target = self._coerce_target_row(target_row, preferred_level)
        if explicit_target is not None:
            return explicit_target

        levels = (preferred_level,) + tuple(
            level for level in self._LEVEL_FALLBACKS if level != preferred_level
        )
        for level in levels:
            if level == "item" and preferred_level != "item":
                continue
            table = self._LEVEL_TABLES[level]
            row_id = (
                self._metadata_database_id(metadata, self._LEVEL_ID_NAMES[level])
                or self._metadata_bundle_database_id(metadata, level)
            )
            if row_id is None:
                continue
            row = self._get_row(table, row_id)
            if row is not None:
                return level, row

        relation_target = self._resolve_target_from_bundle_relation(metadata, preferred_level)
        if relation_target is not None:
            return preferred_level, relation_target

        target_item_id = (
            self._as_int(item_id)
            or self._metadata_database_id(metadata, "item_id")
            or self._metadata_bundle_database_id(metadata, "item")
        )
        if target_item_id is not None:
            by_item = self._resolve_target_from_item_id(target_item_id, preferred_level)
            if by_item is not None:
                return by_item
        return None

    def _coerce_target_row(
        self,
        target_row: Row | Mapping[str, Any] | None,
        preferred_level: str,
    ) -> tuple[str, Row] | None:
        """
        Retain an explicit Row or look up a mapping's preferred-level id.

        Rows use their table's WEMI level when recognized, otherwise the preferred level.
        Unsupported values and unresolved mapping ids produce None.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param target_row: Optional Row or mapping supplying the explicit target.
        :param preferred_level: Canonical WEMI level to try before fallback levels.
        :return: Level/Row pair, or None.
        """
        if target_row is None:
            return None
        if isinstance(target_row, Row):
            level = self._level_for_table(str(target_row.table)) or preferred_level
            return level, target_row
        if not isinstance(target_row, Mapping):
            return None
        table = self._LEVEL_TABLES[preferred_level]
        row_id = self._as_int(target_row.get(self._LEVEL_ID_NAMES[preferred_level]))
        row = self._get_row(table, row_id)
        if row is not None:
            return preferred_level, row
        return None

    def _resolve_target_from_item_id(
        self,
        item_id: int,
        preferred_level: str,
    ) -> tuple[str, Row] | None:
        """
        Walk an item's parent-id chain and return the preferred available level.

        Missing expression/work foreign-key targets fall back to the first resolvable
        interlinked row. This helper follows source hints rather than selecting links by
        primary metadata.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param item_id: Item id anchoring the parent-chain lookup.
        :param preferred_level: Canonical WEMI level to try before fallback levels.
        :return: Preferred or fallback level/Row pair, or None.
        """
        rows: dict[str, Row] = {}
        item_row = self._get_row("items", item_id)
        if item_row is not None:
            rows["item"] = item_row

        manifestation_id = (
            item_row.row_dict.get("item_manifestation_id") if item_row is not None else None
        )
        manifestation_row = self._get_row("manifestations", manifestation_id)
        if manifestation_row is not None:
            rows["manifestation"] = manifestation_row

        expression_id = (
            manifestation_row.row_dict.get("manifestation_expression_id")
            if manifestation_row is not None
            else None
        )
        expression_row = self._get_row("expressions", expression_id)
        if expression_row is None and manifestation_row is not None:
            expression_row = self._first_interlinked_target(manifestation_row, "expressions")
        if expression_row is not None:
            rows["expression"] = expression_row

        work_id = (
            expression_row.row_dict.get("expression_work_id")
            if expression_row is not None
            else None
        )
        work_row = self._get_row("works", work_id)
        if work_row is None and expression_row is not None:
            work_row = self._first_interlinked_target(expression_row, "works")
        if work_row is not None:
            rows["work"] = work_row

        levels = (preferred_level,) + tuple(
            level for level in self._LEVEL_FALLBACKS if level != preferred_level
        )
        for level in levels:
            row = rows.get(level)
            if row is not None:
                return level, row
        return None

    def _first_interlinked_target(self, source_row: Row, secondary_table: str) -> Row | None:
        """
        Return the first resolvable target from database interlink result order.

        Missing target tables and query/column-discovery failures produce None. Primary
        flags and priorities are not used for selection here.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param source_row: Resolved source database Row whose relations or identifiers are
            being changed.
        :param secondary_table: Database table whose interlinked rows are requested.
        :return: First resolved Row, or None.
        """
        if not self._has_table(secondary_table):
            return None
        try:
            link_rows = list(
                self.db.get_interlink_rows(
                    primary_row=source_row,
                    secondary_table=secondary_table,
                )
            )
            target_id_column = self.db.driver_wrapper.get_link_column(
                source_row.table,
                secondary_table,
                self.db.driver_wrapper.get_id_column(secondary_table),
            )
        except Exception:
            return None

        for link_row in link_rows:
            link_map = link_row.row_dict if isinstance(link_row, Row) else dict(link_row)
            target_id = link_map.get(target_id_column)
            target_row = self._get_row(secondary_table, target_id)
            if target_row is not None:
                return target_row
        return None

    def _resolve_target_from_bundle_relation(
        self,
        metadata: Any,
        preferred_level: str,
    ) -> Row | None:
        """
        Find the first resolvable target in a bundle relation for the preferred level.

        Missing/unsupported relation access or a failed getter yields None. Links are
        considered in bucket order without primary selection.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param preferred_level: Canonical WEMI level to try before fallback levels.
        :return: Resolved target Row, or None.
        """
        relation = self._LEVEL_TABLES[preferred_level]
        if not self._bundle_has_relation(metadata, relation):
            return None
        getter = getattr(metadata, "get_relation_links", None)
        if not callable(getter):
            return None
        try:
            links = list(getter(relation))
        except Exception:
            return None
        for link in links:
            target_row = self._target_row_from_relation_target(
                getattr(link, "target", link),
                self._LEVEL_TABLES[preferred_level],
            )
            if target_row is not None:
                return target_row
        return None

    def _target_row_from_relation_target(self, target: Any, table: str) -> Row | None:
        """
        Retain a Row in the requested table or resolve an id from a target mapping.

        Wrong-table Rows, empty mappings and failed id-column discovery produce None.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param target: Row, mapping, identity object or scalar relation target.
        :param table: Database table name used for schema checks or row lookup.
        :return: Matching database Row, or None.
        """
        if isinstance(target, Row):
            return target if str(target.table) == str(table) else None
        mapping = self._target_mapping(target)
        if not mapping:
            return None
        try:
            id_column = self.db.driver_wrapper.get_id_column(table)
        except Exception:
            return None
        return self._get_row(table, mapping.get(id_column))

    @staticmethod
    def _metadata_database_id(metadata: Any, name: str) -> int | None:
        """
        Read an id through a metadata getter, database_ids mapping or fallback attributes.

        Only absent/empty candidates fall through; a selected nonempty value that cannot
        convert returns None. Item ids also try db_id and application_id. Getter failures
        can propagate.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param name: Database-id key requested from the metadata object.
        :return: Integer id, or None.
        """
        getter = getattr(metadata, "get_database_id", None)
        value = getter(name) if callable(getter) else None
        if value in (None, ""):
            database_ids = getattr(metadata, "database_ids", {})
            if isinstance(database_ids, Mapping):
                value = database_ids.get(name)
        if value in (None, ""):
            attr_names = [str(name)]
            if str(name).strip().lower() == "item_id":
                attr_names.extend(["item_id", "db_id", "application_id"])
            for attr_name in dict.fromkeys(attr_names):
                try:
                    value = getattr(metadata, attr_name)
                except Exception:
                    value = None
                if value not in (None, ""):
                    break
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError, OverflowError):
            return None

    def _metadata_bundle_database_id(self, metadata: Any, level: str) -> int | None:
        """
        Read the selected identity's level-specific id attribute.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param level: Canonical WEMI level whose identity attribute is inspected.
        :return: Converted id, or None when the identity/id is absent.
        """
        identity_attr = self._LEVEL_IDENTITY_ATTRIBUTES[level]
        identity = getattr(metadata, identity_attr, None)
        if identity is None:
            return None
        return self._as_int(getattr(identity, self._LEVEL_ID_NAMES[level], None))

    def _desired_terms(
        self,
        metadata: Any,
        field_name: str,
        spec: LegacyRelationFieldSpec,
        target_level: str,
        *,
        include_wemi_relations: bool = True,
    ) -> OrderedDict[str, Any]:
        """
        Collect desired terms from a bundle or legacy fields plus optional WEMI values.

        Individual bundles always use relation targets. For complete slices, WEMI terms
        overwrite equal-spelling legacy keys when included.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param field_name: Canonical supported legacy field name.
        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :param target_level: Canonical WEMI level whose values or links apply to this write.
        :param include_wemi_relations: Include the selected WEMI bundle in addition to
            legacy values when True.
        :return: Ordered mapping of term text to optional row-id hints.
        """
        bundle_level = self._metadata_bundle_level(metadata)
        if bundle_level is not None:
            return self._desired_bundle_terms(metadata, spec, target_level)

        desired = self._desired_legacy_terms(metadata, field_name)
        if include_wemi_relations:
            bundle = self._metadata_wemi_bundle(metadata, target_level)
            if bundle is not None:
                desired.update(self._desired_bundle_terms(bundle, spec, target_level))
        return desired

    def _desired_legacy_terms(
        self,
        metadata: Any,
        field_name: str,
    ) -> OrderedDict[str, Any]:
        """
        Read a legacy field and convert mapping keys, scalars or iterables to term entries.

        AttributeError is treated as an absent field. False or blank terms are omitted after
        string conversion and stripping; mapping values are preserved as row-id hints.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param field_name: Canonical supported legacy field name.
        :return: Ordered term-to-id mapping.
        """
        getter = getattr(metadata, "direct_get", None)
        try:
            value = getter(field_name) if callable(getter) else getattr(metadata, field_name, None)
        except AttributeError:
            value = None
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

    def _desired_bundle_terms(
        self,
        metadata: Any,
        spec: LegacyRelationFieldSpec,
        target_level: str,
    ) -> OrderedDict[str, Any]:
        """
        Collect term text and ids from links applicable to the target WEMI level.

        Missing or failed relation getters produce an empty mapping. Later equal-spelling
        terms replace earlier id hints.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :param target_level: Canonical WEMI level whose values or links apply to this write.
        :return: Ordered term-to-id mapping.
        """
        getter = getattr(metadata, "get_relation_links", None)
        if not callable(getter):
            return OrderedDict()
        try:
            links = list(getter(spec.relation))
        except Exception:
            return OrderedDict()

        out: OrderedDict[str, Any] = OrderedDict()
        for link in links:
            if not self._relation_link_applies_to_level(link, target_level):
                continue
            target = getattr(link, "target", link)
            text, row_id = self._relation_target_term(target, spec)
            if text:
                out[text] = row_id
        return out

    def _desired_relation_links(
        self,
        metadata: Any,
        spec: LegacyRelationFieldSpec,
        target_level: str,
    ) -> dict[str, Any]:
        """
        Index applicable WEMI links by their normalized term comparison key.

        Missing or failed bucket access yields an empty mapping. Later links win when
        normalized keys collide.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :param target_level: Canonical WEMI level whose values or links apply to this write.
        :return: Comparison-key-to-link dictionary.
        """
        bundle = metadata if self._metadata_bundle_level(metadata) is not None else self._metadata_wemi_bundle(
            metadata,
            target_level,
        )
        if bundle is None:
            return {}
        getter = getattr(bundle, "get_relation_links", None)
        if not callable(getter):
            return {}
        try:
            links = list(getter(spec.relation))
        except Exception:
            return {}

        out: dict[str, Any] = {}
        for link in links:
            if not self._relation_link_applies_to_level(link, target_level):
                continue
            text, _row_id = self._relation_target_term(getattr(link, "target", link), spec)
            key = self._term_key(text, spec)
            if key:
                out[key] = link
        return out

    @staticmethod
    def _relation_link_applies_to_level(relation_link: Any, target_level: str) -> bool:
        """
        Accept a link unless its extra mapping names a different source entity type.

        Absent/empty provenance applies to every level; present names compare after
        stripping and lowercasing.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param relation_link: Link whose optional extra/source_entity_type provenance is
            inspected.
        :param target_level: Canonical WEMI level whose values or links apply to this write.
        :return: Whether the link applies to this target level.
        """
        extra = getattr(relation_link, "extra", None)
        if not isinstance(extra, Mapping):
            return True
        source_entity_type = extra.get("source_entity_type")
        if source_entity_type in (None, ""):
            return True
        return str(source_entity_type).strip().lower() == str(target_level).strip().lower()

    def _relation_target_term(
        self,
        target: Any,
        spec: LegacyRelationFieldSpec,
    ) -> tuple[str | None, int | None]:
        """
        Extract text and a row-id hint from a Row, integer id, mapping or scalar target.

        Integer ids are resolved through the relation table, excluding bool from that
        branch. Mappings use configured text columns and id aliases, with row lookup when
        only an id is available. Other values are stripped string forms.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param target: Row, mapping, identity object or scalar relation target.
        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :return: Text/id pair; either component can be None.
        """
        if isinstance(target, Row):
            return self._row_text(target, spec), self._as_int(target.row_id)

        if isinstance(target, int) and not isinstance(target, bool):
            row = self._get_row(spec.table, target)
            return (self._row_text(row, spec) if row is not None else None), int(target)

        mapping = self._target_mapping(target)
        if mapping:
            row_id = self._as_int(
                mapping.get(spec.id_column)
                or mapping.get("row_id")
                or mapping.get("id")
                or mapping.get("primary_id")
            )
            text = self._mapping_text(mapping, spec)
            if text is None and row_id is not None:
                row = self._get_row(spec.table, row_id)
                text = self._row_text(row, spec) if row is not None else None
            return text, row_id

        text = str(target or "").strip()
        return (text or None), None

    @staticmethod
    def _target_mapping(target: Any) -> Mapping[str, Any]:
        """
        Expose a Mapping, row_dict mapping or a callable to_mapping result.

        Only TypeError from to_mapping is suppressed. Other conversion or attribute-access
        errors can propagate.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param target: Row, mapping, identity object or scalar relation target.
        :return: Existing mapping, or an empty dictionary.
        """
        if isinstance(target, Mapping):
            return target
        row_dict = getattr(target, "row_dict", None)
        if isinstance(row_dict, Mapping):
            return row_dict
        to_mapping = getattr(target, "to_mapping", None)
        if callable(to_mapping):
            try:
                mapping = to_mapping()
            except TypeError:
                mapping = None
            if isinstance(mapping, Mapping):
                return mapping
        return {}

    @staticmethod
    def _mapping_text(
        mapping: Mapping[str, Any],
        spec: LegacyRelationFieldSpec,
    ) -> str | None:
        """
        Read the first configured text column whose value is neither None nor empty text.

        The selected value is stringified without trimming.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param mapping: Column-keyed values to inspect without changing the mapping.
        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :return: Text value, or None.
        """
        for column in spec.text_columns:
            value = mapping.get(column)
            if value not in (None, ""):
                return str(value)
        return None

    def _existing_terms(
        self,
        source_row: Row,
        spec: LegacyRelationFieldSpec,
    ) -> dict[str, Row]:
        """
        Resolve existing term links and index their rows by comparison key.

        A failed interlink query yields an empty dictionary. Unresolved or empty-key targets
        are skipped; later colliding keys win.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param source_row: Resolved source database Row whose relations or identifiers are
            being changed.
        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :return: Comparison-key-to-Row dictionary.
        """
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
            key = self._term_key(text, spec)
            if key:
                out[key] = target_row
        return out

    def _target_row_from_link(
        self,
        source_row: Row,
        spec: LegacyRelationFieldSpec,
        link_row: Any,
    ) -> Row | None:
        """
        Resolve the target Row referenced by an interlink payload.

        Failed link-column lookup, missing ids or failed row resolution produce None.
        Invalid mapping conversion can still raise.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param source_row: Resolved source database Row whose relations or identifiers are
            being changed.
        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :param link_row: Interlink Row or mapping-like payload with the target-id column.
        :return: Resolved target Row, or None.
        """
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
        """
        Prefer an existing id, then comparison/text matches, then create a term row.

        A resolvable id is trusted without checking that its text matches. Case-insensitive
        policies may scan all rows when indexed searches miss. Failed creation returns no
        row; successful creation is not rolled back if linking later fails.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :param text: Term text to compare, normalize or persist.
        :param row_id: Optional id candidate converted to int before row lookup.
        :return: Row and creation flag, or (None, False).
        """
        existing_id = self._as_int(row_id)
        if existing_id is not None:
            row = self._get_row(spec.table, existing_id)
            if row is not None:
                return row, False

        case_sensitive = self._is_case_sensitive(spec)
        comparison_column, norm_value = self._comparison_value(spec, text)
        if (
            not case_sensitive
            and comparison_column
            and norm_value
            and self._has_column(spec.table, comparison_column)
        ):
            rows = self._search(spec.table, comparison_column, norm_value)
            if rows:
                return rows[0], False

        for column in spec.text_columns:
            if not self._has_column(spec.table, column):
                continue
            rows = self._search(spec.table, column, text)
            if rows:
                return rows[0], False
            if not case_sensitive:
                rows = self._rows_matching_text_policy(spec, text)
                if rows:
                    return rows[0], False

        payload = self._new_row_payload(
            spec,
            text,
            comparison_column,
            norm_value,
        )
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
        comparison_column: str | None,
        norm_value: str | None,
    ) -> dict[str, Any]:
        """
        Build term-row values from the first available text column and optional comparison column.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :param text: Term text to compare, normalize or persist.
        :param comparison_column: Optional normalized comparison column name.
        :param norm_value: Optional normalized text stored when truthy and supported by
            schema.
        :return: New insertion payload, possibly empty.
        """
        payload: dict[str, Any] = {}
        for column in spec.text_columns:
            if self._has_column(spec.table, column):
                payload[column] = text
                break
        if (
            comparison_column
            and norm_value
            and self._has_column(spec.table, comparison_column)
        ):
            payload[comparison_column] = norm_value
        return payload

    def _link_rows(
        self,
        source_row: Row,
        target_row: Row,
        relation_link: Any | None = None,
    ) -> bool:
        """
        Call the database interlink operation with supported link metadata.

        Missing or false priority, including zero, becomes highest. Boolean primary values
        become integers; type and selected provenance attributes are forwarded, but
        arbitrary extra fields are not.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param source_row: Resolved source database Row whose relations or identifiers are
            being changed.
        :param target_row: Resolved target database Row to link or unlink.
        :param relation_link: Optional relation link carrying write metadata such as type,
            priority and provenance.
        :return: True when the call completes, False on an ordinary exception.
        """
        priority: Any = "highest"
        link_type: str | None = None
        extra_columns: dict[str, Any] = {}
        if relation_link is not None:
            priority = getattr(relation_link, "priority", None) or "highest"
            link_type = getattr(relation_link, "type", None)
            for attr in ("primary", "origin", "source", "policy", "data", "index"):
                value = getattr(relation_link, attr, None)
                if value is None:
                    continue
                extra_columns[attr] = int(value) if attr == "primary" and isinstance(value, bool) else value
        try:
            self.db.interlink_rows(
                primary_row=source_row,
                secondary_row=target_row,
                priority=priority,
                type=link_type,
                **extra_columns,
            )
            return True
        except DatabaseIntegrityError:
            return False
        except Exception:
            return False

    def _unlink_rows(self, source_row: Row, target_row: Row) -> bool:
        """
        Call unlink_interlink when available and suppress ordinary failures.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param source_row: Resolved source database Row whose relations or identifiers are
            being changed.
        :param target_row: Resolved target database Row to link or unlink.
        :return: True when the call completes; False when unavailable or failed.
        """
        unlink = getattr(self.db, "unlink_interlink", None)
        if not callable(unlink):
            return False
        try:
            unlink(primary_row=source_row, secondary_row=target_row)
            return True
        except Exception:
            return False

    def _delete_row(self, row: Row) -> bool:
        """
        Delete a persisted Row using db.delete or an available driver fallback.

        A failing db.delete does not fall through to delete_by_id; the fallback is used only
        when db.delete is not callable.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param row: Database Row to inspect or change.
        :return: True when deletion completes; False for missing ids, capabilities or
            failures.
        """
        if row.row_id is None:
            return False

        delete = getattr(self.db, "delete", None)
        if callable(delete):
            try:
                delete(row)
                return True
            except Exception:
                return False

        delete_by_id = getattr(getattr(self.db, "driver_wrapper", None), "delete_by_id", None)
        if not callable(delete_by_id):
            return False
        try:
            delete_by_id(target_table=str(row.table), row_id=int(row.row_id))
            return True
        except Exception:
            return False

    def _update_row_column(self, row: Row, column: str, value: Any) -> bool:
        """
        Update one persisted column through the driver and then the local row mapping.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param row: Database Row to inspect or change.
        :param column: Database column name to inspect, search or update.
        :param value: Candidate value to inspect or store.
        :return: True when both operations complete; False for missing ids/capabilities or
            an exception.
        """
        if row.row_id is None:
            return False
        update_column = getattr(getattr(self.db, "driver_wrapper", None), "update_column", None)
        if not callable(update_column):
            return False
        try:
            update_column(str(row.table), int(row.row_id), column, value)
            row.row_dict[column] = value
            return True
        except Exception:
            return False

    def _write_identifier_rows(
        self,
        metadata: Any,
        *,
        source_row: Row,
        actual_level: str,
        replace: bool,
        report: LiuXinWEMIMetadataWriteReport,
    ) -> None:
        """
        Reconcile entity identifiers for the source entity and update the report.

        Require entity-type/id/scheme/value columns. Replace mode deletes stale rows;
        matching identifiers may be marked primary and new rows are added. Item-observed
        identifiers are untouched. A scheme is marked claimed before new-row creation, so a
        failed first insertion can affect later primary selection.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param source_row: Resolved source database Row whose relations or identifiers are
            being changed.
        :param actual_level: Resolved WEMI entity level for the source Row.
        :param replace: Delete existing entity identifiers absent from the filtered desired
            set when True.
        :param report: Mutable write report receiving change, skip or error entries.
        :return: None.
        """
        field_name = "identifiers"
        table = "entity_identifiers"
        required_columns = {
            "entity_identifier_entity_type",
            "entity_identifier_entity_id",
            "entity_identifier_scheme",
            "entity_identifier_value",
        }
        if not self._has_table(table):
            report.skipped.append(f"{field_name}: table {table!r} is not present.")
            return
        missing_columns = [
            column for column in sorted(required_columns) if not self._has_column(table, column)
        ]
        if missing_columns:
            report.skipped.append(
                "{}: table {!r} is missing columns {}.".format(
                    field_name,
                    table,
                    ", ".join(missing_columns),
                )
            )
            return

        existing = self._existing_identifiers(source_row, actual_level)
        desired = self._desired_identifiers(
            metadata,
            actual_level,
            include_wemi_relations=not replace,
        )
        desired = self._filter_safe_identifiers(desired, report)

        if replace:
            for key, row in list(existing.items()):
                if key in desired:
                    continue
                if self._delete_row(row):
                    report.rows_removed.append(self._identifier_report_row(row))
                    existing.pop(key, None)
                else:
                    row_ref = self._row_ref(row)
                    report.errors.append(
                        "identifiers: could not remove row {}:{} ({!r}).".format(
                            row_ref["table"],
                            row_ref["row_id"],
                            key,
                        )
                    )

        primary_schemes = {
            scheme
            for scheme, _value in existing
            if existing[(scheme, _value)].row_dict.get("entity_identifier_is_primary")
        }
        if self._has_column(table, "entity_identifier_is_primary"):
            for key, row in existing.items():
                if key not in desired or key[0] in primary_schemes:
                    continue
                if self._update_row_column(row, "entity_identifier_is_primary", 1):
                    report.rows_updated.append(
                        {
                            **self._identifier_report_row(row),
                            "column": "entity_identifier_is_primary",
                            "new_value": 1,
                        }
                    )
                    primary_schemes.add(key[0])
                else:
                    row_ref = self._row_ref(row)
                    report.errors.append(
                        "identifiers: could not mark row {}:{} as primary.".format(
                            row_ref["table"],
                            row_ref["row_id"],
                        )
                    )

        for key, (scheme, value, relation_link) in desired.items():
            if key in existing:
                continue
            primary = 0
            if key[0] not in primary_schemes:
                primary = 1
                primary_schemes.add(key[0])
            row = self._create_identifier_row(
                source_row=source_row,
                actual_level=actual_level,
                scheme=scheme,
                value=value,
                primary=primary,
                relation_link=relation_link,
            )
            if row is None:
                report.skipped.append(
                    f"identifiers: could not create row for {scheme}:{value}."
                )
                continue
            report.rows_added.append(self._identifier_report_row(row))
            existing[key] = row

    def _filter_safe_identifiers(
        self,
        desired: OrderedDict[tuple[str, str], tuple[str, str, Any | None]],
        report: LiuXinWEMIMetadataWriteReport,
    ) -> OrderedDict[tuple[str, str], tuple[str, str, Any | None]]:
        """
        Reject desired identifier pairs with unsupported controls or lone surrogates.

        Scheme validation precedes value validation, and one error is recorded per rejected
        pair.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param desired: Ordered desired values produced by the corresponding collection
            helper.
        :param report: Mutable write report receiving change, skip or error entries.
        :return: New ordered mapping retaining accepted keys and relation links.
        """
        out: OrderedDict[tuple[str, str], tuple[str, str, Any | None]] = OrderedDict()
        for key, (scheme, value, relation_link) in desired.items():
            unsafe_scheme = self._unsafe_text_reason(scheme)
            if unsafe_scheme is not None:
                report.errors.append(
                    f"identifiers: skipped unsafe scheme ({unsafe_scheme})."
                )
                continue
            unsafe_value = self._unsafe_text_reason(value)
            if unsafe_value is not None:
                report.errors.append(
                    f"identifiers: skipped unsafe value ({unsafe_value})."
                )
                continue
            out[key] = (scheme, value, relation_link)
        return out

    def _existing_identifiers(
        self,
        source_row: Row,
        actual_level: str,
    ) -> dict[tuple[str, str], Row]:
        """
        Read source entity identifiers and index them by case-folded scheme/value pairs.

        Rows for other entity types or blank components are skipped; later duplicate keys
        win.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param source_row: Resolved source database Row whose relations or identifiers are
            being changed.
        :param actual_level: Resolved WEMI entity level for the source Row.
        :return: Identifier-key-to-Row dictionary.
        """
        out: dict[tuple[str, str], Row] = {}
        rows = self._search(
            "entity_identifiers",
            "entity_identifier_entity_id",
            int(source_row.row_id),
        )
        for row in rows:
            mapping = row.row_dict
            if str(mapping.get("entity_identifier_entity_type", "")).strip().lower() != actual_level:
                continue
            scheme = self._identifier_component(mapping.get("entity_identifier_scheme"))
            value = self._identifier_component(mapping.get("entity_identifier_value"))
            if scheme is None or value is None:
                continue
            out[(scheme.casefold(), value.casefold())] = row
        return out

    def _desired_identifiers(
        self,
        metadata: Any,
        target_level: str,
        *,
        include_wemi_relations: bool = True,
    ) -> OrderedDict[tuple[str, str], tuple[str, str, Any | None]]:
        """
        Collect unique legacy identifier pairs and optional WEMI identifier links.

        Keys case-fold both scheme and value. First occurrence wins, so a legacy pair can
        retain precedence over a WEMI link's provenance. Individual bundles always
        contribute their applicable links.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param target_level: Canonical WEMI level whose values or links apply to this write.
        :param include_wemi_relations: Include the selected WEMI bundle in addition to
            legacy values when True.
        :return: Ordered normalized-key-to-(scheme, value, link) mapping.
        """
        desired: OrderedDict[tuple[str, str], tuple[str, str, Any | None]] = OrderedDict()
        for scheme, value in self._iter_legacy_identifier_pairs(metadata):
            key = (scheme.casefold(), value.casefold())
            desired.setdefault(key, (scheme, value, None))

        bundle_level = self._metadata_bundle_level(metadata)
        if bundle_level is not None:
            self._add_desired_bundle_identifiers(
                desired,
                metadata,
                target_level,
            )
            return desired

        if include_wemi_relations:
            bundle = self._metadata_wemi_bundle(metadata, target_level)
            if bundle is not None:
                self._add_desired_bundle_identifiers(
                    desired,
                    bundle,
                    target_level,
                )
        return desired

    def _add_desired_bundle_identifiers(
        self,
        desired: OrderedDict[tuple[str, str], tuple[str, str, Any | None]],
        metadata: Any,
        target_level: str,
    ) -> None:
        """
        Append previously unseen applicable bundle identifiers to an ordered desired mapping.

        Missing or failed relation access contributes nothing. Existing normalized keys are
        retained.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param desired: Ordered desired values produced by the corresponding collection
            helper.
        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param target_level: Canonical WEMI level whose values or links apply to this write.
        :return: None.
        """
        getter = getattr(metadata, "get_relation_links", None)
        if not callable(getter):
            return
        try:
            links = list(getter("identifiers"))
        except Exception:
            return
        for link in links:
            if not self._relation_link_applies_to_level(link, target_level):
                continue
            pair = self._identifier_pair_from_target(getattr(link, "target", link))
            if pair is None:
                continue
            scheme, value = pair
            key = (scheme.casefold(), value.casefold())
            desired.setdefault(key, (scheme, value, link))

    def _iter_legacy_identifier_pairs(self, metadata: Any) -> Iterable[tuple[str, str]]:
        """
        Collect stripped scheme/value pairs from the legacy identifier mapping.

        Getter failures and non-mapping results yield no pairs. Mapping-valued schemes
        contribute their keys; blank components are omitted. Duplicate pairs remain for
        later reconciliation.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :return: Tuple of normalized display-text pairs.
        """
        getter = getattr(metadata, "get_identifiers", None)
        try:
            identifiers = getter() if callable(getter) else getattr(metadata, "identifiers", None)
        except Exception:
            identifiers = None
        if not isinstance(identifiers, Mapping):
            return ()

        pairs: list[tuple[str, str]] = []
        for raw_scheme, raw_values in identifiers.items():
            scheme = self._identifier_component(raw_scheme)
            if scheme is None:
                continue
            for raw_value in self._identifier_values(raw_values):
                value = self._identifier_component(raw_value)
                if value is not None:
                    pairs.append((scheme, value))
        return tuple(pairs)

    def _identifier_pair_from_target(self, target: Any) -> tuple[str, str] | None:
        """
        Extract identifier components from supported mapping keys or object attributes.

        Attribute fallback occurs only when the target mapping is empty. Recognized schemes
        and values are stripped but not case-folded here.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param target: Row, mapping, identity object or scalar relation target.
        :return: Scheme/value pair, or None when either component is blank.
        """
        mapping = self._target_mapping(target)
        scheme = self._first_mapping_value(
            mapping,
            (
                "entity_identifier_scheme",
                "item_identifier_scheme",
                "identifier_scheme",
                "scheme",
                "type",
            ),
            target=target,
        )
        value = self._first_mapping_value(
            mapping,
            (
                "entity_identifier_value",
                "item_identifier_value",
                "identifier_value",
                "value",
                "identifier",
            ),
            target=target,
        )
        scheme_text = self._identifier_component(scheme)
        value_text = self._identifier_component(value)
        if scheme_text is None or value_text is None:
            return None
        return scheme_text, value_text

    def _create_identifier_row(
        self,
        *,
        source_row: Row,
        actual_level: str,
        scheme: str,
        value: str,
        primary: int,
        relation_link: Any | None,
    ) -> Row | None:
        """
        Insert an entity identifier for the resolved source Row.

        Optional primary/provenance columns are emitted only when present. Provenance uses a
        truthy link origin or metadata_write_back; creation exceptions yield None.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param source_row: Resolved source database Row whose relations or identifiers are
            being changed.
        :param actual_level: Resolved WEMI entity level for the source Row.
        :param scheme: Stripped identifier scheme to persist.
        :param value: Stripped identifier value to persist.
        :param primary: Truth value normalized to zero or one for an available primary
            column.
        :param relation_link: Optional relation link providing identifier origin provenance.
        :return: Created Row, or None.
        """
        payload = {
            "entity_identifier_entity_type": actual_level,
            "entity_identifier_entity_id": int(source_row.row_id),
            "entity_identifier_scheme": scheme,
            "entity_identifier_value": value,
        }
        if self._has_column("entity_identifiers", "entity_identifier_is_primary"):
            payload["entity_identifier_is_primary"] = int(bool(primary))
        if self._has_column("entity_identifiers", "entity_identifier_provenance"):
            provenance = getattr(relation_link, "origin", None) if relation_link is not None else None
            payload["entity_identifier_provenance"] = provenance or "metadata_write_back"
        try:
            return Row.from_idless_row_dict(
                self.db,
                payload,
                table="entity_identifiers",
                read_only=True,
            )
        except Exception:
            return None

    def _identifier_report_row(self, row: Row) -> dict[str, Any]:
        """
        Describe an entity identifier Row for a write report.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param row: Database Row to inspect or change.
        :return: New dictionary with field/table, stripped scheme/value and row id.
        """
        return {
            "field": "identifiers",
            "table": "entity_identifiers",
            "scheme": self._identifier_component(
                row.row_dict.get("entity_identifier_scheme")
            ),
            "value": self._identifier_component(
                row.row_dict.get("entity_identifier_value")
            ),
            "row_id": int(row.row_id) if row.row_id is not None else None,
        }

    @staticmethod
    def _identifier_values(raw_values: Any) -> tuple[Any, ...]:
        """
        Normalize one scheme's values into a tuple without decoding byte strings.

        Mappings contribute keys, strings/bytes are single values and other iterables are
        consumed. Noniterable scalars become one-element tuples.

        Example:
            >>> LiuXinWEMIMetadataWriter._identifier_values({'one': 1, 'two': 2})
            ('one', 'two')


        :param raw_values: Mapping, scalar, iterable or absent scheme payload.
        :return: Tuple of candidate raw identifier values.
        """
        if raw_values in (None, ""):
            return ()
        if isinstance(raw_values, Mapping):
            return tuple(raw_values.keys())
        if isinstance(raw_values, (str, bytes)):
            return (raw_values,)
        try:
            return tuple(raw_values)
        except TypeError:
            return (raw_values,)

    @staticmethod
    def _identifier_component(value: Any) -> str | None:
        """
        Stringify and strip an identifier component, treating blank values as absent.

        Example:
            >>> LiuXinWEMIMetadataWriter._identifier_component(' doi ')
            'doi'


        :param value: Candidate value to inspect or store.
        :return: Stripped text, or None.
        """
        if value in (None, ""):
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _unsafe_text_reason(value: Any) -> str | None:
        """
        Report the first unsupported C0 control character or surrogate in stringified text.

        Tab, newline and carriage return are allowed. This checks only those code-point
        ranges, not every possible text constraint.

        Example:
            >>> LiuXinWEMIMetadataWriter._unsafe_text_reason('hello' + chr(10)) is None
            True
            >>> LiuXinWEMIMetadataWriter._unsafe_text_reason(chr(0))
            'contains unsupported control character U+0000'


        :param value: Candidate value to inspect or store.
        :return: Reason string naming the code point, or None.
        """
        text = str(value)
        for char in text:
            codepoint = ord(char)
            if codepoint < 0x20 and char not in "\t\n\r":
                return f"contains unsupported control character U+{codepoint:04X}"
            if 0xD800 <= codepoint <= 0xDFFF:
                return f"contains unsupported surrogate U+{codepoint:04X}"
        return None

    @staticmethod
    def _first_mapping_value(
        mapping: Mapping[str, Any],
        keys: tuple[str, ...],
        *,
        target: Any,
    ) -> Any:
        """
        Read the first non-None, nonempty value under candidate keys.

        Object attributes are consulted only when the entire mapping is empty. Zero and
        False remain eligible values.

        Example:
            >>> LiuXinWEMIMetadataWriter._first_mapping_value({'x': 0}, ('x',), target=None)
            0


        :param mapping: Column-keyed values to inspect without changing the mapping.
        :param keys: Candidate keys in precedence order.
        :param target: Object consulted for attributes only when mapping is empty.
        :return: First eligible value, or None.
        """
        for key in keys:
            value = mapping.get(key)
            if value is None and not mapping:
                value = getattr(target, key, None)
            if value not in (None, ""):
                return value
        return None

    def _mark_dirty(self, row: Row, *, reason: str) -> None:
        """
        Best-effort mark a persisted Row dirty, retrying an older signature on TypeError.

        Missing capability/id and ordinary call failures are ignored; the write report is
        not amended.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param row: Database Row to inspect or change.
        :param reason: Reason passed to dirty_record when the source accepts that keyword.
        :return: None.
        """
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
        """
        Read a known table/id pair, returning None for absent ids or ordinary failures.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Database table name used for schema checks or row lookup.
        :param row_id: Optional id candidate converted to int before row lookup.
        :return: Resolved Row, or None.
        """
        if row_id in (None, "") or not self._has_table(table):
            return None
        try:
            return self.db.get_row_from_id(table, int(row_id))
        except Exception:
            return None

    def _search(self, table: str, column: str, value: Any) -> list[Row]:
        """
        Materialize the database search results, suppressing ordinary query/iteration failures.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Database table name used for schema checks or row lookup.
        :param column: Database column name to inspect, search or update.
        :param value: Candidate value to inspect or store.
        :return: List of matching Rows, or an empty list on failure.
        """
        try:
            return list(self.db.search(table=table, column=column, search_term=value))
        except Exception:
            return []

    def _rows_matching_text_policy(
        self,
        spec: LegacyRelationFieldSpec,
        text: str,
    ) -> list[Row]:
        """
        Scan available table Rows for the same normalized comparison key.

        Try iterator_return=True, falling back to the older call signature on TypeError.
        Non-Row entries are skipped; iteration failures discard partial matches.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :param text: Term text to compare, normalize or persist.
        :return: Matching Rows in source order, or an empty list on failure.
        """
        get_all_rows = getattr(self.db, "get_all_rows", None)
        if not callable(get_all_rows):
            return []
        try:
            candidates = get_all_rows(spec.table, iterator_return=True)
        except TypeError:
            try:
                candidates = get_all_rows(spec.table)
            except Exception:
                return []
        except Exception:
            return []

        target_key = self._term_key(text, spec)
        matches: list[Row] = []
        try:
            for row in candidates:
                if not isinstance(row, Row):
                    continue
                if self._term_key(self._row_text(row, spec), spec) == target_key:
                    matches.append(row)
        except Exception:
            return []
        return matches

    def _relation_supported(self, source_table: str, target_table: str) -> bool:
        """
        Ask the driver for a link-table name between two tables.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param source_table: Database table containing the source entity.
        :param target_table: Database table containing candidate relation targets.
        :return: True for a truthy table name; False on lookup failure.
        """
        try:
            return bool(self.db.driver_wrapper.get_link_table_name(source_table, target_table))
        except Exception:
            return False

    def _metadata_bundle_level(self, metadata: Any) -> str | None:
        """
        Infer an individual bundle's level from relation methods and identity attributes.

        Both get_relation_links and relation_names must be callable. The first present
        identity attribute in WEMI order determines the level even if its value is None.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :return: Canonical level, or None when the object is not recognized as a bundle.
        """
        if not callable(getattr(metadata, "get_relation_links", None)):
            return None
        if not callable(getattr(metadata, "relation_names", None)):
            return None
        for level, identity_attr in self._LEVEL_IDENTITY_ATTRIBUTES.items():
            if hasattr(metadata, identity_attr):
                return level
        return None

    def _bundle_relation_names(self, metadata: Any) -> tuple[str, ...]:
        """
        Call relation_names and convert the returned names to strings.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :return: Tuple of relation names, or an empty tuple for missing/failed access.
        """
        relation_names = getattr(metadata, "relation_names", None)
        if not callable(relation_names):
            return ()
        try:
            return tuple(str(name) for name in relation_names())
        except Exception:
            return ()

    def _metadata_wemi_bundle(self, metadata: Any, level: str) -> Any | None:
        """
        Fetch a level through get_wemi_metadata, otherwise through a wemi_stack mapping.

        A callable getter that fails returns None without trying the stack fallback.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param level: Canonical level passed to the getter or stack mapping.
        :return: Current bundle, or None.
        """
        getter = getattr(metadata, "get_wemi_metadata", None)
        if callable(getter):
            try:
                return getter(level)
            except Exception:
                return None
        stack = getattr(metadata, "wemi_stack", None)
        if isinstance(stack, Mapping):
            return stack.get(level)
        return None

    def _bundle_has_relation(self, metadata: Any, relation: str) -> bool:
        """
        Validate a relation name when supported, otherwise test listed relation names.

        A validator that raises returns False without falling through to relation_names.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param metadata: Legacy metadata object, complete WEMI slice or individual WEMI
            bundle supplying desired values.
        :param relation: Canonical relation bucket name.
        :return: Whether the bundle accepts the relation.
        """
        validator = getattr(metadata, "validate_relation_name", None)
        if callable(validator):
            try:
                validator(relation)
                return True
            except Exception:
                return False
        return relation in self._bundle_relation_names(metadata)

    def _has_table(self, table: str) -> bool:
        """
        Check whether either cached schema snapshot includes the table.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Database table name used for schema checks or row lookup.
        :return: True for a known table.
        """
        return table in self._tables or table in self._tables_and_columns

    def _has_column(self, table: str, column: str) -> bool:
        """
        Check the cached table-column mapping for one column.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Database table name used for schema checks or row lookup.
        :param column: Database column name to inspect, search or update.
        :return: True when the column is listed.
        """
        return column in set(self._tables_and_columns.get(table, []))

    def _level_for_table(self, table: str) -> str | None:
        """
        Match a table name to the canonical WEMI table map.

        Names are stringified but not stripped or case-normalized here.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param table: Database table name used for schema checks or row lookup.
        :return: Matching level, or None.
        """
        for level, level_table in self._LEVEL_TABLES.items():
            if str(table) == level_table:
                return level
        return None

    @staticmethod
    def _as_int(value: Any) -> int | None:
        """
        Convert a nonempty candidate to int using Python's normal coercion.

        Example:
            >>> LiuXinWEMIMetadataWriter._as_int('7')
            7
            >>> LiuXinWEMIMetadataWriter._as_int('invalid') is None
            True


        :param value: Candidate value to inspect or store.
        :return: Integer, or None for absent text and common conversion failures.
        """
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError, OverflowError):
            return None

    def _term_key(self, text: Any, spec: LegacyRelationFieldSpec) -> str:
        """
        Compute a comparison key using column normalization metadata when available.

        Without metadata, strip the value and preserve case for case-sensitive columns or
        case-fold otherwise. False input values become empty text in the fallback.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param text: Term text to compare, normalize or persist.
        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :return: Normalized comparison text.
        """
        policy = self._get_column_metadata(spec)
        if policy is not None:
            return self._normalize_text(text, policy.normalization_profile)
        value = str(text or "").strip()
        if self._is_case_sensitive(spec):
            return value
        return value.casefold()

    def _is_case_sensitive(self, spec: LegacyRelationFieldSpec) -> bool:
        """
        Read or cache case sensitivity for the first available configured text column.

        Typed column metadata takes precedence over legacy getters. Missing/failed legacy
        capability defaults to False for compatibility with lightweight adapters.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :return: Whether comparisons should preserve case.
        """
        column = next(
            (name for name in spec.text_columns if self._has_column(spec.table, name)),
            spec.text_columns[0],
        )
        key = (spec.table, column)
        policy = self._get_column_metadata(spec)
        if policy is not None:
            return policy.case_sensitive
        if key in self._column_case_sensitivity:
            return self._column_case_sensitivity[key]

        getter = getattr(self.db, "get_case_sensitivity", None)
        if not callable(getter):
            getter = getattr(self.db, "is_column_case_sensitive", None)
        if callable(getter):
            try:
                case_sensitive = bool(getter(spec.table, column))
            except Exception:
                case_sensitive = False
        else:
            # Preserve the writer's historical case-insensitive behaviour for
            # lightweight adapters that do not yet expose column metadata.
            case_sensitive = False
        self._column_case_sensitivity[key] = case_sensitive
        return case_sensitive

    def _get_column_metadata(
        self,
        spec: LegacyRelationFieldSpec,
    ) -> ColumnMetadata | None:
        """
        Cache typed metadata for the first available configured text column.

        Use the first configured column if none is present. Missing getters, exceptions and
        non-ColumnMetadata results cache None.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :return: ColumnMetadata instance, or None.
        """
        column = next(
            (name for name in spec.text_columns if self._has_column(spec.table, name)),
            spec.text_columns[0],
        )
        key = (spec.table, column)
        if key in self._column_metadata:
            return self._column_metadata[key]

        getter = getattr(self.db, "get_column_metadata", None)
        if callable(getter):
            try:
                metadata = getter(spec.table, column)
            except Exception:
                metadata = None
            if not isinstance(metadata, ColumnMetadata):
                metadata = None
        else:
            metadata = None
        self._column_metadata[key] = metadata
        return metadata

    def _comparison_value(
        self,
        spec: LegacyRelationFieldSpec,
        text: str,
    ) -> tuple[str | None, str | None]:
        """
        Choose a comparison column and normalized value from policy or legacy rules.

        A typed policy is used here only when it supplies a comparison_column.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :param text: Term text to compare, normalize or persist.
        :return: Optional comparison-column/value pair.
        """
        policy = self._get_column_metadata(spec)
        if policy is not None and policy.comparison_column:
            return (
                policy.comparison_column,
                self._normalize_text(text, policy.normalization_profile),
            )
        return spec.norm_column, self._legacy_norm_value(spec, text)

    @staticmethod
    def _normalize_text(
        text: Any,
        profile: ColumnNormalizationProfile,
    ) -> str:
        """
        Normalize stringified text according to an enum profile.

        Profiles support unchanged text, NFC, NFC/trim/casefold, tag-search and title-search
        normalization. False input values become empty strings; unrecognized profiles
        preserve the stringified value.

        Example:
            >>> LiuXinWEMIMetadataWriter._normalize_text(' Café ', ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD)
            'café'


        :param text: Term text to compare, normalize or persist.
        :param profile: ColumnNormalizationProfile selecting the transformation.
        :return: Normalized text.
        """
        value = str(text or "")
        if profile is ColumnNormalizationProfile.NONE:
            return value
        if profile is ColumnNormalizationProfile.UNICODE_NFC:
            return unicodedata.normalize("NFC", value)
        if profile is ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD:
            return unicodedata.normalize("NFC", value).strip().casefold()
        if profile is ColumnNormalizationProfile.TAG_SEARCH_TERM:
            return str(make_tag_search_term(value))
        if profile is ColumnNormalizationProfile.TITLE_SEARCH_TERM:
            return str(make_title_search_term(value))
        return value

    @staticmethod
    def _legacy_norm_value(spec: LegacyRelationFieldSpec, text: str) -> str | None:
        """
        Apply a configured legacy normalization callable when present.

        Example:
            >>> spec = LegacyRelationFieldSpec('term', 'terms', 'terms', ('text',), 'id')
            >>> LiuXinWEMIMetadataWriter._legacy_norm_value(spec, 'Example') is None
            True


        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :param text: Term text to compare, normalize or persist.
        :return: Stringified result, or None for a missing callable or ordinary failure.
        """
        if spec.norm_function is None:
            return None
        try:
            return str(spec.norm_function(text))
        except Exception:
            return None

    @staticmethod
    def _row_text(row: Row, spec: LegacyRelationFieldSpec) -> str | None:
        """
        Return the first configured row text value that is not None or empty text.

        The value is stringified without trimming; false numeric values remain eligible.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param row: Database Row to inspect or change.
        :param spec: Field specification naming the relation table, text/id columns and
            legacy normalization rule.
        :return: Text value, or None.
        """
        for column in spec.text_columns:
            value = row.row_dict.get(column)
            if value not in (None, ""):
                return str(value)
        return None

    @staticmethod
    def _row_ref(row: Row) -> dict[str, Any]:
        """
        Build a compact table/id reference for a report entry.

        Example:
            Exercise this contract with pytest::

                python -m pytest -q tests/metadata/containers/test_item_metadata_hydrator.py


        :param row: Database Row to inspect or change.
        :return: New dictionary with string table name and integer id or None.
        """
        return {
            "table": str(row.table),
            "row_id": int(row.row_id) if row.row_id is not None else None,
        }


__all__ = [
    "LegacyRelationFieldSpec",
    "LiuXinWEMIMetadataWriteReport",
    "LiuXinWEMIMetadataWriter",
]
