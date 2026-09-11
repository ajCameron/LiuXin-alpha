"""
Project Core catalogue reads into shared browse, search, metadata, file, and image payloads.

The backend borrows a surface host for display policy and a CoreSurfaceModel for
schema, rows, and relationship reads. Confirmed absence and explicitly incomplete
optimized queries have empty/fallback paths; query, transport, iteration, and
programming failures remain visible. Payload construction may perform repeated
reads and is not an atomic snapshot or a general JSON-serialization boundary.
Image operations delegate to the shared image backend without decoding pixels.
"""

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
    """
    Build shared surface payloads from borrowed Core queries and host display policies.

    Missing tables/records and explicitly incomplete optimized queries have
    normal empty/fallback paths. Query, transport, and programming failures
    propagate to the caller; fallback is never inferred from an exception.
    Supplied model/image objects are retained unchanged; omitted objects are
    constructed from the host without acquiring ownership of its lifecycle.
    Returned rows and nested payload values are generally not deep-copied.

    Example:
        >>> ReadModelBackend.category_display_name("allbooks")
        'All books'

    :ivar host: Borrowed Core client and surface-specific presentation/search hooks.
    :ivar images: Shared image backend, initialized from host when omitted.
    :ivar model: Shared Core surface model, initialized from host.core when omitted.
    """

    host: ReadModelHostApi
    images: Optional[ImageBackend] = None
    model: CoreSurfaceModel | None = None

    def __post_init__(self) -> None:
        """
        Fill only missing image/model collaborators without querying or validating supplied replacements.

        The image backend receives host, not this instance's model directly; its
        later model selection follows the host's read_model attribute.

        Example:
            >>> backend = ReadModelBackend(host, images=images, model=model)  # doctest: +SKIP


        :return: None after replacing each None collaborator with its default adapter.
        """
        if self.images is None:
            self.images = ImageBackend(self.host)
        if self.model is None:
            self.model = CoreSurfaceModel(self.host.core)

    @property
    def read_source(self) -> CoreSurfaceModel:
        """
        Expose the configured Core surface model under its historical read_source name.

        Example:
            >>> backend.read_source is backend.model  # doctest: +SKIP
            True


        :return: The retained model object without refreshing or copying it.
        :raises AssertionError: If model has been set to None after initialization.
        """

        assert self.model is not None
        return self.model

    def refresh_read_source(self) -> bool:
        """
        Request Core read-source refresh through the retained model without invalidating its schema cache.

        Example:
            >>> refreshed = backend.refresh_read_source()  # doctest: +SKIP


        :return: The model's interpreted refresh receipt, not an independent freshness guarantee.
        """
        return self.read_source.refresh()

    def _table_exists(self, table: str) -> bool:
        """
        Ask the Core model whether a stringified table name occurs in its schema.

        Example:
            >>> exists = backend._table_exists("works")  # doctest: +SKIP


        :param table: Table selector converted to str before the schema check.
        :return: Model membership result; failed schema reads propagate, not False.
        """
        return self.read_source.table_exists(str(table))

    def _all_rows(self, table: str) -> list[object]:
        """
        Materialize every row of a confirmed table, without publishing partial iteration results.

        Example:
            >>> rows = backend._all_rows("works")  # doctest: +SKIP


        :param table: Table selector stringified for membership and row reads.
        :return: New list of original row objects, or an empty list for an absent table.
        """
        if not self._table_exists(table):
            return []
        return list(self.read_source.rows(str(table)))

    def _get_row_from_id(self, table: str, row_id: int) -> object | None:
        """
        Look up an integer-converted row ID only after confirming its table exists.

        Example:
            >>> row = backend._get_row_from_id("works", 7)  # doctest: +SKIP


        :param table: Table name stringified for membership and lookup.
        :param row_id: Identifier converted with int only for a present table.
        :return: Model row or None for an absent table/record; conversion and lookup failures propagate.
        """
        if not self._table_exists(table):
            return None
        return self.read_source.row(str(table), int(row_id))

    def _search_rows(self, table: str, column: str, value: object) -> list[object]:
        """
        Materialize model search results for a present table using the supplied comparison value unchanged.

        Example:
            >>> rows = backend._search_rows("items", "item_manifestation_id", 7)  # doctest: +SKIP


        :param table: Table name stringified for membership and search.
        :param column: Matching column stringified only after the table is confirmed present.
        :param value: Comparison value forwarded without conversion or escaping here.
        :return: New complete result list, or an empty list for an absent table; read/iteration failures propagate.
        """
        if not self._table_exists(table):
            return []
        return list(self.read_source.search(str(table), str(column), value))

    def row_by_id(self, table: str, row_id: int) -> object | None:
        """
        Fetch one row, distinguishing a confirmed absent table/record from a failed lookup.

        Example:
            >>> row = backend.row_by_id("works", 7)  # doctest: +SKIP


        :param table: Catalogue table selected through the model's schema.
        :param row_id: Identifier converted to int when the table exists.
        :return: Original model row or None for absence; schema, conversion, and lookup failures stay visible.
        """
        return self._get_row_from_id(table, row_id)

    def rows_for_table(self, table: str) -> list[object]:
        """
        Return a materialized table through the shared model without host-level fallback.

        Example:
            >>> rows = backend.rows_for_table("works")  # doctest: +SKIP


        :param table: Table selector stringified for schema and row queries.
        :return: New row list, empty for confirmed absence or a successful empty read.
        """
        return self._all_rows(table)

    def table_record_count(self, table: str) -> int:
        """
        Count rows through the model, returning zero immediately for a confirmed absent table.

        Unsupported queries and other failures propagate. Optional callers may
        present Core's explicit ``read_query_unavailable`` code as unavailable.

        Example:
            >>> count = backend.table_record_count("works")  # doctest: +SKIP


        :param table: Table name stringified for schema membership and the count query.
        :return: Model record count, or zero for a confirmed absent table; no row-materialization fallback is attempted.
        """
        if not self._table_exists(table):
            return 0
        return self.read_source.record_count(str(table))

    def search_rows(self, table: str, column: str, value: object) -> list[object]:
        """
        Search a present table and propagate provider errors instead of treating them as no matches.

        Example:
            >>> rows = backend.search_rows("items", "item_manifestation_id", 7)  # doctest: +SKIP


        :param table: Catalogue table stringified for membership and search.
        :param column: Column selector stringified before the model search.
        :param value: Comparison value forwarded unchanged to the model.
        :return: Materialized row list, empty for confirmed table absence or no matches.
        """
        return self._search_rows(table, column, value)

    def _cache_sort_field(self, table: str) -> Optional[str]:
        """
        Select the first host-preferred summary field whose string form is a model column.

        A missing/non-callable preference hook yields no candidates, but columns
        are still requested. Hook and schema failures propagate; no default field
        is synthesized here.

        Example:
            >>> field = backend._cache_sort_field("works")  # doctest: +SKIP


        :param table: Table context supplied unchanged to the preference and column providers.
        :return: Stringified first matching candidate, or None when none qualifies.
        """
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
        """
        Convert a source row and materialize related records, discarding the separate link-metadata result.

        Unrecognized/unsaved rows return before the target-table check. A missing
        target table likewise returns no rows. Conversion and read errors are not
        suppressed or retried through the host.

        Example:
            >>> rows = backend._interlinked_rows(work, "tags")  # doctest: +SKIP


        :param row: CoreRow, attributed legacy row, or mapping accepted by _as_core_row.
        :param secondary_table: Relationship target stringified for schema and related-row queries.
        :return: New list of original related records, or an empty list for unsupported/absent relationships.
        """
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
        """
        Retain a CoreRow or resolve a legacy/mapping row by the first usable table-and-ID hint.

        CoreRow instances return unchanged, including unsaved ones. Otherwise
        table/row_id attributes take precedence; mappings are scanned in model
        table order for a nonempty ID-column value. The first matching lookup is
        final even if it returns None. Numeric conversion and schema/lookup errors
        propagate; arbitrary row values are not copied into a fabricated CoreRow.

        Example:
            >>> backend._as_core_row(core_row) is core_row  # doctest: +SKIP
            True


        :param row: Original row whose class, attributes, or mapping keys supply identity.
        :return: Original CoreRow, model-fetched row, or None without a usable/resolved identity.
        """
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
        """
        Fetch linked records for a recognized saved row without exposing link metadata.

        Example:
            >>> tags = backend.interlinked_rows(work, "tags")  # doctest: +SKIP


        :param row: Core, legacy, or mapping row converted through _as_core_row.
        :param secondary_table: Requested relationship target table.
        :return: Materialized related records, empty for unrecognized/unsaved rows or an absent target; failures propagate.
        """
        return self._interlinked_rows(row, secondary_table)

    def related_rows_by_table(self, row) -> dict[str, list[object]]:
        """
        Group related records using ordered host table selection or the host's compatibility grouping hook.

        Example:
            >>> related = backend.related_rows_by_table(work)  # doctest: +SKIP


        :param row: Source entity passed to the host's relationship-selection policy.
        :return: Nonempty fetched groups for an ordered hook, or the fallback host mapping unchanged.
        """
        return self._related_rows_by_table(row)

    def _related_rows_by_table(self, row) -> dict[str, list[object]]:
        """
        Fetch host-ordered related tables, omitting empty groups, or delegate grouping when no ordered hook exists.

        The compatibility fallback is returned directly without copying/filtering;
        it must not simply call back into this method. Ordered duplicate names are
        queried again and later nonempty groups replace earlier values.

        Example:
            >>> related = backend._related_rows_by_table(work)  # doctest: +SKIP


        :param row: Source row passed unchanged to ordered/fallback host hooks and relationship lookup.
        :return: Insertion-ordered string-table mapping, or the host fallback's original result.
        """
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
        """
        Query work-level agents and sort projected credits by role text, descending numeric priority, then source position.

        Only recognized saved works issue catalog.agents.list. Nonmapping receipts
        or missing agents mean no entries; nonmapping agent elements are skipped.
        Agent values and _catalog_link are shallow-copied. The first existing
        agents/human_agents/org_agents table whose ID column is present wins;
        otherwise agents is the fallback, skipped if absent. A missing ID can
        produce an unsaved CoreRow; invalid non-None IDs raise during int conversion.

        The optional host role formatter supplies display text, otherwise a falsey
        raw role becomes Contributors. Invalid numeric priorities use source
        position as their sort component; raw priority and role are still retained.
        Provider errors remain visible, with no relationship-based credit fallback.

        Example:
            >>> credits = backend._work_credit_entries(work)  # doctest: +SKIP


        :param row: Work identity converted through _as_core_row before the Core agent query.
        :return: New sorted credit dicts containing table, CoreRow, display/raw role, raw priority, and sort_key.
        """
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
        """
        Expose role/priority-ordered work-agent credits for catalogue and protocol consumers.

        Example:
            >>> credits = backend.work_credit_entries(work)  # doctest: +SKIP


        :param row: Work row whose saved Core identity selects the agent-list query.
        :return: Fresh credit-entry list, empty for unsupported identity or a successful empty agent result.
        """
        return self._work_credit_entries(row)

    @staticmethod
    def category_display_name(category: str) -> str:
        """
        Translate built-in category tokens to fixed English labels or title-case an underscore-separated fallback.

        Lookup strips and lowercases a falsey-to-empty string conversion. The
        fallback uses the original converted spelling without stripping whitespace.

        Example:
            >>> ReadModelBackend.category_display_name(" ALLBOOKS ")
            'All books'
            >>> ReadModelBackend.category_display_name("reading_lists")
            'Reading Lists'


        :param category: Category-like value stringified after a falsey-to-empty fallback.
        :return: Built-in label or title-cased fallback text; not a localized display string.
        """
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
        """
        List present agent tables in preferred schema order, with agents as an unconditional final fallback.

        Example:
            >>> tables = backend.author_tables()  # doctest: +SKIP


        :return: Existing agents/human_agents/org_agents names, or ['agents'] even when none exists.
        """
        tables = []
        for table in ("agents", "human_agents", "org_agents"):
            if self._table_exists(table):
                tables.append(table)
        return tables or ["agents"]

    def tag_category_table(self) -> Optional[str]:
        """
        Prefer populated tags, otherwise labels when present, retaining an empty tags-only table as a valid category source.

        Count tags only when it appears in the schema; count/schema errors remain
        visible even when labels is available. Labels is selected without a count.

        Example:
            >>> table = backend.tag_category_table()  # doctest: +SKIP


        :return: tags or labels according to availability/count precedence, otherwise None.
        """
        available = set(self.read_source.table_names())
        if "tags" in available:
            if self.table_record_count("tags") > 0 or "labels" not in available:
                return "tags"
        if "labels" in available:
            return "labels"
        return "tags" if "tags" in available else None

    def work_tag_rows(self, related_rows_by_table: dict[str, list[object]]) -> tuple[Optional[str], list[object]]:
        """
        Select the global tag-category table and copy only that table's supplied related-row list.

        Rows from the other tag/label table are not combined or used as a fallback.

        Example:
            >>> table, tags = backend.work_tag_rows(related)  # doctest: +SKIP


        :param related_rows_by_table: Already discovered relationship groups, not mutated here.
        :return: Selected table and a new list of its original row objects, or (None, []) without a tag source.
        """
        tag_table = self.tag_category_table()
        if tag_table is None:
            return None, []
        return tag_table, list(related_rows_by_table.get(tag_table, []))

    def work_rows(self, *, sorted_by: str) -> list[object]:
        """
        Return ordered works, preferring a complete Core query and otherwise materializing and sorting all rows.

        Only exact recent selects descending work ID; all other values use the
        first available preferred summary field ascending, then lowercase host
        title for the materialized fallback. An incomplete optimized result is
        discarded, not combined with fallback rows. Exceptions do not trigger
        fallback. Recent-ID conversion failures remain visible.

        Example:
            >>> rows = backend.work_rows(sorted_by="recent")  # doctest: +SKIP


        :param sorted_by: Exact recent for newest-ID order, or another token for title-like order.
        :return: New ordered row list, empty for a confirmed absent works table.
        """
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
        """
        Return a work page and total, preferring a complete ordered Core query.

        An explicitly incomplete result or missing sort field selects the
        materialized fallback. Query failures propagate instead of retrying.
        Optimized offsets/limits are int-converted and clamped to zero; fallback
        slices use the original arguments unchanged, including Python negative
        slice semantics. Fallback calls work_rows, which may issue another sort
        query before materializing. A confirmed absent table returns before any
        pagination conversion.

        Example:
            >>> rows, total = backend.work_page(sorted_by="title", limit=20, offset=0)  # doctest: +SKIP


        :param sorted_by: Exact recent selects descending work ID; other tokens select title-like sorting.
        :param limit: Requested page length, clamped only on the optimized query path.
        :param offset: Requested start position, clamped only on the optimized query path.
        :return: New visible-row list and reported total count, or ([], 0) for an absent works table.
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
        """
        Resolve an entity's stripped textual integer ID and return its grouped works relationship.

        Check table existence before parsing. Ordinary integer-conversion errors,
        a missing record, and a missing works group produce an empty list; other
        lookup/grouping failures propagate. No positive-ID constraint is imposed.

        Example:
            >>> works = backend.works_for_linked_entity("tags", " 7 ")  # doctest: +SKIP


        :param table: Entity table checked and queried before traversing its relationships.
        :param raw_row_id: Value converted through str(...).strip() and then int.
        :return: New list of linked work objects, or an empty list for absent/unusable identity.
        """
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
        """
        Build browse entries for exact work, author, tag, or series category tokens.

        allbooks/titles use title ordering; recent/newest use descending work ID.
        Author entries are sorted within each preferred agent table, not globally
        across tables. Tags use the selected tags/labels source; series requires
        its table. Entity counts materialize linked works, while work entries have
        count zero. Unknown or differently cased tokens return no entries.

        Example:
            >>> entries = backend.category_rows("authors")  # doctest: +SKIP


        :param kind: Exact built-in category token, not normalized by this method.
        :return: Fresh entry dicts with table, original row, raw ID, label, count, and host URL or empty text.
        """
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
        """
        Count materialized category entries or works rather than issuing a lightweight table count.

        Only exact recent requests recent work ordering here; newest still counts
        title-ordered works. Entity counts include the category builder's linked
        work reads and can therefore fail on those reads too.

        Example:
            >>> count = backend.browse_count("titles")  # doctest: +SKIP


        :param kind: Exact authors/tags/series or titles/recent/allbooks/newest token.
        :return: Number of category entities or works, or zero for an unknown token.
        """
        if kind in {"authors", "tags", "series"}:
            return len(self.category_rows(kind))
        if kind in {"titles", "recent", "allbooks", "newest"}:
            sorted_by = "recent" if kind == "recent" else "title"
            return len(self.work_rows(sorted_by=sorted_by))
        return 0

    def category_summary_payload(self) -> list[dict[str, object]]:
        """
        Build the fixed five-category navigation summary with fresh materialized counts.

        All books and newest are marked as work lists; authors, tags, and series
        are marked as categories. Reads are repeated per entry, not shared as a
        single snapshot, and failures prevent returning a partial summary.

        Example:
            >>> summary = backend.category_summary_payload()  # doctest: +SKIP


        :return: Ordered name/count/is_category/category dicts for allbooks, newest, authors, tags, and series.
        """
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
        """
        Normalize a category request, sort its entries, and return shallow-copied visible entry dicts.

        popularity sorts by integer count then lowercase label; rating uses a
        constant zero then label, not stored ratings. Other tokens sort by label.
        Only normalized desc reverses the whole key, including ties. Pagination
        uses unvalidated Python slicing; row objects nested in copied entries stay
        shared. Unknown sort tokens are preserved in response metadata.

        Example:
            >>> page = backend.category_items_payload("tags", num=20, offset=0, sort="name", sort_order="asc")  # doctest: +SKIP


        :param category: Falsey-to-empty, stripped lowercase category selector.
        :param num: Requested slice length, without clamping or integer conversion here.
        :param offset: Slice start also echoed unchanged in the result.
        :param sort: Falsey-to-name, stripped lowercase ordering selector.
        :param sort_order: Falsey-to-asc direction; only stripped lowercase desc means descending.
        :return: Category name, full count, actual visible count, normalized ordering, offset, and visible entries.
        """
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
        """
        Project one entity's identity, display strings, and host-provided HTML link without fetching it anew.

        Example:
            >>> summary = backend.entity_summary_payload("works", work)  # doctest: +SKIP


        :param table: Schema/display context passed unchanged to all host hooks.
        :param row: Row supplying the host-selected ID column and display values.
        :return: New id/table/primary/label/html_url mapping retaining raw ID and URL values, including None.
        """
        row_id = _row_value(row, self.host._id_column(table) or "")
        return {
            "id": row_id,
            "table": table,
            "primary": self.host._row_primary_text(table, row),
            "label": self.host._row_label(table, row),
            "html_url": self.host._row_href(table, row),
        }

    def related_payload(self, row) -> dict[str, list[dict[str, object]]]:
        """
        Fetch relationship groups and project each linked entity into its summary payload.

        Example:
            >>> related = backend.related_payload(work)  # doctest: +SKIP


        :param row: Source entity supplied to ordered or compatibility relationship grouping.
        :return: New string-table mapping of summary lists; provider/projection failures propagate without partial return.
        """
        payload: dict[str, list[dict[str, object]]] = {}
        for table, rows in self._related_rows_by_table(row).items():
            payload[str(table)] = [self.entity_summary_payload(str(table), linked_row) for linked_row in rows]
        return payload

    def work_subtitle(self, row) -> str:
        """
        Join up to three credited names, two series names, and three selected tag names into a compact plain-text subtitle.

        Credit ordering follows role/priority, not an author-role filter. Credits
        and relationships are read separately; tag-table selection may query the
        schema/count again. Text uses fixed English prefixes and a middle-dot
        separator without HTML escaping or clipping individual names.

        Example:
            >>> subtitle = backend.work_subtitle(work)  # doctest: +SKIP


        :param row: Work identity used for credit and relationship reads.
        :return: Joined by/Series:/Tags: parts, or empty text when all selected groups are empty.
        """
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
        """
        Compute a normalized work sorting key from display text, bounded relationship names, or numeric identity.

        title uses lowercase display text; author joins the first four credits,
        series the first three names, and tags the first five selected tag names.
        Joined keys use a spaced vertical bar and lowercase conversion. Every
        other selector, including the falsey default date, uses integer work ID
        with a falsey-to-zero fallback rather than an actual timestamp.

        Example:
            >>> key = backend.work_sort_value(work, sort_key="author")  # doctest: +SKIP


        :param row: Work row used for display/identity and any selected relationship reads.
        :param sort_key: Falsey-to-date, stripped lowercase selector; unrecognized values use ID order.
        :return: Lowercase string key or integer work ID; lookup and nonstandard conversion failures propagate.
        """
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
        """
        Discover direct and expression/manifestation/item-linked files, deduplicating by integer file ID.

        Direct rows are visited first; later duplicate IDs replace their row
        without moving its original position. Missing/empty IDs and ordinary
        numeric-conversion failures are skipped, but positive IDs are not required.
        Missing manifestation/item IDs prune those branches; other values are
        passed unchanged to search. Discovery does not filter by role, format,
        readability, or download capability, and provider failures propagate.

        Example:
            >>> files = backend.work_file_rows(related)  # doctest: +SKIP


        :param related_rows_by_table: Existing direct-file and expression groups traversed without mutation here.
        :return: Original file row objects in first-ID discovery order, with later duplicate values retained.
        """
        file_rows_by_id: dict[int, object] = {}

        def add_file_row(file_row) -> None:
            """
            Retain a discovered file under its integer ID, skipping only absent IDs and common conversion failures.

            Example:
                >>> add_file_row({"file_id": "7"})  # doctest: +SKIP


            :param file_row: Subscriptable file row supplying file_id for deduplication.
            :return: None after insertion/replacement or an unusable-ID skip; other row/conversion errors propagate.
            """
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
        """
        Combine raw file metadata with host naming/capability policy and an optional integer size.

        Download and preview URLs depend on capability truthiness, not a separate
        ID-validity or payload-read check. Size prefers truthy file_size_bytes,
        falling back to file_size even when the first field is zero. Absent size
        and TypeError/ValueError/OverflowError conversions become None; other
        failures remain visible. No file content is opened by this method itself.

        Example:
            >>> summary = backend.file_summary_payload(file_row)  # doctest: +SKIP


        :param file_row: File row projected through host capability/name hooks and direct field access.
        :return: New identity, name, store/provenance, capability, route, and size mapping without deep-copy guarantees.
        """
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
        """
        Extend a file summary with linked-entity summaries and selected storage metadata.

        Example:
            >>> detail = backend.file_detail_payload(file_row)  # doctest: +SKIP


        :param file_row: File row used for summary, relationship, and storage-field projection.
        :return: Summary plus related and file mappings; extension prefers a truthy current extension over its original value.
        """
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
        """
        Build the compatibility book-metadata shape from a work, discovered files, credits, and selected facets.

        Supplied relationship groups are reused for formats/tags/series, including
        an explicitly empty dict. Credits and two subtitle calls still perform
        their own reads, so this is not a single cached or atomic snapshot.

        Files are ordered by lowercase suggested filename. Its final suffix gives
        the uppercase format token, default FILE; duplicate tokens remain in the
        formats list while the last matching file wins format_metadata. The first
        sorted file selects main_format. Download URLs are advertised regardless
        of downloadability, while preview URLs depend on preview_kind. Size uses
        the same truthy byte-size fallback and narrow conversion handling as file
        summaries. All credits become authors; only the first series is retained.

        Missing rating/date/time/series-index values are explicit None placeholders.
        Cover, thumbnail, and book routes are synthesized using HTML escaping,
        not URL quoting or an existence check. Work IDs are int-converted when
        nonempty; conversion/provider failures propagate rather than yielding a
        partial metadata dict.

        Example:
            >>> metadata = backend.work_metadata_payload(work, related_rows_by_table=related)  # doctest: +SKIP


        :param row: Work row supplying identity, display title, and optional sort title.
        :param related_rows_by_table: Optional precomputed relationship groups; None requests discovery, not a falsey test.
        :return: Fresh compatibility metadata mapping with format details, facets, synthesized routes, and placeholder fields.
        """
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
        """
        Assemble compatibility work metadata, projected credits, file summaries, and related entity summaries.

        Each section performs its own reads; no relationship/credit snapshot is
        shared across the whole result. Credit entity_type prefers truthy agent,
        human-agent, then organization-agent type fields and strips the result.
        Rows follow credit/file discovery ordering rather than a new global sort.

        Example:
            >>> detail = backend.work_detail_payload(work)  # doctest: +SKIP


        :param row: Work row used for repeated metadata, relationship, and credit projection.
        :return: New work/credits/files/related mapping; any constituent failure prevents a normal return.
        """
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
        """
        Stable-sort a copy of the supplied work list using normalized key and direction selectors.

        Only desc reverses order; other direction tokens use ascending order.
        Unknown keys use numeric work identity through work_sort_value. Sorting
        may read credits/relationships and does not mutate the caller's list.

        Example:
            >>> ordered = backend.sorted_work_rows(rows, sort="title", sort_order="asc")  # doctest: +SKIP


        :param rows: Work objects to retain by reference in the new sorted list.
        :param sort: Falsey-to-title selector normalized with stripping and lowercase conversion.
        :param sort_order: Falsey-to-asc direction normalized before exact desc comparison.
        :return: New ordered list; ties preserve input order and failed keys propagate.
        """
        sort_key = str(sort or "title").strip().lower()
        sort_order_text = str(sort_order or "asc").strip().lower()
        ascending = sort_order_text != "desc"
        return sorted(rows, key=lambda row: self.work_sort_value(row, sort_key=sort_key), reverse=not ascending)

    def work_list_payload(self, rows: list[object], *, num: int, offset: int, sort: str, sort_order: str) -> dict[str, object]:
        """
        Sort and slice supplied works, exposing both visible rows and their usable integer IDs.

        Pagination uses the raw Python slice without clamping. Missing/empty IDs
        are omitted only from book_ids, so num counts IDs and may differ from the
        visible rows length. Invalid nonempty IDs raise. The response preserves
        normalized unknown sort/direction tokens even though sorting uses its
        fallback key/direction behavior.

        Example:
            >>> page = backend.work_list_payload(rows, num=20, offset=0, sort="title", sort_order="asc")  # doctest: +SKIP


        :param rows: Complete input work list, not mutated by sorting or slicing.
        :param num: Requested visible slice length, not an independently validated count.
        :param offset: Raw slice start echoed in the payload.
        :param sort: Falsey-to-title, stripped lowercase selector echoed after normalization.
        :param sort_order: Falsey-to-asc, stripped lowercase direction echoed even when unknown.
        :return: Full row count, visible-ID count/list, visible original row objects, offset, and ordering metadata.
        """
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
        """
        Project each work to metadata keyed by its stringified resulting ID, letting later duplicate keys replace values.

        None/empty IDs are not filtered after metadata generation. Duplicate keys
        keep their original insertion position; failures stop construction without
        returning a partial mapping.

        Example:
            >>> metadata_by_id = backend.books_metadata_payload(rows)  # doctest: +SKIP


        :param rows: Work rows processed in input iteration order.
        :return: New string-ID mapping to independently generated work metadata dicts.
        """
        payload: dict[str, dict[str, object]] = {}
        for row in rows:
            metadata = self.work_metadata_payload(row)
            payload[str(metadata["id"])] = metadata
        return payload

    def search_entries(self, query_text: str, *, table_filter: str = "") -> list[dict[str, object]]:
        """
        Query candidate tables and use host matching/ranking policy to build a globally sorted search-entry list.

        Blank normalized text returns before schema access. A truthy existing
        table filter narrows the search; an absent/unknown filter falls back to
        host public tables or works. Without a callable entry builder, return no
        entries. Per-table candidate columns are optional; complete query records
        are used directly, while explicitly incomplete queries materialize the
        table for host-side matching. Exceptions never trigger fallback.

        Every non-None builder result is retained and sorted by its sort_key.
        There is no result limit, deduplication, or automatic normalization of a
        table filter. Provider/iteration/builder errors remain visible.

        Example:
            >>> entries = backend.search_entries("雪", table_filter="works")  # doctest: +SKIP


        :param query_text: Falsey-to-empty text stringified and stripped before searching and host matching.
        :param table_filter: Optional exact table selector, used only if truthy and confirmed present.
        :return: New ranked list of original builder-returned entry dicts, or an empty list for a normal no-search/no-match outcome.
        """
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
        """
        Slice ranked search entries into public summaries while counting groups across the full result set.

        Query/filter/pagination metadata is echoed as supplied, although searching
        strips query text internally. Slicing is unvalidated. Missing/falsey score
        becomes zero before int conversion; snippet and match-column values become
        text with an empty fallback. HTML links are host values, not rewritten.

        Example:
            >>> result = backend.search_results_payload(query_text="雪", table_filter="works", limit=20, offset=0)  # doctest: +SKIP


        :param query_text: Search text echoed unchanged and normalized only by search_entries.
        :param table_filter: Table-selection hint echoed even if search falls back to public tables.
        :param limit: Raw requested slice length and echoed limit.
        :param offset: Raw slice start and echoed offset.
        :return: Query/filter metadata, visible result summaries, all-result group counts, total, limit, and offset.
        """
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
        """
        Delegate direct and expression/item-linked image discovery to the retained image backend.

        Example:
            >>> images = backend.work_image_rows(related)  # doctest: +SKIP


        :param related_rows_by_table: Existing relationship groups forwarded unchanged to image discovery.
        :return: Image backend's deduplicated row list without an additional copy or error fallback.
        """
        return self.images.work_image_rows(related_rows_by_table)

    def image_download_name(self, image_row) -> str:
        """
        Delegate suggested image filename selection without adding sanitization or validation.

        Example:
            >>> name = backend.image_download_name(image_row)  # doctest: +SKIP


        :param image_row: Image row forwarded unchanged to the shared naming policy.
        :return: Backend-selected name, normally preferring image/original/storage names before cover.bin.
        """
        return self.images.image_download_name(image_row)

    def image_content_type(self, image_row) -> str:
        """
        Delegate declared-or-filename-guessed image MIME selection without inspecting image bytes.

        Example:
            >>> content_type = backend.image_content_type(image_row)  # doctest: +SKIP


        :param image_row: Image row forwarded unchanged to the image backend.
        :return: Backend MIME result, normally application/octet-stream when no type can be inferred.
        """
        return self.images.image_content_type(image_row)

    def image_storage_lookup_metadata(self, image_row) -> dict[str, object]:
        """
        Delegate shallow image metadata projection with compatibility file-field aliases.

        Example:
            >>> metadata = backend.image_storage_lookup_metadata(image_row)  # doctest: +SKIP


        :param image_row: Image row whose projected fields feed storage lookup metadata.
        :return: Image backend's metadata dict without another copy or alias transformation.
        """
        return self.images.image_storage_lookup_metadata(image_row)

    def resolve_storage_image(self, image_row):
        """
        Delegate readable-image resolution without reading content or disguising resolution failures.

        Example:
            >>> stored = backend.resolve_storage_image(image_row)  # doctest: +SKIP


        :param image_row: Image identity forwarded to the shared acquisition-backed resolver.
        :return: Backend's bound byte reader, or None for its missing/invalid/unreadable outcomes.
        """
        return self.images.resolve_storage_image(image_row)

    def resolve_image_target(self, image_row) -> Optional[_ResolvedFileTarget]:
        """
        Delegate image redirect resolution, retaining the image backend's delivery policy and visible failures.

        Example:
            >>> redirect = backend.resolve_image_target(image_row)  # doctest: +SKIP


        :param image_row: Image row used for identity and fallback download-name selection.
        :return: Backend redirect target or None; this wrapper does not validate location or readability.
        """
        return self.images.resolve_image_target(image_row)

    def work_image_row(self, work_row) -> Optional[object]:
        """
        Delegate selection of the first discovered image for a work without imposing a separate cover-role ranking.

        Example:
            >>> image = backend.work_image_row(work)  # doctest: +SKIP


        :param work_row: Work row forwarded unchanged for relationship/image discovery.
        :return: Image backend's selected original row, or None after successful empty discovery.
        """
        return self.images.work_image_row(work_row)

    @staticmethod
    def thumbnail_text(text: str) -> str:
        """
        Use ImageBackend's static Unicode-initial policy independently of any injected image collaborator.

        Example:
            >>> ReadModelBackend.thumbnail_text(" -- ßeta")
            'SS'


        :param text: Title-like value from which the first alphanumeric character is selected.
        :return: Uppercase initial, possibly multiple characters, or ? when no alphanumeric character exists.
        """
        return ImageBackend.thumbnail_text(text)

    def placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        """
        Delegate SVG cover-fallback generation with unchanged dimensions and work context.

        Example:
            >>> svg = backend.placeholder_cover_svg(work, width=120, height=180)  # doctest: +SKIP


        :param work_row: Work row used by the image backend's host for display text.
        :param width: Requested SVG width forwarded without wrapper-side clamping.
        :param height: Requested SVG height forwarded without wrapper-side clamping.
        :return: Image backend's encoded SVG bytes without raster rendering or extra escaping.
        """
        return self.images.placeholder_cover_svg(work_row, width=width, height=height)
