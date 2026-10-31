"""Search WEMI entities and their attached metadata through database read APIs.

Catalog owns locations, matching and traversal semantics. SQL execution stays in
portable macros and repositories. Construction is passive; each find call uses
fresh read caches and returns rows from exactly one WEMI level.

Traversal moves only toward ancestors or only toward descendants from the result
entity. It never turns around at a shared edition to borrow a sibling Work's
metadata. All paths participate, including incomplete paths and multiple Works
embodied in a Manifestation; preferred-ray projections are not search indexes.

This is a scan implementation, not an indexed full-text engine. Pagination limits
returned matches, not database work. Reads do not establish a snapshot transaction;
the caller owns database lifetime and any required consistency scope.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from LiuXin_alpha.catalog.api.common import (
    CatalogNotFoundError,
    DatabaseHandle,
    RowMapping,
    WemiLevel,
)
from LiuXin_alpha.catalog.repositories.base import WEMI_TABLES
from LiuXin_alpha.metadata.text_identity import normalize_identity_text
from LiuXin_alpha.utils.search_query_parser import ParseException, Parser


_LEVELS = tuple(WEMI_TABLES)
_TEXT_PROFILE = "unicode_nfkc_whitespace_casefold"
_ENTITY_COLUMNS = {
    "work": (
        "work_title",
        "work_canonical_title",
        "work_sort_title",
        "work_creator_sort",
        "work_type",
        "work_medium",
        "work_original_year",
        "work_audience",
        "work_discovery_note",
    ),
    "expression": (
        "expression_label",
        "expression_title_override",
        "expression_subtitle",
        "expression_type",
        "expression_mode",
        "expression_year",
        "expression_origin_note",
    ),
    "manifestation": (
        "manifestation_subtitle",
        "manifestation_carrier_type",
        "manifestation_format_detail",
        "manifestation_edition_statement",
        "manifestation_pub_year",
        "manifestation_region_code",
        "manifestation_note",
    ),
    "item": (
        "item_inventory_code",
        "item_location",
        "item_source_name",
        "item_source_detail",
        "item_source_path",
        "item_type",
        "item_condition",
    ),
}
_TITLE_COLUMNS = {
    "work": ("work_title", "work_canonical_title", "work_sort_title"),
    "expression": ("expression_title_override", "expression_subtitle"),
    "manifestation": ("manifestation_subtitle",),
    "item": (),
}
_TERM_COLUMNS = {
    "tag": ("tags", ("tag", "tag_description")),
    "label": ("labels", ("label_text", "label_description")),
    "genre": ("genres", ("genre", "genre_full")),
    "subject": ("subjects", ("subject", "subject_full")),
    "series": ("series", ("series", "series_full")),
    "language": (
        "languages",
        (
            "language",
            "language_code",
            "language_iso639_1",
            "language_iso639_2_b",
            "language_iso639_2_t",
            "language_bcp47_primary",
            "language_bcp47_variants",
        ),
    ),
}
_PROSE_COLUMNS = {
    "note": ("notes", "note"),
    "comment": ("comments", "comment"),
    "synopsis": ("synopses", "synopsis"),
}
_AGENT_ROLES = {"agent": None, "author": "aut", "translator": "trl", "publisher": "pbl"}
_FIELDS = ("title", *_TERM_COLUMNS, *_PROSE_COLUMNS, *_AGENT_ROLES, "identifier")
_ALIASES = {
    "titles": "title",
    "tags": "tag",
    "labels": "label",
    "genres": "genre",
    "subjects": "subject",
    "languages": "language",
    "notes": "note",
    "comments": "comment",
    "synopses": "synopsis",
    "agents": "agent",
    "creators": "agent",
    "authors": "author",
    "translators": "translator",
    "publishers": "publisher",
    "identifiers": "identifier",
}
_LOCATIONS = {"all": (None, "all")}
for _field in (*_FIELDS, *_ALIASES):
    _LOCATIONS[_field] = (None, _ALIASES.get(_field, _field))
for _level, _table in WEMI_TABLES.items():
    for _prefix in (_level, _table):
        _LOCATIONS[_prefix] = (_level, "all")
        for _field in (*_FIELDS, *_ALIASES):
            _LOCATIONS[f"{_prefix}.{_field}"] = (
                _level,
                _ALIASES.get(_field, _field),
            )


def _text(value: object) -> str:
    return normalize_identity_text(str(value), _TEXT_PROFILE)


class CatalogSearch:
    """Find WEMI rows using Boolean queries and semantic field locations.

    Example:
        >>> catalog.search.find('title:"Dune" and author:"Herbert"')  # doctest: +SKIP
        >>> catalog.search.find('work.title:"Dune"', level="item")  # doctest: +SKIP

    Terms use normalized substring matching; an initial ``=`` requests equality.
    AND, OR, NOT, parentheses and quoted phrases use the shared query parser.
    Unqualified fields inspect all ancestors and descendants of each result.
    ``work.tag:fiction`` restricts that field to reachable Works. Boolean terms
    are evaluated independently for each result, not correlated to one child row.
    """

    def __init__(self, db: DatabaseHandle, repositories: Any, hierarchy: Any) -> None:
        """Retain borrowed read dependencies without accessing the database."""
        self.db = db
        self.repositories = repositories
        self.hierarchy = hierarchy

    def find(
        self,
        query: str,
        *,
        level: WemiLevel = "work",
        limit: int | None = 100,
        offset: int = 0,
    ) -> tuple[RowMapping, ...]:
        """Return matching rows at one level, ordered by ascending entity ID.

        An empty query lists that level without reading related metadata. Queries
        and paging arguments are validated before database access. Missing linked
        records and database errors propagate; failures never become empty hits.
        Result dictionaries are copies and cannot write through to the database.

        :param query: Boolean search text; unknown colon prefixes remain literal.
        :param level: Work, Expression, Manifestation or Item; defaults to Work.
        :param limit: Maximum returned matches, or None for all; zero returns none.
        :param offset: Nonnegative count of matching rows to skip.
        :return: Tuple of row mappings using the selected level's physical columns.
        :raises ParseException: Invalid syntax or an empty located/equality term.
        :raises TypeError: Query or paging arguments have the wrong type.
        :raises ValueError: Level is unknown or a paging argument is negative.
        """
        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        if not isinstance(query, str):
            raise TypeError("query must be a string")
        for name, value in (("limit", limit), ("offset", offset)):
            if name == "limit" and value is None:
                continue
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError(f"{name} must be an integer")
            if value < 0:
                raise ValueError(f"{name} cannot be negative")
        try:
            tree = Parser().parse(query, _LOCATIONS) if query.strip() else None
            if tree is not None:
                self._validate_terms(tree)
        except RecursionError as exc:
            raise ParseException("Search expression is too deeply nested") from exc
        if limit == 0:
            return ()
        context = _SearchReads(self)
        rows = self.db.macros.get_rows(
            WEMI_TABLES[level],
            order_by=(f"{level}_id",),
        )
        matches: list[RowMapping] = []
        skipped = 0
        for row in rows:
            if tree is not None and not context.matches(tree, level, row):
                continue
            if skipped < offset:
                skipped += 1
                continue
            matches.append(dict(row))
            if limit is not None and len(matches) >= limit:
                break
        return tuple(matches)

    @staticmethod
    def _validate_terms(tree: list) -> None:
        if tree[0] == "token":
            term = tree[2][1:] if tree[2].startswith("=") else tree[2]
            if not term.strip():
                raise ParseException("Search terms cannot be empty")
        else:
            for child in tree[1:]:
                CatalogSearch._validate_terms(child)


class _SearchReads:
    """Cache reads only for one query, without hiding missing rows or failures."""

    def __init__(self, search: CatalogSearch) -> None:
        self.search = search
        self.adjacencies: dict[tuple, tuple] = {}
        self.paths: dict[tuple, tuple] = {}
        self.values: dict[tuple, tuple[str, ...]] = {}
        self.related: dict[tuple, tuple] = {}

    def matches(self, tree: list, level: WemiLevel, root: RowMapping) -> bool:
        operator = tree[0]
        if operator == "and":
            return self.matches(tree[1], level, root) and self.matches(
                tree[2], level, root
            )
        if operator == "or":
            return self.matches(tree[1], level, root) or self.matches(
                tree[2], level, root
            )
        if operator == "not":
            return not self.matches(tree[1], level, root)
        target, field = _LOCATIONS[tree[1]]
        raw = tree[2]
        exact = raw.startswith("=")
        term = _text(raw[1:] if exact else raw)
        for target_level in (target,) if target is not None else _LEVELS:
            for row in self.reachable(level, root, target_level):
                for value in self.field_values(target_level, row, field):
                    if (term == value) if exact else (term in value):
                        return True
        return False

    def reachable(self, level: WemiLevel, root: RowMapping, target: WemiLevel) -> tuple:
        """Follow a monotonic WEMI path; never expand to siblings via a shared node."""
        key = (level, root[f"{level}_id"], target)
        if key not in self.paths:
            current = (root,)
            start, end = _LEVELS.index(level), _LEVELS.index(target)
            step = 1 if end > start else -1
            direction = "children" if step == 1 else "parents"
            for index in range(start, end, step):
                source_level = _LEVELS[index]
                destination = _LEVELS[index + step]
                found = {}
                for row in current:
                    adjacency_key = (source_level, row[f"{source_level}_id"], direction)
                    if adjacency_key not in self.adjacencies:
                        read = getattr(self.search.hierarchy, direction)
                        self.adjacencies[adjacency_key] = read(
                            level=source_level,
                            entity_id=adjacency_key[1],
                        ).entities
                    for related in self.adjacencies[adjacency_key]:
                        found[related[f"{destination}_id"]] = related
                current = tuple(found.values())
            self.paths[key] = current
        return self.paths[key]

    def field_values(
        self, level: WemiLevel, row: RowMapping, field: str
    ) -> tuple[str, ...]:
        key = (level, row[f"{level}_id"], field)
        if key not in self.values:
            self.values[key] = tuple(
                _text(value) for value in self.raw_values(level, row, field)
            )
        return self.values[key]

    @staticmethod
    def columns(row: RowMapping, columns: Iterable[str]) -> Iterable[object]:
        for column in columns:
            value = row.get(column)
            if value is not None and value != "":
                yield value

    def raw_values(
        self, level: WemiLevel, row: RowMapping, field: str
    ) -> Iterable[object]:
        entity_id = row[f"{level}_id"]
        repositories = self.search.repositories
        if field == "all":
            yield from self.columns(row, _ENTITY_COLUMNS[level])
            # Specific credit roles are already covered by the unfiltered agent field.
            for name in _FIELDS:
                if name not in ("author", "translator", "publisher"):
                    yield from self.field_values(level, row, name)
        elif field == "title":
            yield from self.columns(row, _TITLE_COLUMNS[level])
        elif field in _AGENT_ROLES:
            role = _AGENT_ROLES[field]
            for credit in repositories.agents.list_for_wemi(
                level=level, entity_id=entity_id
            ):
                if role is None or credit["_catalog_link"]["type"] == role:
                    yield from self.columns(
                        credit,
                        ("agent_canonical_name", "agent_sort_name", "agent_aliases"),
                    )
        elif field == "identifier":
            identifiers = repositories.identifiers.list_for_wemi(
                level=level, entity_id=entity_id
            )
            for identifier in identifiers:
                yield from self.columns(
                    identifier, ("entity_identifier_scheme", "entity_identifier_value")
                )
            if level == "item":
                for identifier in repositories.item_identifiers.list_for_item(
                    entity_id
                ):
                    yield from self.columns(
                        identifier, ("item_identifier_scheme", "item_identifier_value")
                    )
        elif field in _PROSE_COLUMNS:
            table, column = _PROSE_COLUMNS[field]
            for related in self.linked_rows(level, entity_id, table):
                yield from self.columns(related, (column,))
        else:
            table, columns = _TERM_COLUMNS[field]
            for related in self.linked_rows(level, entity_id, table):
                yield from self.columns(related, columns)
            if field == "language":
                language_id = row.get(f"{level}_language_id")
                if level == "work":
                    language_id = row.get("work_original_language_id")
                if language_id is not None:
                    yield from self.columns(
                        self.require_row("languages", language_id), columns
                    )

    def require_row(self, table: str, entity_id: int) -> RowMapping:
        row = self.search.db.macros.get_row(table, entity_id)
        if row is None:
            raise CatalogNotFoundError(f"Missing linked {table} row: {entity_id}")
        return row

    def linked_rows(self, level: WemiLevel, entity_id: int, table: str) -> tuple:
        key = (level, entity_id, table)
        if key not in self.related:
            spec = self.search.db.driver_wrapper.get_link_spec(
                WEMI_TABLES[level], table
            )
            self.related[key] = (
                ()
                if spec is None
                else tuple(
                    self.require_row(table, link.secondary_id)
                    for link in self.search.db.macros.get_link_rows(spec, entity_id)
                )
            )
        return self.related[key]


__all__ = ["CatalogSearch"]
