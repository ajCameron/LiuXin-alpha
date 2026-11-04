"""Map Catalog search fields onto physical predicates and monotonic WEMI paths.

This module chooses bibliographic meaning; the database compiler only validates
and executes physical tables, columns and relationships. No preferred projection
or sibling expansion participates in a search.
"""

from LiuXin_alpha.databases.db_types.macro_types import LINK_TYPE_UNSET
from LiuXin_alpha.databases.db_types.query_types import (
    BooleanMatch,
    ForeignMatch,
    LinkedMatch,
    TextMatch,
    ValueMatch,
)

from .database_search import (
    _AGENT_ROLES,
    _ENTITY_COLUMNS,
    _FIELDS,
    _LEVELS,
    _LOCATIONS,
    _PROSE_COLUMNS,
    _TERM_COLUMNS,
    _TITLE_COLUMNS,
    WEMI_TABLES,
    _text,
)


class PhysicalSearchPlan:
    """Prepare one fresh schema-aware query without reading entity payloads."""

    def __init__(self, db):
        self.wrapper = db.driver_wrapper
        self.columns = {}
        self.links = {}

    def available(self, table):
        if table not in self.columns:
            self.columns[table] = set(self.wrapper.get_column_headings(table))
        return self.columns[table]

    def text(self, table, columns, term, exact):
        return TextMatch(
            tuple(c for c in columns if c in self.available(table)), term, exact
        )

    def linked(self, table, target, predicate, role=LINK_TYPE_UNSET):
        key = (table, target)
        if key not in self.links:
            self.links[key] = self.wrapper.get_link_spec(table, target)
        spec = self.links[key]
        return (
            BooleanMatch("or", ())
            if spec is None
            else LinkedMatch(spec, predicate, role)
        )

    def field(self, level, field, term, exact):
        table = WEMI_TABLES[level]
        if field == "all":
            return BooleanMatch(
                "or",
                (
                    self.text(table, _ENTITY_COLUMNS[level], term, exact),
                    *(
                        self.field(level, name, term, exact)
                        for name in _FIELDS
                        if name not in {"author", "translator", "publisher"}
                    ),
                ),
            )
        if field == "title":
            return self.text(table, _TITLE_COLUMNS[level], term, exact)
        if field in _AGENT_ROLES:
            return self.linked(
                table,
                "agents",
                self.text(
                    "agents",
                    ("agent_canonical_name", "agent_sort_name", "agent_aliases"),
                    term,
                    exact,
                ),
                _AGENT_ROLES[field] or LINK_TYPE_UNSET,
            )
        if field == "identifier":
            curated = ForeignMatch(
                "entity_identifiers",
                f"{level}_id",
                "entity_identifier_entity_id",
                BooleanMatch(
                    "and",
                    (
                        ValueMatch("entity_identifier_entity_type", level),
                        self.text(
                            "entity_identifiers",
                            ("entity_identifier_scheme", "entity_identifier_value"),
                            term,
                            exact,
                        ),
                    ),
                ),
            )
            if level != "item":
                return curated
            return BooleanMatch(
                "or",
                (
                    curated,
                    ForeignMatch(
                        "item_identifiers",
                        "item_id",
                        "item_identifier_item_id",
                        self.text(
                            "item_identifiers",
                            ("item_identifier_scheme", "item_identifier_value"),
                            term,
                            exact,
                        ),
                    ),
                ),
            )
        if field in _PROSE_COLUMNS:
            target, column = _PROSE_COLUMNS[field]
            return self.linked(table, target, self.text(target, (column,), term, exact))
        target, columns = _TERM_COLUMNS[field]
        match = self.text(target, columns, term, exact)
        linked = self.linked(table, target, match)
        language_column = (
            "work_original_language_id" if level == "work" else f"{level}_language_id"
        )
        if field == "language" and language_column in self.available(table):
            return BooleanMatch(
                "or",
                (
                    linked,
                    ForeignMatch(
                        "languages", language_column, "language_id", match, True
                    ),
                ),
            )
        return linked

    def path(self, source, target, predicate):
        start, end = _LEVELS.index(source), _LEVELS.index(target)
        step = 1 if end > start else -1
        for index in reversed(range(start, end, step)):
            level, following = _LEVELS[index], _LEVELS[index + step]
            if level == "manifestation" and following == "item":
                predicate = ForeignMatch(
                    "items", "manifestation_id", "item_manifestation_id", predicate
                )
            elif level == "item" and following == "manifestation":
                predicate = ForeignMatch(
                    "manifestations",
                    "item_manifestation_id",
                    "manifestation_id",
                    predicate,
                    True,
                )
            else:
                predicate = self.linked(
                    WEMI_TABLES[level], WEMI_TABLES[following], predicate
                )
        return predicate

    def build(self, tree, level):
        if tree is None:
            return BooleanMatch("and", ())
        if tree[0] != "token":
            return BooleanMatch(
                tree[0], tuple(self.build(child, level) for child in tree[1:])
            )
        target, field = _LOCATIONS[tree[1]]
        raw = tree[2]
        exact = raw.startswith("=")
        term = _text(raw[1:] if exact else raw)
        return BooleanMatch(
            "or",
            tuple(
                self.path(
                    level, destination, self.field(destination, field, term, exact)
                )
                for destination in ((target,) if target else _LEVELS)
            ),
        )
