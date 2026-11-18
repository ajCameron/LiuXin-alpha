"""Evaluate physical predicates over storage reads without bibliographic policy."""

from __future__ import annotations

import operator
import re
from collections.abc import Mapping
from typing import Any, cast

from LiuXin_alpha.caches.query_contracts import PredicateReadEngine
from LiuXin_alpha.databases.api import RowAPI
from LiuXin_alpha.databases.db_types.macro_types import LINK_TYPE_UNSET
from LiuXin_alpha.databases.db_types.query_types import (
    BooleanMatch,
    ComparisonMatch,
    CountMatch,
    ForeignMatch,
    IdentifierMatch,
    LinkedMatch,
    MissingSearchReference,
    ProvidedMatch,
    RowPredicate,
    TextMatch,
    ValueMatch,
)
from LiuXin_alpha.databases.search_functions import (
    identifier_key,
    normalized_identifier_key,
    text_key,
)
from LiuXin_alpha.errors import InputIntegrityError

COMPARISONS = {
    "eq": operator.eq,
    "ne": operator.ne,
    "lt": operator.lt,
    "lte": operator.le,
    "gt": operator.gt,
    "gte": operator.ge,
}


def compare(value: Any, op: str, expected: Any) -> bool:
    """Evaluate a typed comparison; missing or incomparable ordered values fail."""
    if op == "is_null":
        return (value is not None) if expected is False else (value is None)
    if op == "eq":
        return value == expected
    if op == "ne":
        return value != expected
    if op == "in":
        return value in expected
    if value is None:
        return False
    if op == "contains":
        return expected in value
    if op == "prefix":
        return isinstance(value, str) and value.startswith(expected)
    if op == "regex":
        return isinstance(value, str) and re.search(expected, value) is not None
    try:
        return COMPARISONS[op](value, expected)
    except TypeError:
        return False


class PredicateEvaluator:
    """Validate a complete tree, check references and narrow candidate sets."""

    def __init__(self, engine: PredicateReadEngine) -> None:
        self.engine = engine
        self.edges: dict[tuple[str, int, frozenset[int]], dict[int, set[int]]] = {}
        self._rows: dict[tuple[str, int], Mapping[str, Any]] = {}

    def value(self, table: str, row_id: int, field: str) -> Any:
        """Share one row observation among predicates within this execution."""
        column = field.removeprefix(table + ".")
        cached_table = self.engine._table(table)
        if column not in cached_table.column_headings:
            return self.engine._value(table, row_id, field)
        key = (table, row_id)
        if key not in self._rows:
            self._rows[key] = cached_table.get_row_snapshot(row_id)
        return self._rows[key].get(column)

    def validate(self, table: str, node: RowPredicate) -> None:
        self.engine._table(table)

        def field(name: str) -> str:
            return self.engine._canonical_field(table, name)

        if isinstance(node, ProvidedMatch):
            node.provider.validate(table)
        elif isinstance(node, BooleanMatch):
            if node.operator not in {"and", "or", "not"}:
                raise InputIntegrityError("Unknown predicate operator")
            if node.operator == "not" and len(node.operands) != 1:
                raise InputIntegrityError("NOT requires one operand")
            for child in node.operands:
                self.validate(table, child)
        elif isinstance(node, LinkedMatch):
            spec = node.link_spec
            if spec.primary_table != table:
                raise InputIntegrityError(
                    "Relationship predicate has the wrong source table"
                )
            link = self.engine.storage.get_link_table(table, spec.secondary_table)
            actual = getattr(link, "link_spec", None)
            if actual is not None:
                forward = actual.primary_table == table
                source_column = (
                    actual.primary_link_col if forward else actual.secondary_link_col
                )
                target_column = (
                    actual.secondary_link_col if forward else actual.primary_link_col
                )
                if (
                    spec.link_table,
                    spec.primary_link_col,
                    spec.secondary_link_col,
                    spec.type_link_col,
                ) != (
                    actual.link_table,
                    source_column,
                    target_column,
                    actual.type_link_col,
                ):
                    raise InputIntegrityError(
                        "Relationship predicate does not match cached topology"
                    )
            if node.link_type is not LINK_TYPE_UNSET and not spec.typed:
                raise InputIntegrityError("Cannot filter an untyped predicate by type")
            field(spec.primary_id_col)
            self.engine._canonical_field(spec.secondary_table, spec.secondary_id_col)
            self.validate(spec.secondary_table, node.predicate)
        elif isinstance(node, ForeignMatch):
            field(node.local_column)
            self.engine._canonical_field(node.table, node.remote_column)
            self.validate(node.table, node.predicate)
        elif isinstance(node, TextMatch):
            for column in node.columns:
                field(column)
        elif isinstance(node, IdentifierMatch):
            field(node.scheme_column)
            field(node.value_column)
        elif isinstance(node, (ValueMatch, ComparisonMatch, CountMatch)):
            field(node.column)
            if isinstance(node, (ComparisonMatch, CountMatch)):
                allowed = set(COMPARISONS)
                if isinstance(node, ComparisonMatch):
                    allowed |= {"in", "contains", "prefix", "regex", "is_null"}
                if node.operator not in allowed:
                    raise InputIntegrityError("Unknown comparison operator")
                if node.operator == "regex":
                    re.compile(node.value)
        else:
            raise InputIntegrityError("Unsupported physical row predicate")

    def relations(
        self, table: str, node: LinkedMatch | ForeignMatch, ids: set[int]
    ) -> dict[int, set[int]]:
        key = (table, id(node), frozenset(ids))
        if key in self.edges:
            return self.edges[key]
        result: dict[int, set[int]] = {}
        value = self.value
        if isinstance(node, LinkedMatch):
            spec = node.link_spec
            target = spec.secondary_table
            targets = self.engine._all_ids(target)
            for row_id in ids:
                related = set()
                rows = self.engine.storage.get_link_rows_for_source(
                    table,
                    row_id,
                    target,
                    require_ordering=False,
                )
                for raw in rows:
                    row = (
                        raw
                        if isinstance(raw, Mapping)
                        else cast(Mapping[str, Any], cast(RowAPI, raw).row_dict)
                    )
                    if node.link_type is not LINK_TYPE_UNSET:
                        if (
                            row.get(spec.type_link_col)
                            if spec.type_link_col is not None
                            else None
                        ) != node.link_type:
                            continue
                    target_id = int(row[spec.secondary_link_col])
                    if target_id not in targets:
                        raise MissingSearchReference(target)
                    related.add(target_id)
                result[row_id] = related
        else:
            target = node.table
            reverse: dict[Any, set[int]] = {}
            for target_id in self.engine._all_ids(target):
                remote = value(target, target_id, node.remote_column)
                if remote is not None:
                    reverse.setdefault(remote, set()).add(target_id)
            for row_id in ids:
                local = value(table, row_id, node.local_column)
                related = set() if local is None else reverse.get(local, set())
                if node.require_target and local is not None and not related:
                    raise MissingSearchReference(target)
                result[row_id] = related
        self.edges[key] = result
        return result

    def check_references(self, table: str, node: RowPredicate, ids: set[int]) -> None:
        """Match SQL integrity guards even in Boolean branches that cannot match."""
        if isinstance(node, BooleanMatch):
            for child in node.operands:
                self.check_references(table, child, ids)
        elif isinstance(node, (ForeignMatch, LinkedMatch)):
            edges = self.relations(table, node, ids)
            target = (
                node.table
                if isinstance(node, ForeignMatch)
                else node.link_spec.secondary_table
            )
            self.check_references(target, node.predicate, set().union(*edges.values()))

    def evaluate(self, table: str, node: RowPredicate, ids: set[int]) -> set[int]:
        if isinstance(node, ProvidedMatch):
            result = set(node.provider.match(table, frozenset(ids)))
            if not result <= ids:
                raise InputIntegrityError(
                    "Predicate provider returned IDs outside its candidates"
                )
            return result
        if isinstance(node, BooleanMatch):
            if node.operator == "not":
                return ids - self.evaluate(table, node.operands[0], ids)
            if node.operator == "and":
                result = ids.copy()
                for child in node.operands:
                    result &= self.evaluate(table, child, result)
                return result
            result = set()
            for child in node.operands:
                result |= self.evaluate(table, child, ids - result)
            return result
        if isinstance(node, (LinkedMatch, ForeignMatch)):
            edges = self.relations(table, node, ids)
            target = (
                node.table
                if isinstance(node, ForeignMatch)
                else node.link_spec.secondary_table
            )
            target_matches = self.evaluate(
                target, node.predicate, set().union(*edges.values())
            )
            return {
                row_id for row_id in ids if edges.get(row_id, set()) & target_matches
            }
        value = self.value
        if isinstance(node, TextMatch):

            def matches(row_id: int) -> bool:
                for column in node.columns:
                    text = text_key(value(table, row_id, column))
                    if text is not None and (
                        text == node.term if node.exact else node.term in text
                    ):
                        return True
                return False
        elif isinstance(node, IdentifierMatch):
            key = normalized_identifier_key(
                node.normalized_scheme, node.normalized_value
            )

            def matches(row_id: int) -> bool:
                return (
                    identifier_key(
                        value(table, row_id, node.scheme_column),
                        value(table, row_id, node.value_column),
                    )
                    == key
                )
        else:

            def matches(row_id: int) -> bool:
                item = value(table, row_id, node.column)
                if isinstance(node, CountMatch):
                    item = (
                        0
                        if item is None
                        else len(item)
                        if isinstance(item, (tuple, list, set, frozenset, dict))
                        else 1
                    )
                return compare(
                    item,
                    "eq" if isinstance(node, ValueMatch) else node.operator,
                    node.value,
                )

        return {row_id for row_id in ids if matches(row_id)}
