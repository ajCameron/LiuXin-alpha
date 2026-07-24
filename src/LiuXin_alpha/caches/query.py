"""Backend-neutral structured query execution over a storage cache."""

from __future__ import annotations

import unicodedata

from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Optional, cast

from LiuXin_alpha.caches.api.cache_api import (
    CacheFilterOperator,
    CachePredicate,
    CacheQuery,
    CacheQueryResult,
    CacheRecord,
    CacheRelation,
    CacheSort,
    UnknownCacheFieldError,
    UnknownCacheTableError,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_cache_api import (
    StorageCacheAPI,
)


def normalize_cache_text(value: Any) -> str:
    """Normalize text for cache-owned matching without changing stored values."""

    return unicodedata.normalize("NFKC", str(value)).casefold()


def _flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Mapping):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return " ".join(_flatten_text(item) for item in value)
    return normalize_cache_text(value)


def _trigrams(value: str) -> frozenset[str]:
    if len(value) < 3:
        return frozenset()
    return frozenset(value[index : index + 3] for index in range(len(value) - 2))


def _sort_value(value: Any) -> tuple[int, Any]:
    if isinstance(value, (list, tuple, set, frozenset)):
        value = tuple(_sort_value(item) for item in value)
        return (3, value)
    if isinstance(value, bool):
        return (0, int(value))
    if isinstance(value, (int, float)):
        return (1, value)
    if isinstance(value, str):
        return (2, (normalize_cache_text(value), value))
    return (4, repr(value))


class CacheQueryEngine:
    """Common query engine shared by all storage-cache plugins."""

    def __init__(self, storage: StorageCacheAPI) -> None:
        self.storage = storage
        self._text_values: dict[tuple[str, str], dict[int, str]] = {}
        self._text_trigrams: dict[tuple[str, str], dict[str, set[int]]] = {}

    def reset(self) -> None:
        """Discard all lazily built indexes."""

        self._text_values.clear()
        self._text_trigrams.clear()

    def _table(self, table_name: str) -> Any:
        if not self.storage.has_main_table(str(table_name)):
            raise UnknownCacheTableError(str(table_name))
        return self.storage.get_main_table(str(table_name))

    def _canonical_field(self, table: str, field: str) -> str:
        requested = str(field)
        table_cache = self._table(table)
        if requested in table_cache.column_headings:
            requested = f"{table}.{requested}"
        try:
            resolved = self.storage.get_field(requested)
        except (KeyError, TypeError) as exc:
            raise UnknownCacheFieldError(requested) from exc

        owner = getattr(
            resolved,
            "table_name",
            getattr(resolved, "src_table_name", None),
        )
        if owner is not None and str(owner) != str(table):
            raise UnknownCacheFieldError(
                f"field {requested!r} is owned by {owner!r}, not {table!r}"
            )
        return str(getattr(resolved, "field_key"))

    def _value(self, table: str, row_id: int, field: str) -> Any:
        canonical = self._canonical_field(table, field)
        return self.storage.get_cached_value(int(row_id), canonical)

    def _all_ids(self, table: str) -> set[int]:
        return {int(row_id) for row_id in self._table(table).row_ids}

    def _candidate_ids_for_predicate(
        self,
        table: str,
        predicate: CachePredicate,
    ) -> Optional[set[int]]:
        canonical = self._canonical_field(table, predicate.field)
        field = self.storage.get_field(canonical)

        values: tuple[Any, ...]
        if predicate.operator == CacheFilterOperator.EQ:
            values = (predicate.value,)
        elif predicate.operator == CacheFilterOperator.IN:
            values = tuple(predicate.value)
        else:
            return None

        relation_getter = getattr(field, "get_src_ids_from_value", None)
        if callable(relation_getter):
            ids: set[int] = set()
            for value in values:
                matching_ids = cast(Iterable[Any], relation_getter(value))
                ids.update(int(row_id) for row_id in matching_ids)
            return ids

        table_cache = self._table(table)
        column_name = str(getattr(field, "column_name", canonical.rsplit(".", 1)[-1]))
        getter = getattr(table_cache, "get_ids_for_value", None)
        if callable(getter):
            ids = set()
            for value in values:
                matching_ids = cast(Iterable[Any], getter(column_name, value))
                ids.update(int(row_id) for row_id in matching_ids)
            return ids
        return None

    @staticmethod
    def _matches_predicate(value: Any, predicate: CachePredicate) -> bool:
        operator = predicate.operator
        expected = predicate.value

        if operator == CacheFilterOperator.IS_NULL:
            is_null = value is None
            return is_null if expected is not False else not is_null
        if operator == CacheFilterOperator.EQ:
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                return expected in value
            return bool(value == expected)
        if operator == CacheFilterOperator.IN:
            expected_values = tuple(expected)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                return any(item in expected_values for item in value)
            return value in expected_values
        if operator == CacheFilterOperator.CONTAINS:
            return normalize_cache_text(expected) in _flatten_text(value)
        if operator == CacheFilterOperator.PREFIX:
            return _flatten_text(value).startswith(normalize_cache_text(expected))

        try:
            if operator == CacheFilterOperator.LT:
                return bool(value < expected)
            if operator == CacheFilterOperator.LTE:
                return bool(value <= expected)
            if operator == CacheFilterOperator.GT:
                return bool(value > expected)
            if operator == CacheFilterOperator.GTE:
                return bool(value >= expected)
        except TypeError:
            return False
        return False  # type: ignore[unreachable]

    def _build_text_index(self, table: str, field: str) -> tuple[dict[int, str], dict[str, set[int]]]:
        canonical = self._canonical_field(table, field)
        key = (str(table), canonical)
        values = self._text_values.get(key)
        trigrams = self._text_trigrams.get(key)
        if values is not None and trigrams is not None:
            return values, trigrams

        values = {}
        trigrams = {}
        for row_id in self._all_ids(table):
            text = _flatten_text(self.storage.get_cached_value(row_id, canonical))
            values[row_id] = text
            for trigram in _trigrams(text):
                trigrams.setdefault(trigram, set()).add(row_id)
        self._text_values[key] = values
        self._text_trigrams[key] = trigrams
        return values, trigrams

    def _text_candidate_ids(
        self,
        table: str,
        text: str,
        fields: Sequence[str],
    ) -> set[int]:
        terms = tuple(
            term for term in normalize_cache_text(text).split() if term
        )
        if not terms:
            return self._all_ids(table)

        indexes = [self._build_text_index(table, field) for field in fields]
        all_ids = self._all_ids(table)
        candidate_ids = set(all_ids)
        for term in terms:
            grams = _trigrams(term)
            term_candidates: set[int]
            if not grams:
                term_candidates = set(all_ids)
            else:
                term_candidates = set()
                for _values, trigram_index in indexes:
                    field_candidates: Optional[set[int]] = None
                    for gram in grams:
                        gram_ids = trigram_index.get(gram, set())
                        field_candidates = (
                            set(gram_ids)
                            if field_candidates is None
                            else field_candidates & gram_ids
                        )
                    term_candidates.update(field_candidates or ())
            candidate_ids &= term_candidates

        return {
            row_id
            for row_id in candidate_ids
            if all(
                any(term in values.get(row_id, "") for values, _index in indexes)
                for term in terms
            )
        }

    def _ids_for_relation(self, table: str, relation: CacheRelation) -> set[int]:
        target_ids = tuple(int(value) for value in relation.ids)
        try:
            link_table = self.storage.get_link_table(table, relation.table)
            getter = getattr(link_table, "get_links_for_dst", None)
            if not callable(getter):
                getter = getattr(link_table, "get_link_for_dst", None)
            if not callable(getter):
                raise TypeError("link table does not expose destination getters")
            ids: set[int] = set()
            for target_id in target_ids:
                raw_links = getter(
                    target_id,
                    type_filter=relation.type_filter,
                )
                if raw_links is None:
                    continue
                links: Sequence[Any] = (
                    raw_links
                    if isinstance(raw_links, Sequence)
                    else (raw_links,)
                )
                ids.update(int(getattr(link, "src_id")) for link in links)
            return ids
        except KeyError:
            try:
                link_table = self.storage.get_link_table(relation.table, table)
            except KeyError:
                return set()
            getter = getattr(link_table, "get_links_for_src", None)
            if not callable(getter):
                getter = getattr(link_table, "get_link_for_src", None)
            if not callable(getter):
                raise TypeError("link table does not expose source getters")
            ids = set()
            for target_id in target_ids:
                raw_links = getter(
                    target_id,
                    type_filter=relation.type_filter,
                )
                if raw_links is None:
                    continue
                links = (
                    raw_links
                    if isinstance(raw_links, Sequence)
                    else (raw_links,)
                )
                ids.update(int(getattr(link, "dst_id")) for link in links)
            return ids

    def _sort_ids(
        self,
        table: str,
        ids: Iterable[int],
        sort_specs: Sequence[CacheSort],
    ) -> list[int]:
        ordered = sorted(int(row_id) for row_id in ids)
        for sort_spec in reversed(tuple(sort_specs)):
            present: list[int] = []
            missing: list[int] = []
            values: dict[int, Any] = {}
            for row_id in ordered:
                value = self._value(table, row_id, sort_spec.field)
                values[row_id] = value
                (missing if value is None else present).append(row_id)
            present.sort(
                key=lambda row_id: _sort_value(values[row_id]),
                reverse=not sort_spec.ascending,
            )
            ordered = present + missing
        return ordered

    def _record(
        self,
        table: str,
        row_id: int,
        projection: Sequence[str],
    ) -> CacheRecord:
        table_cache = self._table(table)
        if not projection:
            return CacheRecord(
                table=table,
                row_id=row_id,
                values=table_cache.get_row_snapshot(row_id),
            )

        values: dict[str, Any] = {}
        for requested in projection:
            values[str(requested)] = self._value(table, row_id, requested)
        id_column = str(table_cache.id_column)
        if id_column not in values and f"{table}.{id_column}" not in values:
            values[id_column] = int(row_id)
        return CacheRecord(table=table, row_id=row_id, values=values)

    def get(self, table: str, row_id: int) -> Optional[CacheRecord]:
        table_cache = self._table(table)
        if not table_cache.has_id(int(row_id)):
            return None
        return self._record(table, int(row_id), ())

    def query(self, query: CacheQuery, *, generation: int) -> CacheQueryResult:
        ids = self._all_ids(query.table)

        if query.relation is not None:
            ids &= self._ids_for_relation(query.table, query.relation)

        for predicate in query.predicates:
            candidates = self._candidate_ids_for_predicate(query.table, predicate)
            if candidates is not None:
                ids &= candidates
            ids = {
                row_id
                for row_id in ids
                if self._matches_predicate(
                    self._value(query.table, row_id, predicate.field),
                    predicate,
                )
            }

        if query.text.strip():
            text_fields = query.text_fields
            if not text_fields:
                text_fields = tuple(
                    str(column)
                    for column in self._table(query.table).column_headings
                )
            ids &= self._text_candidate_ids(query.table, query.text, text_fields)

        ordered = self._sort_ids(query.table, ids, query.sort)
        total_count = len(ordered)
        end = None if query.limit is None else query.offset + query.limit
        visible = ordered[query.offset:end]
        records = tuple(
            self._record(query.table, row_id, query.projection)
            for row_id in visible
        )
        return CacheQueryResult(
            records=records,
            total_count=total_count,
            offset=query.offset,
            limit=query.limit,
            complete=True,
            generation=generation,
        )

    def related_ids(
        self,
        source_table: str,
        source_ids: Iterable[int],
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> tuple[int, ...]:
        ordered: list[int] = []
        seen: set[int] = set()
        try:
            link_table = self.storage.get_link_table(source_table, target_table)
            getter = getattr(link_table, "get_links_for_src", None)
            supports_ordering = callable(getter)
            if not callable(getter):
                getter = getattr(link_table, "get_link_for_src", None)
            id_attribute = "dst_id"
        except KeyError:
            link_table = self.storage.get_link_table(target_table, source_table)
            getter = getattr(link_table, "get_links_for_dst", None)
            supports_ordering = callable(getter)
            if not callable(getter):
                getter = getattr(link_table, "get_link_for_dst", None)
            id_attribute = "src_id"
        if not callable(getter):
            raise TypeError("link table does not expose relation getters")

        for source_id in source_ids:
            if supports_ordering:
                raw_links = getter(
                    int(source_id),
                    require_ordering=True,
                    type_filter=type_filter,
                )
            else:
                raw_links = getter(
                    int(source_id),
                    type_filter=type_filter,
                )
            if raw_links is None:
                continue
            links = (
                raw_links
                if isinstance(raw_links, Sequence)
                else (raw_links,)
            )
            for link in links:
                target_id = int(getattr(link, id_attribute))
                if target_id not in seen:
                    seen.add(target_id)
                    ordered.append(target_id)
        return tuple(ordered)


__all__ = ["CacheQueryEngine", "normalize_cache_text"]
