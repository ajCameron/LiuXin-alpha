"""
Execute structured queries through a storage-cache backend's read surface.

The engine owns predicate evaluation, relation constraints, stable ordering,
paging and lazily built text/trigram indexes. It does not own lifecycle, locks
or dependency refresh; Cache coordinates those and resets indexes when needed.
Stored values are returned unchanged, with normalization confined to matching
and sort keys. Live storage can still perform database reads underneath.
"""

from __future__ import annotations

import unicodedata
from collections import OrderedDict
from collections.abc import Hashable, Iterable, Mapping, Sequence, Set
from copy import deepcopy
from dataclasses import dataclass, fields, is_dataclass
from heapq import nlargest, nsmallest
from math import isnan
from operator import index
from typing import Any, cast

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
from LiuXin_alpha.caches.predicates import PredicateEvaluator
from LiuXin_alpha.caches.query_contracts import QueryLink, QueryStorage, QueryTable
from LiuXin_alpha.databases.db_types.macro_types import LINK_TYPE_UNSET
from LiuXin_alpha.databases.db_types.query_types import (
    BooleanMatch,
    ComparisonMatch,
    CountMatch,
    IdentifierMatch,
    ProvidedMatch,
    RowPredicate,
    TextMatch,
    ValueMatch,
)


def _freeze_key(value: Any) -> Hashable:
    """Snapshot supported query operands without identity- or repr-based keys."""
    if value is LINK_TYPE_UNSET:
        return ("link-type-unset",)
    if isinstance(value, ProvidedMatch):
        raise TypeError("Execution providers require fresh evaluation")
    if value is None or isinstance(value, (str, bytes, bool, int, float)):
        return (type(value), value)
    if is_dataclass(value) and not isinstance(value, type):
        return (
            type(value),
            tuple((f.name, _freeze_key(getattr(value, f.name))) for f in fields(value)),
        )
    if isinstance(value, Mapping):
        return (
            type(value),
            frozenset((_freeze_key(k), _freeze_key(v)) for k, v in value.items()),
        )
    if isinstance(value, (tuple, list)):
        return (type(value), tuple(_freeze_key(v) for v in value))
    if isinstance(value, (set, frozenset)):
        return (type(value), frozenset(_freeze_key(v) for v in value))
    raise TypeError("Query operand cannot be cached")


def normalize_cache_text(value: Any) -> str:
    """
    Apply Unicode compatibility normalization and case folding to string form.

    This does not modify the stored value. None becomes the literal text none;
    _flatten_text handles nulls separately when indexing field values.

    Example:
        >>> normalize_cache_text("Ｓtraße")
        'strasse'


    :param value: Value converted with str before normalization.
    :return: NFKC-normalized, casefolded string; whitespace and punctuation are retained.
    """

    return unicodedata.normalize("NFKC", str(value)).casefold()


def _flatten_text(value: Any) -> str:
    """
    Flatten nested mapping values and sequences into normalized searchable text.

    None becomes empty text. Mapping keys are ignored. Sequence recursion excludes
    str, bytes and bytearray; other objects use their string representation. Empty
    elements can contribute spaces, and cyclic containers are not detected.

    Example:
        >>> _flatten_text({"ignored": ["Café", None, "BOOK"]})
        'café  book'


    :param value: Scalar, mapping or sequence whose textual values should be searched.
    :return: Normalized scalar text or recursively flattened values joined with spaces.
    """

    if value is None:
        return ""
    if isinstance(value, Mapping):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return " ".join(_flatten_text(item) for item in value)
    return normalize_cache_text(value)


def _trigrams(value: str) -> frozenset[str]:
    """
    Collect distinct overlapping three-character slices of a string.

    Example:
        >>> _trigrams("aaaa") == frozenset({"aaa"})
        True


    :param value: Already prepared text; no normalization is performed here.
    :return: Frozenset of trigrams, empty for strings shorter than three characters.
    """

    if len(value) < 3:
        return frozenset()
    return frozenset(value[index : index + 3] for index in range(len(value) - 2))


class _UnorderedSortValue(ValueError):
    """A NaN requires the existing stable full sort instead of heap selection."""


@dataclass(frozen=True, slots=True)
class _DescendingSortKey:
    """Reverse one present-value component without reversing nulls or row-ID ties."""

    value: tuple[int, Any]

    def __lt__(self, other: _DescendingSortKey) -> bool:
        return self.value > other.value


def _sort_value(value: Any, *, require_total_order: bool = False) -> tuple[int, Any]:
    """
    Build a ranked comparison key for mixed cached values.

    Booleans precede numbers, then strings, then list/tuple/set/frozenset values,
    then other objects ordered by repr. Strings use normalized text followed by
    original text. Collections recurse in their own iteration order, so unordered
    sets do not acquire an independent deterministic element order here.

    Example:
        >>> _sort_value(False), _sort_value("ALPHA")
        ((0, 0), (2, ('alpha', 'ALPHA')))


    :param value: Stored value to order; missing values are handled by the caller.
    :param require_total_order: Reject NaNs, including nested values, for heap selection.
    :return: Tuple of type rank and comparable payload for supported value families.
    """

    if isinstance(value, (list, tuple, set, frozenset)):
        value = tuple(
            _sort_value(item, require_total_order=require_total_order) for item in value
        )
        return (3, value)
    if isinstance(value, bool):
        return (0, int(value))
    if isinstance(value, (int, float)):
        if require_total_order and isinstance(value, float) and isnan(value):
            raise _UnorderedSortValue
        return (1, value)
    if isinstance(value, str):
        return (2, (normalize_cache_text(value), value))
    return (4, repr(value))


class CacheQueryEngine:
    """
    Evaluate common cache queries while retaining reusable text indexes.

    Attach initialized storage supplied by the facade. This object performs no
    lifecycle checks or locking and does not detect external changes. Call reset
    when cached data changes; the facade also resets text indexes before live
    reads. Query results are shallow record snapshots with complete=True.

    Example:
        Given initialized storage, construct ``CacheQueryEngine(storage)`` and
        call ``engine.query(CacheQuery("books"), generation=1)``.
    """

    def __init__(
        self,
        storage: QueryStorage,
        *,
        plan_cache_size: int = 100,
        result_cache_size: int = 50,
    ) -> None:
        """
        Retain storage and create empty dictionaries for lazy text indexes.

        Example:
            >>> marker = object()
            >>> CacheQueryEngine(marker).storage is marker
            True


        :param storage: Backend implementing cached tables, fields and relation reads.
        :return: None; retains the backend without reading or validating it.
        """

        self.storage = storage
        if plan_cache_size < 0 or result_cache_size < 0:
            raise ValueError("Query cache limits cannot be negative")
        self.plan_cache_size = plan_cache_size
        self.result_cache_size = result_cache_size
        self._plans: OrderedDict[Hashable, None] = OrderedDict()
        # Private, exclusively owned sets: queries only read them, while bounded
        # repair transfers them across generations under the facade lock. Public
        # results contain independent tuples/records, never these mutable sets.
        self._results: OrderedDict[Hashable, set[int]] = OrderedDict()
        self._result_queries: OrderedDict[Hashable, CacheQuery] = OrderedDict()
        self.result_hits = 0
        self.repaired_results = 0
        self._text_values: dict[tuple[str, str], dict[int, str]] = {}
        self._text_trigrams: dict[tuple[str, str], dict[str, set[int]]] = {}

    def reset(self, *, keep_plans: bool = False) -> None:
        """
        Discard normalized text values and trigram indexes for every field.

        Example:
            >>> engine = CacheQueryEngine(object())
            >>> engine._text_values[("books", "title")] = {1: "old"}
            >>> engine.reset()
            >>> engine._text_values
            {}


        :return: None; clears both index dictionaries without changing storage.
        """

        self._text_values.clear()
        self._text_trigrams.clear()
        self._results.clear()
        self._result_queries.clear()
        if not keep_plans:
            self._plans.clear()

    @staticmethod
    def _remember[K, V](cache: OrderedDict[K, V], key: K, value: V, limit: int) -> None:
        if limit:
            cache[key] = value
            cache.move_to_end(key)
            while len(cache) > limit:
                cache.popitem(last=False)

    @staticmethod
    def _result_key(query: CacheQuery, generation: int) -> Hashable:
        return _freeze_key(
            (
                generation,
                query.table,
                query.predicates,
                query.relation,
                query.text,
                query.text_fields,
                query.where,
            )
        )

    def row_local_results(self) -> list[tuple[CacheQuery, set[int]]]:
        """Transfer row-local membership to a pending repair before resetting.

        The caller must reset this engine before changing the saved sets, and
        serialize the complete invalidate/refresh/repair boundary. These private
        sets are not immutable snapshots or part of the public query result.
        """
        result = []
        for key, ids in self._results.items():
            query = self._result_queries[key]
            columns = self._table(query.table).column_headings

            def physical(
                name: str, query: CacheQuery = query, columns: Sequence[str] = columns
            ) -> bool:
                return name.removeprefix(query.table + ".") in columns

            def local(node: RowPredicate | None) -> bool:
                if node is None:
                    return True
                if isinstance(node, BooleanMatch):
                    return all(local(child) for child in node.operands)
                if isinstance(node, (ValueMatch, ComparisonMatch, CountMatch)):
                    return physical(node.column)
                if isinstance(node, TextMatch):
                    return all(physical(column) for column in node.columns)
                if isinstance(node, IdentifierMatch):
                    return physical(node.scheme_column) and physical(node.value_column)
                return False

            if (
                query.relation is None
                and not query.text.strip()
                and local(query.where)
                and all(physical(item.field) for item in query.predicates)
            ):
                result.append((query, ids))
        return result

    def repair_results(
        self,
        saved: list[tuple[CacheQuery, set[int]]],
        changes: Mapping[str, set[int]],
        *,
        generation: int,
    ) -> None:
        """Repair proven row-local membership only within a bounded work budget.

        Call after resetting the old generation and a successful bounded refresh.
        Saved membership is exclusively owned and edited in place, avoiding a
        population-sized copy. Graph, text-index and provider queries are excluded
        by row_local_results; larger updates remain evicted.
        """
        if sum(len(changes.get(query.table, ())) for query, _ in saved) > 50:
            return
        for query, previous in saved:
            changed = changes.get(query.table, set())
            if changed:
                evaluator = PredicateEvaluator(self)
                self._validate_query(query, evaluator)
                matches = self._matching_ids(query, evaluator, changed)
                previous.difference_update(changed)
                previous.update(matches)
            key = self._result_key(query, generation)
            self._remember(self._results, key, previous, self.result_cache_size)
            self._remember(self._result_queries, key, query, self.result_cache_size)
            self.repaired_results += 1

    def _validate_query(self, query: CacheQuery, evaluator: PredicateEvaluator) -> None:
        self._table(query.table)
        for predicate in query.predicates:
            self._canonical_field(query.table, predicate.field)
        for sort in query.sort:
            self._canonical_field(query.table, sort.field)
        for name in (*query.projection, *query.text_fields):
            self._canonical_field(query.table, name)
        for predicate in query.predicates:
            if predicate.operator not in set(CacheFilterOperator):
                raise ValueError("Unknown cache filter operator")
        if query.relation is not None:
            self._table(query.relation.table)
            self.storage.get_link_table(query.table, query.relation.table)
        if query.where is not None:
            evaluator.validate(query.table, query.where)

    def _table(self, table_name: str) -> QueryTable:
        """
        Resolve a cached main table after checking backend membership.

        Example:
            Resolving a missing table raises UnknownCacheTableError before any row
            lookup can be interpreted as a known row miss.


        :param table_name: Table name converted to str for both storage calls.
        :return: Backend main-table cache object.
        :raises UnknownCacheTableError: Storage does not report the requested main table.
        """

        if not self.storage.has_main_table(str(table_name)):
            raise UnknownCacheTableError(str(table_name))
        return self.storage.get_main_table(str(table_name))

    def _canonical_field(self, table: str, field: str) -> str:
        """
        Resolve a field key and check its declared ownership against the base table.

        Qualify a bare column found in table.column_headings as table.column. Wrap
        KeyError or TypeError from storage.get_field as UnknownCacheFieldError. If
        the field exposes table_name, or otherwise src_table_name, require a matching
        owner when it is non-None. Other resolution/attribute errors propagate.

        Example:
            For a books table with a title column, the bare request title is resolved
            through the qualified key books.title.


        :param table: Base table used for bare-column qualification and owner validation.
        :param field: Requested bare column, qualified column or relation-field key.
        :return: Resolved field_key converted to str.
        :raises UnknownCacheTableError: The base table is not cached.
        :raises UnknownCacheFieldError: Field resolution fails with KeyError/TypeError or the declared owner differs.
        """

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
        return str(resolved.field_key)

    def _value(self, table: str, row_id: int, field: str) -> Any:
        """
        Read one field value after canonical resolution and row-ID conversion.

        Example:
            A text value returned by this helper retains its original Unicode form;
            normalization occurs only when building matching or sorting keys.


        :param table: Base table owning the requested field.
        :param row_id: Row identity converted with int.
        :param field: Bare or qualified field request accepted by _canonical_field.
        :return: Backend cached value, preserving its stored type and nested objects.
        """

        canonical = self._canonical_field(table, field)
        return self.storage.get_cached_value(int(row_id), canonical)

    def _all_ids(self, table: str) -> set[int]:
        """
        Materialize distinct integer IDs from a cached main table.

        Example:
            Duplicate or equivalent integer-convertible backend IDs collapse into one
            candidate ID for structured filtering.


        :param table: Main table name to resolve.
        :return: Set of integer row identities with no traversal ordering guarantee.
        """

        return {int(row_id) for row_id in self._table(table).row_ids}

    def _candidate_ids_for_predicate(
        self,
        table: str,
        predicate: CachePredicate,
    ) -> set[int] | None:
        """
        Use available reverse lookups to narrow EQ or IN predicate candidates.

        Resolve the field even for unsupported optimization operators. For EQ/IN,
        prefer field.get_src_ids_from_value, then main_table.get_ids_for_value with
        the resolved column name. Union converted IDs for all operands. An empty set
        means no candidates; None requests a scan. The caller still checks actual
        values after narrowing, and lookup errors propagate.

        Example:
            An IN predicate for ratings (3, 5) unions IDs from both reverse lookups
            before the query evaluates each remaining row value.


        :param table: Base table whose rows are filtered.
        :param predicate: Predicate with a resolvable field and prepared operand.
        :return: Set of candidate row IDs, or None when no supported lookup path is available.
        """

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
        """
        Evaluate a single predicate against a scalar or projected sequence value.

        EQ and IN use membership for Sequence values except str/bytes. CONTAINS and
        PREFIX compare normalized operand text with recursively flattened field text.
        IS_NULL checks identity with None, inverted only when its operand is False
        by identity. Ordered comparisons use Python operators and suppress TypeError
        only; other comparison, membership or normalization failures propagate.

        Example:
            >>> predicate = CachePredicate("title", CacheFilterOperator.CONTAINS, "CAFÉ")
            >>> CacheQueryEngine._matches_predicate("Café Society", predicate)
            True
            >>> CacheQueryEngine._matches_predicate(None, CachePredicate("x", CacheFilterOperator.LT, 3))
            False


        :param value: Cached field value to test without mutation.
        :param predicate: Predicate whose operator and operand select matching behavior.
        :return: Boolean match result; unsupported operators and ordered TypeError comparisons are false.
        """

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

    def _build_text_index(
        self, table: str, field: str
    ) -> tuple[dict[int, str], dict[str, set[int]]]:
        """
        Lazily index one field's flattened text and per-trigram row memberships.

        Reuse the pair only when both caches already contain the table/field key.
        Otherwise read all current row IDs and values, then publish both dictionaries
        after successful construction. Returned dictionaries are internal mutable
        indexes; callers must treat them as read-only. Freshness depends on reset.

        Example:
            Repeated text predicates over books.title reuse its index until the facade
            invalidates or resets the engine.


        :param table: Base table containing the rows to index.
        :param field: Field request resolved to a canonical key.
        :return: Pair of internal dictionaries: row ID to text, and trigram to row-ID set.
        """

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
        """
        Match every normalized search term in at least one selected field.

        Different terms may match different fields. Intersect trigram candidates
        within each field and union across fields, then verify actual substrings to
        remove false positives. Terms shorter than three characters use all rows as
        candidates. No terms returns every row; nonempty terms with no fields match
        nothing. Indexes remain dependent on caller-managed freshness.

        Example:
            Searching alpha history across title and tag fields can match a title
            containing alpha with a separate tag value containing history.


        :param table: Base table supplying the candidate row universe.
        :param text: Search text split on whitespace after normalization and case folding.
        :param fields: Field requests across which each term may match.
        :return: Set of row IDs whose indexed field text contains every term.
        """

        terms = tuple(term for term in normalize_cache_text(text).split() if term)
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
                    field_candidates: set[int] | None = None
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
        """
        Find base-table IDs linked to any target ID in a relation constraint.

        Try table-to-target and destination getters first, then reverse orientation
        on KeyError. Prefer plural getters and fall back to singular ones. Getter
        results are treated as sequences or one link object, skipping None. KeyError
        anywhere in the forward attempt triggers reverse lookup; reverse getter
        failures propagate. This constraint does not request link ordering.

        Example:
            A relation to tags (7, 8) selects books linked to either target tag, with
            duplicate book IDs collapsed.


        :param table: Base table whose source-side IDs are requested.
        :param relation: Target table, IDs and optional type filter.
        :return: Set of linked base-table IDs; empty when both directed routes are absent.
        :raises TypeError: A resolved link table exposes neither required getter form.
        """

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
                links = cast(
                    Sequence[QueryLink],
                    raw_links if isinstance(raw_links, Sequence) else (raw_links,),
                )
                ids.update(int(link.src_id) for link in links)
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
                raise TypeError("link table does not expose source getters") from None
            ids = set()
            for target_id in target_ids:
                raw_links = getter(
                    target_id,
                    type_filter=relation.type_filter,
                )
                if raw_links is None:
                    continue
                links = cast(
                    Sequence[QueryLink],
                    raw_links if isinstance(raw_links, Sequence) else (raw_links,),
                )
                ids.update(int(link.dst_id) for link in links)
            return ids

    def _sort_ids(
        self,
        table: str,
        ids: Iterable[int],
        sort_specs: Sequence[CacheSort],
    ) -> list[int]:
        """
        Order row IDs stably by requested fields with nulls last in either direction.

        Apply components in reverse order using stable sorts. For each component,
        separate None values and append them after present values even for descending
        order. Mixed present values use _sort_value. Initial row-ID order breaks ties
        when the value keys tie; unordered collection values retain their iteration
        order in the key.

        Example:
            Sorting by descending rating keeps unrated books at the end and leaves
            equal rating keys ordered by row ID.


        :param table: Base table owning sort fields.
        :param ids: Row IDs converted to int and initially sorted ascending.
        :param sort_specs: Sort components in decreasing precedence order.
        :return: List of ordered IDs; repeated input IDs are retained.
        """

        ordered = sorted(int(row_id) for row_id in ids)
        for sort_spec in reversed(tuple(sort_specs)):
            canonical = self._canonical_field(table, sort_spec.field)
            present: list[int] = []
            missing: list[int] = []
            values: dict[int, Any] = {}
            for row_id in ordered:
                value = self.storage.get_cached_value(row_id, canonical)
                values[row_id] = value
                (missing if value is None else present).append(row_id)
            present.sort(
                key=lambda row_id: _sort_value(values[row_id]),
                reverse=not sort_spec.ascending,
            )
            ordered = present + missing
        return ordered

    def _page_ids(
        self,
        table: str,
        ids: Set[int],
        sort_specs: Sequence[CacheSort],
        offset: int,
        limit: int | None,
    ) -> list[int]:
        """Select a small prefix or suffix; use stable full sorting for larger pages.

        Keep at most the smaller of the requested prefix and remaining suffix,
        only when it is less than a quarter of the matches. Every requested sort
        value is read, but only retained heap entries need stored keys. Nulls stay
        last in each direction, with ascending row identity breaking complete ties.
        NaNs lack a total ordering, so preserve their legacy stable-sort behaviour.
        Bounds are integer-normalized and fields validated by the caller first.
        """
        total = len(ids)
        if limit == 0 or offset >= total:
            return []
        end = total if limit is None else min(total, offset + limit)
        tail = total - offset
        if min(end, tail) * 4 >= total:
            return self._sort_ids(table, ids, sort_specs)[offset:end]

        resolved = tuple(
            (self._canonical_field(table, spec.field), bool(spec.ascending))
            for spec in sort_specs
        )
        # Compare the primary component directly, including descending values.
        # Only components with the opposite direction need a reversing wrapper.
        reverse = not resolved[0][1] if resolved else False

        def key(row_id: int) -> tuple[Any, ...]:
            parts: list[Any] = []
            for field, ascending in resolved:
                value = self.storage.get_cached_value(row_id, field)
                parts.append((value is None) != reverse)
                if value is None:
                    parts.append(())
                else:
                    component = _sort_value(value, require_total_order=True)
                    parts.append(
                        component
                        if ascending != reverse
                        else _DescendingSortKey(component)
                    )
            return (*parts, -row_id if reverse else row_id)

        try:
            # Default ordering needs only IDs, without reading any field values.
            if end <= tail:
                select = nlargest if reverse else nsmallest
                return select(end, ids, key=key if resolved else None)[offset:end]
            select = nsmallest if reverse else nlargest
            ordered = select(tail, ids, key=key if resolved else None)
            ordered.reverse()
            return ordered[: end - offset]
        except _UnorderedSortValue:
            return self._sort_ids(table, ids, sort_specs)[offset:end]

    def _record(
        self,
        table: str,
        row_id: int,
        projection: Sequence[str],
    ) -> CacheRecord:
        """
        Materialize a full row or requested fields into a shallow cache record.

        Projected keys retain the requested spelling. Add the table's bare ID column
        unless either its bare or table-qualified key was already requested. Duplicate
        projection keys overwrite earlier values. Full rows use get_row_snapshot
        directly; field and backend read errors propagate.

        Example:
            A title-only projection also receives the table's ID column so mapping
            consumers can retain row identity.


        :param table: Cached main-table name.
        :param row_id: Row ID to materialize; the caller establishes row existence.
        :param projection: Requested field names; empty selects the full row snapshot.
        :return: CacheRecord with top-level values copied into a read-only mapping.
        """

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

    def get(self, table: str, row_id: int) -> CacheRecord | None:
        """
        Return a complete row snapshot when the cached table contains its ID.

        Unknown tables raise rather than returning None. This engine method does
        not perform readiness checks, refresh dependencies or return lookup metadata.

        Example:
            The facade wraps an engine result of None in a complete MISS lookup.


        :param table: Main table name.
        :param row_id: Requested row identity converted with int.
        :return: CacheRecord for an existing row, otherwise None.
        """

        table_cache = self._table(table)
        if not table_cache.has_id(int(row_id)):
            return None
        return self._record(table, int(row_id), ())

    def query(self, query: CacheQuery, *, generation: int) -> CacheQueryResult:
        """
        Filter, sort, page and materialize a structured query through storage.

        Begin with all table IDs, intersect an optional relation constraint, and
        apply predicates in order using available candidate lookups plus value checks.
        Nonblank text uses explicit text_fields or all main-table columns. Select
        ordered page IDs and project only visible rows. Empty pages skip ordering;
        small pages use bounded selection when sort values have a total ordering.
        All fields and predicate branches are validated before filtering,
        including branches with no candidates. The method does not take a lock or create a database
        snapshot, so live backend reads may observe changes during evaluation.

        Example:
            A request with limit=0 can return no records while total_count still
            reports all matching rows.


        :param query: Prepared CacheQuery carrying table, constraints, ordering and projection.
        :param generation: Facade generation copied into the result without validation.
        :return: Complete CacheQueryResult with visible records and the total count before paging.
        """

        evaluator = PredicateEvaluator(self)
        try:
            plan_key = _freeze_key(query)
            result_key = self._result_key(query, generation)
        except TypeError:
            plan_key = result_key = None
        if plan_key is None or plan_key not in self._plans:
            self._validate_query(query, evaluator)
            if plan_key is not None:
                self._remember(self._plans, plan_key, None, self.plan_cache_size)
        else:
            self._plans.move_to_end(plan_key)
        offset = index(query.offset)
        limit = None if query.limit is None else index(query.limit)
        ids: Set[int]
        if result_key is not None and result_key in self._results:
            self.result_hits += 1
            self._results.move_to_end(result_key)
            self._result_queries.move_to_end(result_key)
            ids = self._results[result_key]
        else:
            ids = self._matching_ids(query, evaluator)
            if result_key is not None:
                self._remember(self._results, result_key, ids, self.result_cache_size)
                self._remember(
                    self._result_queries,
                    result_key,
                    deepcopy(query, {id(LINK_TYPE_UNSET): LINK_TYPE_UNSET}),
                    self.result_cache_size,
                )

        total_count = len(ids)
        visible = self._page_ids(query.table, ids, query.sort, offset, limit)
        records = tuple(
            self._record(query.table, row_id, query.projection) for row_id in visible
        )
        return CacheQueryResult(
            records, total_count, query.offset, query.limit, True, generation
        )

    def _matching_ids(
        self,
        query: CacheQuery,
        evaluator: PredicateEvaluator,
        candidates: set[int] | None = None,
    ) -> set[int]:
        """Evaluate filters once; result caching never retains mutable records."""
        if candidates is None:
            ids = self._all_ids(query.table)
        else:
            table = self._table(query.table)
            ids = {row_id for row_id in candidates if table.has_id(row_id)}
        if query.where is not None:
            evaluator.check_references(query.table, query.where, ids)
            ids = evaluator.evaluate(query.table, query.where, ids)

        if query.relation is not None:
            ids &= self._ids_for_relation(query.table, query.relation)

        for predicate in query.predicates:
            # Restricted repairs already have a small candidate set. Expanding
            # an exact-value index here could copy an entire matching population.
            if candidates is None:
                indexed = self._candidate_ids_for_predicate(query.table, predicate)
                if indexed is not None:
                    ids &= indexed
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
                    str(column) for column in self._table(query.table).column_headings
                )
            ids &= self._text_candidate_ids(query.table, query.text, text_fields)

        return ids

    def related_ids(
        self,
        source_table: str,
        source_ids: Iterable[int],
        target_table: str,
        *,
        type_filter: str | None = None,
    ) -> tuple[int, ...]:
        """
        Traverse related target IDs once each in source and link traversal order.

        Try the forward link route, otherwise reverse on route-lookup KeyError.
        Plural getters receive require_ordering=True; singular getters do not. Skip
        None link results and deduplicate across all sources. This does not verify
        that the target main-table rows exist; the facade does that during materialization.

        Example:
            If sources 1 and 2 yield target IDs (8, 7) and (7, 9), this returns
            (8, 7, 9).


        :param source_table: Table containing supplied source IDs.
        :param source_ids: Source row IDs consumed and converted with int in supplied order.
        :param target_table: Other endpoint table whose IDs are returned.
        :param type_filter: Optional type restriction forwarded to the link getter.
        :return: Tuple of distinct target IDs in first-seen traversal order.
        :raises KeyError: Neither link-table orientation resolves; getter KeyErrors also propagate.
        :raises TypeError: The resolved link table has neither required relation getter.
        """

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
            links = cast(
                Sequence[QueryLink],
                raw_links if isinstance(raw_links, Sequence) else (raw_links,),
            )
            for link in links:
                target_id = int(getattr(link, id_attribute))
                if target_id not in seen:
                    seen.add(target_id)
                    ordered.append(target_id)
        return tuple(ordered)


__all__ = ["CacheQueryEngine", "normalize_cache_text"]
