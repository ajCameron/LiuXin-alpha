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


def _sort_value(value: Any) -> tuple[int, Any]:
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
    :return: Tuple of type rank and comparable payload for supported value families.
    """

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

    def __init__(self, storage: StorageCacheAPI) -> None:
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
        self._text_values: dict[tuple[str, str], dict[int, str]] = {}
        self._text_trigrams: dict[tuple[str, str], dict[str, set[int]]] = {}

    def reset(self) -> None:
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

    def _table(self, table_name: str) -> Any:
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
        return str(getattr(resolved, "field_key"))

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
    ) -> Optional[set[int]]:
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

    def _build_text_index(self, table: str, field: str) -> tuple[dict[int, str], dict[str, set[int]]]:
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

    def get(self, table: str, row_id: int) -> Optional[CacheRecord]:
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
        Nonblank text uses explicit text_fields or all main-table columns. Sort before
        paging and project only visible rows. Fields used only in empty result paths
        may never be resolved. The method does not take a lock or create a database
        snapshot, so live backend reads may observe changes during evaluation.

        Example:
            A request with limit=0 can return no records while total_count still
            reports all matching rows.


        :param query: Prepared CacheQuery carrying table, constraints, ordering and projection.
        :param generation: Facade generation copied into the result without validation.
        :return: Complete CacheQueryResult with visible records and the total count before paging.
        """

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
