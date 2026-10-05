#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Evaluate Calibre-compatible queries over Catalog caches and stored search definitions.

Matching supports text, typed fields, grouped aliases and user categories.
SavedSearchQueries persists query text; LRUCache stores derived values.
Search coordinates parsing/cache reuse around these components. Existing
match/matchkind aliases and legacy parser behavior remain compatibility
surfaces, including mode-specific candidate and cache limitations.
"""

from __future__ import unicode_literals, division, absolute_import, print_function, annotations

import re
import weakref
from collections import deque
from functools import partial

from typing import Union, Literal, TYPE_CHECKING, Optional, Any

from LiuXin_alpha.constants import preferred_encoding
from LiuXin_alpha.catalog.search.field_searches.boolean_search import BooleanSearch
from LiuXin_alpha.catalog.search.field_searches.date_search import DateSearch
from LiuXin_alpha.catalog.search.field_searches.numeric_search import NumericSearch

from LiuXin_alpha.utils.config.config_base import prefs
from LiuXin_alpha.utils.text.icu import lower as icu_lower, primary_contains, sort_key
from LiuXin_alpha.utils.localization import _, lang_map, canonicalize_lang
from LiuXin_alpha.utils.search_query_parser import SearchQueryParser, ParseException

from LiuXin_alpha.utils.libraries.liuxin_six import basestring, iterkeys, six_unicode as unicode

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"

CONTAINS_MATCH = 0
EQUALS_MATCH = 1
REGEXP_MATCH = 2

# Utils {{{


def _matchkind(query: str) -> tuple[Union[Literal[0], Literal[1], Literal[2]], str]:
    """
    Select contains, equals or regex matching from a query prefix.

    For text longer than one character, a leading backslash escapes the next
    prefix; = selects equality and ~ selects regex. ICU-lowercase nonregex
    queries, preserving regex case because escapes may be case-sensitive.

    Example:
        >>> _matchkind("=Title")
        (1, 'title')


    :param query: Query text; one-character prefixes remain literal contains queries.
    :return: Pair of match-kind constant and processed query.
    """
    match_kind = CONTAINS_MATCH

    if len(query) > 1:
        if query.startswith("\\"):
            query = query[1:]
        elif query.startswith("="):
            match_kind = EQUALS_MATCH
            query = query[1:]
        elif query.startswith("~"):
            match_kind = REGEXP_MATCH
            query = query[1:]

    assert match_kind in (0, 1, 2)

    # leave case in regexps because it can be significant e.g. \S \W \D
    if match_kind != REGEXP_MATCH:
        query = icu_lower(query)
    return match_kind, query


matchkind = _matchkind


def _match(
        query: str,
        value: tuple[str],
        matchkind,
        use_primary_find_in_search: bool = True) -> bool:
    """
    Match any supplied text value using contains, equality or regex rules.

    Values are ICU-lowercased. Equality supports a leading dot for hierarchical
    prefixes and two dots for exact dot-component matches. Two-dot preprocessing
    also removes one dot before other modes. Regex uses case-insensitive Unicode
    search; re.error is suppressed for incomplete search-ahead expressions. Other
    errors propagate, including an empty equality query indexing query[0].

    Example:
        >>> _match(".books", ("books.fiction",), EQUALS_MATCH)
        True


    :param query: Processed query, normally from _matchkind.
    :param value: Iterable of text values, despite the single-element tuple annotation.
    :param matchkind: CONTAINS_MATCH, EQUALS_MATCH or REGEXP_MATCH; unknown values do not match.
    :param use_primary_find_in_search: Use ICU primary_contains for contains mode; false uses substring membership.
    :return: True on the first match, otherwise False.
    """
    if query.startswith(".."):
        query = query[1:]
        sq = query[1:]
        internal_match_ok = True
    else:
        internal_match_ok = False

    for t in value:
        # ignore regexp exceptions, required because search-ahead tries before typing is finished
        try:
            t = icu_lower(t)
            if matchkind == EQUALS_MATCH:
                if internal_match_ok:
                    if query == t:
                        return True
                    comps = [c.strip() for c in t.split(".") if c.strip()]
                    for comp in comps:
                        if sq == comp:
                            return True
                elif query[0] == ".":
                    if t.startswith(query[1:]):
                        ql = len(query) - 1
                        if (len(t) == ql) or (t[ql : ql + 1] == "."):
                            return True
                elif query == t:
                    return True

            elif matchkind == REGEXP_MATCH:
                if re.search(query, t, re.I | re.UNICODE):
                    return True

            elif matchkind == CONTAINS_MATCH:
                if use_primary_find_in_search:
                    if primary_contains(query, t):
                        return True
                elif query in t:
                    return True

        except re.error:
            pass

    return False


match = _match


class KeyPairSearch: # {{{
    """
    Search mapping-valued fields by key/value patterns or presence.

    Example:
        An identifiers mapping can be searched with ``isbn:123`` or ``isbn:false``.
    """
    def __call__(self, query: str, field_iter, candidates, use_primary_find: bool) -> set[int]:
        """
        Collect IDs whose mapping entries match a key/value query.

        Without a colon, match values across all keys. Processed true/false values
        select presence logic even with explicit =/~ prefixes. Empty key/value
        patterns impose no constraint on that side. Positive results rely on the
        iterator to restrict IDs to candidates; no final intersection is performed.

        Example:
            A key:true query tests the exact processed key's truthy value, ignoring its
            pattern match mode.


        :param query: Text split at its first colon; key/value sides use _matchkind.
        :param field_iter: Zero-argument callable yielding (mapping, ID set) pairs.
        :param candidates: Candidate ID set used for false/presence complements; not mutated.
        :param use_primary_find: ICU primary matching preference for ordinary key/value patterns.
        :return: Union of matching ID sets or candidates minus IDs with a present value.
        """
        matches = set()
        if ":" in query:
            q = [q.strip() for q in query.partition(":")[0::2]]
            keyq, valq = q
            keyq_mkind, keyq = _matchkind(keyq)
            valq_mkind, valq = _matchkind(valq)
        else:
            keyq = keyq_mkind = ""
            valq_mkind, valq = _matchkind(query)

        if valq in {"true", "false"}:
            found = set()
            if keyq:
                for val, book_ids in field_iter():
                    if val and val.get(keyq, False):
                        found |= book_ids
            else:
                for val, book_ids in field_iter():
                    if val:
                        found |= book_ids

            return found if valq == "true" else candidates - found

        for m, book_ids in field_iter():
            for key, val in m.items():
                if keyq and not _match(
                    keyq,
                    (key,),
                    keyq_mkind,
                    use_primary_find_in_search=use_primary_find,
                ):
                    continue
                if valq and not _match(
                    valq,
                    (val,),
                    valq_mkind,
                    use_primary_find_in_search=use_primary_find,
                ):
                    continue
                matches |= book_ids
                break
        return matches


# }}}


# Todo: Probably should not be here - actually a cache thing?
class SavedSearchQueries:  # {{{
    """
    Retain named query text backed by one database preference.

    Hold only a weak database reference. Non-weak-referenceable handles and
    None produce an inert instance with an empty mapping. Mutating operations
    are no-ops after the database is gone; this stores queries, not result sets.

    Example:
        ``SavedSearchQueries(None, "saved")`` permits lookup/names without persistence.
    """
    queries = {}
    opt_name = ""

    def __init__(self, db: "DatabaseAPI", _opt_name) -> None:
        """
        Capture a weak database reference and load its saved-query preference.

        Example:
            A non-weak-referenceable handle is treated as absent, even if it exposes preference methods.


        :param db: Database exposing pref/set_pref/_set_pref, or None.
        :param _opt_name: Preference key used for reading and subsequent writes.
        :return: None; sets an instance mapping from the database or {}.
        """
        self.opt_name = _opt_name
        try:
            self._db = weakref.ref(db)
        except TypeError:
            # db could be None
            self._db = lambda: None
        self.load_from_db()

    @property
    def db(self) -> Optional["DatabaseAPI"]:
        """
        Dereference the borrowed database handle.

        Example:
            Keeping SavedSearchQueries alive does not keep its database alive.


        :return: Live database object, or None if absent/collected.
        """
        return self._db()

    def load_from_db(self) -> None:
        """
        Replace local query state with the database preference value.

        Example:
            Calling this after database collection resets queries to {}.


        :return: None; retains the returned preference mapping without copying or validation.
        """
        db = self.db
        if db is not None:
            self.queries = db.pref(self.opt_name, default={})
        else:
            self.queries = {}

    # Todo: This is an adaptor, and so should be with the adaptors
    @staticmethod
    def force_unicode(x: Any) -> str:
        """
        Preserve text or decode a byte-like value with replacement.

        Example:
            A malformed byte sequence is replaced during decoding; arbitrary integers are not stringified.


        :param x: Text, or object supporting decode(preferred_encoding, "replace").
        :return: Unicode text; unsupported objects can raise AttributeError.
        """
        if not isinstance(x, unicode):
            x = x.decode(preferred_encoding, "replace")
        return x

    def add(self, name: Any, value: Any) -> None:
        """
        Set a decoded name and stripped query, then persist with set_pref.

        The local map changes before persistence; write failure does not restore it.

        Example:
            An existing name is overwritten, while whitespace in the name is preserved.


        :param name: Query name; decoded but not stripped.
        :param value: Query text; decoded and stripped.
        :return: None; no action without a live database.
        """
        db = self.db
        if db is not None:
            self.queries[self.force_unicode(name)] = self.force_unicode(value).strip()
            db.set_pref(self.opt_name, self.queries)

    def lookup(self, name: str) -> Optional[str]:
        """
        Read a saved query by its decoded exact name.

        Example:
            Lookup continues to use the loaded map even if the weak database reference expires.


        :param name: Query name; no whitespace trimming.
        :return: Stored value, or None for an unknown name.
        """
        return self.queries.get(self.force_unicode(name), None)

    def delete(self, name):
        """
        Remove a decoded name and persist the resulting mapping.

        Persistence failure leaves the local deletion applied.

        Example:
            An absent database makes deletion a no-op on local state.


        :param name: Exact query name to remove.
        :return: None; missing names are tolerated and still trigger set_pref when connected.
        """

        db = self.db
        if db is not None:
            self.queries.pop(self.force_unicode(name), False)
            db.set_pref(self.opt_name, self.queries)

    def rename(self, old_name, new_name):
        """
        Assign the old value to a new name, remove the old key and persist.

        No operation occurs without a live database. Local mutation precedes the
        private persistence call and is not rolled back on failure.

        Example:
            Renaming a missing old name stores None under the new name; renaming a name
            to itself removes it.


        :param old_name: Old name decoded without trimming.
        :param new_name: New name decoded without trimming; existing values are overwritten.
        :return: None; uses _set_pref when the database is live.
        """

        db = self.db
        if db is not None:
            self.queries[self.force_unicode(new_name)] = self.queries.get(self.force_unicode(old_name), None)
            self.queries.pop(self.force_unicode(old_name), False)
            db._set_pref(self.opt_name, self.queries)

    def set_all(self, smap):
        """
        Adopt a caller-supplied query mapping and persist it directly.

        Example:
            Subsequent mutation of the supplied mapping is visible in local query state.


        :param smap: Mapping retained by reference; names/values are not decoded or stripped.
        :return: None; uses _set_pref, or does nothing without a live database.
        """

        db = self.db
        if db is not None:
            self.queries = smap
            db._set_pref(self.opt_name, smap)

    def names(self):
        """
        Sort stored names using the ICU collation key.

        Example:
            Names are returned in collation order rather than insertion order.


        :return: New list of query names.
        """

        return sorted(iterkeys(self.queries), key=sort_key)


# }}}


class Parser(SearchQueryParser):  # {{{
    """
    Dispatch parsed searches over cached, virtual and typed field values.

    The parser borrows a cache and candidate universe, tracks virtual-field
    use, and delegates expression parsing to SearchQueryParser. Group aliases
    and category matching retain legacy recursion/complement semantics.

    Example:
        A title query uses text matching, while a numeric ID query iterates the
        candidate IDs directly.
    """

    def __init__(
        self,
        dbcache,
        all_book_ids,
        gst,
        date_search,
        num_search,
        bool_search,
        keypair_search,
        limit_search_columns,
        limit_search_columns_to,
        locations,
        virtual_fields,
        lookup_saved_search,
        parse_cache,
    ):
        """
        Bind field-search collaborators and initialize optimized expression parsing.

        A false-valued virtual mapping is replaced by a new dictionary. Location
        generators can be exhausted while creating all_search_locations before
        base initialization; supply a reusable collection.

        Example:
            If no marked virtual field is supplied, this parser serves as its empty-value fallback.


        :param dbcache: Borrowed cache exposing fields, field_metadata, metadata proxies and preferences.
        :param all_book_ids: Candidate universe retained by reference.
        :param gst: Grouped-search configuration retained by reference.
        :param date_search: Date-search callable.
        :param num_search: Numeric-search callable.
        :param bool_search: Boolean-search callable.
        :param keypair_search: Mapping key/value search callable.
        :param limit_search_columns: Whether an all-location search may be restricted.
        :param limit_search_columns_to: Locations allowed for restricted all searches.
        :param locations: Reusable location collection passed to both frozenset and the base parser.
        :param virtual_fields: Optional mapping of virtual fields; a truthy supplied mapping is mutated to add marked if absent.
        :param lookup_saved_search: Saved-search lookup callback passed to the base parser.
        :param parse_cache: Shared parse cache passed to the base parser.
        :return: None; configures collaborators without reading searchable values.
        """

        self.dbcache, self.all_book_ids = dbcache, all_book_ids
        self.all_search_locations = frozenset(locations)
        self.grouped_search_terms = gst
        self.date_search, self.num_search = date_search, num_search
        self.bool_search, self.keypair_search = bool_search, keypair_search
        self.limit_search_columns, self.limit_search_columns_to = (
            limit_search_columns,
            limit_search_columns_to,
        )
        self.virtual_fields = virtual_fields or {}
        if "marked" not in self.virtual_fields:
            self.virtual_fields["marked"] = self
        SearchQueryParser.__init__(
            self,
            locations,
            optimize=True,
            lookup_saved_search=lookup_saved_search,
            parse_cache=parse_cache,
        )

    @property
    def field_metadata(self):
        """
        Expose the bound cache's field descriptor container.

        Example:
            This property fails once an owning Search detaches dbcache after evaluation.


        :return: Live dbcache.field_metadata reference.
        """

        return self.dbcache.field_metadata

    def universal_set(self):
        """
        Expose the borrowed universe of candidate book IDs.

        Example:
            Callers must copy this set before mutating a derived search universe.


        :return: The original all_book_ids object, without a copy.
        """

        return self.all_book_ids

    def field_iter(self, name, candidates):
        """
        Delegate value iteration to a real or fallback virtual field.

        Missing real and virtual keys propagate KeyError. The metadata proxy is
        obtained before field lookup; this method does not intersect returned IDs.

        Example:
            Using a virtual field sets virtual_field_used=True.


        :param name: Field key resolved in cache.fields, then virtual_fields on KeyError.
        :param candidates: Candidate IDs forwarded unchanged.
        :return: Result of field.iter_searchable_values(metadata_proxy, candidates).
        """

        get_metadata = self.dbcache._get_proxy_metadata
        try:
            field = self.dbcache.fields[name]
        except KeyError:
            field = self.virtual_fields[name]
            self.virtual_field_used = True
        return field.iter_searchable_values(get_metadata, candidates)

    def iter_searchable_values(self, *args, **kwargs):
        """
        Supply no values when the parser acts as the marked virtual field.

        Example:
            The default marked field contributes no matches until a real virtual field is supplied.


        :param args: Ignored positional field-iteration arguments.
        :param kwargs: Ignored keyword field-iteration arguments.
        :return: Empty iterator.
        """

        return iter(())

    def parse(self, *args, **kwargs):
        """
        Reset virtual-field tracking before delegating expression evaluation.

        The current base parser replaces an explicit candidates argument with
        universal_set(), so parse-level candidate restriction is ignored.
        Base-parser exceptions propagate after the flag reset.

        Example:
            Each parse starts with virtual_field_used=False; field iteration can set it during evaluation.


        :param args: Base-parser arguments, normally query and optional candidate IDs.
        :param kwargs: Base-parser keyword arguments passed unchanged.
        :return: Matched ID set returned by SearchQueryParser.parse.
        """

        self.virtual_field_used = False
        return SearchQueryParser.parse(self, *args, **kwargs)

    def get_matches(self, location, query, candidates=None, allow_recursion=True):
        """
        Dispatch one location/query pair to typed, grouped or text matching.

        Resolve aliases through field metadata. Dispatch dates, numbers, multiplicity
        counts, booleans and colon-separated mappings before generic text. ISBN
        location injects an exact isbn key query. User categories use their own
        lookup. Restricted all searches union only configured valid locations.
        Generic all omits virtual fields, uuid, id and series_sort; presence tests
        use truthy nonblank values. Languages resolve names/codes, numeric all-field
        matching compares converted values, and text matching honors prefix modes.
        Virtual-field use is recorded. No broad exception handling hides collaborator
        errors or unsupported recursive groups.

        Example:
            A grouped false query complements against all_book_ids, which can include
            IDs outside a supplied candidate subset.


        :param location: Allowed search location; checked before trimming/lowercasing aliases.
        :param query: Query text; blank/whitespace input returns no matches.
        :param candidates: Optional candidate set; None uses all_book_ids and supplied sets are not mutated.
        :param allow_recursion: Allow one grouped-alias expansion; nested grouped expansion raises.
        :return: Matched ID set, subject to legacy group-complement and iterator behavior.
        :raises ParseException: A grouped alias expands into another group while recursion is disabled.
        """
        # If candidates is not None, it must not be modified. Changing its value will break query optimization in the
        # search parser
        matches = set()

        if candidates is None:
            candidates = self.all_book_ids
        if not candidates or not query or not query.strip():
            return matches
        if location not in self.all_search_locations:
            return matches

        if len(location) > 2 and location.startswith("@") and location[1:] in self.grouped_search_terms:
            location = location[1:]

        # get metadata key associated with the search term. Eliminates
        # dealing with plurals and other aliases
        original_location = location
        location = self.field_metadata.search_term_to_field_key(icu_lower(location.strip()))
        # grouped search terms
        if isinstance(location, list):
            if allow_recursion:
                if query.lower() == "false":
                    invert = True
                    query = "true"
                else:
                    invert = False
                for loc in location:
                    c = candidates.copy()
                    m = self.get_matches(loc, query, candidates=c, allow_recursion=False)
                    matches |= m
                    c -= m
                    if len(c) == 0:
                        break
                if invert:
                    matches = self.all_book_ids - matches
                return matches
            raise ParseException(_("Recursive query group detected: {0}").format(query))

        # If the user has asked to restrict searching over all field, apply
        # that restriction
        if location == "all" and self.limit_search_columns and self.limit_search_columns_to:
            terms = set()
            for l in self.limit_search_columns_to:
                l = icu_lower(l.strip())
                if l and l != "all" and l in self.all_search_locations:
                    terms.add(l)
            if terms:
                c = candidates.copy()
                for l in terms:
                    m = self.get_matches(l, query, candidates=c, allow_recursion=allow_recursion)
                    matches |= m
                    c -= m
                    if len(c) == 0:
                        break
                return matches

        upf = prefs["use_primary_find_in_search"]

        if location in self.field_metadata:
            fm = self.field_metadata[location]
            dt = fm["datatype"]

            # take care of dates special case
            if dt == "datetime" or (dt == "composite" and fm["display"].get("composite_sort", "") == "date"):
                if location == "date":
                    location = "timestamp"
                return self.date_search(icu_lower(query), partial(self.field_iter, location, candidates))

            # take care of numbers special case
            if dt in ("rating", "int", "float") or (
                dt == "composite" and fm["display"].get("composite_sort", "") == "number"
            ):
                if location == "id":
                    is_many = False

                    def fi(default_value=None):
                        """
                        Yield candidate IDs as their own numeric field values.

                        Example:
                            Numeric ID matching needs no cached field lookup because IDs are their own values.


                        :param default_value: Unused compatibility default argument.
                        :return: Iterator of (ID, singleton ID set) pairs from the enclosing candidates.
                        """

                        for qid in candidates:
                            yield qid, {qid}

                else:
                    field = self.dbcache.fields[location]
                    fi, is_many = (
                        partial(self.field_iter, location, candidates),
                        field.is_many,
                    )
                return self.num_search(icu_lower(query), fi, location, dt, candidates, is_many=is_many)

            # take care of the 'count' operator for is_multiples
            if fm["is_multiple"] and len(query) > 1 and query[0] == "#" and query[1] in "=<>!":
                return self.num_search(
                    icu_lower(query[1:]),
                    partial(self.dbcache.fields[location].iter_counts, candidates),
                    location,
                    dt,
                    candidates,
                )

            # take care of boolean special case
            if dt == "bool":
                return self.bool_search(
                    icu_lower(query),
                    partial(self.field_iter, location, candidates),
                    self.dbcache._pref("bools_are_tristate"),
                )

            # special case: colon-separated fields such as identifiers. isbn
            # is a special case within the case
            if fm.get("is_csp", False):
                field_iter = partial(self.field_iter, location, candidates)
                if location == "identifiers" and original_location == "isbn":
                    return self.keypair_search("=isbn:" + query, field_iter, candidates, upf)
                return self.keypair_search(query, field_iter, candidates, upf)

        # check for user categories
        if len(location) >= 2 and location.startswith("@"):
            return self.get_user_category_matches(location[1:], icu_lower(query), candidates)

        # Everything else (and 'all' matches)
        matchkind, query = _matchkind(query)
        all_locs = set()
        text_fields = set()
        field_metadata = {}

        for x, fm in self.field_metadata.items():
            if x.startswith("@"):
                continue
            if fm["search_terms"] and x not in {"series_sort", "id"}:
                if x not in self.virtual_fields and x != "uuid":
                    # We dont search virtual fields because if we do, search
                    # caching will not be used
                    all_locs.add(x)
                field_metadata[x] = fm
                if fm["datatype"] in {
                    "composite",
                    "text",
                    "comments",
                    "series",
                    "enumeration",
                }:
                    text_fields.add(x)

        locations = all_locs if location == "all" else {location}

        current_candidates = set(candidates)

        try:
            rating_query = int(float(query)) * 2
        except (TypeError, ValueError, OverflowError):
            rating_query = None

        try:
            int_query = int(float(query))
        except (TypeError, ValueError, OverflowError):
            int_query = None

        try:
            float_query = float(query)
        except (TypeError, ValueError, OverflowError):
            float_query = None

        for location in locations:
            current_candidates -= matches
            q = query
            if location == "languages":
                q = canonicalize_lang(query)
                if q is None:
                    lm = lang_map()
                    rm = {v.lower(): k for k, v in lm.items()}
                    q = rm.get(query, query)

            if matchkind == CONTAINS_MATCH and q in {"true", "false"}:
                found = set()
                for val, book_ids in self.field_iter(location, current_candidates):
                    if val and (not hasattr(val, "strip") or val.strip()):
                        found |= book_ids
                matches |= found if q == "true" else (current_candidates - found)
                continue

            dt = field_metadata.get(location, {}).get("datatype", None)
            if dt == "rating":
                if rating_query is not None:
                    for val, book_ids in self.field_iter(location, current_candidates):
                        if val == rating_query:
                            matches |= book_ids
                continue

            if dt == "float":
                if float_query is not None:
                    for val, book_ids in self.field_iter(location, current_candidates):
                        if val == float_query:
                            matches |= book_ids
                continue

            if dt == "int":
                if int_query is not None:
                    for val, book_ids in self.field_iter(location, current_candidates):
                        if val == int_query:
                            matches |= book_ids
                continue

            if location in text_fields:
                # Todo: Broken, for some reason, and a low fix priority
                if location in ["cover", "covers"]:
                    continue

                for val, book_ids in self.field_iter(location, current_candidates):
                    if val is not None:
                        if isinstance(val, basestring):
                            val = (val,)
                        if _match(q, val, matchkind, use_primary_find_in_search=upf):
                            matches |= book_ids

        return matches

    def get_user_category_matches(self, location, query, candidates):
        """
        Union exact member searches for a user category and optional subcategories.

        Read user_categories preference. Each member triggers an exact-value search
        on its category; matches are removed from a local candidate copy. Other
        queries of sufficient length are treated as membership requests, without
        validating that they spell true.

        Example:
            ``.false`` includes subcategories before complementing within candidates.


        :param location: Category name without its leading @.
        :param query: At least two characters; a leading dot includes subcategories, false inverts membership.
        :param candidates: Candidate set copied for progressive member filtering.
        :return: Matched candidate IDs, or their complement for false.
        """

        matches = set()
        if len(query) < 2:
            return matches

        user_cats = self.dbcache._pref("user_categories")
        c = set(candidates)

        if query.startswith("."):
            check_subcats = True
            query = query[1:]
        else:
            check_subcats = False

        for key in user_cats:
            if key == location or (check_subcats and key.startswith(location + ".")):
                for (item, category, ign) in user_cats[key]:
                    s = self.get_matches(category, "=" + item, candidates=c)
                    c -= s
                    matches |= s
        if query == "false":
            return candidates - matches
        return matches


# }}}


class LRUCache(object):  # {{{
    """
    Store bounded values with separate insertion and access order.

    Age tracking controls eviction, but iteration follows dictionary insertion
    order. Re-adding an existing key refreshes age without replacing its value.
    The object is unsynchronized and does not copy stored values.

    Example:
        A repeated add for the same key retains its first value; use pop first
        when replacement is required.
    """

    def __init__(self, limit=50):
        """
        Create empty value and age maps with an unchecked capacity.

        Example:
            A zero or negative capacity causes the first add to evict from an empty deque.


        :param limit: Capacity used by add; supply a positive integer.
        :return: None; initializes an empty dictionary and deque.
        """

        self.item_map = {}
        self.age_map = deque()
        self.limit = limit

    def _move_up(self, key):
        """
        Move an existing key to the newest end of the age deque.

        Example:
            This internal helper assumes dictionary/deque consistency; absent keys can raise ValueError.


        :param key: Key expected to be present in the nonempty age deque.
        :return: None; leaves an already-newest key in place.
        """

        if key != self.age_map[-1]:
            self.age_map.remove(key)
            self.age_map.append(key)

    def add(self, key, val):
        """
        Insert a new value or refresh an existing key without overwriting it.

        Subscription assignment is an alias of this method and also preserves
        existing values.

        Example:
            >>> cache = LRUCache(2)
            >>> cache.add("key", 1)
            >>> cache.add("key", 2)
            >>> cache["key"]
            1


        :param key: Hashable cache key.
        :param val: Value retained by reference only for a newly inserted key.
        :return: None; evicts the oldest entry if capacity is reached.
        """

        if key in self.item_map:
            self._move_up(key)
            return

        if len(self.age_map) >= self.limit:
            self.item_map.pop(self.age_map.popleft())

        self.item_map[key] = val
        self.age_map.append(key)

    __setitem__ = add

    def get(self, key, default=None):
        """
        Retrieve a value and refresh age unless it is identical to the default.

        The identity comparison also skips age refresh for any stored object that
        is the exact default object.

        Example:
            A stored None read with default=None is returned without refreshing its age.


        :param key: Hashable cache key.
        :param default: Fallback object returned unchanged for a missing key.
        :return: Stored value or supplied default.
        """

        ans = self.item_map.get(key, default)
        if ans is not default:
            self._move_up(key)
        return ans

    def clear(self):
        """
        Empty both value storage and age tracking.

        Example:
            After clear, len(cache) is zero and a new insertion starts a fresh age order.


        :return: None; capacity remains unchanged.
        """

        self.item_map.clear()
        self.age_map.clear()

    def pop(self, key, default=None):
        """
        Discard a key from both maps without returning its value.

        A missing age entry is tolerated through ValueError handling.

        Example:
            Unlike dict.pop, this method cannot be used to retrieve the removed value.


        :param key: Hashable key to remove.
        :param default: Fallback passed to dictionary pop; its value is discarded.
        :return: Always None on success, whether the key existed or not.
        """

        self.item_map.pop(key, default)
        try:
            self.age_map.remove(key)
        except ValueError:
            pass

    def __contains__(self, key):
        """
        Check value-map membership without changing access age.

        Example:
            ``key in cache`` does not protect that key from eviction by refreshing it.


        :param key: Hashable key to inspect.
        :return: Whether a stored value exists for the key.
        """

        return key in self.item_map

    def __len__(self):
        """
        Count entries in the age deque.

        Example:
            The count normally equals stored keys; direct external mutation of maps can break that invariant.


        :return: Tracked entry count, assuming both maps remain consistent.
        """

        return len(self.age_map)

    def __getitem__(self, key):
        """
        Read using get semantics, including None on a missing key.

        Example:
            ``cache["missing"]`` returns None instead of raising KeyError.


        :param key: Hashable key to look up.
        :return: Stored value or None; successful non-None reads refresh age.
        """

        return self.get(key)

    def __iter__(self):
        """
        Iterate live key/value pairs in dictionary insertion order.

        Example:
            Accessing an older entry changes eviction age but not its iteration position.


        :return: Iterator over item_map.items(); mutation may invalidate it.
        """

        return iter(self.item_map.items())


# }}}


class Search(object):
    """
    Represents a search of the database.

    Example:
        Exercise Search through its owning regression module::

            python -m pytest -q tests/catalog/test_search_core.py
    """

    MAX_CACHE_UPDATE = 50

    def __init__(self, db, opt_name, all_search_locations=()):
        """
        Initialize and validate the Search state.

        Example:
            Exercise Search.init through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param db: Value supplied for db under the catalog contract.
        :param opt_name: Value supplied for opt name under the catalog contract.
        :param all_search_locations: Value supplied for all search locations under the
            catalog contract.
        :return: None; the function records state or raises through its assertions.
        """
        self.all_search_locations = all_search_locations
        self.date_search = DateSearch()
        self.num_search = NumericSearch()
        self.bool_search = BooleanSearch()
        self.keypair_search = KeyPairSearch()
        self.saved_searches = SavedSearchQueries(db, opt_name)
        self.cache = LRUCache()
        self.parse_cache = LRUCache(limit=100)

    def get_saved_searches(self):
        """
        Return or iterate get saved searches from the normalized catalog state.

        Example:
            Exercise Search.get saved searches through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self.saved_searches

    def change_locations(self, newlocs):
        """
        Perform the catalog change locations operation under explicit validation and ordering rules.

        Example:
            Exercise Search.change locations through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param newlocs: Value supplied for newlocs under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        if frozenset(newlocs) != frozenset(self.all_search_locations):
            self.clear_caches()
            self.parse_cache.clear()
        self.all_search_locations = newlocs

    def update_or_clear(self, dbcache, book_ids=None):
        """
        Perform the catalog update or clear operation under explicit validation and ordering rules.

        Example:
            Exercise Search.update or clear through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param dbcache: Value supplied for dbcache under the catalog contract.
        :param book_ids: Catalog record identities included in the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if book_ids and (len(book_ids) * len(self.cache)) <= self.MAX_CACHE_UPDATE:
            self.update_caches(dbcache, book_ids)
        else:
            self.clear_caches()

    def clear_caches(self):
        """
        Perform the catalog clear caches operation under explicit validation and ordering rules.

        Example:
            Exercise Search.clear caches through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: The deterministic value, row, identity or collection described above.
        """
        self.cache.clear()

    def update_caches(self, dbcache, book_ids):
        """
        Perform the catalog update caches operation under explicit validation and ordering rules.

        Example:
            Exercise Search.update caches through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param dbcache: Value supplied for dbcache under the catalog contract.
        :param book_ids: Catalog record identities included in the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        sqp = self.create_parser(dbcache)
        try:
            return self._update_caches(sqp, book_ids)
        finally:
            sqp.dbcache = sqp.lookup_saved_search = None

    def discard_books(self, book_ids):
        """
        Perform the catalog discard books operation under explicit validation and ordering rules.

        Example:
            Exercise Search.discard books through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param book_ids: Catalog record identities included in the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        book_ids = set(book_ids)
        for query, result in self.cache:
            result.difference_update(book_ids)

    def _update_caches(self, sqp, book_ids):
        """
        Perform the catalog update caches operation under explicit validation and ordering rules.

        Example:
            Exercise Search.update caches through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param sqp: Value supplied for sqp under the catalog contract.
        :param book_ids: Catalog record identities included in the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        book_ids = sqp.all_book_ids = set(book_ids)
        remove = set()
        for query, result in tuple(self.cache):
            try:
                matches = sqp.parse(query)
            except ParseException:
                remove.add(query)
            else:
                # remove books that no longer match
                result.difference_update(book_ids - matches)
                # add books that now match but did not before
                result.update(matches)
        for query in remove:
            self.cache.pop(query)

    def create_parser(self, dbcache, virtual_fields=None):
        """
        Perform the catalog create parser operation under explicit validation and ordering rules.

        Example:
            Exercise Search.create parser through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param dbcache: Value supplied for dbcache under the catalog contract.
        :param virtual_fields: Value supplied for virtual fields under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        return Parser(
            dbcache,
            set(),
            dbcache._pref("grouped_search_terms"),
            self.date_search,
            self.num_search,
            self.bool_search,
            self.keypair_search,
            prefs["limit_search_columns"],
            prefs["limit_search_columns_to"],
            self.all_search_locations,
            virtual_fields,
            self.saved_searches.lookup,
            self.parse_cache,
        )

    def __call__(self, dbcache, query, search_restriction, virtual_fields=None, book_ids=None):
        """
        Return the set of ids of all records that match the specified query and restriction

        Example:
            Exercise Search.call through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param dbcache: Value supplied for dbcache under the catalog contract.
        :param query: Parsed or textual catalog query to evaluate.
        :param search_restriction: Value supplied for search restriction under the catalog
            contract.
        :param virtual_fields: Value supplied for virtual fields under the catalog contract.
        :param book_ids: Catalog record identities included in the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        # We construct a new parser instance per search as the parse is not
        # thread safe.
        sqp = self.create_parser(dbcache, virtual_fields)
        try:
            return self._do_search(sqp, query, search_restriction, dbcache, book_ids=book_ids)
        finally:
            sqp.dbcache = sqp.lookup_saved_search = None

    def _do_search(self, sqp, query, search_restriction, dbcache, book_ids=None):
        """
        Do the search, caching the results. Results are cached only if the search is on the full library and no virtual field is searched on

        Example:
            Exercise Search.do search through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param sqp: Value supplied for sqp under the catalog contract.
        :param query: Parsed or textual catalog query to evaluate.
        :param search_restriction: Value supplied for search restriction under the catalog
            contract.
        :param dbcache: Value supplied for dbcache under the catalog contract.
        :param book_ids: Catalog record identities included in the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if isinstance(search_restriction, bytes):
            search_restriction = search_restriction.decode("utf-8")
        if isinstance(query, bytes):
            query = query.decode("utf-8")

        query = query.strip()
        if book_ids is None and query and not search_restriction:
            cached = self.cache.get(query)
            if cached is not None:
                return cached

        restricted_ids = all_book_ids = dbcache._all_book_ids(type=set)
        if search_restriction and search_restriction.strip():
            cached = self.cache.get(search_restriction.strip())
            if cached is None:
                sqp.all_book_ids = all_book_ids if book_ids is None else book_ids
                restricted_ids = sqp.parse(search_restriction)
                if not sqp.virtual_field_used and sqp.all_book_ids is all_book_ids:
                    self.cache.add(search_restriction.strip(), restricted_ids)
            else:
                restricted_ids = cached
                if book_ids is not None:
                    restricted_ids = book_ids.intersection(restricted_ids)
        elif book_ids is not None:
            restricted_ids = book_ids

        if not query:
            return restricted_ids

        if restricted_ids is all_book_ids:
            cached = self.cache.get(query)
            if cached is not None:
                return cached

        sqp.all_book_ids = restricted_ids
        result = sqp.parse(query)

        if not sqp.virtual_field_used and sqp.all_book_ids is all_book_ids:
            self.cache.add(query, result)

        return result

    @staticmethod
    def populate_all_locations(locations_dict):
        """
        Receives a location_dict - populates the all field (creating it if it isn't set)

        Example:
            Exercise Search.populate all locations through its owning regression module::

                python -m pytest -q tests/catalog/test_search_core.py


        :param locations_dict: Value supplied for locations dict under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        if u'all' in locations_dict:
            del locations_dict[u'all']

        all_columns_set = set()
        for location in locations_dict:
            columns = locations_dict[location]
            if isinstance(columns, str):
                all_columns_set.add(columns)
            elif hasattr(columns, '__iter__'):
                for column in columns:
                    all_columns_set.add(column)
            else:
                all_columns_set.add(columns)

        locations_dict[u'all'] = tuple(sorted(all_columns_set))
        return locations_dict
