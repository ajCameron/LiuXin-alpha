#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Front end for the search functionality.

Allows you to search strings and/or the database.
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
    Determines the type of search to be run from a query.

    :param query:
    :return:
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
    Generates matches based on the query.

    :param query:
    :param value:
    :param matchkind: CONTAINS, REGEXP or EQUALS
    :param use_primary_find_in_search:
    :return:
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
    Execute the query on every key
    """
    def __call__(self, query: str, field_iter, candidates, use_primary_find: bool) -> set[int]:
        """
        Preform a key search for the query.

        :param query:
        :param field_iter:
        :param candidates:
        :param use_primary_find:
        :return:
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
    The saved results of running a bunch of search queries.
    """
    queries = {}
    opt_name = ""

    def __init__(self, db: "DatabaseAPI", _opt_name) -> None:
        """
        Startup the saved searched queries cache.

        :param db:
        :param _opt_name:
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
        Proxy for the database.

        :return:
        """
        return self._db()

    def load_from_db(self) -> None:
        """
        Preform a load onto the database from the local search cache.

        :return:
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
        Coerce an object to Unicode and return the result.

        :param x:
        :return:
        """
        if not isinstance(x, unicode):
            x = x.decode(preferred_encoding, "replace")
        return x

    def add(self, name: Any, value: Any) -> None:
        """
        Add an object to the cache, coercing it to Unicode as we go.

        :param name:
        :param value:
        :return:
        """
        db = self.db
        if db is not None:
            self.queries[self.force_unicode(name)] = self.force_unicode(value).strip()
            db.set_pref(self.opt_name, self.queries)

    def lookup(self, name: str) -> Optional[str]:
        """
        Retrieve and return a value from the cache.

        :param name:
        :return:
        """
        return self.queries.get(self.force_unicode(name), None)

    def delete(self, name):
        db = self.db
        if db is not None:
            self.queries.pop(self.force_unicode(name), False)
            db.set_pref(self.opt_name, self.queries)

    def rename(self, old_name, new_name):
        db = self.db
        if db is not None:
            self.queries[self.force_unicode(new_name)] = self.queries.get(self.force_unicode(old_name), None)
            self.queries.pop(self.force_unicode(old_name), False)
            db._set_pref(self.opt_name, self.queries)

    def set_all(self, smap):
        db = self.db
        if db is not None:
            self.queries = smap
            db._set_pref(self.opt_name, smap)

    def names(self):
        return sorted(iterkeys(self.queries), key=sort_key)


# }}}


class Parser(SearchQueryParser):  # {{{
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
        return self.dbcache.field_metadata

    def universal_set(self):
        return self.all_book_ids

    def field_iter(self, name, candidates):
        get_metadata = self.dbcache._get_proxy_metadata
        try:
            field = self.dbcache.fields[name]
        except KeyError:
            field = self.virtual_fields[name]
            self.virtual_field_used = True
        return field.iter_searchable_values(get_metadata, candidates)

    def iter_searchable_values(self, *args, **kwargs):
        return iter(())

    def parse(self, *args, **kwargs):
        self.virtual_field_used = False
        return SearchQueryParser.parse(self, *args, **kwargs)

    def get_matches(self, location, query, candidates=None, allow_recursion=True):
        """
        Preform the search and returns the matches.
        :param location: One of the locations in the database.
        :param query: The query to match against
        :param candidates:
        :param allow_recursion:
        :return:
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
    A simple Least-Recently-Used cache
    """

    def __init__(self, limit=50):
        self.item_map = {}
        self.age_map = deque()
        self.limit = limit

    def _move_up(self, key):
        if key != self.age_map[-1]:
            self.age_map.remove(key)
            self.age_map.append(key)

    def add(self, key, val):
        if key in self.item_map:
            self._move_up(key)
            return

        if len(self.age_map) >= self.limit:
            self.item_map.pop(self.age_map.popleft())

        self.item_map[key] = val
        self.age_map.append(key)

    __setitem__ = add

    def get(self, key, default=None):
        ans = self.item_map.get(key, default)
        if ans is not default:
            self._move_up(key)
        return ans

    def clear(self):
        self.item_map.clear()
        self.age_map.clear()

    def pop(self, key, default=None):
        self.item_map.pop(key, default)
        try:
            self.age_map.remove(key)
        except ValueError:
            pass

    def __contains__(self, key):
        return key in self.item_map

    def __len__(self):
        return len(self.age_map)

    def __getitem__(self, key):
        return self.get(key)

    def __iter__(self):
        return iter(self.item_map.items())


# }}}


class Search(object):
    """
    Represents a search of the database.
    """

    MAX_CACHE_UPDATE = 50

    def __init__(self, db, opt_name, all_search_locations=()):
        self.all_search_locations = all_search_locations
        self.date_search = DateSearch()
        self.num_search = NumericSearch()
        self.bool_search = BooleanSearch()
        self.keypair_search = KeyPairSearch()
        self.saved_searches = SavedSearchQueries(db, opt_name)
        self.cache = LRUCache()
        self.parse_cache = LRUCache(limit=100)

    def get_saved_searches(self):
        return self.saved_searches

    def change_locations(self, newlocs):
        if frozenset(newlocs) != frozenset(self.all_search_locations):
            self.clear_caches()
            self.parse_cache.clear()
        self.all_search_locations = newlocs

    def update_or_clear(self, dbcache, book_ids=None):
        if book_ids and (len(book_ids) * len(self.cache)) <= self.MAX_CACHE_UPDATE:
            self.update_caches(dbcache, book_ids)
        else:
            self.clear_caches()

    def clear_caches(self):
        self.cache.clear()

    def update_caches(self, dbcache, book_ids):
        sqp = self.create_parser(dbcache)
        try:
            return self._update_caches(sqp, book_ids)
        finally:
            sqp.dbcache = sqp.lookup_saved_search = None

    def discard_books(self, book_ids):
        book_ids = set(book_ids)
        for query, result in self.cache:
            result.difference_update(book_ids)

    def _update_caches(self, sqp, book_ids):
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
        Return the set of ids of all records that match the specified
        query and restriction
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
        Do the search, caching the results. Results are cached only if the search is on the full library and no virtual
         field is searched on
        :param sqp:
        :param query:
        :param search_restriction:
        :param dbcache:
        :param book_ids:
        :return:
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

        :return:
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
