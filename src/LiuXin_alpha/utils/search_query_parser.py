#!/usr/bin/env  python
# encoding: utf-8

# Parses a query into a form which should be acceptable by the database_driver complex_search function.
# Makes it easier to write database drivers by rendering all the searches into a consistent format before they are
# passed into them.

# Syntax is intended to be close to that used by Google.

"""
Tokenize and evaluate saved and ad-hoc search query expressions.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise search query parser through a consuming regression::

        python -m pytest -q tests/catalog/test_search_core.py
"""
import re
import weakref

from LiuXin_alpha.constants import preferred_encoding

from LiuXin_alpha.errors import InputIntegrityError

from LiuXin_alpha.utils.text.icu import lower as icu_lower
from LiuXin_alpha.utils.localization import _

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"


class SavedSearchQueries(object):
    """
    This class manages access to the preference holding the saved search queries. It exists to ensure that unicode is used throughout, and also to permit adding other fields, such as whether the search is a 'favorite'

    Example:
        Exercise SavedSearchQueries through a consuming regression::

            python -m pytest -q tests/catalog/test_search_core.py
    """

    queries = {}
    opt_name = ""

    def __init__(self, db, _opt_name):
        """
        Initialize and validate the SavedSearchQueries state.

        Example:
            Exercise SavedSearchQueries.  init   through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param db: Value supplied for db under the utility contract.
        :param _opt_name: Value supplied for opt name under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.opt_name = _opt_name
        if db is not None:
            self.queries = db.prefs.get(self.opt_name, {})
        else:
            self.queries = {}
        try:
            self._db = weakref.ref(db)
        except TypeError:
            # db could be None
            self._db = lambda: None

    @property
    def db(self):
        """
        Perform the db utility operation under explicit compatibility rules.

        Example:
            Exercise SavedSearchQueries.db through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._db()

    def force_unicode(self, x):
        """
        Perform the force unicode utility operation under explicit compatibility rules.

        Example:
            Exercise SavedSearchQueries.force unicode through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param x: Value supplied for x under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not isinstance(x, str):
            x = x.decode(preferred_encoding, "replace")
        return x

    def add(self, name, value):
        """
        Perform the add utility operation under explicit compatibility rules.

        Example:
            Exercise SavedSearchQueries.add through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param value: Value normalized, stored, formatted or returned.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        db = self.db
        if db is not None:
            self.queries[self.force_unicode(name)] = self.force_unicode(value).strip()
            db.prefs[self.opt_name] = self.queries

    def lookup(self, name):
        """
        Perform the lookup utility operation under explicit compatibility rules.

        Example:
            Exercise SavedSearchQueries.lookup through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.queries.get(self.force_unicode(name), None)

    def delete(self, name):
        """
        Perform the delete utility operation under explicit compatibility rules.

        Example:
            Exercise SavedSearchQueries.delete through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        db = self.db
        if db is not None:
            self.queries.pop(self.force_unicode(name), False)
            db.prefs[self.opt_name] = self.queries

    def rename(self, old_name, new_name):
        """
        Perform the rename utility operation under explicit compatibility rules.

        Example:
            Exercise SavedSearchQueries.rename through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param old_name: Value supplied for old name under the utility contract.
        :param new_name: Value supplied for new name under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        db = self.db
        if db is not None:
            self.queries[self.force_unicode(new_name)] = self.queries.get(self.force_unicode(old_name), None)
            self.queries.pop(self.force_unicode(old_name), False)
            db.prefs[self.opt_name] = self.queries

    def set_all(self, smap):
        """
        Set all under the documented compatibility and safety rules.

        Example:
            Exercise SavedSearchQueries.set all through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param smap: Value supplied for smap under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        db = self.db
        if db is not None:
            self.queries = db.prefs[self.opt_name] = smap

    def names(self):
        """
        Perform the names utility operation under explicit compatibility rules.

        Example:
            Exercise SavedSearchQueries.names through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return sorted(self.queries.keys(), key=sort_key)


"""
Create a global instance of the saved searches. It is global so that the searches
are common across all instances of the parser (devices, library, etc).
"""
ss = SavedSearchQueries(None, None)


def set_saved_searches(db, opt_name):
    """
    Set saved searches under the documented compatibility and safety rules.

    Example:
        Exercise set saved searches through a consuming regression::

            python -m pytest -q tests/catalog/test_search_core.py


    :param db: Value supplied for db under the utility contract.
    :param opt_name: Value supplied for opt name under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    global ss
    ss = SavedSearchQueries(db, opt_name)


def saved_searches():
    """
    Perform the saved searches utility operation under explicit compatibility rules.

    Example:
        Exercise saved searches through a consuming regression::

            python -m pytest -q tests/catalog/test_search_core.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    global ss
    return ss


def global_lookup_saved_search(name):
    """
    Perform the global lookup saved search utility operation under explicit compatibility rules.

    Example:
        Exercise global lookup saved search through a consuming regression::

            python -m pytest -q tests/catalog/test_search_core.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return ss.lookup(name)


class ParseException(Exception):
    """
    Provide the ParseException utility contract with explicit state and cleanup behavior.

    Example:
        Exercise ParseException through a consuming regression::

            python -m pytest -q tests/catalog/test_search_core.py
    """
    def msg(self):
        """
        Perform the msg utility operation under explicit compatibility rules.

        Example:
            Exercise ParseException.msg through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if len(self.args) > 0:
            return self.args[0]
        return ""


def _make_func(template, name, **kwargs):
    """
    Perform the make func utility operation under explicit compatibility rules.

    Example:
        Exercise  make func through a consuming regression::

            python -m pytest -q tests/catalog/test_search_core.py


    :param template: Template expression parsed or evaluated.
    :param name: Field, file, function or resource name addressed by the operation.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    l = globals()
    kwargs["name"] = name
    kwargs["func"] = kwargs.get("func", "sort_key")
    exec(template.format(**kwargs), l)
    return l[name]


_sort_key_template = """
def {name}(obj):
    try:
        try:
            return {collator}.{func}(obj)
        except AttributeError:
            return {collator_func}().{func}(obj)
    except TypeError:
        if isinstance(obj, bytes):
            try:
                obj = obj.decode(sys.getdefaultencoding())
            except ValueError:
                return obj
            return {collator}.{func}(obj)
    return b''
"""

sort_key = _make_func(
    _sort_key_template,
    "sort_key",
    collator="_sort_collator",
    collator_func="sort_collator",
)


def default_loc_normalize(string):
    """
    Default loc_normalize function for the Parser object - hopefully to be replaced with a better one.

    Example:
        Exercise default loc normalize through a consuming regression::

            python -m pytest -q tests/catalog/test_search_core.py


    :param string: Value supplied for string under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return string


class Parser(object):
    """
    Parses a string, splitting it down into a nested index of indexes. Which can then be processed back into a language appropriate query by the database_driver

    Example:
        Exercise Parser through a consuming regression::

            python -m pytest -q tests/catalog/test_search_core.py
    """

    OPCODE = 1
    WORD = 2
    QUOTED_WORD = 3
    EOF = 4

    # Used to translate named constants into numerical values
    lex_scanner = re.Scanner(
        [
            (r"[()]", lambda x, t: (1, t)),
            (r'@.+?:[^")\s]+', lambda x, t: (2, six_unicode(t))),
            (r'[^"()\s]+', lambda x, t: (2, six_unicode(t))),
            (r'".*?((?<!\\)")', lambda x, t: (3, t[1:-1])),
            (r"\s+", None),
        ],
        flags=re.DOTALL,
    )

    def __init__(self):
        """
        Initializes a Parser object.

        Example:
            Exercise Parser.  init   through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: None; validated state is stored on the receiving object.
        """
        self.loc_normalize = None
        self.current_token = 0
        self.tokens = None
        self.locations = None

    def token(self, advance=False):
        """
        Perform the token utility operation under explicit compatibility rules.

        Example:
            Exercise Parser.token through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param advance: Value supplied for advance under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.is_eof():
            return None
        res = self.tokens[self.current_token][1]
        if advance:
            self.current_token += 1
        return res

    def lcase_token(self, advance=False):
        """
        Transform the token into lower case.

        Example:
            Exercise Parser.lcase token through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param advance: Value supplied for advance under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.is_eof():
            return None
        res = self.tokens[self.current_token][1]
        if advance:
            self.current_token += 1
        return icu_lower(res)

    def token_type(self):
        """
        Perform the token type utility operation under explicit compatibility rules.

        Example:
            Exercise Parser.token type through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.is_eof():
            return self.EOF
        return self.tokens[self.current_token][0]

    def is_eof(self):
        """
        Return or update whether is eof holds for the compatibility value.

        Example:
            Exercise Parser.is eof through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: True when the documented condition holds; otherwise False.
        """
        return self.current_token >= len(self.tokens)

    def advance(self):
        """
        Perform the advance utility operation under explicit compatibility rules.

        Example:
            Exercise Parser.advance through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.current_token += 1

    def parse(self, expr, locations, loc_normalize=None):
        """
        Parse an expression into a common container format.

        Example:
            Exercise Parser.parse through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param expr: Value supplied for expr under the utility contract.
        :param locations: Value supplied for locations under the utility contract.
        :param loc_normalize: Value supplied for loc normalize under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.locations = locations
        if loc_normalize is None:
            self.loc_normalize = default_loc_normalize
        else:
            self.loc_normalize = loc_normalize

        # Strip out escaped backslashes, quotes and parens so that the
        # lex scanner doesn't get confused. We put them back later.
        expr = expr.replace("\\\\", "\x01").replace('\\"', "\x02")
        expr = expr.replace("\\(", "\x03").replace("\\)", "\x04")
        self.tokens = self.lex_scanner.scan(expr)[0]
        for (i, tok) in enumerate(self.tokens):
            tt, tv = tok
            if tt == self.WORD or tt == self.QUOTED_WORD:
                self.tokens[i] = (
                    tt,
                    tv.replace("\x01", "\\").replace("\x02", '"').replace("\x03", "(").replace("\x04", ")"),
                )

        self.current_token = 0
        prog = self.or_expression()
        if not self.is_eof():
            raise ParseException(_("Extra characters at end of search"))
        return prog

    def or_expression(self):
        """
        Perform the or expression utility operation under explicit compatibility rules.

        Example:
            Exercise Parser.or expression through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        lhs = self.and_expression()
        if self.lcase_token() == "or":
            self.advance()
            return ["or", lhs, self.or_expression()]
        return lhs

    def and_expression(self):
        """
        Perform the and expression utility operation under explicit compatibility rules.

        Example:
            Exercise Parser.and expression through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        lhs = self.not_expression()
        if self.lcase_token() == "and":
            self.advance()
            return ["and", lhs, self.and_expression()]

        # Account for the optional 'and'
        if self.token_type() in [self.WORD, self.QUOTED_WORD] and self.lcase_token() != "or":
            return ["and", lhs, self.and_expression()]
        return lhs

    def not_expression(self):
        """
        Perform the not expression utility operation under explicit compatibility rules.

        Example:
            Exercise Parser.not expression through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.lcase_token() == "not":
            self.advance()
            return ["not", self.not_expression()]
        return self.location_expression()

    def location_expression(self):
        """
        Perform the location expression utility operation under explicit compatibility rules.

        Example:
            Exercise Parser.location expression through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.token_type() == self.OPCODE and self.token() == "(":
            self.advance()
            res = self.or_expression()
            if self.token_type() != self.OPCODE or self.token(advance=True) != ")":
                raise ParseException(_("missing )"))
            return res
        if self.token_type() not in (self.WORD, self.QUOTED_WORD):
            raise ParseException(_("Invalid syntax. Expected a lookup name or a word"))

        return self.base_token()

    def base_token(self):
        """
        Perform the base token utility operation under explicit compatibility rules.

        Example:
            Exercise Parser.base token through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.token_type() == self.QUOTED_WORD:
            return ["token", "all", self.token(advance=True)]

        words = self.token(advance=True).split(":")

        # The complexity here comes from having colon-separated search
        # values. That forces us to check that the first "word" in a colon-
        # separated group is a valid location. If not, then the token must
        # be reconstructed. We also have the problem that locations can be
        # followed by quoted strings that appear as the next token. and that
        # tokens can be a sequence of colons.

        # We have a location if there is more than one word and the first
        # word is in locations. This check could produce a "wrong" answer if
        # the search string is something like 'author: "foo"' because it
        # will be interpreted as 'author:"foo"'. I am choosing to accept the
        # possible error. The expression should be written '"author:" foo'
        if len(words) > 1 and words[0].lower() in self.locations:
            loc = words[0].lower()
            words = words[1:]
            if len(words) == 1 and self.token_type() == self.QUOTED_WORD:
                return ["token", loc, self.token(advance=True)]
            return ["token", icu_lower(loc), ":".join(words)]

        return ["token", "all", ":".join(words)]


class SearchQueryParser(object):
    """
    Parses a search query.

    Example:
        Exercise SearchQueryParser through a consuming regression::

            python -m pytest -q tests/catalog/test_search_core.py
    """

    def __init__(
        self,
        locations,
        test=False,
        optimize=False,
        lookup_saved_search=None,
        parse_cache=None,
    ):
        """
        Initializes a SearchQueryParser object with the provided list of locations (and an optional lookup_saved_search)

        Example:
            Exercise SearchQueryParser.  init   through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param locations: Value supplied for locations under the utility contract.
        :param test: Value supplied for test under the utility contract.
        :param optimize: Value supplied for optimize under the utility contract.
        :param lookup_saved_search: Value supplied for lookup saved search under the utility
            contract.
        :param parse_cache: Value supplied for parse cache under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.sqp_initialize(locations, test=test, optimize=optimize)
        self.parser = Parser()
        self.lookup_saved_search = global_lookup_saved_search if lookup_saved_search is None else lookup_saved_search
        self.sqp_parse_cache = parse_cache

    def sqp_change_locations(self, locations):
        """
        Change locations that the parser can look in.

        Example:
            Exercise SearchQueryParser.sqp change locations through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param locations: Value supplied for locations under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.sqp_initialize(locations, optimize=self.optimize)
        if self.sqp_parse_cache is not None:
            self.sqp_parse_cache.clear()

    def sqp_initialize(self, locations, test=False, optimize=False):
        """
        Perform the sqp initialize utility operation under explicit compatibility rules.

        Example:
            Exercise SearchQueryParser.sqp initialize through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param locations: Value supplied for locations under the utility contract.
        :param test: Value supplied for test under the utility contract.
        :param optimize: Value supplied for optimize under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if isinstance(locations, dict):
            self.locations = locations.keys()
        elif hasattr(locations, "__iter__"):
            self.locations = locations
        else:
            err_str = "Locations list is not of a recognized form (isn't an iterator).\n"
            err_str += "locations: " + repr(locations) + "\n"
            raise InputIntegrityError(err_str)
        self._tests_failed = False
        self.optimize = optimize

    def parse(self, query, candidates=None):
        # empty the list of searches used for recursion testing
        """
        Perform the parse utility operation under explicit compatibility rules.

        Example:
            Exercise SearchQueryParser.parse through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param query: Search expression parsed or evaluated by the utility.
        :param candidates: Value supplied for candidates under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.recurse_level = 0
        self.searches_seen = set([])
        candidates = self.universal_set()
        return self._parse(query, candidates=candidates)

    # this parse is used internally because it doesn't clear the
    # recursive search test list. However, we permit seeing the
    # same search a few times because the search might appear within
    # another search.
    def _parse(self, query, candidates=None):
        """
        Perform the parse utility operation under explicit compatibility rules.

        Example:
            Exercise SearchQueryParser. parse through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param query: Search expression parsed or evaluated by the utility.
        :param candidates: Value supplied for candidates under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.recurse_level += 1
        try:
            res = self.sqp_parse_cache.get(query, None)
        except AttributeError:
            res = None
        if res is None:
            try:
                res = self.parser.parse(query, self.locations)
            except RuntimeError:
                raise ParseException(_("Failed to parse query, recursion limit reached: %s") % repr(query))
            if self.sqp_parse_cache is not None:
                self.sqp_parse_cache[query] = res
        if candidates is None:
            candidates = self.universal_set()
        t = self.evaluate(res, candidates)
        self.recurse_level -= 1
        return t

    def method(self, group_name):
        """
        Perform the method utility operation under explicit compatibility rules.

        Example:
            Exercise SearchQueryParser.method through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param group_name: Value supplied for group name under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return getattr(self, "evaluate_" + group_name)

    # Recurse through the structure, searching using each term in turn - can evaluate and, or, not - which should be
    # enough to evaluate any query
    def evaluate(self, parse_result, candidates):
        """
        Evaluate this registered template function against metadata, formatter state and local variables.

        Example:
            Exercise SearchQueryParser.evaluate through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param parse_result: Value supplied for parse result under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.method(parse_result[0])(parse_result[1:], candidates)

    def evaluate_and(self, argument, candidates):
        # RHS checks only those items matched by LHS
        # returns result of RHS check: RHmatches(LHmatches(c))
        #  return self.evaluate(argument[0]).intersection(self.evaluate(argument[1]))
        """
        Perform the evaluate and utility operation under explicit compatibility rules.

        Example:
            Exercise SearchQueryParser.evaluate and through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param argument: Value supplied for argument under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        l = self.evaluate(argument[0], candidates)
        return l.intersection(self.evaluate(argument[1], l))

    def evaluate_or(self, argument, candidates):
        # RHS checks only those elements not matched by LHS
        # returns LHS union RHS: LHmatches(c) + RHmatches(c-LHmatches(c))
        #  return self.evaluate(argument[0]).union(self.evaluate(argument[1]))
        """
        Perform the evaluate or utility operation under explicit compatibility rules.

        Example:
            Exercise SearchQueryParser.evaluate or through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param argument: Value supplied for argument under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        l = self.evaluate(argument[0], candidates)
        return l.union(self.evaluate(argument[1], candidates.difference(l)))

    def evaluate_not(self, argument, candidates):
        # unary op checks only candidates. Result: list of items matching
        # returns: c - matches(c)
        #  return self.universal_set().difference(self.evaluate(argument[0]))
        """
        Perform the evaluate not utility operation under explicit compatibility rules.

        Example:
            Exercise SearchQueryParser.evaluate not through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param argument: Value supplied for argument under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return candidates.difference(self.evaluate(argument[0], candidates))

    #     def evaluate_parenthesis(self, argument, candidates):
    #         return self.evaluate(argument[0], candidates)

    def evaluate_token(self, argument, candidates):
        """
        Perform the evaluate token utility operation under explicit compatibility rules.

        Example:
            Exercise SearchQueryParser.evaluate token through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param argument: Value supplied for argument under the utility contract.
        :param candidates: Value supplied for candidates under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        location = argument[0]
        query = argument[1]
        if location.lower() == "search":
            if query.startswith("="):
                query = query[1:]
            try:
                if query in self.searches_seen:
                    raise ParseException(_("Recursive saved search: {0}").format(query))
                if self.recurse_level > 5:
                    self.searches_seen.add(query)
                return self._parse(self.lookup_saved_search(query), candidates)
            except ParseException as e:
                raise e
            except:  # convert all exceptions (e.g., missing key) to a parse error
                import traceback

                traceback.print_exc()
                raise ParseException(_("Unknown error in saved search: {0}").format(query))
        return self._get_matches(location, query, candidates)

    def _get_matches(self, location, query, candidates):
        """
        Perform the get matches utility operation under explicit compatibility rules.

        Example:
            Exercise SearchQueryParser. get matches through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param location: Value supplied for location under the utility contract.
        :param query: Search expression parsed or evaluated by the utility.
        :param candidates: Value supplied for candidates under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.optimize:
            return self.get_matches(location, query, candidates=candidates)
        else:
            return self.get_matches(location, query)

    def get_matches(self, location, query, candidates=None):
        """
        Should return the set of matches for :param:'location` and :param:`query`. The search must be performed over all entries if :param:`candidates` is None otherwise only over the items in candidates.

        Example:
            Exercise SearchQueryParser.get matches through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :param location: Value supplied for location under the utility contract.
        :param query: Search expression parsed or evaluated by the utility.
        :param candidates: Value supplied for candidates under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return set([])

    def universal_set(self):
        """
        Should return the set of all matches - currently the empty set.

        Example:
            Exercise SearchQueryParser.universal set through a consuming regression::

                python -m pytest -q tests/catalog/test_search_core.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return set([])
