
"""
Adapt SQL values and provide aggregate, collation and Calibre sorting callbacks.

SQLite driver connection setup registers these helpers; importing this module does not
register them. JSON adapters preserve supported JSON values, while the legacy set format
has limited quoting and round-trip behavior. Aggregate classes own their state; legacy
factory functions return explicit state/step/finalize triples. Title article patterns
are cached against language keys and depend on preferences.
"""

# Todo: Consider not having this here?

from __future__ import print_function, annotations

import json
import re

from copy import deepcopy

from typing import Any, Optional, Literal, Union, Callable

from LiuXin_alpha.preferences import preferences
from LiuXin_alpha.utils.text import isbytestring

from LiuXin_alpha.utils.libraries.liuxin_six import iterkeys, force_unicode, six_unicode, force_cmp

from LiuXin_alpha.utils.language_tools.icu import sort_key


#
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# HELPER FUNCTIONS WHICH DO NOT NEED THE DATABASE TO WORK START HERE
# ----------------------------------------------------------------------------------------------------------------------

# Helper functions which allow direct use of sets a column of a table

# Todo: Actually, perhaps, write this
def py_date_converter(date_string):
    """
    Return the supplied date payload unchanged.

    This placeholder performs no date parsing or null conversion; SQLite byte payloads
    remain bytes.

    Example:
        >>> py_date_converter(b"2026-09-19")
        b'2026-09-19'
        >>> py_date_converter(None) is None
        True


    :param date_string: Date payload or other value to pass through unchanged.
    :return: The original object.
    """
    return date_string


# Todo: wwwhhhyyyyyyyy
# Todo: Replace with enhanced json?
def py_set_converter(py_set_string):
    """
    Parse the legacy comma-separated, single-quoted set format from bytes.

    Decode bytes as UTF-8 with replacement, collapse doubled apostrophes and scan quoted
    fragments. Empty elements disappear and duplicates collapse. The shared byte-string
    predicate also matches str, whose bytes conversion here raises TypeError; pass
    encoded bytes. Unexpected unquoted characters or escaped apostrophes outside an
    element raise DatabaseDriverError, but unmatched trailing quotes are not fully
    validated. Apostrophes in values do not reliably round-trip.

    Example:
        >>> py_set_converter(b"'a','b','a'") == {"a", "b"}
        True
        >>> py_set_converter(b"''")
        set()


    :param py_set_string: Legacy serialized set, normally the bytes supplied by an SQLite converter.
    :return: A set of nonempty decoded string fragments.
    """
    from LiuXin_alpha.errors import DatabaseDriverError

    py_set_string = deepcopy(py_set_string)
    if isbytestring(py_set_string):
        py_set_string = bytes(py_set_string).decode("utf-8", errors="replace")
    # Accounting for the way SQL escapes quotes
    py_set_string = py_set_string.replace("''", "'")

    py_set = set()
    last_char = " "
    current_string = ""
    accumulation_mode = False
    for char in py_set_string:
        if char == "'" and last_char != "\\":
            accumulation_mode = not accumulation_mode
            if current_string:
                py_set.add(current_string)
                current_string = ""
        elif char == "'" and last_char == "\\":
            # The SQL \ used to escape a quote is no longer needed
            if accumulation_mode:
                current_string = current_string[:-1]
                current_string += char
            else:
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"
                raise DatabaseDriverError(err_str)

        elif char == '"' and last_char == "\\":
            # The SQL \ used to escape a double quote is no longer needed
            if accumulation_mode:
                current_string = current_string[:-1]
                current_string += char
            else:
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"

        elif accumulation_mode:
            current_string += char

        else:
            if char != ",":
                err_str = "parsing a string into a python set has gone wrong.\n"
                err_str += "py_set_string: " + repr(py_set_string) + "\n"
                raise DatabaseDriverError(err_str)
        last_char = char

    return py_set


def py_set_adapter(py_set):
    """
    Serialize copied set elements as quoted, comma-separated text.

    Coerce values to Unicode, double apostrophes and backslash-escape double quotes.
    Iteration order is unspecified; an empty set produces two apostrophes. This legacy
    format is lossy for empty elements and some quoted values; use bound parameters when
    storing it.

    Example:
        >>> py_set_converter(py_set_adapter({"a", "b"}).encode("utf-8")) == {"a", "b"}
        True


    :param py_set: Iterable of values, conventionally a set, copied before serialization.
    :return: Legacy serialized text, without surrounding set braces.
    """

    py_set = deepcopy(py_set)
    py_list = []
    for element in py_set:
        # Coerce to unicode, escape any SQL special characters, then add to the list of elements
        element = force_unicode(element)
        element = element.replace("'", "''")
        element = element.replace('"', '\\"')
        py_list.append(element)
    return "'" + "','".join(py_list) + "'"


# Todo: Make safe - can currently be used to execute arbitrary code
def py_list_converter(py_list_string):
    """
    Decode JSON without enforcing a list result or converting elements to text.

    JSON parsing errors propagate; input is data, not executable Python.

    Example:
        >>> py_list_converter('[1, null, "a"]')
        [1, None, 'a']


    :param py_list_string: JSON text, bytes or bytearray accepted by json.loads.
    :return: The decoded JSON value, which need not be a list.
    """
    return json.loads(py_list_string)


def py_list_adapter(py_list):
    """
    Encode a value using the default JSON serializer.

    Unsupported values raise TypeError; list elements retain their JSON-compatible
    types.

    Example:
        >>> py_list_adapter([1, None, "a"])
        '[1, null, "a"]'


    :param py_list: JSON-serializable value, conventionally a list.
    :return: JSON text.
    """
    return json.dumps(py_list)


def py_dict_converter(py_dict_string: str) -> dict[str, str]:
    """
    Decode JSON without enforcing dictionary or string-value types.

    Example:
        >>> py_dict_converter('{"count": 2}')
        {'count': 2}


    :param py_dict_string: JSON input accepted by json.loads; malformed input raises its decoding errors.
    :return: The decoded JSON value, despite the narrower dictionary annotation.
    """
    import json

    return json.loads(py_dict_string)


def py_dict_adapter(py_dict: dict[str, str]) -> str:
    """
    Encode a mapping with the default JSON serializer.

    Supported key types follow JSON rules and become text; non-string values are allowed
    when JSON-serializable.

    Example:
        >>> py_dict_adapter({"count": 2})
        '{"count": 2}'


    :param py_dict: JSON-serializable mapping; no string-only validation is applied.
    :return: JSON text.
    """
    import json

    return json.dumps(py_dict)



class PyListAggregate:
    """
    Accumulate SQL values in arrival order and serialize them as JSON.

    Duplicates and None values are retained. The same instance retains its state after
    finalization.

    Example:
        >>> import sqlite3
        >>> conn = sqlite3.connect(":memory:")
        >>> conn.create_aggregate("pylist", 1, PyListAggregate)
        >>> conn.execute("SELECT pylist(value) FROM (SELECT 2 AS value UNION ALL SELECT 1)").fetchone()[0]
        '[2, 1]'
        >>> conn.close()
    """

    def __init__(self) -> None:
        """
        Create an empty list accumulator.

        Example:
            ``aggregate = PyListAggregate()`` starts a fresh SQL group.


        :return: ``None``.
        """
        self.py_list = []

    def step(self, value: Any) -> None:
        """
        Append one value without copying, coercion or filtering.

        Example:
            >>> aggregate = PyListAggregate()
            >>> aggregate.step(None)
            >>> aggregate.finalize()
            '[null]'


        :param value: Value to retain; it must be JSON-serializable by finalization time.
        :return: ``None``.
        """
        self.py_list.append(value)

    def finalize(self) -> str:
        """
        Serialize the accumulated values as JSON without clearing the list.

        Example:
            >>> PyListAggregate().finalize()
            '[]'


        :return: JSON array text; serialization errors propagate.
        """
        return py_list_adapter(self.py_list)


class PySetAggregate:
    """
    Deduplicate values and join them in the unescaped legacy set format.

    Steps require hashable values, but finalization requires strings. Ordering is
    unspecified and quoting is not escaped.

    Example:
        >>> aggregate = PySetAggregate()
        >>> aggregate.step("a")
        >>> aggregate.step("a")
        >>> aggregate.finalize() == "'a'"
        True
    """

    def __init__(self) -> None:
        """
        Create an empty set accumulator.

        Example:
            ``aggregate = PySetAggregate()`` starts a group without values.


        :return: ``None``.
        """
        self.py_set = set()

    def step(self, value: Any) -> None:
        """
        Add a hashable value to the set, collapsing duplicates.

        No string conversion occurs; a non-string may be accepted here but fail during
        finalization.

        Example:
            ``aggregate.step("a")`` retains one copy of the string.


        :param value: Hashable value to accumulate; use strings for successful finalization.
        :return: ``None``.
        """
        self.py_set.add(value)

    # Todo: replace with json
    def finalize(self) -> str:
        """
        Join stored strings between apostrophes without escaping or clearing them.

        Example:
            >>> PySetAggregate().finalize() == "''"
            True


        :return: Quoted comma-separated text; empty state yields two apostrophes.
        """
        return "'" + "','".join(self.py_set) + "'"


# Helper functions used to make aggregate short strings (for example makes the creators_sort field for a title.
class SortAggregate:
    """
    Join arrival-ordered strings with space-ampersand-space after trimming quote edges.

    Unlike indexed sort aggregates, this class does not reorder values.

    Example:
        >>> aggregate = SortAggregate()
        >>> aggregate.step("'Alice'")
        >>> aggregate.step("Bob")
        >>> aggregate.finalize()
        'Alice & Bob'
    """

    def __init__(self) -> None:
        """
        Create an empty ordered string accumulator.

        Example:
            ``aggregate = SortAggregate()`` starts a new group.


        :return: ``None``.
        """
        self.py_list = []

    def step(self, value: Any) -> None:
        """
        Remove at most one leading and one trailing apostrophe, then append.

        Whitespace, duplicate values and empty strings are retained. None and non-string
        inputs are not specially handled.

        Example:
            ``aggregate.step("'Alice'")`` appends ``Alice``.


        :param value: String to strip at its quote edges and accumulate.
        :return: ``None``.
        """
        if value.startswith("'"):
            value = value[1:]

        if value.endswith("'"):
            value = value[:-1]

        self.py_list.append(value)

    def finalize(self) -> str:
        """
        Join stored strings with space-ampersand-space, retaining the accumulator.

        Example:
            >>> SortAggregate().finalize()
            ''


        :return: Joined text, or the empty string for no values.
        """
        return " & ".join(self.py_list)


class SqliteAumSortedConcatenate:
    """
    Serialize author/sort/link triples by index for a Calibre author map.

    Join fields with ::: and entries with :#:. Duplicate indexes replace earlier
    entries; None authors are ignored. Delimiter characters are not escaped.

    Example:
        >>> aggregate = SqliteAumSortedConcatenate()
        >>> aggregate.step(2, "Bob", "Bob", "")
        >>> aggregate.step(1, "Alice", "Alice", "")
        >>> aggregate.finalize()
        'Alice:::Alice::::#:Bob:::Bob:::'
    """

    def __init__(self) -> None:
        """
        Create an empty index-to-author-entry mapping.

        Example:
            ``aggregate = SqliteAumSortedConcatenate()`` starts a fresh author map.


        :return: ``None``.
        """
        self.ctxt = dict()

    def step(self, ndx: int, author: Optional[str], sort: Optional[str], link: Optional[str]) -> None:
        """
        Replace the entry at an index when author is not None.

        Convert all three fields through six_unicode, including None sort/link values,
        which become the text None.

        Example:
            >>> aggregate = SqliteAumSortedConcatenate()
            >>> aggregate.step(1, "Alice", None, None)
            >>> aggregate.finalize()
            'Alice:::None:::None'


        :param ndx: Hashable ordering key; all retained keys must be mutually sortable.
        :param author: Author name; ``None`` skips this entry without removing a prior value at the same index.
        :param sort: Author-sort text included in the serialized entry.
        :param link: Author-link text included in the serialized entry.
        :return: ``None``.
        """
        if author is not None:
            self.ctxt[ndx] = ":::".join((six_unicode(author), six_unicode(sort), six_unicode(link)))

    def finalize(self) -> Optional[str]:
        """
        Join retained author entries in key order without clearing the mapping.

        A single entry is returned directly; sorting is needed only for multiple
        entries.

        Example:
            >>> SqliteAumSortedConcatenate().finalize() is None
            True


        :return: Serialized author entries, or ``None`` for empty state.
        """
        ctxt = self.ctxt
        keys = list(iterkeys(ctxt))
        l = len(keys)
        if l == 0:
            return None
        if l == 1:
            return ctxt[keys[0]]
        return ":#:".join([ctxt[v] for v in sorted(keys)])


class SqliteSortedConcatenate:
    """
    Join non-null strings in index order, retaining only the last value per index.

    Example:
        >>> aggregate = SqliteSortedConcatenate("|")
        >>> aggregate.step(2, "B")
        >>> aggregate.step(1, "A")
        >>> aggregate.step(2, "C")
        >>> aggregate.finalize()
        'A|C'
    """
    def __init__(self, sep: str = ",") -> None:
        """
        Store the separator and create an empty index/value mapping.

        Example:
            ``aggregate = SqliteSortedConcatenate("|")`` uses vertical bars between values.


        :param sep: String separator inserted between accumulated values.
        :return: ``None``.
        """
        self.sep = sep
        self.ctxt = dict()

    def step(self, ndx: int, value: Any) -> None:
        """
        Store a non-null value at its index, replacing an earlier value.

        None is ignored and does not remove a prior entry. Values are not coerced to
        strings.

        Example:
            ``aggregate.step(1, "A")`` sets the value at index one.


        :param ndx: Hashable ordering key; all retained keys must be mutually sortable.
        :param value: Value to store, normally a string; ``None`` is skipped.
        :return: ``None``.
        """
        if value is not None:
            self.ctxt[ndx] = value

    def finalize(self) -> Optional[str]:
        """
        Sort the keys and join their values without clearing accumulated state.

        Incomparable keys or non-string retained values raise TypeError.

        Example:
            >>> SqliteSortedConcatenate().finalize() is None
            True


        :return: Joined string, or ``None`` for no retained entries.
        """
        ctxt = self.ctxt
        if len(ctxt) == 0:
            return None
        return self.sep.join(map(ctxt.get, sorted(iterkeys(ctxt))))


class SqliteIdentifiersConcat:
    """
    Join key/value identifier pairs in arrival order, without deduplication or escaping.

    Example:
        >>> aggregate = SqliteIdentifiersConcat()
        >>> aggregate.step("isbn", "123")
        >>> aggregate.step("other", None)
        >>> aggregate.finalize()
        'isbn:123,other:None'
    """
    def __init__(self) -> None:
        """
        Create an empty list of formatted identifier pairs.

        Example:
            ``aggregate = SqliteIdentifiersConcat()`` starts a new identifier map.


        :return: ``None``.
        """
        self.ctxt = []

    def step(self, key, val):
        """
        Append a colon-separated pair using string formatting for both values.

        Example:
            ``aggregate.step("isbn", 123)`` appends ``isbn:123``.


        :param key: Identifier type converted with percent-s formatting.
        :param val: Identifier value converted with percent-s formatting, including None.
        :return: ``None``.
        """
        self.ctxt.append("%s:%s" % (key, val))

    def finalize(self):
        """
        Join formatted identifier pairs with commas without clearing the list.

        Example:
            >>> SqliteIdentifiersConcat().finalize()
            ''


        :return: Comma-separated identifiers, or the empty string.
        """
        return ",".join(self.ctxt)


# Extra collators {{{
def pynocase(one: Any, two: Any, encoding: str = "utf-8") -> bool:
    """
    Compare lowercased operands using three-way ordering.

    Try decoding byte-string inputs with replacement; decoding errors are suppressed
    before lower() is called. This uses lowercase conversion, not Unicode case folding,
    and requires comparable lowered values.

    Example:
        >>> pynocase(b"ALPHA", "alpha")
        0
        >>> pynocase("a", "b")
        -1


    :param one: Left string or byte-string operand.
    :param two: Right string or byte-string operand.
    :param encoding: Codec used for attempted byte decoding.
    :return: Integer -1, 0 or 1, despite the bool annotation.
    """

    if isbytestring(one):
        try:
            one = one.decode(encoding, "replace")
        except:
            pass

    if isbytestring(two):
        try:
            two = two.decode(encoding, "replace")
        except:
            pass

    return force_cmp(one.lower(), two.lower())


def _author_to_author_sort(x: str) -> str:
    """
    Replace vertical bars with commas and delegate author sorting to metadata tooling.

    Falsy names return the empty string. Nonempty names follow the metadata helper's
    configured author-sort rules.

    Example:
        >>> _author_to_author_sort("")
        ''


    :param x: Author name using vertical bars for embedded commas, or an empty value.
    :return: The delegated author-sort string, or empty text.
    """
    from LiuXin_alpha.metadata.ebook_metadata_tools import author_to_author_sort
    if not x:
        return ""
    return author_to_author_sort(x.replace("|", ","))


def icu_collator(s1: Any, s2: Aby) -> bool:
    """
    Compare Unicode sort keys from the shared ICU-facing helper.

    The current helper returns lowercased text. Operands are passed to str with an
    explicit UTF-8 encoding, so byte inputs are supported but Python str inputs raise
    TypeError. This wrapper does not itself implement locale-specific collation.

    Example:
        >>> icu_collator(b"Book", b"book")
        0


    :param s1: Left byte-like value decoded as UTF-8; Python str is rejected.
    :param s2: Right byte-like value decoded as UTF-8; Python str is rejected.
    :return: Integer -1, 0 or 1 from comparing sort keys, despite the bool annotation.
    """
    return force_cmp(sort_key(force_unicode(s1, "utf-8")), sort_key(force_unicode(s2, "utf-8")))


# }}}


# Unused aggregators {{{
def Concatenate(sep: str = ","):
    """
    Create a legacy state/step/finalize triple for arrival-ordered concatenation.

    Skip None values, retain duplicates and return None for empty state. The result is a
    callback triple, not an SQLite aggregate instance with methods.

    Example:
        >>> state, step, finish = Concatenate("|")
        >>> step(state, "A")
        >>> step(state, None)
        >>> step(state, "B")
        >>> finish(state)
        'A|B'


    :param sep: String separator inserted between accumulated values.
    :return: A fresh list, its two-argument step callback and its one-argument finalizer.
    """

    def step(ctxt, value):
        """
        Append a non-null value to the supplied state without coercion.

        Example:
            The callback returned by ``Concatenate()`` accepts ``step(state, "A")``.


        :param ctxt: Mutable list returned by the factory.
        :param value: String to append; ``None`` is ignored.
        :return: ``None``.
        """
        if value is not None:
            ctxt.append(value)

    def finalize(ctxt):
        """
        Join nonempty state using the factory separator without clearing it.

        Example:
            For a fresh factory state, ``finish(state)`` returns ``None``.


        :param ctxt: List of strings accumulated by the step callback.
        :return: Joined text, or ``None`` for empty state.
        """
        if not ctxt:
            return None
        return sep.join(ctxt)

    return [], step, finalize


def StupidConcatenate(sep=","):
    """
    Create a diagnostic concatenation triple whose finalizer deliberately asserts.

    The assertion message is the joined state. With Python optimization disabling
    assertions, the finalizer returns None instead.

    Example:
        >>> state, step, finish = StupidConcatenate("|")
        >>> step(state, "diagnostic")
        >>> finish(state)
        Traceback (most recent call last):
        ...
        AssertionError: diagnostic


    :param sep: String separator inserted between accumulated values.
    :return: A fresh list, an append callback and a deliberately failing finalizer.
    """

    def step(ctxt, value):
        """
        Append non-null values for the finalizer's diagnostic message.

        Example:
            ``step(state, "diagnostic")`` stores text for the eventual assertion.


        :param ctxt: Mutable list returned by StupidConcatenate.
        :param value: String to append; ``None`` is skipped.
        :return: ``None``.
        """
        if value is not None:
            ctxt.append(value)

    def finalize(ctxt):
        """
        Raise AssertionError with joined state when assertions are enabled.

        Non-string values can fail while building the message. Under optimized Python
        the assert is removed.

        Example:
            ``finish(["diagnostic"])`` raises ``AssertionError: diagnostic`` in normal execution.


        :param ctxt: List of strings used to build the assertion message.
        :return: No normal result with assertions enabled; ``None`` when optimized away.
        """
        assert True is False, sep.join(ctxt)

    return [], step, finalize


def SortedConcatenate(sep=","):
    """
    Create an indexed concatenation triple with the last non-null value per key.

    Values must be strings and keys mutually sortable by finalization time.

    Example:
        >>> state, step, finish = SortedConcatenate("|")
        >>> step(state, 2, "B")
        >>> step(state, 1, "A")
        >>> finish(state)
        'A|B'


    :param sep: String separator inserted between accumulated values.
    :return: A fresh dictionary, its three-argument step callback and its finalizer.
    """

    def step(ctxt, ndx, value):
        """
        Store a non-null value at a key, replacing its prior value.

        None leaves any existing value untouched.

        Example:
            ``step(state, 2, "B")`` stores a value for indexed finalization.


        :param ctxt: Mutable index/value mapping returned by the factory.
        :param ndx: Hashable ordering key; all retained keys must be mutually sortable.
        :param value: String value to retain, or ``None`` to ignore.
        :return: ``None``.
        """
        if value is not None:
            ctxt[ndx] = value

    def finalize(ctxt):
        """
        Join values by sorted keys using the factory separator.

        Example:
            The finalizer returned by ``SortedConcatenate()`` returns ``None`` for an empty mapping.


        :param ctxt: Index/string mapping; sorting and join errors propagate.
        :return: Joined text, or ``None`` for empty state.
        """
        if len(ctxt) == 0:
            return None
        return sep.join(map(ctxt.get, sorted(iterkeys(ctxt))))

    return {}, step, finalize


def IdentifiersConcat() -> tuple[list[str], Callable[[list[str], Any, Any], None], Callable[[list[str], ], str]]:
    """
    Create a callback triple that serializes identifier pairs in arrival order.

    Fields are string-formatted, with no escaping or deduplication.

    Example:
        >>> state, step, finish = IdentifiersConcat()
        >>> step(state, "isbn", 123)
        >>> step(state, "other", None)
        >>> finish(state)
        'isbn:123,other:None'


    :return: A fresh list, a key/value append callback and a comma-joining finalizer.
    """

    def step(ctxt: list[str], key: Any, val: Any) -> None:
        """
        Append a colon-separated, string-formatted identifier pair.

        Example:
            ``step(state, "isbn", 123)`` appends ``isbn:123``.


        :param ctxt: Mutable list of serialized identifier pairs.
        :param key: Identifier type formatted as text.
        :param val: Identifier value formatted as text, including None.
        :return: ``None``.
        """
        ctxt.append("%s:%s" % (key, val))

    def finalize(ctxt: list[str]) -> str:
        """
        Join serialized identifier pairs with commas, leaving state intact.

        Example:
            >>> state, step, finish = IdentifiersConcat()
            >>> finish(state)
            ''


        :param ctxt: List of formatted identifier strings.
        :return: Comma-separated text, or the empty string.
        """
        return ",".join(ctxt)

    return [], step, finalize


def AumSortedConcatenate() -> tuple[
    dict[int, str],
    Callable[[dict[int, str], int, Optional[str], Optional[str], Optional[str]], None],
    Callable[[dict[int, str]], Optional[str]]
]:
    """
    Create callbacks for an indexed author/sort/link map.

    Skip None authors and replace duplicate indexes. Unlike the class-based counterpart,
    this factory joins fields directly: all three must be strings for a retained entry.
    Field and entry delimiters are not escaped.

    Example:
        >>> state, step, finish = AumSortedConcatenate()
        >>> step(state, 1, "Alice", "Alice", "")
        >>> finish(state)
        'Alice:::Alice:::'


    :return: A fresh dictionary, a five-argument step callback and an ordered finalizer.
    """

    def step(
            ctxt: dict[int, str],
            ndx: int,
            author: Optional[str],
            sort: Optional[str],
            link: Optional[str]) -> None:
        """
        Join non-null-author fields with ::: and store the result at its index.

        Sort and link must also be strings despite their Optional annotations.

        Example:
            ``step(state, 1, "Alice", "Alice", "")`` stores one formatted author entry.


        :param ctxt: Mutable index-to-formatted-entry mapping.
        :param ndx: Hashable ordering key; all retained keys must be mutually sortable.
        :param author: Author name; ``None`` skips this entry without removing a prior value at the same index.
        :param sort: String author-sort value; None raises TypeError for a retained author.
        :param link: String link value; None raises TypeError for a retained author.
        :return: ``None``.
        """
        if author is not None:
            ctxt[ndx] = ":::".join((author, sort, link))

    def finalize(ctxt: dict[int, str]) -> Optional[str]:
        """
        Join retained author entries by index using :#: between entries.

        A single entry is returned directly; state is not cleared.

        Example:
            >>> state, step, finish = AumSortedConcatenate()
            >>> finish(state) is None
            True


        :param ctxt: Index-to-formatted-entry mapping accumulated by the step callback.
        :return: Serialized author map, or ``None`` for no entries.
        """
        keys = list(iterkeys(ctxt))
        l = len(keys)
        if l == 0:
            return None
        if l == 1:
            return ctxt[keys[0]]
        return ":#:".join([ctxt[v] for v in sorted(keys)])

    return {}, step, finalize


# }}}


class DynamicFilter:
    """
    Hold replaceable allowed IDs behind a named integer-valued membership callback.

    Construction starts with no allowed IDs; the name is stored for callers and does not
    register an SQL function.

    Example:
        >>> allowed = DynamicFilter("books_filter")
        >>> allowed.change([1, 3])
        >>> (allowed(1), allowed(2))
        (1, 0)
    """

    def __init__(self, name: str) -> None:
        """
        Store the filter name and initialize an empty immutable ID set.

        Example:
            ``allowed = DynamicFilter("books_filter")`` initially rejects every ID.


        :param name: Label retained on the filter; no SQL registration is performed.
        :return: ``None``.
        """
        self.name = name
        self.ids = frozenset([])

    def __call__(self, id_: Any) -> int:
        """
        Return integer membership in the current allowed-ID set.

        Example:
            >>> DynamicFilter("empty")(1)
            0


        :param id_: ID tested using Python frozenset membership semantics.
        :return: One when the ID is present, otherwise zero.
        """
        return int(id_ in self.ids)

    def change(self, ids):
        """
        Replace all allowed IDs with a frozenset built from the iterable.

        Duplicates collapse; invalid/unhashable elements raise before the old set is
        replaced.

        Example:
            >>> allowed = DynamicFilter("books_filter")
            >>> allowed.change([1, 1])
            >>> allowed.change([2])
            >>> (allowed(1), allowed(2))
            (0, 1)


        :param ids: Iterable of hashable IDs; an empty iterable clears the filter.
        :return: ``None``.
        """
        self.ids = frozenset(ids)


# Todo: This should be over in metadata somewhere
_title_pats = {}
_ignore_starts = "'\"" + "".join([chr(x) for x in range(0x2018, 0x201E)] + [chr(0x2032), chr(0x2033)])


def _get_title_sort_pat(lang=None):
    """
    Build and cache a leading-article regex for the requested language key.

    For None, consult the default-language preference. Missing language patterns fall
    back to configured English patterns, then built-in English articles; invalid regex
    compilation also uses the built-in fallback. Pattern alternatives come from a
    frozenset, with no precedence guarantee. Cache entries, including the None key, are
    not invalidated when preferences change.

    Example:
        >>> _get_title_sort_pat("eng") is _get_title_sort_pat("eng")
        True


    :param lang: Hashable language key, or ``None`` to consult the default-language preference.
    :return: Cached case-insensitive compiled regex matching an initial article.
    """
    ans = _title_pats.get(lang, None)
    if ans is not None:
        return ans

    q = lang
    if q is None:
        q = preferences.get("default_language_for_title_sort")

    data = preferences.get("per_language_title_sort_articles", {})
    try:
        ans = data.get(q, None)
    except AttributeError:
        ans = None
    try:
        ans = frozenset(ans) if ans else frozenset(data["eng"])
    except Exception:
        ans = frozenset((r"A\s+", r"The\s+", r"An\s+"))
    ans = "^(%s)" % "|".join(ans)
    try:
        ans = re.compile(ans, re.IGNORECASE)
    except Exception:
        ans = re.compile(r"^(A|The|An)\s+", re.IGNORECASE)
    _title_pats[lang] = ans
    return ans


def title_sort(title, order=None, lang=None):
    """
    Create a title sort string using strict or article-moving library order.

    Falsy input gives empty text. Convert other input with str and trim whitespace.
    Strictly alphabetic order stops there; all other orders remove at most one opening
    quote character and move a matched leading article to a comma-separated suffix.
    Article matching uses the preference-backed language cache.

    Example:
        >>> title_sort("  The Book  ", order="strictly_alphabetic")
        'The Book'
        >>> title_sort(None)
        ''


    :param title: Title coerced with str after the initial falsy check.
    :param order: ``strictly_alphabetic`` preserves article order; ``None`` reads the sorting preference.
    :param lang: Language key passed to the cached article-pattern helper.
    :return: The stripped or article-reordered title string.
    """
    if not title:
        return ""
    if order is None:
        order = preferences.get("title_series_sorting", "library_order")
    title = str(title).strip()
    if order == "strictly_alphabetic":
        return title
    if title and title[0] in _ignore_starts:
        title = title[1:]
    match = _get_title_sort_pat(lang).search(title)
    if match:
        try:
            prep = match.group(1)
        except IndexError:
            pass
        else:
            title = title[len(prep) :] + ", " + prep
            if title and title[0] in _ignore_starts:
                title = title[1:]
    return title.strip()
