#!/usr/bin/env  python

"""
Provide metadata author/title formatting, series indexes, resources, identifier checks, and OPF parsing helpers.

Author separators and the XML parser are initialized at import. Article patterns are
cached by language. Resource objects describe paths or URLs without opening them;
parsing and directory enumeration perform the I/O described by their helpers.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/test_utils_coverage.py
"""

from __future__ import annotations

import os
import re
import sys
from collections import namedtuple
from urllib.parse import quote, unquote, urlparse

from LiuXin_alpha.utils.libraries.liuxin_etree import LXML_AVAILABLE, etree

_HAS_LXML = bool(LXML_AVAILABLE)

from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.file_formats.oeb.base import OPF
from LiuXin_alpha.preferences import preferences as tweaks
from LiuXin_alpha.utils.libraries.calibre_chardet import xml_to_unicode
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log, prints
from LiuXin_alpha.utils.mine_types import guess_type
from LiuXin_alpha.utils.paths import relpath
from LiuXin_alpha.utils.text import remove_bracketed_text

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"


if _HAS_LXML:
    PARSER = etree.XMLParser(recover=True, no_network=True)
else:
    PARSER = etree.XMLParser()

OPFVersion = namedtuple("OPFVersion", "major minor patch")


# Todo: Consolidate ebook metadata tools
try:
    _author_pat = re.compile(tweaks["authors_split_regex"])
except KeyError as e:
    err_str = "authors_split_regex not found in tweaks - falling back to default - %s"
    default_log.exception(err_str, e)
    _author_pat = re.compile(r"(?i),?\s+(and|with)\s+")
except Exception as e:
    err_str = "Unknown exception when trying to compile 'authors_split_regex' - bad regex? - Falling back to default - %s"
    err_str = default_log.exception(err_str, e)
    _author_pat = re.compile(r"(?i),?\s+(and|with)\s+")


def soft_float_to_int(num: Union[float, int]) -> Union[int, float]:
    """
    Coerce non-floats with float(), then convert mathematically integral values to int.

    Example:
        >>> (soft_float_to_int(3.0), soft_float_to_int('3.5'))
        (3, 3.5)


    :param num: Numeric or float-convertible value.
    :return: Integer when the float is integral, otherwise the float, including NaN/inf.
        Conversion can lose large-integer precision or raise.
    """
    if not isinstance(num, float):
        num = float(num)

    if num.is_integer():
        return int(num)
    else:
        return num


def string_to_authors(raw: str) -> List[str]:
    """
    Decode the legacy ampersand token, protect doubled ampersands, apply the compiled separator pattern, and split names.

    Trim segments without title-casing and drop empty ones. U+FFFF is the temporary
    ampersand escape. Supported UnicodeDecodeError branches log and continue or return
    the unsplit value; other errors propagate.

    Example:
        >>> string_to_authors('Ada && Bob & Grace')
        ['Ada & Bob', 'Grace']


    :param raw: Encoded author string using ampersands, doubled ampersands, or the
        ,0420, token.
    :return: List of author strings, or an empty list for falsy input.
    """
    if not raw:
        return []

    # Cope with the xml safe (no ampersand) form produced by authors_to_string
    raw = raw.replace(",0420,", "&")

    # Cope with escaped ampersands (which are replaced with double ampersands during encode)
    try:
        raw = raw.replace("&&", "\uffff")
    except UnicodeDecodeError as e:
        err_str = "Error while trying to replace double amersands with properly escaped ampersands"
        default_log.log_exception(err_str, e, "ERROR", ("raw", raw))

    # Apply the author pat
    raw = _author_pat.sub("&", raw)

    # Split and return
    try:
        authors = [a.strip().replace("\uffff", "&") for a in raw.split("&")]
    except UnicodeDecodeError as e:
        err_str = "Error while trying to replace escaped ampersands with raw ampersands"
        default_log.log_exception(err_str, e, "ERROR", ("raw", raw))
        return [raw]
    return [a for a in authors if a]


def authors_to_string(authors: Iterable[str], xml_safe: bool = False) -> str:
    """
    Double embedded ampersands and join truthy author names with spaced ampersands.

    When xml_safe is true, replace every resulting ampersand with ,0420,; this is a
    legacy delimiter encoding, not general XML escaping.

    Example:
        >>> authors_to_string(['Ada & Bob', 'Grace'], xml_safe=True)
        'Ada ,0420,,0420, Bob ,0420, Grace'


    :param authors: Optional iterable of author strings; falsy entries are omitted.
    :param xml_safe: Whether to substitute the legacy ampersand token.
    :return: Encoded author string, or empty string for None.
    """
    if authors is not None:
        enc_str = " & ".join([a.replace("&", "&&") for a in authors if a])
        if not xml_safe:
            return enc_str
        else:
            # This construction is highly unlikely to be found in nature
            return enc_str.replace("&", ",0420,")
    else:
        return ""


# Todo: This is not actually working
def author_to_author_sort(
        author: str, method: Literal["copy", "comma", "nocomma"] = "comma"
) -> str:
    """
    Derive a surname-first sort name with optional comma insertion and preference-based prefix/suffix handling.

    Strip bracketed text for tokenization. Preserve the original for short names, copy
    mode/copywords, exhausted prefix/suffix tokens, or comma-mode names already
    containing commas. None selects the preference with comma fallback; missing
    copywords become an empty set, while missing prefix/suffix settings propagate.

    Example:
        >>> author_to_author_sort('Ada Lovelace', method='copy')
        'Ada Lovelace'


    :param author: Author name string to reorder.
    :param method: copy, comma, or nocomma; defaults to comma, with None requesting the
        preference.
    :return: Derived sort string, unchanged original author, or empty string for falsy
        input.
    """
    if not author:
        return ""
    sauthor = remove_bracketed_text(author).strip()
    tokens = sauthor.split()
    if len(tokens) < 2:
        return author
    if method is None:
        try:
            method = tweaks["author_sort_copy_method"]
        except KeyError:
            # Seems to be the default method
            method = "comma"

    ltoks = frozenset(x.lower() for x in tokens)
    try:
        copy_words = frozenset(x.lower() for x in tweaks["author_name_copywords"])
    except KeyError:
        copy_words = frozenset()
    if ltoks.intersection(copy_words):
        method = "copy"

    if method == "copy":
        return author

    prefixes = set([y.lower() for y in tweaks["author_name_prefixes"]])
    prefixes |= set([y + "." for y in prefixes])
    while True:
        if not tokens:
            return author
        tok = tokens[0].lower()
        if tok in prefixes:
            tokens = tokens[1:]
        else:
            break

    # Todo: Move this from constants over to tweaks
    suffixes = set([y.lower() for y in tweaks["author_name_suffixes"]])
    suffixes |= set([y + "." for y in suffixes])

    suffix = ""
    while True:
        if not tokens:
            return author
        last = tokens[-1].lower()
        if last in suffixes:
            suffix = tokens[-1] + " " + suffix
            tokens = tokens[:-1]
        else:
            break
    suffix = suffix.strip()

    if method == "comma" and "," in "".join(tokens):
        return author

    atokens = tokens[-1:] + tokens[:-1]
    num_toks = len(atokens)
    if suffix:
        atokens.append(suffix)

    if method != "nocomma" and num_toks > 1:
        atokens[0] += ","

    return " ".join(atokens)


def authors_to_sort_string(authors):
    """
    Sort each author with the default comma method and join results with spaced ampersands.

    Example:
        >>> authors_to_sort_string(['Plato'])
        'Plato'


    :param authors: Iterable of author-name strings.
    :return: Combined author-sort string; no separate ampersand escaping is performed.
    """
    return " & ".join(map(author_to_author_sort, authors))


_title_pats = {}


# Todo: We have multiple functions with the same name
def get_title_sort_pat(lang: Optional[str] = None) -> Optional[re.Pattern[str]]:
    """
    Resolve and cache a case-insensitive leading-article pattern under the original language argument.

    Use the preferred/default locale when absent. Canonicalization TypeError falls back
    to no language; missing or invalid article data falls back to English patterns, and
    invalid regexes use the built-in A/The/An pattern. Cached entries retain old
    preference values.

    Example:
        >>> bool(get_title_sort_pat('eng').match('The Book'))
        True


    :param lang: Optional language selector and cache key.
    :return: Compiled regex; may mutate the module cache.
    """
    from LiuXin_alpha.utils.localization import canonicalize_lang, get_lang

    ans = _title_pats.get(lang, None)
    if ans is not None:
        return ans
    q = lang

    if lang is None:
        q = tweaks["default_language_for_title_sort"]
        if q is None:
            q = get_lang()
    try:
        q = canonicalize_lang(q) if q else q
    except TypeError:
        q = None
    data = tweaks["per_language_title_sort_articles"]
    try:
        ans = data.get(q, None)
    except AttributeError:
        ans = None  # invalid tweak value
    try:
        ans = frozenset(ans) if ans else frozenset(data["eng"])
    except:
        ans = frozenset((r"A\s+", r"The\s+", r"An\s+"))
    ans = "|".join(ans)
    ans = "^(%s)" % ans
    try:
        ans = re.compile(ans, re.IGNORECASE)
    except:
        ans = re.compile(r"^(A|The|An)\s+", re.IGNORECASE)
    _title_pats[lang] = ans
    return ans


_ignore_starts: str = "'\"" + "".join([chr(x) for x in range(0x2018, 0x201E)] + [chr(0x2032), chr(0x2033)])


def title_sort(title: str, order: Optional[str] = None, lang: Optional[str] = None) -> str:
    """
    Strip a title and, unless strictly alphabetic, remove a leading quote and move a matching article to the end.

    Example:
        >>> title_sort(' The Book ', order='strictly_alphabetic')
        'The Book'


    :param title: Title string to transform.
    :param order: Optional sorting mode; None reads the preference, while
        strictly_alphabetic only strips whitespace.
    :param lang: Optional language selector for the article-pattern cache.
    :return: Title sort string using the cached language article pattern.
    """
    if order is None:
        order = tweaks["title_series_sorting"]
    title = title.strip()
    if order == "strictly_alphabetic":
        return title
    if title and title[0] in _ignore_starts:
        title = title[1:]
    match = get_title_sort_pat(lang).search(title)
    if match:
        try:
            prep = match.group(1)
        except IndexError:
            pass
        else:
            title = title[len(prep) :] + ", " + prep
            if title[0] in _ignore_starts:
                title = title[1:]
    return title.strip()


coding = tuple(zip(
    [1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1],
    ["M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"],
))


def roman(num: int) -> str:
    """
    Render an integral numeric value from one through 3999 as an uppercase Roman numeral.

    Example:
        >>> (roman(4), roman(4000), roman(2.5))
        ('IV', '4000', '2.5')


    :param num: Numeric candidate tested for bounds and integrality.
    :return: Roman numeral, or str(num) for out-of-range or fractional numbers;
        unsupported comparisons/conversions propagate.
    """
    if num <= 0 or num >= 4000 or int(num) != num:
        return str(num)
    result = []
    for d, r in coding:
        while num >= d:
            result.append(r)
            num -= d
    return "".join(result)


def fmt_sidx(i: Optional[str, int, float], fmt: str = "%.2f", use_roman: bool = False) -> str:
    """
    Treat None/empty as one, convert to float, and format integral values as decimal or Roman numerals.

    Use the supplied percent-format string only for fractional values. A
    float-conversion TypeError returns the original value’s string form; ValueError,
    nonfinite integer conversion, and formatting errors propagate.

    Example:
        >>> (fmt_sidx(None), fmt_sidx('2.50'), fmt_sidx(4, use_roman=True))
        ('1', '2.50', 'IV')


    :param i: Series index or float-convertible input; None and empty string mean one.
    :param fmt: Percent-style fractional format, default %.2f.
    :param use_roman: Whether integral values use roman() instead of decimal formatting.
    :return: Formatted series index string.
    """
    if i is None or i == "":
        i = 1
    try:
        i = float(i)
    except TypeError:
        return str(i)
    if int(i) == float(i):
        return roman(int(i)) if use_roman else "%d" % int(i)
    return fmt % i


class Resource:
    """
    Describe a local path or an unchanged remote URL with a MIME guess and optional local fragment.

    Construction and href formatting do not check existence or fetch remote content.

    Example:
        >>> resource = Resource('https://example.invalid/book', is_path=False)
        >>> (resource.path, resource.href())
        (None, 'https://example.invalid/book')
    """

    def __init__(
            self,
            href_or_path: str,
            basedir: Union[str, bytes] = os.getcwd(),
            is_path: bool = True) -> None:
        """
        Store a path or parse a URL, using a MIME guess with application/octet-stream fallback.

        Relative path inputs become absolute under basedir; absolute path inputs are
        retained. URL inputs with empty/file schemes use their decoded path and fragment,
        ignoring authority and query; other schemes retain the original href. The default
        basedir was evaluated when this module was imported.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :param href_or_path: Filesystem path when is_path is true, otherwise URL text.
        :param basedir: Base directory for relative local paths; byte values decode with
            filesystem encoding and replacement errors.
        :param is_path: Whether to interpret the input directly as a filesystem path.
        :return: None; initializes descriptive state without opening a resource.
        """
        if isinstance(basedir, bytes):
            basedir = basedir.decode(sys.getfilesystemencoding(), "replace")
        self._href = None
        self._basedir = basedir
        self.path = None
        self.fragment = ""
        try:
            self.mime_type = guess_type(href_or_path)[0]
        except:
            self.mime_type = None
        if self.mime_type is None:
            self.mime_type = "application/octet-stream"
        if is_path:
            path = href_or_path
            if not os.path.isabs(path):
                path = os.path.abspath(os.path.join(basedir, path))
            if isinstance(path, bytes):
                path = path.decode(sys.getfilesystemencoding())
            self.path = path
        else:
            url = urlparse(href_or_path)
            if url[0] not in ("", "file"):
                self._href = href_or_path
            else:
                pc = url[2]
                if isinstance(pc, bytes):
                    pc = pc.decode("utf-8", "replace")
                pc = unquote(pc)
                self.path = os.path.abspath(os.path.join(basedir, pc.replace("/", os.sep)))
                self.fragment = unquote(url[-1])

    def href(self, basedir: Optional[str] = None) -> str:
        """
        Return the original remote href or a quoted local path relative to the resolved base, with a quoted fragment.

        Use the stored base when truthy, otherwise the current directory. Equal path/base
        yields only the fragment. relpath OSError falls back to the stored absolute path; no
        existence check occurs.

        Example:
            >>> Resource('https://example.invalid/a book', is_path=False).href()
            'https://example.invalid/a book'


        :param basedir: Optional base override; None uses stored base or the current working
            directory.
        :return: Remote href unchanged or formatted local URL string.
        """

        if basedir is None:
            if self._basedir:
                basedir = self._basedir
            else:
                basedir = os.getcwd()
        if isinstance(basedir, bytes):
            basedir = basedir.decode(sys.getfilesystemencoding(), "replace")
        if self.path is None:
            return self._href
        frag = "#" + quote(self.fragment) if self.fragment else ""
        if self.path == basedir:
            return "" + frag
        try:
            rpath = relpath(self.path, basedir)
        except OSError:  # On windows path and basedir could be on different drives
            rpath = self.path
        if isinstance(rpath, bytes):
            rpath = rpath.decode("utf-8", "replace")
        return quote(rpath.replace(os.sep, "/")) + frag

    @classmethod
    def from_path(cls, path, basedir=None):
        """
        Construct a path resource using the current working directory when basedir is omitted.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :param path: Filesystem path forwarded to the constructor.
        :param basedir: Optional base; None resolves os.getcwd() at call time.
        :return: New instance of cls with is_path=True.
        """
        return cls(path, basedir=os.getcwd() if basedir is None else basedir, is_path=True)

    def set_basedir(self, path):
        """
        Replace the stored base directory without modifying the resource path or validating the new base.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :param path: New base value retained as supplied.
        :return: None; affects later relative href formatting.
        """
        self._basedir = path

    def basedir(self):
        """
        Return the stored base directory value without resolving a fallback.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :return: Current _basedir value.
        """
        return self._basedir

    def __repr__(self):
        """
        Render the stored path and currently computed href in a Resource(...) representation.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :return: Representation string; href errors propagate.
        """
        return "Resource(%s, %s)" % (repr(self.path), repr(self.href()))


class ResourceCollection:
    """
    Keep an ordered mutable list of Resource references, with list-like access and shared-object base updates.

    Example:
        >>> collection = ResourceCollection()
        >>> (len(collection), bool(collection), repr(collection))
        (0, False, '[]')
    """
    _resources: list[Resource]

    def __init__(self) -> None:
        """
        Initialize an empty resource list.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :return: None.
        """
        self._resources = []

    def __iter__(self) -> Iterator[Resource]:
        """
        Yield stored resource references in current list order without taking a snapshot.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :return: Generator over the backing list.
        """
        for r in self._resources:
            yield r

    def __len__(self) -> int:
        """
        Return the backing list’s current length.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :return: Number of stored entries.
        """
        return len(self._resources)

    def __getitem__(self, index):
        """
        Delegate integer indexing or slicing directly to the backing list.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :param index: Integer index or slice accepted by list indexing.
        :return: Resource reference for an index, or a list for a slice; normal list errors
            propagate.
        """
        return self._resources[index]

    def __bool__(self):
        """
        Test whether at least one entry is stored.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :return: True for a nonempty collection, otherwise False.
        """
        return len(self._resources) > 0

    def __str__(self):
        """
        Join the repr of each stored resource inside list-style brackets.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :return: Display string; resource representation errors propagate.
        """
        resources = map(repr, self)
        return "[%s]" % ", ".join(resources)

    def __repr__(self):
        """
        Return the collection’s string representation.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :return: List-style representation of stored resources.
        """
        return str(self)

    def append(self, resource):
        """
        Append a Resource reference after checking isinstance, permitting duplicates and subclasses.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :param resource: Resource instance retained without copying.
        :return: None; raises ValueError for other objects.
        """
        if not isinstance(resource, Resource):
            raise ValueError("Can only append objects of type Resource")
        self._resources.append(resource)

    def remove(self, resource: Resource) -> None:
        """
        Remove the first list entry equal to the supplied object.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :param resource: Entry to remove; Resource normally uses object identity equality.
        :return: None; raises ValueError when no matching entry exists.
        """
        self._resources.remove(resource)

    def replace(self, start: int, end: int, items: list[Resource]) -> None:
        """
        Assign the supplied items into the backing list’s start:end slice without checking element types.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :param start: Slice start, including negative positions.
        :param end: Exclusive slice end.
        :param items: Replacement iterable; entries are retained without copying or type
            validation.
        :return: None; normal list slice-assignment semantics apply.
        """
        self._resources[start:end] = items

    @staticmethod
    def from_directory_contents(top: str, topdown: bool = True) -> "ResourceCollection":
        """
        Walk a directory tree and append path resources for every filename yielded by os.walk.

        Use top as each resource’s base and make discovered file paths absolute. Preserve
        filesystem traversal order without sorting; default os.walk handling ignores
        directory scan errors and does not follow directory symlinks. No file contents are
        read.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :param top: Directory tree root and base used for relative hrefs.
        :param topdown: Traversal direction forwarded to os.walk.
        :return: New ResourceCollection; a missing or unreadable root can produce an empty
            collection.
        """
        collection = ResourceCollection()
        for dirpath, _dirnames, filenames in os.walk(top, topdown=topdown):
            for filename in filenames:
                path = os.path.abspath(os.path.join(dirpath, filename))
                collection.append(Resource.from_path(path, basedir=top))
        return collection

    def set_basedir(self, path):
        """
        Assign the supplied base to every stored resource in iteration order.

        Example:
            Exercise the owning behavior with pytest::

                python -m pytest -q tests/metadata/test_utils_coverage.py


        :param path: New base directory forwarded unchanged to each resource.
        :return: None; mutates shared Resource objects, potentially also visible through
            other collections.
        """
        for res in self:
            res.set_basedir(path)


def validate_identifier(typ: str, val: str) -> None:
    """
    Validate only an exact lowercase isbn type using the uncleaned candidate’s ten- or thirteen-character checksum.

    Example:
        >>> validate_identifier('isbn', '0261103571')


    :param typ: Identifier type; only isbn is supported with this exact spelling.
    :param val: Candidate string, checked without separator removal or case conversion.
    :return: None for a valid candidate; InputIntegrityError for bad length/checksum and
        NotImplementedError for any other type.
    """
    status = False

    if typ == "isbn":
        if len(val) == 10:
            status = check_isbn10(val)
        elif len(val) == 13:
            status = check_isbn13(val)
    else:
        raise NotImplementedError

    if not status:
        raise InputIntegrityError("Identifier did not pass validation")


def check_isbn10(isbn: str) -> Optional[str]:
    """
    Check the first ten characters for an ISBN-10 checksum, allowing uppercase X as the final checked character.

    No exact-length or cleanup pass occurs; suffix characters are retained on success.
    Ordinary checksum exceptions are logged and then return None unless logging raises.

    Example:
        >>> check_isbn10('0261103571')
        '0261103571'


    :param isbn: Candidate whose first ten positions are checked.
    :return: Original input on success, otherwise None.
    """
    try:
        digits = [_ for _ in map(int, isbn[:9])]
        products = [(i + 1) * digits[i] for i in range(9)]
        check = sum(products) % 11
        if (check == 10 and isbn[9] == "X") or check == int(isbn[9]):
            return isbn
    except Exception as e:
        info_str = "Unable to check for ISBN-10."
        default_log.log_exception(info_str, e, "INFO")
    return None


def check_isbn13(isbn: str) -> Optional[str]:
    """
    Check the first thirteen characters with alternating ISBN-13 weights without validating a prefix or exact length.

    Ordinary checksum exceptions are logged and yield None unless logging raises.

    Example:
        >>> check_isbn13('9780261103573')
        '9780261103573'


    :param isbn: Candidate whose first thirteen positions are checked.
    :return: Original input on success, including any unchecked suffix, otherwise None.
    """
    try:
        digits = [_ for _ in map(int, isbn[:12])]
        products = [(1 if i % 2 == 0 else 3) * digits[i] for i in range(12)]
        check = 10 - (sum(products) % 10)
        if check == 10:
            check = 0
        if str(check) == isbn[12]:
            return isbn
    except Exception as e:
        info_str = "Unable to check for ISBN-13."
        default_log.log_exception(info_str, e, "INFO")
    return None


def check_isbn(isbn: str) -> Optional[str]:
    """
    Uppercase and remove non-digit/non-X characters, reject repeated-digit candidates, and dispatch by exact cleaned length.

    Example:
        >>> check_isbn('0-261-10357-1')
        '0261103571'


    :param isbn: Candidate string; falsy input returns None.
    :return: Cleaned valid ten- or thirteen-character ISBN, otherwise None; unsupported
        truthy input types can raise during cleanup.
    """
    if not isbn:
        return None
    isbn = re.sub(r"[^0-9X]", "", isbn.upper())
    all_same = re.match(r"(\d)\1{9,12}$", isbn)
    if all_same is not None:
        return None
    if len(isbn) == 10:
        return check_isbn10(isbn)
    if len(isbn) == 13:
        return check_isbn13(isbn)
    return None


# Todo: Was an actual bug in calibre
def check_issn(issn: str) -> Optional[str]:
    """
    Clean to digits/X and check the first eight characters with the ISSN checksum, retaining any cleaned suffix.

    There is no exact-length check. Ordinary checksum exceptions are logged and yield
    None unless logging raises; cleanup errors propagate.

    Example:
        >>> check_issn('2049-3630')
        '20493630'


    :param issn: Candidate string; falsy values return None.
    :return: Cleaned candidate on success, otherwise None.
    """
    if not issn:
        return None
    issn = re.sub(r"[^0-9X]", "", issn.upper())
    try:
        digits = map(int, issn[:7])
        products = [(8 - i) * d for i, d in enumerate(digits)]
        check = 11 - sum(products) % 11
        if (check == 10 and issn[7] == "X") or check == int(issn[7]) or (check == 11 and issn[7] == "0"):
            return issn
    except Exception as e:
        info_str = "Unable to check for ISSN."
        default_log.log_exception(info_str, e, "INFO")
    return None


def format_isbn(isbn: str) -> str:
    """
    Validate an ISBN and insert hyphens at fixed slice positions for its cleaned length.

    Example:
        >>> format_isbn('0261103571')
        '02-6110-357-1'


    :param isbn: Candidate passed to check_isbn.
    :return: Formatted valid ISBN or the original invalid input; this is not
        registration-group-aware hyphenation.
    """
    cisbn = check_isbn(isbn)
    if not cisbn:
        return isbn
    i = cisbn
    if len(i) == 10:
        return "-".join((i[:2], i[2:6], i[6:9], i[9]))
    return "-".join((i[:3], i[3:5], i[5:9], i[9:12], i[12]))


def check_doi(doi: str) -> Optional[str]:
    """
    Find the first substring containing 10., exactly four digits, a slash, and non-whitespace suffix text.

    Example:
        >>> check_doi('See 10.1234/example')
        '10.1234/example'


    :param doi: Candidate string; falsy values return None.
    :return: Matching substring, including attached non-whitespace punctuation, or None.
    """
    if not doi:
        return None
    doi_check = re.search(r"10\.\d{4}/\S+", doi)
    if doi_check is not None:
        return doi_check.group()
    return None


# ----------------------------------------------------------------------------------------------------------------------
#
# - CONVENIENCE METHODS TO ACCESS THE METADATA CLASSES


def calibreMetaInformation(title, authors=(_("Unknown"),)):
    """
    Construct core Calibre metadata from a title/authors pair or an object exposing both attributes.

    A metadata-like first argument overrides the authors argument and is supplied as
    other to the constructor. The default Unknown author was translated when this
    function was defined.

    Example:
        >>> calibreMetaInformation('Example', ['Ada']).title
        'Example'


    :param title: Title value or metadata-like object with title and authors attributes.
    :param authors: Author sequence used only when title is not metadata-like.
    :return: New core calibreMetadata instance.
    """
    from LiuXin_alpha.metadata.book.base import calibreMetadata

    mi = None
    if hasattr(title, "title") and hasattr(title, "authors"):
        mi = title
        title = mi.title
        authors = mi.authors
    return calibreMetadata(title, authors, other=mi)

#
# ----------------------------------------------------------------------------------------------------------------------


def parse_opf_version(raw: str) -> OPFVersion:
    """
    Parse dotted integer version components, pad to three, and discard extras.

    An unparseable major defaults to 2.0.0; a later malformed component preserves the
    major and zeroes minor/patch. Negative components are accepted.

    Example:
        >>> parse_opf_version('4.bad')
        OPFVersion(major=4, minor=0, patch=0)


    :param raw: Dotted version string; falsy input uses the default major.
    :return: OPFVersion namedtuple with major, minor, and patch integers.
    """
    parts = (raw or "").split(".")
    try:
        major = int(parts[0])
    except Exception:
        return OPFVersion(2, 0, 0)
    try:
        v = list(map(int, raw.split(".")))
    except Exception:
        v = [major, 0, 0]
    while len(v) < 3:
        v.append(0)
    v = v[:3]
    return OPFVersion(*v)


def parse_opf(stream_or_path):
    """
    Read XML from the current stream position, an existing short filesystem path, or a raw payload, then parse its root.

    Treat non-stream inputs shorter than 4096 as paths only when they exist. Close files
    opened here, but leave caller streams open and consumed without position
    restoration. Decode XML, discard text before the first opening angle bracket, and
    parse with the module parser; lxml enables recovery. The helper does not validate
    OPF namespace or schema.

    Example:
        >>> parse_opf('<package/>').tag
        'package'


    :param stream_or_path: Readable stream, path-like object, or raw XML bytes/string
        accepted by the decoder.
    :return: Parsed XML root; empty data raises ValueError, parser errors propagate, and
        a None parser result raises ValueError.
    """
    stream = stream_or_path
    if not hasattr(stream, "read"):
        stream = os.fspath(stream) if isinstance(stream, os.PathLike) else stream
        if len(stream) < 4096 and os.path.exists(stream):
            with open(stream, "rb") as opf_stream:
                raw = opf_stream.read()
        else:
            raw = stream
    else:
        raw = stream.read()
    if not raw:
        raise ValueError("Empty file: " + getattr(stream, "name", "stream"))
    raw, encoding = xml_to_unicode(raw, strip_encoding_pats=True, resolve_entities=True, assume_utf8=True)
    raw = raw[raw.find("<") :]
    root = etree.fromstring(raw, PARSER)
    if root is None:
        raise ValueError("Not an OPF file")
    return root


def normalize_languages(opf_languages, mi_languages):
    """
    Normalize metadata languages while retaining matching OPF regions.

    Blank inputs are dropped, underscores become hyphens, and known languages prefer
    two-letter codes. A supplied metadata region wins over the OPF region. This is a
    compact language/region round-trip helper: script and private-use subtags are not
    preserved as a full BCP-47 tag, and unknown language names remain lower-cased.

    Example:
        >>> normalize_languages(['en-US', 'zh-Hant-TW'], ['eng', 'zho'])
        ['en-US', 'zh-TW']


    :param opf_languages: Iterable of original OPF language strings used for region
        fallback.
    :param mi_languages: Iterable of replacement language strings or empty values.
    :return: New list in metadata-language order, including duplicates.
    """
    from LiuXin_alpha.utils.libraries.iso639.iso639_tools import (
        lang_as_iso639_1 as fallback_lang_as_iso639_1,
    )
    from LiuXin_alpha.utils.localization import canonicalize_lang, lang_as_iso639_1

    LocaleCode = namedtuple("LocaleCode", "langcode countrycode")

    def parse(x):
        """
        Split one language token into the local language/region pair.

        Canonicalize the primary language when known. Prefer a two-character or numeric
        three-character tail part as the region; otherwise use the first tail part,
        upper-cased. Ignore the other tail parts.

        Example:
            >>> normalize_languages(['en-US', 'zh-Hant-TW'], ['eng', 'zho'])
            ['en-US', 'zh-TW']


        :param x: Language string or false value; underscores are accepted as separators.
        :return: LocaleCode pair, or None for a blank or missing primary language.
        """
        raw = (x or "").strip()
        if not raw:
            return None
        raw = raw.replace("_", "-")
        primary, _, tail = raw.partition("-")
        primary = primary.strip()
        if not primary:
            return None

        # Keep unknown values in a stable lower-cased form so we round-trip
        # instead of dropping potentially valid-but-unlisted BCP-47 tags.
        langcode = canonicalize_lang(primary) or primary.lower()
        tail_parts = [part.strip() for part in tail.split("-") if part.strip()]
        country = ""
        if tail_parts:
            country = next(
                (
                    part
                    for part in tail_parts
                    if len(part) == 2 or (len(part) == 3 and part.isdigit())
                ),
                tail_parts[0],
            )
            country = country.upper()
        return LocaleCode(langcode=langcode, countrycode=(country or None))

    def iso2(lc):
        """
        Resolve a language code through the localization helper and ISO table fallback.

        Example:
            >>> normalize_languages(['en-US', 'zh-Hant-TW'], ['eng', 'zho'])
            ['en-US', 'zh-TW']


        :param lc: Primary language code to look up.
        :return: Preferred two-letter code when available, otherwise the lookup result.
        """
        lc2 = lang_as_iso639_1(lc)
        if not lc2 or len(lc2) != 2:
            lc2 = fallback_lang_as_iso639_1(lc) or lc2
        return lc2

    opf_languages = list(filter(None, map(parse, opf_languages)))
    cc_map = {}
    for c in opf_languages:
        cc_map[c.langcode] = c.countrycode
        lc2 = iso2(c.langcode)
        if lc2:
            cc_map.setdefault(lc2, c.countrycode)
    mi_languages = filter(None, map(parse, mi_languages))

    def norm(x):
        """
        Format a parsed metadata language using its region or the captured OPF fallback.

        Example:
            >>> normalize_languages(['en-US', 'zh-Hant-TW'], ['eng', 'zho'])
            ['en-US', 'zh-TW']


        :param x: LocaleCode pair produced by the enclosing parser.
        :return: Language code optionally followed by a hyphen and region.
        """
        lc = x.langcode
        cc = x.countrycode or cc_map.get(lc, None)
        lc2 = iso2(lc)
        if not cc and lc2:
            cc = cc_map.get(lc2, None)
        lc = lc2 or lc
        if cc:
            lc += "-" + cc
        return lc

    return list(map(norm, mi_languages))


def ensure_unique(template, existing):
    """
    Find an unused name by inserting numbered suffixes before the final extension.

    The original name is returned when available. The membership collection is read
    only; the chosen name is not reserved.

    Example:
        >>> ensure_unique('cover.jpg', {'cover.jpg', 'cover-1.jpg'})
        'cover-2.jpg'


    :param template: Preferred file name or identifier string.
    :param existing: Collection supporting membership checks against existing names.
    :return: Original name or first available name with a -1, -2, or later suffix.
    """
    b, e = template.rpartition(".")[::2]
    if b and e:
        e = "." + e
    else:
        b, e = template, ""
    q = template
    c = 0
    while q in existing:
        c += 1
        q = "%s-%d%s" % (b, c, e)
    return q


def create_manifest_item(root, href_template, id_template, media_type=None):
    """
    Append an OPF manifest item with unique href and id attributes.

    Collect existing attributes across the root using lxml XPath or the ElementTree
    fallback. Infer the media type from the original href when it is not supplied,
    falling back to application/octet-stream. A missing direct manifest child is left
    missing.

    Example:
        >>> from xml.etree import ElementTree as ET
        >>> root = ET.Element(OPF('package'))
        >>> manifest = ET.SubElement(root, OPF('manifest'))
        >>> create_manifest_item(root, 'cover.jpg', 'cover').get('media-type')
        'image/jpeg'


    :param root: Package root to inspect and mutate.
    :param href_template: Preferred relative resource href; collisions receive numbered
        suffixes.
    :param id_template: Preferred item id; collisions receive numbered suffixes.
    :param media_type: Explicit media type, or a false value to infer it.
    :return: Appended element, or None if the root has no OPF manifest.
    """
    if hasattr(root, "xpath"):
        all_ids = frozenset(root.xpath("//*/@id"))
        all_hrefs = frozenset(root.xpath("//*/@href"))
    else:
        # xml.etree fallback: gather attribute values by walking descendants.
        all_ids = frozenset(e.get("id") for e in root.iter() if e.get("id"))
        all_hrefs = frozenset(e.get("href") for e in root.iter() if e.get("href"))
    href = ensure_unique(href_template, all_hrefs)
    item_id = ensure_unique(id_template, all_ids)
    manifest = root.find(OPF("manifest"))
    if manifest is not None:
        i = manifest.makeelement(OPF("item"), {})
        i.set("href", href), i.set("id", item_id)
        mtype = media_type
        if not mtype:
            guessed = guess_type(href_template)
            mtype = guessed[0] if isinstance(guessed, tuple) else guessed
            mtype = mtype or "application/octet-stream"
        i.set("media-type", mtype)
        manifest.append(i)
        return i


def pretty_print_opf(root):
    """
    Reorder OPF sections and apply XML indentation in place.

    The polishing helpers sort metadata and manifest entries, then adjust text and tails
    for pretty serialization. The root must support lxml XPath; this helper does not
    write a file.

    Example:
        >>> from lxml import etree
        >>> root = etree.Element(OPF('package'))
        >>> manifest = etree.SubElement(root, OPF('manifest'))
        >>> pretty_print_opf(root)
        >>> root.text.isspace()
        True


    :param root: Mutable lxml OPF package element.
    :return: None.
    """
    from LiuXin_alpha.file_formats.oeb.polish.pretty import pretty_opf, pretty_xml_tree

    pretty_opf(root)
    pretty_xml_tree(root)
