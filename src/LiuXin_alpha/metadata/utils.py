#!/usr/bin/env  python

"""
Provide metadata author formatting, series indexes, resources, and identifier checks.

Author separators are initialized at import. Resource objects describe paths or URLs
without opening them; directory enumeration performs the I/O described by its helper.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/test_utils_coverage.py
"""

from __future__ import annotations

import os
import re
import sys
from urllib.parse import quote, unquote, urlparse

from typing import (
    Iterable,
    Iterator,
    List,
    Optional,
    Union,
)

from LiuXin_alpha.metadata.identifiers import (
    check_isbn10,
    check_isbn13,
)
from LiuXin_alpha.metadata.identifiers import (
    validate_legacy_identifier as _validate_legacy_identifier,
)
from LiuXin_alpha.preferences import preferences as tweaks
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.mine_types import guess_type
from LiuXin_alpha.utils.paths import relpath

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"

# Historical import surface retained while the policy owner lives in
# ``metadata.identifiers``.
validate_identifier = _validate_legacy_identifier


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
