#!/usr/bin/env  python

"""
Utils for metadata processing.
"""

from __future__ import annotations

import os
import sys
import re
from urllib.parse import urlparse
from collections import namedtuple

from lxml import etree

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union, Literal, Iterator, TypeVar, Type

from LiuXin_alpha.errors import InputIntegrityError

from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.text import remove_bracketed_text
from LiuXin_alpha.utils.paths import relpath
from LiuXin_alpha.utils.mine_types import guess_type
from LiuXin_alpha.utils.logging import prints
from LiuXin_alpha.preferences import preferences as tweaks
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"



PARSER = etree.XMLParser(recover=True, no_network=True)

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
    If a float is an integer then convert it to such - otherwise leave it as a float.

    :param num:
    :return:
    """
    if not isinstance(num, float):
        num = float(num)

    if num.is_integer():
        return int(num)
    else:
        return num


def string_to_authors(raw: str) -> List[str]:
    """
    Split down a string containing multiple authors names and return them as a list of strings,

    :param raw: An encoded string of authors
    :return:
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
    Take an iterable of authors and return them as a string.

    :param authors:
    :param xml_safe: If True, then uses double commands instead of ampersands
    :return:
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
    Takes an author name and produces a sort string from it.

    :param author: The name of the author to transform
    :param method: 'copy' - Just make a straight copy of the author name
                   'comma' - Try and put the first name after the last name, separated by a comma
                   'nocomma' - Same as with comma, but no actual comma
    :type method: string indicating the mode
    :return:
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

    if method == "comma":
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
    return " & ".join(map(author_to_author_sort, authors))


_title_pats = {}


# Todo: We have multiple functions with the same name
def get_title_sort_pat(lang: Optional[str] = None) -> Optional[re.Pattern[str]]:
    """
    Return the title sort pattern for the given language.

    :param lang:
    :return:
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
    Return the title sort pattern for the given language.

    :param title:
    :param order:
    :param lang:
    :return:
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


coding = zip(
    [1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1],
    ["M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"],
)


def roman(num: int) -> str:
    """
    Return the roman numeral of the given number.

    :param num:
    :return:
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
    Format series index.

    :param i:
    :param fmt:
    :param use_roman:
    :return:
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
    Represents a resource (usually a file on the filesystem or a URL pointing to the web.

    Such resources are commonly referred to in OPF files.

    They have the interface:

    :member:`path`
    :member:`mime_type`
    :method:`href`

    """

    def __init__(
            self,
            href_or_path: str,
            basedir: Union[str, bytes] = os.getcwd(),
            is_path: bool = True) -> None:
        """
        Startup a resource in the OPF.

        :param href_or_path:
        :param basedir:
        :param is_path:
        """
        from urllib import unquote

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
                if isinstance(pc, str):
                    pc = pc.encode("utf-8")
                pc = unquote(pc).decode("utf-8")
                self.path = os.path.abspath(os.path.join(basedir, pc.replace("/", os.sep)))
                self.fragment = unquote(url[-1])

    def href(self, basedir: Optional[str] = None) -> str:
        """
        Return a URL pointing to this resource. If it is a file on the filesystem the URL is relative to `basedir`.

        `basedir`: If None, the basedir of this resource is used (see :method:`set_basedir`).
        If this resource has no basedir, then the current working directory is used as the basedir.
        """

        from urllib import quote

        if basedir is None:
            if self._basedir:
                basedir = self._basedir
            else:
                basedir = os.getcwdu()
        if self.path is None:
            return self._href
        f = self.fragment.encode("utf-8") if isinstance(self.fragment, unicode) else self.fragment
        frag = "#" + quote(f) if self.fragment else ""
        if self.path == basedir:
            return "" + frag
        try:
            rpath = relpath(self.path, basedir)
        except OSError:  # On windows path and basedir could be on different drives
            rpath = self.path
        if isinstance(rpath, unicode):
            rpath = rpath.encode("utf-8")
        return quote(rpath.replace(os.sep, "/")) + frag

    def set_basedir(self, path):
        self._basedir = path

    def basedir(self):
        return self._basedir

    def __repr__(self):
        return "Resource(%s, %s)" % (repr(self.path), repr(self.href()))


class ResourceCollection:
    """
    A collection of resources.
    """
    _resources: list[Resource]

    def __init__(self) -> None:
        """
        Initialize a collection of resources.
        """
        self._resources = []

    def __iter__(self) -> Iterator[Resource]:
        """
        Itterate over the collection.

        :return:
        """
        for r in self._resources:
            yield r

    def __len__(self) -> int:
        """
        Return the number of resources in the collection.

        :return:
        """
        return len(self._resources)

    def __getitem__(self, index):
        return self._resources[index]

    def __bool__(self):
        return len(self._resources) > 0

    def __str__(self):
        resources = map(repr, self)
        return "[%s]" % ", ".join(resources)

    def __repr__(self):
        return str(self)

    def append(self, resource):
        if not isinstance(resource, Resource):
            raise ValueError("Can only append objects of type Resource")
        self._resources.append(resource)

    def remove(self, resource: Resource) -> None:
        """
        Remove a resource from the collection.

        :param resource:
        :return:
        """
        self._resources.remove(resource)

    def replace(self, start: int, end: int, items: list[Resource]) -> None:
        """
        Same as list[start:end] = items.

        :param start:
        :param end:
        :param items:
        :return:
        """
        self._resources[start:end] = items

    @staticmethod
    def from_directory_contents(top: str, topdown: bool = True) -> "ResourceCollection":
        """
        Initialize a collection of resources from a directory.

        :param top:
        :param topdown:
        :return:
        """
        collection = ResourceCollection()
        for spec in os.walk(top, topdown=topdown):
            path = os.path.abspath(os.path.join(spec[0], spec[1]))
            res = Resource.from_path(path)
            res.set_basedir(top)
            collection.append(res)
        return collection

    def set_basedir(self, path):
        for res in self:
            res.set_basedir(path)


def validate_identifier(typ: str, val: str) -> None:
    """
    Preform validation checks on the given identifier - raises InputIntegrityError if the id is not valid.

    :param typ:
    :param val:
    :return:
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
    Checks to see if the first ten digits of a string is a valid ISBN-10 string.

    :param isbn:
    :return:
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
    Checks to see if the first thirteen digits of a string is a valid ISBN-13 string.

    By the point this function is called filtering is assumed to have been done to remove any illegal characters.
    :param isbn:
    :return:
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
    Ids the type of ISBN we're dealing with - checks to see if it's valid.

    :param isbn:
    :return:
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
    Checks to make sure that the given issn string is valid - returns None if it isn't.

    :param issn: The issn string to check
    :return:
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
    Render an isbn into a more easily readable format.

    :param isbn:
    :return:
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
    Check if something that looks like a DOI (Digital Object Identifier) is present anywhere in the string.

    :param doi:
    :return:
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
    Convenient encapsulation of book metadata, needed for compatibility

    :param title: title or ``_('Unknown')`` or a MetaInformation object (or something with a similar interface that
                  can be read from)
    :param authors: List of strings or []
    :return:
    """
    from LiuXin.metadata.book.base import calibreMetadata

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
    Returns the opf version from an opf string.

    :param raw:
    :return:
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
    Take an opf file as a stream, string or path. Tries to guess which is which and then parses the

    :param stream_or_path:
    :return:
    """
    stream = stream_or_path
    if not hasattr(stream, "read"):
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
    Preserve original country codes and use 2-letter lang codes where possible
    :param opf_languages:
    :param mi_languages:
    :return:
    """

    def parse(x):
        try:
            return parse_lang_code(x)
        except ValueError:
            return None

    opf_languages = filter(None, map(parse, opf_languages))
    cc_map = {c.langcode: c.countrycode for c in opf_languages}
    mi_languages = filter(None, map(parse, mi_languages))

    def norm(x):
        lc = x.langcode
        cc = x.countrycode or cc_map.get(lc, None)
        lc = lang_as_iso639_1(lc) or lc
        if cc:
            lc += "-" + cc
        return lc

    return list(map(norm, mi_languages))


def ensure_unique(template, existing):
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
    all_ids = frozenset(root.xpath("//*/@id"))
    all_hrefs = frozenset(root.xpath("//*/@href"))
    href = ensure_unique(href_template, all_hrefs)
    item_id = ensure_unique(id_template, all_ids)
    manifest = root.find(OPF("manifest"))
    if manifest is not None:
        i = manifest.makeelement(OPF("item"))
        i.set("href", href), i.set("id", item_id)
        i.set("media-type", media_type or guess_type(href_template))
        manifest.append(i)
        return i


def pretty_print_opf(root):
    from LiuXin.file_formats.oeb.polish.pretty import pretty_opf, pretty_xml_tree

    pretty_opf(root)
    pretty_xml_tree(root)
