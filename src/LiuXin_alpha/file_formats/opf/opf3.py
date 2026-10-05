#!/usr/bin/env python2
# vim:fileencoding=utf-8
# License: GPLv3 Copyright: 2016, Kovid Goyal <kovid at kovidgoyal.net>

"""
Read, refine and serialize OPF 3 metadata and package relationships.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise opf3 through a consuming regression::

        python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import json
import re
from collections import defaultdict, namedtuple
from functools import wraps

from LiuXin_alpha.utils.libraries.liuxin_etree import etree

from LiuXin_alpha.file_formats.oeb.base import OPF2_NSMAP, OPF, DC

from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import Metadata as calibreMetadata

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData as MetaData

from LiuXin_alpha.metadata.book.json_codec import (
    object_to_unicode,
    decode_is_multiple,
    encode_is_multiple,
)
from LiuXin_alpha.metadata.ebook_metadata_tools import (
    check_isbn,
    authors_to_string,
    string_to_authors,
)
from LiuXin_alpha.metadata.utils import (
    parse_opf,
    pretty_print_opf,
    ensure_unique,
    normalize_languages,
    create_manifest_item,
)

from LiuXin_alpha.utils.logging import prints
from LiuXin_alpha.utils.config.config_tools import from_json, to_json
from LiuXin_alpha.utils.date import (
    parse_date as parse_date_,
    fix_only_date,
    is_date_undefined,
    isoformat,
)
from LiuXin_alpha.utils.iso8601 import parse_iso8601
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems, six_map
from LiuXin_alpha.utils.localization import canonicalize_lang, trans as _

# Utils {{{
_xpath_cache = {}
_re_cache = {}


def uniq(vals: _typing.Any) -> _typing.Any:
    """
    Remove all duplicates from vals, while preserving order.

    Example:
        Exercise uniq through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param vals: Value supplied for vals under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    vals = vals or ()
    seen = set()
    seen_add = seen.add
    return list(x for x in vals if x not in seen and not seen_add(x))


def dump_dict(cats: _typing.Any) -> _typing.Any:
    """
    Perform the dump dict operation under explicit file-format and conversion rules.

    Example:
        Exercise dump dict through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param cats: Value supplied for cats under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return json.dumps(object_to_unicode(cats or {}), ensure_ascii=False, skipkeys=True)


def XPath(x: _typing.Any) -> _typing.Any:
    """
    Perform the XPath operation under explicit file-format and conversion rules.

    Example:
        Exercise XPath through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return _xpath_cache[x]
    except KeyError:
        _xpath_cache[x] = ans = etree.XPath(x, namespaces=OPF2_NSMAP)
        return ans


def regex(r: _typing.Any, flags: int = 0) -> _typing.Any:
    """
    Perform the regex operation under explicit file-format and conversion rules.

    Example:
        Exercise regex through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param r: Value supplied for r under the utility contract.
    :param flags: Value supplied for flags under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return _re_cache[(r, flags)]
    except KeyError:
        _re_cache[(r, flags)] = ans = re.compile(r, flags)
        return ans


def remove_refines(e: _typing.Any, refines: _typing.Any) -> None:
    """
    Perform the remove refines operation under explicit file-format and conversion rules.

    Example:
        Exercise remove refines through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param e: Value supplied for e under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for x in refines[e.get("id")]:
        x.getparent().remove(x)
    refines.pop(e.get("id"), None)


def remove_element(e: _typing.Any, refines: _typing.Any) -> None:
    """
    Perform the remove element operation under explicit file-format and conversion rules.

    Example:
        Exercise remove element through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param e: Value supplied for e under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    remove_refines(e, refines)
    e.getparent().remove(e)


def properties_for_id(item_id: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Perform the properties for id operation under explicit file-format and conversion rules.

    Example:
        Exercise properties for id through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param item_id: Value supplied for item id under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = {}
    if item_id:
        for elem in refines[item_id]:
            key = elem.get("property")
            if key:
                val = (elem.text or "").strip()
                if val:
                    ans[key] = val
    return ans


def properties_for_id_with_scheme(item_id: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Perform the properties for id with scheme operation under explicit file-format and conversion rules.

    Example:
        Exercise properties for id with scheme through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param item_id: Value supplied for item id under the utility contract.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = {}
    if item_id:
        for elem in refines[item_id]:
            key = elem.get("property")
            if key:
                val = (elem.text or "").strip()
                if val:
                    scheme = elem.get("scheme") or None
                    scheme_ns = None
                    if scheme is not None:
                        p, r = scheme.partition(":")[::2]
                        if p and r:
                            ns = prefixes.get(p)
                            if ns:
                                scheme_ns = ns
                                scheme = r
                    ans[key] = (scheme_ns, scheme, val)
    return ans


def getroot(elem: _typing.Any) -> _typing.Any:
    """
    Perform the getroot operation under explicit file-format and conversion rules.

    Example:
        Exercise getroot through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param elem: Value supplied for elem under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    while True:
        q = elem.getparent()
        if q is None:
            return elem
        elem = q


def ensure_id(elem: _typing.Any) -> _typing.Any:
    """
    Perform the ensure id operation under explicit file-format and conversion rules.

    Example:
        Exercise ensure id through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param elem: Value supplied for elem under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    root = getroot(elem)
    eid = elem.get("id")
    if not eid:
        eid = ensure_unique("id", frozenset(XPath("//*/@id")(root)))
        elem.set("id", eid)
    return eid


def normalize_whitespace(text: _typing.Any) -> _typing.Any:
    """
    Normalize whitespace under the format's safety and compatibility rules.

    Example:
        Exercise normalize whitespace through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not text:
        return text
    return re.sub(r"\s+", " ", text).strip()


def simple_text(f: _typing.Any) -> _typing.Any:
    """
    Perform the simple text operation under explicit file-format and conversion rules.

    Example:
        Exercise simple text through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param f: Value supplied for f under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    @wraps(f)
    def wrapper(*args: _typing.Any, **kw: _typing.Any) -> _typing.Any:
        """
        Perform the wrapper operation under explicit file-format and conversion rules.

        Example:
            Exercise simple text.wrapper through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kw: Value supplied for kw under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return normalize_whitespace(f(*args, **kw))

    return wrapper


def items_with_property(root: _typing.Any, q: _typing.Any, prefixes: _typing.Any = None) -> _typing.Iterator[_typing.Any]:
    """
    Perform the items with property operation under explicit file-format and conversion rules.

    Example:
        Exercise items with property through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param q: Value supplied for q under the utility contract.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """
    if prefixes is None:
        prefixes = read_prefixes(root)
    q = expand_prefix(q, known_prefixes).lower()
    for item in XPath("./opf:manifest/opf:item[@properties]")(root):
        for prop in (item.get("properties") or "").lower().split():
            prop = expand_prefix(prop, prefixes)
            if prop == q:
                yield item
                break


# }}}

# Prefixes {{{

# http://www.idpf.org/epub/vocab/package/pfx/
reserved_prefixes = {
    "dcterms": "http://purl.org/dc/terms/",
    "epubsc": "http://idpf.org/epub/vocab/sc/#",
    "marc": "http://id.loc.gov/vocabulary/",
    "media": "http://www.idpf.org/epub/vocab/overlays/#",
    "onix": "http://www.editeur.org/ONIX/book/codelists/current.html#",
    "rendition": "http://www.idpf.org/vocab/rendition/#",
    "schema": "http://schema.org/",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
}

CALIBRE_PREFIX = "https://calibre-ebook.com"
known_prefixes = reserved_prefixes.copy()
known_prefixes["calibre"] = CALIBRE_PREFIX


def parse_prefixes(x: _typing.Any) -> _typing.Any:
    """
    Parse prefixes under the format's safety and compatibility rules.

    Example:
        Exercise parse prefixes through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return {m.group(1): m.group(2) for m in re.finditer(r"(\S+): \s*(\S+)", x)}


def read_prefixes(root: _typing.Any) -> _typing.Any:
    """
    Read prefixes under the format's safety and compatibility rules.

    Example:
        Exercise read prefixes through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = reserved_prefixes.copy()
    ans.update(parse_prefixes(root.get("prefix") or ""))
    return ans


def expand_prefix(raw: _typing.Any, prefixes: _typing.Any) -> _typing.Any:
    """
    Perform the expand prefix operation under explicit file-format and conversion rules.

    Example:
        Exercise expand prefix through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param raw: Value supplied for raw under the utility contract.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return regex(r"(\S+)\s*:\s*(\S+)").sub(
        lambda m: (prefixes.get(m.group(1), m.group(1)) + ":" + m.group(2)), raw or ""
    )


def ensure_prefix(root: _typing.Any, prefixes: _typing.Any, prefix: _typing.Any, value: _typing.Any = None) -> None:
    """
    Perform the ensure prefix operation under explicit file-format and conversion rules.

    Example:
        Exercise ensure prefix through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param prefix: Text prepended to the formatted or selected result.
    :param value: Value normalized, stored, formatted or returned.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if prefixes is None:
        prefixes = read_prefixes(root)
    prefixes[prefix] = value or reserved_prefixes[prefix]
    prefixes = {k: v for k, v in iteritems(prefixes) if reserved_prefixes.get(k) != v}
    if prefixes:
        root.set("prefix", " ".join("%s: %s" % (k, v) for k, v in iteritems(prefixes)))
    else:
        root.attrib.pop("prefix", None)


# }}}


# Refines {{{
def read_refines(root: _typing.Any) -> _typing.Any:
    """
    Read refines under the format's safety and compatibility rules.

    Example:
        Exercise read refines through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = defaultdict(list)
    for meta in XPath("./opf:metadata/opf:meta[@refines]")(root):
        r = meta.get("refines") or ""
        if r.startswith("#"):
            ans[r[1:]].append(meta)
    return ans


def refdef(prop: _typing.Any, val: _typing.Any, scheme: _typing.Any = None) -> tuple[_typing.Any, ...]:
    """
    Perform the refdef operation under explicit file-format and conversion rules.

    Example:
        Exercise refdef through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param prop: Value supplied for prop under the utility contract.
    :param val: Template or metadata value evaluated by the operation.
    :param scheme: Value supplied for scheme under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return prop, val, scheme


def set_refines(elem: _typing.Any, existing_refines: _typing.Any, *new_refines: _typing.Any) -> None:
    """
    Set refines under the format's safety and compatibility rules.

    Example:
        Exercise set refines through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param elem: Value supplied for elem under the utility contract.
    :param existing_refines: Value supplied for existing refines under the utility
        contract.
    :param new_refines: Value supplied for new refines under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    eid = ensure_id(elem)
    remove_refines(elem, existing_refines)
    for ref in reversed(new_refines):
        prop, val, scheme = ref
        r = elem.makeelement(OPF("meta"))
        r.set("refines", "#" + eid), r.set("property", prop)
        r.text = val.strip()
        if scheme:
            r.set("scheme", scheme)
        p = elem.getparent()
        p.insert(p.index(elem) + 1, r)


# }}}


# Identifiers {{{
def parse_identifier(ident: _typing.Any, val: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Parse identifier under the format's safety and compatibility rules.

    Example:
        Exercise parse identifier through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param ident: Value supplied for ident under the utility contract.
    :param val: Template or metadata value evaluated by the operation.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    idid = ident.get("id")
    refines = refines[idid]
    lval = val.lower()

    def finalize(local_scheme: _typing.Any, local_val: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Perform the finalize operation under explicit file-format and conversion rules.

        Example:
            Exercise parse identifier.finalize through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


        :param local_scheme: Value supplied for local scheme under the utility contract.
        :param local_val: Value supplied for local val under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not local_scheme or not local_val:
            return None, None
        local_scheme = local_scheme.lower()
        if local_scheme in ("http", "https"):
            return None, None
        if local_scheme.startswith("isbn"):
            local_scheme = "isbn"
        if local_scheme == "isbn":
            local_val = local_val.split(":")[-1]
            local_val = check_isbn(local_val)
            if local_val is None:
                return None, None
        return local_scheme, local_val

    # Try the OPF 2 style opf:scheme attribute, which will be present, for example, in EPUB 3 files that have had their
    # metadata set by an application that only understands EPUB 2.
    scheme = ident.get(OPF("scheme"))
    if scheme and not lval.startswith("urn:"):
        return finalize(scheme, val)

    # Technically, we should be looking for refines that define the scheme, but
    # the IDioticPF created such a bad spec that they got their own
    # examples wrong, so I cannot be bothered doing this.
    # http://www.idpf.org/epub/301/spec/epub-publications-errata/

    # Parse the value for the scheme
    if lval.startswith("urn:"):
        val = val[4:]

    prefix, rest = val.partition(":")[::2]
    return finalize(prefix, rest)


def read_identifiers(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read identifiers under the format's safety and compatibility rules.

    Example:
        Exercise read identifiers through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = defaultdict(list)
    for ident in XPath("./opf:metadata/dc:identifier")(root):
        val = (ident.text or "").strip()
        if val:
            scheme, val = parse_identifier(ident, val, refines)
            if scheme and val:
                ans[scheme].append(val)
    return ans


def set_identifiers(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, new_identifiers: _typing.Any, force_identifiers: bool = False) -> None:
    """
    Set identifiers under the format's safety and compatibility rules.

    Example:
        Exercise set identifiers through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param new_identifiers: Value supplied for new identifiers under the utility
        contract.
    :param force_identifiers: Value supplied for force identifiers under the utility
        contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    uid = root.get("unique-identifier")
    package_identifier = None
    for ident in XPath("./opf:metadata/dc:identifier")(root):
        if uid is not None and uid == ident.get("id"):
            package_identifier = ident
            continue
        val = (ident.text or "").strip()
        if not val:
            ident.getparent().remove(ident)
            continue
        scheme, val = parse_identifier(ident, val, refines)
        if not scheme or not val or force_identifiers or scheme in new_identifiers:
            remove_element(ident, refines)
            continue
    metadata = XPath("./opf:metadata")(root)[0]
    for scheme, val in iteritems(new_identifiers):
        ident = metadata.makeelement(DC("identifier"))
        ident.text = "%s:%s" % (scheme, val)
        if package_identifier is None:
            metadata.append(ident)
        else:
            p = package_identifier.getparent()
            p.insert(p.index(package_identifier), ident)


def identifier_writer(name: _typing.Any) -> _typing.Any:
    """
    Perform the identifier writer operation under explicit file-format and conversion rules.

    Example:
        Exercise identifier writer through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def writer(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, ival: _typing.Any = None) -> None:
        """
        Perform the writer operation under explicit file-format and conversion rules.

        Example:
            Exercise identifier writer.writer through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


        :param root: Root directory that bounds path resolution or traversal.
        :param prefixes: Value supplied for prefixes under the utility contract.
        :param refines: Value supplied for refines under the utility contract.
        :param ival: Value supplied for ival under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        uid = root.get("unique-identifier")
        package_identifier = None
        for ident in XPath("./opf:metadata/dc:identifier")(root):
            is_package_id = uid is not None and uid == ident.get("id")
            if is_package_id:
                package_identifier = ident
            val = (ident.text or "").strip()
            if (val.startswith(name + ":") or ident.get(OPF("scheme")) == name) and not is_package_id:
                remove_element(ident, refines)
        metadata = XPath("./opf:metadata")(root)[0]
        if ival:
            ident = metadata.makeelement(DC("identifier"))
            ident.text = "%s:%s" % (name, ival)
            if package_identifier is None:
                metadata.append(ident)
            else:
                p = package_identifier.getparent()
                p.insert(p.index(package_identifier), ident)

    return writer


set_application_id = identifier_writer("calibre")
set_uuid = identifier_writer("uuid")

# }}}

# Title {{{


def find_main_title(root: _typing.Any, refines: _typing.Any, remove_blanks: bool = False) -> _typing.Any:
    """
    Find main title under the format's safety and compatibility rules.

    Example:
        Exercise find main title through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param refines: Value supplied for refines under the utility contract.
    :param remove_blanks: Value supplied for remove blanks under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    first_title = None
    for title in XPath("./opf:metadata/dc:title")(root):
        if not title.text or not title.text.strip():
            if remove_blanks:
                remove_element(title, refines)
            continue
        if first_title is None:
            first_title = title
        props = properties_for_id(title.get("id"), refines)
        if props.get("title-type") == "main":
            main_title = title
            break
    else:
        main_title = first_title
    return main_title


@simple_text
def read_title(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read title under the format's safety and compatibility rules.

    Example:
        Exercise read title through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    main_title = find_main_title(root, refines)
    return None if main_title is None else main_title.text.strip()


@simple_text
def read_title_sort(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read title sort under the format's safety and compatibility rules.

    Example:
        Exercise read title sort through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    main_title = find_main_title(root, refines)
    if main_title is not None:
        fa = properties_for_id(main_title.get("id"), refines).get("file-as")
        if fa:
            return fa
    # Look for OPF 2.0 style title_sort
    for m in XPath('./opf:metadata/opf:meta[@name="calibre:title_sort"]')(root):
        ans = m.get("content")
        if ans:
            return ans


def set_title(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, title: _typing.Any, title_sort: _typing.Any = None) -> None:
    """
    Set title under the format's safety and compatibility rules.

    Example:
        Exercise set title through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param title: Value supplied for title under the utility contract.
    :param title_sort: Value supplied for title sort under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    main_title = find_main_title(root, refines, remove_blanks=True)
    if main_title is None:
        m = XPath("./opf:metadata")(root)[0]
        main_title = m.makeelement("dc:title")
        m.insert(0, main_title)
    main_title.text = title or None
    title_id = ensure_id(main_title)

    # Preserve non-title refines attached to the main title element.
    existing = list(refines.get(title_id, ()))
    kept = []
    for meta in existing:
        prop = (meta.get("property") or "").strip().lower()
        if prop in {"title-type", "file-as"}:
            meta.getparent().remove(meta)
        else:
            kept.append(meta)
    if kept:
        refines[title_id] = kept
    else:
        refines.pop(title_id, None)

    new_refines = [refdef("title-type", "main")]
    if title_sort:
        new_refines.append(refdef("file-as", title_sort))
    parent = main_title.getparent()
    insert_pos = parent.index(main_title)
    for prop, val, _scheme in new_refines:
        r = main_title.makeelement(OPF("meta"))
        r.set("refines", "#" + title_id), r.set("property", prop)
        r.text = val.strip()
        insert_pos += 1
        parent.insert(insert_pos, r)
        refines[title_id].append(r)
    for m in XPath('./opf:metadata/opf:meta[@name="calibre:title_sort"]')(root):
        remove_element(m, refines)


# }}}


# Languages {{{
def read_languages(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read languages under the format's safety and compatibility rules.

    Example:
        Exercise read languages through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = []
    for lang in XPath("./opf:metadata/dc:language")(root):
        val = canonicalize_lang((lang.text or "").strip())
        if val and val not in ans and val != "und":
            ans.append(val)
    return uniq(ans)


def set_languages(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, languages: _typing.Any) -> None:
    """
    Set languages under the format's safety and compatibility rules.

    Example:
        Exercise set languages through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param languages: Value supplied for languages under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    opf_languages = []
    for lang in XPath("./opf:metadata/dc:language")(root):
        remove_element(lang, refines)
        val = (lang.text or "").strip()
        if val:
            opf_languages.append(val)
    languages = filter(lambda x: x and x != "und", normalize_languages(opf_languages, languages))
    if not languages:
        # EPUB spec says dc:language is required
        languages = ["und"]
    metadata = XPath("./opf:metadata")(root)[0]
    for lang in uniq(languages):
        l = metadata.makeelement(DC("language"))
        l.text = lang
        metadata.append(l)


# }}}

# Creator/Contributor {{{

Author = namedtuple("Author", "name sort")


def is_relators_role(props: _typing.Any, q: _typing.Any) -> bool:
    """
    Return whether is relators role holds for the supplied ebook data.

    Example:
        Exercise is relators role through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param props: Value supplied for props under the utility contract.
    :param q: Value supplied for q under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    role = props.get("role")
    if role:
        scheme_ns, scheme, role = role
        return role.lower() == q and (
            scheme_ns is None or (scheme_ns, scheme) == (reserved_prefixes["marc"], "relators")
        )
    return False


def read_authors(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read authors under the format's safety and compatibility rules.

    Example:
        Exercise read authors through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    roled_authors, unroled_authors = [], []

    def author(item: _typing.Any, props: _typing.Any, val: _typing.Any) -> _typing.Any:
        """
        Perform the author operation under explicit file-format and conversion rules.

        Example:
            Exercise read authors.author through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


        :param item: Value supplied for item under the utility contract.
        :param props: Value supplied for props under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        file_as = props.get("file-as")
        if file_as:
            aus = file_as[-1]
        else:
            aus = item.get(OPF("file-as")) or None
        return Author(normalize_whitespace(val), normalize_whitespace(aus))

    for item in XPath("./opf:metadata/dc:creator")(root):
        val = (item.text or "").strip()
        if val:
            props = properties_for_id_with_scheme(item.get("id"), prefixes, refines)
            role = props.get("role")
            opf_role = item.get(OPF("role"))
            if role:
                if is_relators_role(props, "aut"):
                    roled_authors.append(author(item, props, val))
            elif opf_role:
                if opf_role.lower() == "aut":
                    roled_authors.append(author(item, props, val))
            else:
                unroled_authors.append(author(item, props, val))

    return uniq(roled_authors or unroled_authors)


def set_authors(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, authors: _typing.Any) -> None:
    """
    Set authors under the format's safety and compatibility rules.

    Example:
        Exercise set authors through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param authors: Value supplied for authors under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    ensure_prefix(root, prefixes, "marc")
    for item in XPath("./opf:metadata/dc:creator")(root):
        props = properties_for_id_with_scheme(item.get("id"), prefixes, refines)
        opf_role = item.get(OPF("role"))
        if (opf_role and opf_role.lower() != "aut") or (props.get("role") and not is_relators_role(props, "aut")):
            continue
        remove_element(item, refines)
    metadata = XPath("./opf:metadata")(root)[0]
    for author in authors:
        a = metadata.makeelement(DC("creator"))
        aid = ensure_id(a)
        a.text = author.name
        metadata.append(a)
        m = metadata.makeelement(
            OPF("meta"),
            attrib={
                "refines": "#" + aid,
                "property": "role",
                "scheme": "marc:relators",
            },
        )
        m.text = "aut"
        metadata.append(m)
        if author.sort:
            m = metadata.makeelement(OPF("meta"), attrib={"refines": "#" + aid, "property": "file-as"})
            m.text = author.sort
            metadata.append(m)


def read_book_producers(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read book producers under the format's safety and compatibility rules.

    Example:
        Exercise read book producers through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = []
    for item in XPath("./opf:metadata/dc:contributor")(root):
        val = (item.text or "").strip()
        if val:
            props = properties_for_id_with_scheme(item.get("id"), prefixes, refines)
            role = props.get("role")
            opf_role = item.get(OPF("role"))
            if role:
                if is_relators_role(props, "bkp"):
                    ans.append(normalize_whitespace(val))
            elif opf_role and opf_role.lower() == "bkp":
                ans.append(normalize_whitespace(val))
    return ans


def set_book_producers(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, producers: _typing.Any) -> None:
    """
    Set book producers under the format's safety and compatibility rules.

    Example:
        Exercise set book producers through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param producers: Value supplied for producers under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for item in XPath("./opf:metadata/dc:contributor")(root):
        props = properties_for_id_with_scheme(item.get("id"), prefixes, refines)
        opf_role = item.get(OPF("role"))
        if (opf_role and opf_role.lower() != "bkp") or (props.get("role") and not is_relators_role(props, "bkp")):
            continue
        remove_element(item, refines)
    metadata = XPath("./opf:metadata")(root)[0]
    for bkp in producers:
        a = metadata.makeelement(DC("contributor"))
        aid = ensure_id(a)
        a.text = bkp
        metadata.append(a)
        m = metadata.makeelement(
            OPF("meta"),
            attrib={
                "refines": "#" + aid,
                "property": "role",
                "scheme": "marc:relators",
            },
        )
        m.text = "bkp"
        metadata.append(m)


# }}}

# Dates {{{


def parse_date(raw: _typing.Any, is_w3cdtf: bool = False) -> _typing.Any:
    """
    Parse date under the format's safety and compatibility rules.

    Example:
        Exercise parse date through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param raw: Value supplied for raw under the utility contract.
    :param is_w3cdtf: Value supplied for is w3cdtf under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    raw = raw.strip()
    if is_w3cdtf:
        ans = parse_iso8601(raw, assume_utc=True)
        if "T" not in raw and " " not in raw:
            ans = fix_only_date(ans)
    else:
        ans = parse_date_(raw, assume_utc=True)
        if " " not in raw and "T" not in raw and (ans.hour, ans.minute, ans.second) == (0, 0, 0):
            ans = fix_only_date(ans)
    return ans


def read_pubdate(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read pubdate under the format's safety and compatibility rules.

    Example:
        Exercise read pubdate through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for date in XPath("./opf:metadata/dc:date")(root):
        val = (date.text or "").strip()
        if val:
            try:
                return parse_date(val)
            except Exception:
                continue


def set_pubdate(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, val: _typing.Any) -> None:
    """
    Set pubdate under the format's safety and compatibility rules.

    Example:
        Exercise set pubdate through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param val: Template or metadata value evaluated by the operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for date in XPath("./opf:metadata/dc:date")(root):
        remove_element(date, refines)
    if not is_date_undefined(val):
        val = isoformat(val)
        m = XPath("./opf:metadata")(root)[0]
        d = m.makeelement(DC("date"))
        d.text = val
        m.append(d)


def read_timestamp(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read timestamp under the format's safety and compatibility rules.

    Example:
        Exercise read timestamp through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    pq = "%s:timestamp" % CALIBRE_PREFIX
    sq = "%s:w3cdtf" % reserved_prefixes["dcterms"]
    for meta in XPath("./opf:metadata/opf:meta[@property]")(root):
        val = (meta.text or "").strip()
        if val:
            prop = expand_prefix(meta.get("property"), prefixes)
            if prop.lower() == pq:
                scheme = expand_prefix(meta.get("scheme"), prefixes).lower()
                try:
                    return parse_date(val, is_w3cdtf=scheme == sq)
                except Exception:
                    continue
    for meta in XPath('./opf:metadata/opf:meta[@name="calibre:timestamp"]')(root):
        val = meta.get("content")
        if val:
            try:
                return parse_date(val, is_w3cdtf=True)
            except Exception:
                continue


def set_timestamp(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, val: _typing.Any) -> None:
    """
    Set timestamp under the format's safety and compatibility rules.

    Example:
        Exercise set timestamp through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param val: Template or metadata value evaluated by the operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    ensure_prefix(root, prefixes, "calibre", CALIBRE_PREFIX)
    ensure_prefix(root, prefixes, "dcterms")
    pq = "%s:timestamp" % CALIBRE_PREFIX
    for meta in XPath("./opf:metadata/opf:meta")(root):
        prop = expand_prefix(meta.get("property"), prefixes)
        if prop.lower() == pq or meta.get("name") == "calibre:timestamp":
            remove_element(meta, refines)
    if not is_date_undefined(val):
        val = isoformat(val)
        m = XPath("./opf:metadata")(root)[0]
        d = m.makeelement(
            OPF("meta"),
            attrib={"property": "calibre:timestamp", "scheme": "dcterms:W3CDTF"},
        )
        d.text = val
        m.append(d)


def read_last_modified(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read last modified under the format's safety and compatibility rules.

    Example:
        Exercise read last modified through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    pq = "%s:modified" % reserved_prefixes["dcterms"]
    sq = "%s:w3cdtf" % reserved_prefixes["dcterms"]
    for meta in XPath("./opf:metadata/opf:meta[@property]")(root):
        val = (meta.text or "").strip()
        if val:
            prop = expand_prefix(meta.get("property"), prefixes)
            if prop.lower() == pq:
                scheme = expand_prefix(meta.get("scheme"), prefixes).lower()
                try:
                    return parse_date(val, is_w3cdtf=scheme == sq)
                except Exception:
                    continue


# }}}

# Comments {{{


def read_comments(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read comments under the format's safety and compatibility rules.

    Example:
        Exercise read comments through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = ""
    for dc in XPath("./opf:metadata/dc:description")(root):
        if dc.text:
            ans += "\n" + dc.text.strip()
    return ans.strip()


def set_comments(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, val: _typing.Any) -> None:
    """
    Set comments under the format's safety and compatibility rules.

    Example:
        Exercise set comments through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param val: Template or metadata value evaluated by the operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for dc in XPath("./opf:metadata/dc:description")(root):
        remove_element(dc, refines)
    m = XPath("./opf:metadata")(root)[0]
    if val:
        val = val.strip()
        if val:
            c = m.makeelement(DC("description"))
            c.text = val
            m.append(c)


# }}}

# Publisher {{{


@simple_text
def read_publisher(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read publisher under the format's safety and compatibility rules.

    Example:
        Exercise read publisher through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for dc in XPath("./opf:metadata/dc:publisher")(root):
        if dc.text:
            return dc.text


def set_publisher(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, val: _typing.Any) -> None:
    """
    Set publisher under the format's safety and compatibility rules.

    Example:
        Exercise set publisher through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param val: Template or metadata value evaluated by the operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for dc in XPath("./opf:metadata/dc:publisher")(root):
        remove_element(dc, refines)
    m = XPath("./opf:metadata")(root)[0]
    if val:
        val = val.strip()
        if val:
            c = m.makeelement(DC("publisher"))
            c.text = normalize_whitespace(val)
            m.append(c)


# }}}

# Tags {{{


def read_tags(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read tags under the format's safety and compatibility rules.

    Example:
        Exercise read tags through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = []
    for dc in XPath("./opf:metadata/dc:subject")(root):
        if dc.text:
            ans.extend(six_map(normalize_whitespace, dc.text.split(",")))
    return uniq(filter(None, ans))


def set_tags(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, val: _typing.Any) -> None:
    """
    Set tags under the format's safety and compatibility rules.

    Example:
        Exercise set tags through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param val: Template or metadata value evaluated by the operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for dc in XPath("./opf:metadata/dc:subject")(root):
        remove_element(dc, refines)
    m = XPath("./opf:metadata")(root)[0]
    if val:
        val = uniq(filter(None, val))
        for x in val:
            c = m.makeelement(DC("subject"))
            c.text = normalize_whitespace(x)
            if c.text:
                m.append(c)


# }}}

# Rating {{{


def read_rating(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read rating under the format's safety and compatibility rules.

    Example:
        Exercise read rating through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    pq = "%s:rating" % CALIBRE_PREFIX
    for meta in XPath("./opf:metadata/opf:meta[@property]")(root):
        val = (meta.text or "").strip()
        if val:
            prop = expand_prefix(meta.get("property"), prefixes)
            if prop.lower() == pq:
                try:
                    return float(val)
                except Exception:
                    continue
    for meta in XPath('./opf:metadata/opf:meta[@name="calibre:rating"]')(root):
        val = meta.get("content")
        if val:
            try:
                return float(val)
            except Exception:
                continue


def set_rating(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, val: _typing.Any) -> None:
    """
    Set rating under the format's safety and compatibility rules.

    Example:
        Exercise set rating through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param val: Template or metadata value evaluated by the operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    pq = "%s:rating" % CALIBRE_PREFIX
    for meta in XPath('./opf:metadata/opf:meta[@name="calibre:rating"]')(root):
        remove_element(meta, refines)
    for meta in XPath("./opf:metadata/opf:meta[@property]")(root):
        prop = expand_prefix(meta.get("property"), prefixes)
        if prop.lower() == pq:
            remove_element(meta, refines)
    if val:
        ensure_prefix(root, prefixes, "calibre", CALIBRE_PREFIX)
        m = XPath("./opf:metadata")(root)[0]
        d = m.makeelement(OPF("meta"), attrib={"property": "calibre:rating"})
        d.text = "%.2g" % val
        m.append(d)


# }}}

# Series {{{


def read_series(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Read series under the format's safety and compatibility rules.

    Example:
        Exercise read series through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    series_index = 1.0
    for meta in XPath('./opf:metadata/opf:meta[@property="belongs-to-collection" and @id]')(root):
        val = (meta.text or "").strip()
        if val:
            props = properties_for_id(meta.get("id"), refines)
            if props.get("collection-type") == "series":
                try:
                    series_index = float(props.get("group-position").strip())
                except Exception:
                    pass
                return normalize_whitespace(val), series_index
    for si in XPath('./opf:metadata/opf:meta[@name="calibre:series_index"]/@content')(root):
        try:
            series_index = float(si)
            break
        except:
            pass
    for s in XPath('./opf:metadata/opf:meta[@name="calibre:series"]/@content')(root):
        s = normalize_whitespace(s)
        if s:
            return s, series_index
    return None, series_index


def set_series(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, series: _typing.Any, series_index: _typing.Any) -> None:
    """
    Set series under the format's safety and compatibility rules.

    Example:
        Exercise set series through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param series: Value supplied for series under the utility contract.
    :param series_index: Value supplied for series index under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for meta in XPath('./opf:metadata/opf:meta[@name="calibre:series" or @name="calibre:series_index"]')(root):
        remove_element(meta, refines)
    for meta in XPath('./opf:metadata/opf:meta[@property="belongs-to-collection"]')(root):
        # Preserve non-series collection metadata and refinements; only rewrite
        # series collections owned by calibre/LiuXin.
        meta_id = meta.get("id")
        if not meta_id:
            continue
        props = properties_for_id(meta_id, refines)
        collection_type = (props.get("collection-type") or "").strip().lower()
        if collection_type == "series":
            remove_element(meta, refines)
    m = XPath("./opf:metadata")(root)[0]
    if series:
        d = m.makeelement(OPF("meta"), attrib={"property": "belongs-to-collection"})
        d.text = series
        m.append(d)
        set_refines(
            d,
            refines,
            refdef("collection-type", "series"),
            refdef("group-position", "%.2g" % series_index),
        )


# }}}

# User metadata {{{


def dict_reader(name: _typing.Any, load: _typing.Any = json.loads, try2: bool = True) -> _typing.Any:
    """
    Perform the dict reader operation under explicit file-format and conversion rules.

    Example:
        Exercise dict reader through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param load: Value supplied for load under the utility contract.
    :param try2: Value supplied for try2 under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    pq = "%s:%s" % (CALIBRE_PREFIX, name)

    def reader(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
        """
        Perform the reader operation under explicit file-format and conversion rules.

        Example:
            Exercise dict reader.reader through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


        :param root: Root directory that bounds path resolution or traversal.
        :param prefixes: Value supplied for prefixes under the utility contract.
        :param refines: Value supplied for refines under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for meta in XPath("./opf:metadata/opf:meta[@property]")(root):
            val = (meta.text or "").strip()
            if val:
                prop = expand_prefix(meta.get("property"), prefixes)
                if prop.lower() == pq:
                    try:
                        ans = load(val)
                        if isinstance(ans, dict):
                            return ans
                    except Exception:
                        continue
        if try2:
            for meta in XPath('./opf:metadata/opf:meta[@name="calibre:%s"]' % name)(root):
                val = meta.get("content")
                if val:
                    try:
                        ans = load(val)
                        if isinstance(ans, dict):
                            return ans
                    except Exception:
                        continue

    return reader


read_user_categories = dict_reader("user_categories")
read_author_link_map = dict_reader("author_link_map")


def dict_writer(name: _typing.Any, serialize: _typing.Any = dump_dict, remove2: bool = True) -> _typing.Any:
    """
    Perform the dict writer operation under explicit file-format and conversion rules.

    Example:
        Exercise dict writer through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param serialize: Value supplied for serialize under the utility contract.
    :param remove2: Value supplied for remove2 under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    pq = "%s:%s" % (CALIBRE_PREFIX, name)

    def writer(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, val: _typing.Any) -> None:
        """
        Perform the writer operation under explicit file-format and conversion rules.

        Example:
            Exercise dict writer.writer through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


        :param root: Root directory that bounds path resolution or traversal.
        :param prefixes: Value supplied for prefixes under the utility contract.
        :param refines: Value supplied for refines under the utility contract.
        :param val: Template or metadata value evaluated by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if remove2:
            for meta in XPath('./opf:metadata/opf:meta[@name="calibre:%s"]' % name)(root):
                remove_element(meta, refines)
        for meta in XPath("./opf:metadata/opf:meta[@property]")(root):
            prop = expand_prefix(meta.get("property"), prefixes)
            if prop.lower() == pq:
                remove_element(meta, refines)
        if val:
            ensure_prefix(root, prefixes, "calibre", CALIBRE_PREFIX)
            m = XPath("./opf:metadata")(root)[0]
            d = m.makeelement(OPF("meta"), attrib={"property": "calibre:%s" % name})
            d.text = serialize(val)
            m.append(d)

    return writer


set_user_categories = dict_writer("user_categories")
set_author_link_map = dict_writer("author_link_map")


def deserialize_user_metadata(val: _typing.Any) -> _typing.Any:
    """
    Perform the deserialize user metadata operation under explicit file-format and conversion rules.

    Example:
        Exercise deserialize user metadata through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param val: Template or metadata value evaluated by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    val = json.loads(val, object_hook=from_json)
    ans = {}
    for name, fm in iteritems(val):
        decode_is_multiple(fm)
        ans[name] = fm
    return ans


read_user_metadata3 = dict_reader("user_metadata", load=deserialize_user_metadata, try2=False)


def read_user_metadata2(root: _typing.Any) -> _typing.Any:
    """
    Read user metadata2 under the format's safety and compatibility rules.

    Example:
        Exercise read user metadata2 through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = {}
    for meta in XPath('./opf:metadata/opf:meta[starts-with(@name, "calibre:user_metadata:")]')(root):
        name = meta.get("name")
        name = ":".join(name.split(":")[2:])
        if not name or not name.startswith("#"):
            continue
        fm = meta.get("content")
        try:
            fm = json.loads(fm, object_hook=from_json)
            decode_is_multiple(fm)
            ans[name] = fm
        except Exception:
            prints("Failed to read user metadata:", name)
            import traceback

            traceback.print_exc()
            continue
    return ans


def read_user_metadata(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> bool:
    """
    Read user metadata under the format's safety and compatibility rules.

    Example:
        Exercise read user metadata through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return read_user_metadata3(root, prefixes, refines) or read_user_metadata2(root)


def serialize_user_metadata(val: _typing.Any) -> _typing.Any:
    """
    Serialize user metadata under the format's safety and compatibility rules.

    Example:
        Exercise serialize user metadata through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param val: Template or metadata value evaluated by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return json.dumps(
        object_to_unicode(val),
        ensure_ascii=False,
        default=to_json,
        indent=2,
        sort_keys=True,
    )


set_user_metadata3 = dict_writer("user_metadata", serialize=serialize_user_metadata, remove2=False)


def set_user_metadata(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, val: _typing.Any) -> None:
    """
    Set user metadata under the format's safety and compatibility rules.

    Example:
        Exercise set user metadata through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param val: Template or metadata value evaluated by the operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for meta in XPath('./opf:metadata/opf:meta[starts-with(@name, "calibre:user_metadata:")]')(root):
        remove_element(meta, refines)
    if val:
        nval = {}
        for name, fm in val.items():
            fm = fm.copy()
            encode_is_multiple(fm)
            nval[name] = fm
        set_user_metadata3(root, prefixes, refines, nval)


# }}}

# Covers {{{


def read_raster_cover(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> _typing.Any:
    """
    Read raster cover under the format's safety and compatibility rules.

    Example:
        Exercise read raster cover through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def get_href(local_item: _typing.Any) -> _typing.Any:
        """
        Return href under the format's safety and compatibility rules.

        Example:
            Exercise read raster cover.get href through a consuming regression::

                python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


        :param local_item: Value supplied for local item under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        mt = local_item.get("media-type")
        if mt and "xml" not in mt and "html" not in mt:
            local_href = local_item.get("href")
            if local_href:
                return local_href

    for item in items_with_property(root, "cover-image", prefixes):
        href = get_href(item)
        if href:
            return href

    for item_id in XPath('./opf:metadata/opf:meta[@name="cover"]/@content')(root):
        for item in XPath("./opf:manifest/opf:item[@id and @href and @media-type]")(root):
            if item.get("id") == item_id:
                href = get_href(item)
                if href:
                    return href


def ensure_is_only_raster_cover(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any, raster_cover_item_href: _typing.Any) -> None:
    """
    Perform the ensure is only raster cover operation under explicit file-format and conversion rules.

    Example:
        Exercise ensure is only raster cover through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :param raster_cover_item_href: Value supplied for raster cover item href under the
        utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for item in XPath('./opf:metadata/opf:meta[@name="cover"]')(root):
        remove_element(item, refines)
    for item in items_with_property(root, "cover-image", prefixes):
        prop = normalize_whitespace(item.get("properties").replace("cover-image", ""))
        if prop:
            item.set("properties", prop)
        else:
            del item.attrib["properties"]
    for item in XPath("./opf:manifest/opf:item")(root):
        if item.get("href") == raster_cover_item_href:
            item.set(
                "properties",
                normalize_whitespace((item.get("properties") or "") + " cover-image"),
            )


# }}}

# Reading/setting Metadata objects {{{


def first_spine_item(root: _typing.Any, prefixes: _typing.Any, refines: _typing.Any) -> bool:
    """
    Return the href to the first spine item.

    Example:
        Exercise first spine item through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :param refines: Value supplied for refines under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for i in XPath("./opf:spine/opf:itemref/@idref")(root):
        for item in XPath("./opf:manifest/opf:item")(root):
            if item.get("id") == i:
                return item.get("href") or None


def read_metadata(root: _typing.Any, ver: _typing.Any = None, return_extra_data: bool = False, calibre_md_rtn: bool = True) -> _typing.Any:
    """
    Reads metadata from an opf file

    Example:
        Exercise read metadata through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param ver: Value supplied for ver under the utility contract.
    :param return_extra_data: Value supplied for return extra data under the utility
        contract.
    :param calibre_md_rtn: Value supplied for calibre md rtn under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if calibre_md_rtn:
        ans = calibreMetadata(_("Unknown"), [_("Unknown")])
    else:
        ans = MetaData(_("Unknown"), [_("Unknown")])

    prefixes, refines = read_prefixes(root), read_refines(root)
    identifiers = read_identifiers(root, prefixes, refines)
    ids = {}

    for key, vals in iteritems(identifiers):
        if key == "calibre":
            ans.application_id = vals[0]
        elif key == "uuid":
            ans.uuid = vals[0]
        else:
            ids[key] = vals[0]

    ans.set_identifiers(ids)
    ans.title = read_title(root, prefixes, refines) or ans.title
    ans.title_sort = read_title_sort(root, prefixes, refines) or ans.title_sort
    ans.languages = read_languages(root, prefixes, refines) or ans.languages
    auts, aus = [], []
    for a in read_authors(root, prefixes, refines):
        auts.append(a.name), aus.append(a.sort)
    ans.authors = auts or ans.authors
    ans.author_sort = authors_to_string(aus) or ans.author_sort
    bkp = read_book_producers(root, prefixes, refines)
    if bkp:
        ans.book_producer = bkp[0]
    pd = read_pubdate(root, prefixes, refines)
    if not is_date_undefined(pd):
        ans.pubdate = pd
    ts = read_timestamp(root, prefixes, refines)
    if not is_date_undefined(ts):
        ans.timestamp = ts
    lm = read_last_modified(root, prefixes, refines)
    if not is_date_undefined(lm):
        ans.last_modified = lm
    ans.comments = read_comments(root, prefixes, refines) or ans.comments
    ans.publisher = read_publisher(root, prefixes, refines) or ans.publisher
    ans.tags = read_tags(root, prefixes, refines) or ans.tags
    ans.rating = read_rating(root, prefixes, refines) or ans.rating
    s, si = read_series(root, prefixes, refines)
    if s:
        ans.series, ans.series_index = s, si
    ans.author_link_map = read_author_link_map(root, prefixes, refines) or getattr(ans, "author_link_map", {})
    ans.user_categories = read_user_categories(root, prefixes, refines) or getattr(ans, "user_categories", {})
    for name, fm in iteritems((read_user_metadata(root, prefixes, refines) or {})):
        ans.set_user_metadata(name, fm)
    if return_extra_data:
        ans = (
            ans,
            ver,
            read_raster_cover(root, prefixes, refines),
            first_spine_item(root, prefixes, refines),
        )
    return ans


def get_metadata(stream: _typing.Any, calibre_md_rtn: bool = True) -> _typing.Any:
    """
    get_metadata from an OPF file.

    Example:
        Exercise get metadata through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param stream: Input or output stream wrapped by the terminal or compatibility
        layer.
    :param calibre_md_rtn: Value supplied for calibre md rtn under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    root = parse_opf(stream)
    return read_metadata(root, calibre_md_rtn=calibre_md_rtn)


def apply_metadata(
    root: _typing.Any,
    mi: _typing.Any,
    cover_prefix: str = "",
    cover_data: _typing.Any = None,
    apply_null: bool = False,
    update_timestamp: bool = False,
    force_identifiers: bool = False,
    add_missing_cover: bool = True,
) -> _typing.Any:
    """
    Perform the apply metadata operation under explicit file-format and conversion rules.

    Example:
        Exercise apply metadata through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param mi: Metadata object exposed to the template function.
    :param cover_prefix: Value supplied for cover prefix under the utility contract.
    :param cover_data: Value supplied for cover data under the utility contract.
    :param apply_null: Value supplied for apply null under the utility contract.
    :param update_timestamp: Value supplied for update timestamp under the utility
        contract.
    :param force_identifiers: Value supplied for force identifiers under the utility
        contract.
    :param add_missing_cover: Value supplied for add missing cover under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    prefixes, refines = read_prefixes(root), read_refines(root)
    current_mi = read_metadata(root)

    if apply_null:

        def ok(x: _typing.Any) -> bool:
            """
            Perform the ok operation under explicit file-format and conversion rules.

            Example:
                Exercise apply metadata.ok through a consuming regression::

                    python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


            :param x: Value supplied for x under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return True

    else:

        def ok(x: _typing.Any) -> _typing.Any:
            """
            Perform the ok operation under explicit file-format and conversion rules.

            Example:
                Exercise apply metadata.ok through a consuming regression::

                    python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


            :param x: Value supplied for x under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return not mi.is_null(x)

    if ok("identifiers"):
        set_identifiers(root, prefixes, refines, mi.identifiers, force_identifiers=force_identifiers)
    if ok("title"):
        set_title(root, prefixes, refines, mi.title, mi.title_sort)
    if ok("languages"):
        set_languages(root, prefixes, refines, mi.languages)
    if ok("book_producer"):
        set_book_producers(root, prefixes, refines, (mi.book_producer,))
    aus = string_to_authors(mi.author_sort or "")
    authors = []
    for i, aut in enumerate(mi.authors):
        authors.append(Author(aut, aus[i] if i < len(aus) else None))
    if authors or apply_null:
        set_authors(root, prefixes, refines, authors)
    if ok("pubdate"):
        set_pubdate(root, prefixes, refines, mi.pubdate)
    if update_timestamp and mi.timestamp is not None:
        set_timestamp(root, prefixes, refines, mi.timestamp)
    if ok("comments"):
        set_comments(root, prefixes, refines, mi.comments)
    if ok("publisher"):
        set_publisher(root, prefixes, refines, mi.publisher)
    if ok("tags"):
        set_tags(root, prefixes, refines, mi.tags)
    if ok("rating") and mi.rating > 0.1:
        set_rating(root, prefixes, refines, mi.rating)
    if ok("series"):
        set_series(root, prefixes, refines, mi.series, mi.series_index or 1)
    if ok("author_link_map"):
        set_author_link_map(root, prefixes, refines, getattr(mi, "author_link_map", None))
    if ok("user_categories"):
        set_user_categories(root, prefixes, refines, getattr(mi, "user_categories", None))
    # We ignore apply_null for the next two to match the behavior with opf2.py
    application_id = getattr(mi, "application_id", None)
    if application_id:
        set_application_id(root, prefixes, refines, application_id)
    uuid = getattr(mi, "uuid", None)
    if uuid:
        set_uuid(root, prefixes, refines, uuid)
    new_user_metadata, current_user_metadata = (
        mi.get_all_user_metadata(True),
        current_mi.get_all_user_metadata(True),
    )
    missing = object()
    for key in tuple(new_user_metadata):
        meta = new_user_metadata.get(key)
        if meta is None:
            if apply_null:
                new_user_metadata[key] = None
            continue
        dt = meta.get("datatype")
        if dt == "text" and meta.get("is_multiple"):
            val = mi.get(key, [])
            if val or apply_null:
                current_user_metadata[key] = meta
        elif dt in {"int", "float", "bool"}:
            val = mi.get(key, missing)
            if val is missing:
                if apply_null:
                    current_user_metadata[key] = meta
            elif apply_null or val is not None:
                current_user_metadata[key] = meta
        elif apply_null or not mi.is_null(key):
            current_user_metadata[key] = meta

    set_user_metadata(root, prefixes, refines, current_user_metadata)
    raster_cover = read_raster_cover(root, prefixes, refines)
    if not raster_cover and cover_data and add_missing_cover:
        if cover_prefix and not cover_prefix.endswith("/"):
            cover_prefix += "/"
        name = cover_prefix + "cover.jpg"
        i = create_manifest_item(root, name, "cover")
        if i is not None:
            ensure_is_only_raster_cover(root, prefixes, refines, name)
            raster_cover = name

    pretty_print_opf(root)
    return raster_cover


def set_metadata(
    stream: _typing.Any,
    mi: _typing.Any,
    cover_prefix: str = "",
    cover_data: _typing.Any = None,
    apply_null: bool = False,
    update_timestamp: bool = False,
    force_identifiers: bool = False,
    add_missing_cover: bool = True,
) -> _typing.Any:
    """
    Update document metadata while preserving unrelated package state.

    Example:
        Exercise set metadata through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf3_smoke.py


    :param stream: Input or output stream wrapped by the terminal or compatibility
        layer.
    :param mi: Metadata object exposed to the template function.
    :param cover_prefix: Value supplied for cover prefix under the utility contract.
    :param cover_data: Value supplied for cover data under the utility contract.
    :param apply_null: Value supplied for apply null under the utility contract.
    :param update_timestamp: Value supplied for update timestamp under the utility
        contract.
    :param force_identifiers: Value supplied for force identifiers under the utility
        contract.
    :param add_missing_cover: Value supplied for add missing cover under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    root = parse_opf(stream)
    return apply_metadata(
        root,
        mi,
        cover_prefix=cover_prefix,
        cover_data=cover_data,
        apply_null=apply_null,
        update_timestamp=update_timestamp,
        force_identifiers=force_identifiers,
    )


# }}}


if __name__ == "__main__":
    import sys

    print(get_metadata(open(sys.argv[-1], "rb")))
