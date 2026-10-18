"""Small operations for parsing, normalizing, and rewriting OPF packages.

These helpers belong to the OPF format layer.  Keeping them here prevents the
general metadata utility module from depending on OEB namespaces and polishing
implementations.

Example:
    Exercise the helpers with their focused regression module::

        python -m pytest -q tests/file_formats/opf/test_opf_utils.py
"""

from __future__ import annotations

import os
from collections import namedtuple
from collections.abc import Collection, Iterable
from typing import Any

from LiuXin_alpha.file_formats.oeb.base import OPF
from LiuXin_alpha.utils.libraries.calibre_chardet import xml_to_unicode
from LiuXin_alpha.utils.libraries.liuxin_etree import LXML_AVAILABLE, etree
from LiuXin_alpha.utils.localization import canonicalize_lang, lang_as_iso639_1
from LiuXin_alpha.utils.mine_types import guess_type

if LXML_AVAILABLE:
    PARSER = etree.XMLParser(recover=True, no_network=True)
else:
    PARSER = etree.XMLParser()

OPFVersion = namedtuple("OPFVersion", "major minor patch")


def parse_opf_version(raw: str) -> OPFVersion:
    """Parse a dotted OPF version, defaulting invalid values to version 2.0.0."""
    parts = (raw or "").split(".")
    try:
        major = int(parts[0])
    except Exception:
        return OPFVersion(2, 0, 0)
    try:
        version = list(map(int, raw.split(".")))
    except Exception:
        version = [major, 0, 0]
    while len(version) < 3:
        version.append(0)
    return OPFVersion(*version[:3])


def parse_opf(stream_or_path: Any) -> Any:
    """Parse an OPF root from a stream, existing path, or raw XML payload."""
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
    raw, _encoding = xml_to_unicode(
        raw,
        strip_encoding_pats=True,
        resolve_entities=True,
        assume_utf8=True,
    )
    raw = raw[raw.find("<") :]
    root = etree.fromstring(raw, PARSER)
    if root is None:
        raise ValueError("Not an OPF file")
    return root


def normalize_languages(
    opf_languages: Iterable[str | None],
    metadata_languages: Iterable[str | None],
) -> list[str]:
    """Normalize metadata languages while retaining matching OPF regions."""
    from LiuXin_alpha.utils.libraries.iso639.iso639_tools import (
        lang_as_iso639_1 as fallback_lang_as_iso639_1,
    )

    LocaleCode = namedtuple("LocaleCode", "langcode countrycode")

    def parse(value: str | None) -> Any | None:
        raw = (value or "").strip()
        if not raw:
            return None
        raw = raw.replace("_", "-")
        primary, _, tail = raw.partition("-")
        primary = primary.strip()
        if not primary:
            return None
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
            ).upper()
        return LocaleCode(langcode=langcode, countrycode=(country or None))

    def iso2(language_code: str) -> str | None:
        result = lang_as_iso639_1(language_code)
        if not result or len(result) != 2:
            result = fallback_lang_as_iso639_1(language_code) or result
        return result

    parsed_opf_languages = list(filter(None, map(parse, opf_languages)))
    country_by_language = {}
    for language in parsed_opf_languages:
        country_by_language[language.langcode] = language.countrycode
        iso_language = iso2(language.langcode)
        if iso_language:
            country_by_language.setdefault(iso_language, language.countrycode)

    def normalize(language: Any) -> str:
        language_code = language.langcode
        country_code = language.countrycode or country_by_language.get(language_code)
        iso_language = iso2(language_code)
        if not country_code and iso_language:
            country_code = country_by_language.get(iso_language)
        language_code = iso_language or language_code
        if country_code:
            language_code += "-" + country_code
        return language_code

    return list(map(normalize, filter(None, map(parse, metadata_languages))))


def ensure_unique(template: str, existing: Collection[str]) -> str:
    """Return the template or the first unused numbered variant of it."""
    base, extension = template.rpartition(".")[::2]
    if base and extension:
        extension = "." + extension
    else:
        base, extension = template, ""
    candidate = template
    counter = 0
    while candidate in existing:
        counter += 1
        candidate = f"{base}-{counter}{extension}"
    return candidate


def create_manifest_item(
    root: Any,
    href_template: str,
    id_template: str,
    media_type: str | None = None,
) -> Any | None:
    """Append and return a uniquely named OPF manifest item when a manifest exists."""
    if hasattr(root, "xpath"):
        all_ids = frozenset(root.xpath("//*/@id"))
        all_hrefs = frozenset(root.xpath("//*/@href"))
    else:
        all_ids = frozenset(element.get("id") for element in root.iter() if element.get("id"))
        all_hrefs = frozenset(element.get("href") for element in root.iter() if element.get("href"))
    href = ensure_unique(href_template, all_hrefs)
    item_id = ensure_unique(id_template, all_ids)
    manifest = root.find(OPF("manifest"))
    if manifest is None:
        return None
    item = manifest.makeelement(OPF("item"), {})
    item.set("href", href)
    item.set("id", item_id)
    resolved_media_type = media_type
    if not resolved_media_type:
        guessed = guess_type(href_template)
        resolved_media_type = guessed[0] if isinstance(guessed, tuple) else guessed
        resolved_media_type = resolved_media_type or "application/octet-stream"
    item.set("media-type", resolved_media_type)
    manifest.append(item)
    return item


def pretty_print_opf(root: Any) -> None:
    """Reorder OPF sections and apply XML indentation in place."""
    from LiuXin_alpha.file_formats.oeb.polish.pretty import pretty_opf, pretty_xml_tree

    pretty_opf(root)
    pretty_xml_tree(root)
