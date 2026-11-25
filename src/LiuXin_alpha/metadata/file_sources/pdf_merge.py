"""
Combine a PDF's Info dictionary and XMP packet into one set of fields, by a chosen policy.

A PDF may say the same things twice: in its Info dictionary and in its XMP packet.
Writers that keep both in step write the same values, but editors that update one
leave the other stale. Three policies are offered; the ``pdf_metadata_merge``
preference chooses one, and the PDF reader plugins name theirs:

``newest`` (the default)
    Fields correspond as the XMP specification maps them: Title to ``dc:title``,
    Author to ``dc:creator``, Keywords to ``pdf:Keywords`` and ``dc:subject``,
    Creator to ``xmp:CreatorTool``, Producer to ``pdf:Producer``, and the dates to
    ``xmp:CreateDate`` and ``xmp:ModifyDate``. (The specification maps Subject to
    ``dc:description``; see the Subject below.)
    The source changed last wins every field it has, as the PDF specification
    recommends: the Info dictionary when its ``ModDate`` is later than the packet's
    ``xmp:MetadataDate`` or ``xmp:ModifyDate``, otherwise the packet, which PDF 2.0
    makes the main record. A field the winner lacks comes from the other.
    Identifiers come from both. XMP values that are escaped PDF strings (Nitro PDF
    writes ``\\376\\377…``) are ignored, not taken as text.

``calibre``
    calibre's rule: the packet's title, authors and tags win unless the Info
    dictionary is newer; every other field the packet has wins regardless. A packet
    whose title is an escaped PDF string is ignored whole, as calibre ignores it.

``union``
    LiuXin's earlier rule: the packet's title, authors, publisher and description
    win, and the tags are the Info dictionary's tags followed by the packet's
    subjects.

Dates are given in ISO 8601 when they can be read, whichever source they came from.

The Info dictionary's Subject is read by the ``read_subject_fields_as`` preference
(see ``subject_fields``), whichever the policy. At its default, ``subjects``, it
holds subject headings. Under ``calibre`` it is the first of the Info dictionary's
tags, as calibre reads it, and the description comes from the packet alone.

Example:
    >>> info = info_fields({"Title": "Old", "Subject": "Sea", "Keywords": "a", "ModDate": "D:20200101000000Z"})
    >>> xmp = xmp_fields({"dc": {"title": {"x-default": "New"}}, "xmp": {"MetadataDate": "2019-01-01T00:00:00Z"}})
    >>> combined = combine(info, xmp, "newest")
    >>> combined.title, combined.tags
    ('Old', ('Sea', 'a'))
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

from LiuXin_alpha.metadata.constants import (
    INFO_DICT_VALUE_DROP_SET,
    PRODUCER_DROP_REGEX_SET,
)
from LiuXin_alpha.metadata.file_sources.subject_fields import split_subjects
from LiuXin_alpha.metadata.utils import check_doi, check_isbn, string_to_authors

__all__ = [
    "DEFAULT_MERGE_POLICY",
    "MERGE_POLICIES",
    "MERGE_PREFERENCE",
    "PdfFields",
    "combine",
    "info_fields",
    "merge_policy",
    "parse_pdf_date",
    "parse_xmp_date",
    "xmp_fields",
]

#: The policies, and what each does in a few words.
MERGE_POLICIES: Mapping[str, str] = {
    "newest": "the source changed last wins each field, mapped as the XMP specification maps them",
    "calibre": "calibre's rule: the packet wins unless the Info dictionary is newer",
    "union": "LiuXin's earlier rule: the packet wins single fields, and the tags of both are kept",
}
DEFAULT_MERGE_POLICY = "newest"
#: The preference that chooses the policy.
MERGE_PREFERENCE = "pdf_metadata_merge"

_SPACE = re.compile(r"\s+")
_KEYWORD_SEPARATOR = re.compile(r"[,;]")
_PDF_DATE = re.compile(
    r"\s*(?:D:)?(\d{4})(\d{2})?(\d{2})?(\d{2})?(\d{2})?(\d{2})?\s*(?:([Zz])|([+-])(\d{2})(?:'?(\d{2}))?'?)?"
)
_XMP_DATE = re.compile(
    r"\s*(\d{4})(?:-(\d{2})(?:-(\d{2})(?:T(\d{2}):(\d{2})(?::(\d{2})(?:\.\d{1,9})?)?)?)?)?\s*(?:([Zz])|([+-])(\d{2}):?(\d{2}))?"
)
_ESCAPED_PDF_STRING = r"\376\377"


@dataclass(frozen=True)
class PdfFields:
    """
    The fields one source of a PDF's metadata gives, before or after combining.

    Example:
        >>> PdfFields(title="T").authors
        ()
    """

    title: str | None = None
    authors: tuple[str, ...] = ()
    comments: str | None = None
    tags: tuple[str, ...] = ()
    #: Subject headings: the Info dictionary's Subject, when the
    #: ``read_subject_fields_as`` preference reads it as subjects.
    subjects: tuple[str, ...] = ()
    publisher: str | None = None
    producers: tuple[str, ...] = ()
    created: str | None = None
    modified: str | None = None
    uuid: str | None = None
    languages: tuple[str, ...] = ()
    identifiers: dict[str, str] = field(default_factory=dict)
    #: When this source's metadata last changed, for choosing between sources.
    changed: datetime | None = None
    #: The packet's title is an escaped PDF string, so calibre ignores the packet.
    corrupt: bool = False


def merge_policy(explicit: str | None = None) -> str:
    """
    Choose a policy: the one asked for, else the ``pdf_metadata_merge`` preference, else ``newest``.

    Example:
        >>> merge_policy("calibre"), merge_policy("no such policy")
        ('calibre', 'newest')


    :param explicit: A policy named by the caller, such as a reader plugin.
    :return: A key of :data:`MERGE_POLICIES`.
    """
    chosen: Any = explicit
    if chosen is None:
        try:
            from LiuXin_alpha.configuration import preferences

            chosen = preferences.get(MERGE_PREFERENCE)
        except Exception:
            chosen = None
    return (
        chosen
        if isinstance(chosen, str) and chosen in MERGE_POLICIES
        else DEFAULT_MERGE_POLICY
    )


def _matches_any(patterns: Iterable[str], text: str) -> bool:
    """
    Tell whether any pattern matches the start of the text, as LiuXin's drop lists are applied.

    Example:
        >>> _matches_any({r".*LaTeX.*"}, "pdfTeX with LaTeX"), _matches_any({r"x"}, "ax")
        (True, False)


    :param patterns: Regular expressions.
    :param text: The text.
    :return: Whether one matches.
    """
    return any(re.match(pattern, text) for pattern in patterns)


def _text(value: Any) -> str | None:
    """
    Normalise a value to one line of text, or ``None`` when it is empty or a placeholder.

    Example:
        >>> _text("  Two\\n lines "), _text(""), _text(None)
        ('Two lines', None, None)


    :param value: The value.
    :return: The text.
    """
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        value = ", ".join(str(item) for item in value if item)
    text = _SPACE.sub(" ", str(value)).strip()
    if not text or _matches_any(INFO_DICT_VALUE_DROP_SET, text):
        return None
    return text


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    """
    Drop empty values and repeats, ignoring case, keeping the first spelling and the order.

    Example:
        >>> _unique(["Alpha", "beta", "ALPHA", "", "Beta "])
        ('Alpha', 'beta')


    :param values: The values.
    :return: The distinct values.
    """
    seen: set[str] = set()
    out = []
    for value in values:
        text = _text(value)
        if text and text.casefold() not in seen:
            seen.add(text.casefold())
            out.append(text)
    return tuple(out)


def _keywords(value: Any) -> tuple[list[str], dict[str, str]]:
    """
    Split keywords on commas and semicolons into tags, taking ISBNs and DOIs out as identifiers.

    Example:
        >>> _keywords("alpha; beta, 9780306406157")
        (['alpha', 'beta'], {'isbn': '9780306406157'})


    :param value: Keywords as text, or a list.
    :return: The tags and the identifiers.
    """
    items = value if isinstance(value, (list, tuple)) else [value] if value else []
    tags: list[str] = []
    identifiers: dict[str, str] = {}
    for item in items:
        for part in _KEYWORD_SEPARATOR.split(str(item)):
            text = _text(part)
            if not text:
                continue
            isbn = check_isbn(text)
            if isbn:
                identifiers.setdefault("isbn", isbn)
                continue
            doi = check_doi(text)
            if doi:
                identifiers.setdefault("doi", doi)
                continue
            tags.append(text)
    return tags, identifiers


def _authors(values: Iterable[Any]) -> tuple[str, ...]:
    """
    Split author fields into names.

    Example:
        >>> _authors(["Ann Author & Bob Writer"])
        ('Ann Author', 'Bob Writer')


    :param values: Author fields.
    :return: The names.
    """
    names: list[str] = []
    for value in values:
        text = _text(value)
        if text:
            names.extend(string_to_authors(text))
    return _unique(names)


def _producers(values: Iterable[Any]) -> tuple[str, ...]:
    """
    Keep producer names that are not boilerplate LiuXin drops.

    Example:
        >>> _producers(["Corpus Creator", None])
        ('Corpus Creator',)


    :param values: Creator and producer fields.
    :return: The names kept.
    """
    return _unique(
        text
        for text in (_text(value) for value in values)
        if text and not _matches_any(PRODUCER_DROP_REGEX_SET, text)
    )


def _zone(
    utc: str | None, sign: str | None, hours: str | None, minutes: str | None
) -> timezone:
    """
    Build a time zone from a date's zone parts; a date without one is taken as UTC.

    Example:
        >>> _zone(None, "-", "05", "30")
        datetime.timezone(datetime.timedelta(days=-1, seconds=66600))


    :param utc: ``Z`` for UTC.
    :param sign: ``+`` or ``-``.
    :param hours: The offset's hours.
    :param minutes: The offset's minutes.
    :return: The zone.
    """
    if utc or not sign or not hours:
        return UTC
    offset = timedelta(hours=int(hours), minutes=int(minutes or 0))
    return timezone(-offset if sign == "-" else offset)


def _stamp(match: re.Match[str]) -> datetime | None:
    """
    Build a time from a date pattern's groups: year, month, day, hour, minute, second, then zone.

    Example:
        >>> _stamp(_PDF_DATE.match("D:20190230"))  # February has no 30th


    :param match: A match of ``_PDF_DATE`` or ``_XMP_DATE``.
    :return: The time, or ``None`` when the parts make no date.
    """
    year, month, day, hour, minute, second, utc, sign, zone_hours, zone_minutes = (
        match.groups()
    )
    try:
        return datetime(
            int(year),
            int(month or 1),
            int(day or 1),
            int(hour or 0),
            int(minute or 0),
            int(second or 0),
            tzinfo=_zone(utc, sign, zone_hours, zone_minutes),
        )
    except ValueError:
        return None


def parse_pdf_date(raw: Any) -> datetime | None:
    """
    Read a PDF date (``D:YYYYMMDDHHmmSSOHH'mm'``), with missing parts at their first value.

    Example:
        >>> parse_pdf_date("D:20190601120000+02'00'").isoformat()
        '2019-06-01T12:00:00+02:00'
        >>> parse_pdf_date("D:2019").isoformat(), parse_pdf_date("garbage")
        ('2019-01-01T00:00:00+00:00', None)


    :param raw: The date.
    :return: The time, or ``None`` when it is not a date.
    """
    match = _PDF_DATE.match(raw) if isinstance(raw, str) else None
    if match is None:
        return None
    return _stamp(match)


def parse_xmp_date(raw: Any) -> datetime | None:
    """
    Read an XMP date (ISO 8601, as precise as the writer chose).

    Example:
        >>> parse_xmp_date("2021-03-04T05:06:07-05:00").isoformat()
        '2021-03-04T05:06:07-05:00'
        >>> parse_xmp_date("2021").isoformat(), parse_xmp_date("soon")
        ('2021-01-01T00:00:00+00:00', None)


    :param raw: The date.
    :return: The time, or ``None`` when it is not a date.
    """
    match = _XMP_DATE.match(raw) if isinstance(raw, str) else None
    if match is None:
        return None
    return _stamp(match)


def _iso(raw: str | None, parse: Any) -> str | None:
    """
    Give a date in ISO 8601 when it can be read, else as written.

    Example:
        >>> _iso("D:20190601", parse_pdf_date), _iso("Spring 2019", parse_pdf_date)
        ('2019-06-01T00:00:00+00:00', 'Spring 2019')


    :param raw: The date as written, or ``None``.
    :param parse: :func:`parse_pdf_date` or :func:`parse_xmp_date`.
    :return: The date.
    """
    stamp = parse(raw) if raw else None
    return stamp.isoformat() if isinstance(stamp, datetime) else raw


def info_fields(
    info: Mapping[str, Any], *, subject_is_tag: bool = True, subject_as_subjects: bool = False
) -> PdfFields:
    """
    Read the fields of an Info dictionary, already decoded to text.

    Example:
        >>> fields = info_fields({"Author": "Ann & Bob", "Keywords": "a, b", "Subject": "S"})
        >>> fields.authors, fields.tags, fields.comments
        (('Ann', 'Bob'), ('S', 'a', 'b'), None)
        >>> info_fields({"Keywords": "a, b", "Subject": "S"}, subject_is_tag=False).comments
        'S'
        >>> as_subjects = info_fields({"Subject": "History, Modern; Sea"}, subject_as_subjects=True)
        >>> as_subjects.subjects, as_subjects.comments
        (('History, Modern', 'Sea'), None)


    :param info: The Info dictionary's entries, by name.
    :param subject_is_tag: Take the Subject as the first tag, as calibre does (the
        reader always does); False takes it as the description the XMP specification
        maps it to.
    :param subject_as_subjects: Take the Subject as subject headings, split at semicolons,
        whatever ``subject_is_tag`` says.
    :return: The fields.
    """
    lowered = {str(key).lower(): value for key, value in info.items()}
    tags, identifiers = _keywords(lowered.get("keywords"))
    subject = _text(lowered.get("subject"))
    subjects = tuple(split_subjects(subject)) if subject_as_subjects else ()
    if subject_as_subjects:
        subject = None
    elif subject_is_tag and subject:
        tags.insert(0, subject)
    publisher = _text(lowered.get("publisher") or lowered.get("ebx_publisher"))
    return PdfFields(
        title=_text(lowered.get("title")),
        authors=_authors([lowered.get("author")]),
        comments=None if subject_is_tag else subject,
        tags=_unique(tags),
        subjects=subjects,
        publisher=publisher.lstrip("/") if publisher else None,
        producers=_producers([lowered.get("creator"), lowered.get("producer")]),
        created=_iso(_text(lowered.get("creationdate")), parse_pdf_date),
        modified=_iso(_text(lowered.get("moddate")), parse_pdf_date),
        identifiers=identifiers,
        changed=parse_pdf_date(lowered.get("moddate")),
    )


def _xmp_text(value: Any) -> str | None:
    """
    Take an XMP value as text: the default of a language alternative, the first of a list.

    Example:
        >>> _xmp_text({"fr": "Titre", "x-default": "Title"}), _xmp_text(["a", "b"]), _xmp_text(r"\\376\\377x")
        ('Title', 'a', None)


    :param value: A simple value, a list, or a language alternative.
    :return: The text, or ``None``; an escaped PDF string counts as none.
    """
    if isinstance(value, dict):
        value = value.get("x-default") or next(
            (item for item in value.values() if item), None
        )
    elif isinstance(value, list):
        value = next((item for item in value if item), None)
    text = _text(value)
    if text is None or text.startswith(_ESCAPED_PDF_STRING):
        return None
    return text


def _xmp_list(value: Any) -> list[str]:
    """
    Take an XMP value as a list of texts, dropping escaped PDF strings.

    Example:
        >>> _xmp_list(["a", None, "b"]), _xmp_list("one")
        (['a', 'b'], ['one'])


    :param value: A list, a language alternative, or a simple value.
    :return: The texts.
    """
    items = (
        value
        if isinstance(value, list)
        else list(value.values())
        if isinstance(value, dict)
        else [value]
    )
    return [
        text
        for text in (_text(item) for item in items)
        if text and not text.startswith(_ESCAPED_PDF_STRING)
    ]


def xmp_fields(xmp: Mapping[str, Any]) -> PdfFields:
    """
    Read the fields of an XMP packet, as ``xmp_to_dict`` gives it (namespace, then property).

    Example:
        >>> fields = xmp_fields({"dc": {"creator": ["Ann", "Bob"], "subject": ["x"]}, "prism": {"doi": "10.1000/1"}})
        >>> fields.authors, fields.tags, fields.identifiers
        (('Ann', 'Bob'), ('x',), {'doi': '10.1000/1'})


    :param xmp: The packet's properties.
    :return: The fields.
    """

    def space(name: str) -> Mapping[str, Any]:
        """
        Give one namespace's properties.

        Example:
            >>> space("dc")  # doctest: +SKIP
            {}


        :param name: The namespace's prefix.
        :return: Its properties, or nothing.
        """
        found = xmp.get(name)
        return found if isinstance(found, Mapping) else {}

    dc, basic, pdf, media = space("dc"), space("xmp"), space("pdf"), space("xapmm")
    tags, identifiers = _keywords(_xmp_list(dc.get("subject")))
    if not tags:
        tags, keyword_ids = _keywords(_xmp_text(pdf.get("Keywords")))
        identifiers.update(keyword_ids)
    for scheme, check in (("doi", check_doi), ("isbn", check_isbn)):
        named = [space(prefix).get(scheme) for prefix in ("prism", "pdfx")]
        for value in (*named, *_xmp_list(dc.get("identifier"))):
            text = _xmp_text(value) if value else None
            found = check(text) if text else None
            if found:
                identifiers.setdefault(scheme, found)
                break
    document = _xmp_text(media.get("DocumentID"))
    if document and document.lower().startswith("uuid:"):
        document = document.split(":", 1)[1]
    stamps = [
        parse_xmp_date(_xmp_text(basic.get(name)))
        for name in ("MetadataDate", "ModifyDate")
    ]
    known = [stamp for stamp in stamps if stamp is not None]
    title = dc.get("title")
    raw_title = _text(title.get("x-default") if isinstance(title, dict) else title)
    return PdfFields(
        title=_xmp_text(title),
        authors=_authors(_xmp_list(dc.get("creator"))),
        comments=_xmp_text(dc.get("description")),
        tags=_unique(tags),
        publisher=_xmp_text(dc.get("publisher")),
        producers=_producers(
            [_xmp_text(basic.get("CreatorTool")), _xmp_text(pdf.get("Producer"))]
        ),
        created=_iso(_xmp_text(basic.get("CreateDate")), parse_xmp_date),
        modified=_iso(_xmp_text(basic.get("ModifyDate")), parse_xmp_date),
        uuid=document,
        languages=_unique(_xmp_list(dc.get("language"))),
        identifiers=identifiers,
        changed=max(known) if known else None,
        corrupt=bool(raw_title and raw_title.startswith(_ESCAPED_PDF_STRING)),
    )


def _info_is_newer(info: PdfFields, xmp: PdfFields) -> bool:
    """
    Tell whether the Info dictionary changed after the packet, which then gives way.

    Example:
        >>> early, late = parse_pdf_date("D:2020"), parse_pdf_date("D:2021")
        >>> _info_is_newer(PdfFields(changed=late), PdfFields(changed=early)), _info_is_newer(PdfFields(changed=late), PdfFields())
        (True, False)


    :param info: The Info dictionary's fields.
    :param xmp: The packet's fields.
    :return: Whether both dates are known and the Info dictionary's is later.
    """
    return (
        info.changed is not None
        and xmp.changed is not None
        and info.changed > xmp.changed
    )


def _first(*values: Any) -> Any:
    """
    Give the first value that is not empty.

    Example:
        >>> _first(None, (), "x", "y")
        'x'


    :param values: The candidates, best first.
    :return: The first non-empty one, or the last.
    """
    for value in values:
        if value:
            return value
    return values[-1]


def combine(info: PdfFields, xmp: PdfFields | None, policy: str) -> PdfFields:
    """
    Combine the two sources by a policy (``union`` is applied by the PDF reader itself).

    Example:
        >>> old = info_fields({"Title": "Info", "Keywords": "i", "ModDate": "D:2022"})
        >>> new = xmp_fields({"dc": {"title": "XMP", "subject": ["x"]}, "xmp": {"MetadataDate": "2021"}})
        >>> combine(old, new, "newest").title, combine(old, new, "calibre").tags
        ('Info', ('i',))


    :param info: The Info dictionary's fields.
    :param xmp: The packet's fields, or ``None`` when there is no packet.
    :param policy: ``newest`` or ``calibre``.
    :return: The combined fields.
    :raises ValueError: For any other policy.
    """
    if policy not in ("newest", "calibre"):
        raise ValueError(
            f"no PDF metadata merge policy named {policy!r} is combined here"
        )
    if xmp is None:
        return info
    preferred, other = (info, xmp) if _info_is_newer(info, xmp) else (xmp, info)
    if policy == "newest":
        return PdfFields(
            title=_first(preferred.title, other.title),
            authors=_first(preferred.authors, other.authors),
            comments=_first(preferred.comments, other.comments),
            tags=_first(preferred.tags, other.tags),
            publisher=_first(preferred.publisher, other.publisher),
            producers=_first(preferred.producers, other.producers),
            created=_first(preferred.created, other.created),
            modified=_first(preferred.modified, other.modified),
            uuid=_first(preferred.uuid, other.uuid),
            languages=_first(preferred.languages, other.languages),
            identifiers={**other.identifiers, **preferred.identifiers},
            subjects=info.subjects,
            changed=preferred.changed,
        )
    # calibre's consolidate_metadata: the packet replaces every field it has, then the
    # title, authors and tags go back to the Info dictionary's if that is newer.
    if xmp.corrupt:
        return info
    combined = PdfFields(
        title=_first(xmp.title, info.title),
        authors=_first(xmp.authors, info.authors),
        comments=_first(xmp.comments, info.comments),
        tags=_first(xmp.tags, info.tags),
        publisher=_first(xmp.publisher, info.publisher),
        producers=_first(xmp.producers, info.producers),
        created=_first(xmp.created, info.created),
        modified=_first(xmp.modified, info.modified),
        uuid=_first(xmp.uuid, info.uuid),
        languages=_first(xmp.languages, info.languages),
        identifiers={**info.identifiers, **xmp.identifiers},
        subjects=info.subjects,
        changed=preferred.changed,
    )
    if preferred is info:
        combined = replace(
            combined, title=info.title, authors=info.authors, tags=info.tags
        )
    return combined
