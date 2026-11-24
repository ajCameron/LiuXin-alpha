"""
Choose how a document's own Subject field is read and written: as subjects, or as calibre does.

LiuXin keeps subject headings apart from tags (see ``dev-docs/metadata/subjects.md``).
Several formats have a field named Subject that calibre files elsewhere:

- HTML's ``<meta name="subject">``, which calibre reads as tags;
- RTF's ``\\subject``, which calibre reads and writes as the comments;
- PDF's Info ``/Subject``, which calibre reads as the first tag, and which the XMP
  specification maps to the description (LiuXin's ``newest`` merge policy);
- ODT's ``dc:subject``, which calibre does not read.

The ``read_subject_fields_as`` preference chooses:

``subjects`` (the default)
    These fields hold subject headings, several joined by semicolons. They are
    read into the record's ``subjects``, and the writers fill them from it. RTF
    then keeps the comments in ``\\doccomm``, and PDF keeps the description in the
    XMP packet's ``dc:description``.
``calibre``
    Each is read and written as calibre does, and the writers keep the subjects
    among the tags.

Example:
    >>> subject_field_reading("calibre"), subject_field_reading("sideways")
    ('calibre', 'subjects')
    >>> split_subjects("History, Modern -- 20th century; Sea stories;")
    ['History, Modern -- 20th century', 'Sea stories']
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

__all__ = [
    "DEFAULT_SUBJECT_FIELD_READING",
    "SUBJECT_FIELD_READINGS",
    "SUBJECT_FIELDS_PREFERENCE",
    "join_subjects",
    "reads_subjects",
    "set_subjects",
    "split_subjects",
    "subjects_of",
    "subject_field_reading",
]

#: The preference that chooses the reading.
SUBJECT_FIELDS_PREFERENCE = "read_subject_fields_as"
#: The readings offered, with what each means.
SUBJECT_FIELD_READINGS: Mapping[str, str] = {
    "subjects": "a document's Subject field holds subject headings, read and written as subjects",
    "calibre": "a document's Subject field is read and written as calibre does",
}
DEFAULT_SUBJECT_FIELD_READING = "subjects"

_SPACE = re.compile(r"\s+")


def subject_field_reading(explicit: str | None = None) -> str:
    """
    Choose a reading: the one asked for, else the preference, else ``subjects``.

    Example:
        >>> subject_field_reading("subjects")
        'subjects'


    :param explicit: A reading named by the caller.
    :return: A key of :data:`SUBJECT_FIELD_READINGS`.
    """
    chosen: Any = explicit
    if chosen is None:
        try:
            from LiuXin_alpha.configuration import preferences

            chosen = preferences.get(SUBJECT_FIELDS_PREFERENCE)
        except Exception:
            chosen = None
    return chosen if isinstance(chosen, str) and chosen in SUBJECT_FIELD_READINGS else DEFAULT_SUBJECT_FIELD_READING


def reads_subjects(explicit: str | None = None) -> bool:
    """
    Tell whether a document's Subject field holds subjects under the chosen reading.

    Example:
        >>> reads_subjects("subjects"), reads_subjects("calibre")
        (True, False)


    :param explicit: A reading named by the caller.
    :return: True for ``subjects``.
    """
    return subject_field_reading(explicit) == "subjects"


def split_subjects(raw: str | None) -> list[str]:
    """
    Split a Subject field into subject headings, at semicolons only.

    A heading may hold commas (``History, Modern -- 20th century``), so only
    semicolons separate them. Space is collapsed; empty and repeated headings
    (case ignored) are dropped.

    Example:
        >>> split_subjects(" Sea  stories ; sea stories;;Whales ")
        ['Sea stories', 'Whales']


    :param raw: The field's text.
    :return: The headings.
    """
    headings: list[str] = []
    seen: set[str] = set()
    for part in (raw or "").split(";"):
        heading = _SPACE.sub(" ", part).strip()
        if heading and heading.lower() not in seen:
            seen.add(heading.lower())
            headings.append(heading)
    return headings


def join_subjects(subjects: Iterable[Any] | None) -> str:
    """
    Join subject headings for a single Subject field, as :func:`split_subjects` splits them.

    Example:
        >>> join_subjects(["History, Modern -- 20th century", " ", "Sea stories"])
        'History, Modern -- 20th century; Sea stories'


    :param subjects: The headings.
    :return: The field's text; empty when there are none.
    """
    return "; ".join(text for text in (_SPACE.sub(" ", str(s)).strip() for s in subjects or []) if text)


def set_subjects(mi: Any, subjects: Iterable[str]) -> None:
    """
    Put subject headings into a record, whichever kind: calibre's form keeps them in ``subjects``, LiuXin's in ``subject``.

    Example:
        >>> from LiuXin_alpha.metadata.book.base import calibreMetadata
        >>> book = calibreMetadata("Example")
        >>> set_subjects(book, ["Sea stories"])
        >>> book.subjects
        ['Sea stories']


    :param mi: The record.
    :param subjects: The headings; nothing is set when there are none.
    :return: None.
    """
    headings = [str(s) for s in subjects if str(s).strip()]
    if not headings:
        return
    if callable(getattr(mi, "to_calibre", None)):
        mi.subject = headings
    else:
        mi.subjects = headings


def subjects_of(mi: Any) -> list[str]:
    """
    Take the subject headings from a record, whichever kind: ``subjects`` or LiuXin's ``subject``.

    Example:
        >>> from LiuXin_alpha.metadata.book.base import calibreMetadata
        >>> book = calibreMetadata("Example")
        >>> book.subjects = ["Sea stories"]
        >>> subjects_of(book), subjects_of(object())
        (['Sea stories'], [])


    :param mi: The record.
    :return: The headings, in order.
    """
    if callable(getattr(mi, "to_calibre", None)):
        raw = getattr(mi, "subject", None)
    else:
        raw = getattr(mi, "subjects", None)
    return [str(s) for s in (raw or []) if str(s).strip()]
