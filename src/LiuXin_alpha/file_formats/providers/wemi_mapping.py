"""
Place calibre metadata fields at the WEMI levels they describe.

calibre keeps every field on one book record. LiuXin keeps each value at the
Work, Expression, Manifestation or Item it is about. This module holds the one
mapping between the two, as the briefing's "Readings and claims" decided: a
calibre reading is mapped when its values are accepted, and the LiuXin readers
use the same mapping to produce WEMI readings.

**The mapping is provisional.** It follows FRBR's usual split: what a work is
called and who created it at the Work; language, description and contributors
to a particular text (translators, editors, illustrators) at the Expression;
publisher, date, identifiers and producers at the Manifestation. Fields the
mapping does not place are kept, not dropped, under ``unplaced`` at the
Manifestation, so a reading never loses what its source said.

The module imports only the standard library and the metadata value types.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

from LiuXin_alpha.file_formats.providers.metadata import (
    JSONValue,
    WEMIFields,
    WEMILevel,
)

ROLE_LEVELS: Mapping[str, WEMILevel] = {
    "authors": "work",
    "composers": "work",
    "editors": "expression",
    "translators": "expression",
    "illustrators": "expression",
    "artists": "expression",
    "colorists": "expression",
    "directors": "expression",
    "producers": "manifestation",
    "book_producer": "manifestation",
    "cover_artists": "manifestation",
}
"""Where each creator role belongs. A role not listed goes to the Expression."""

FIELD_LEVELS: Mapping[str, tuple[WEMILevel, str]] = {
    "title_sort": ("work", "title_sort"),
    "author_sort": ("work", "author_sort"),
    "tags": ("work", "subjects"),
    "languages": ("expression", "languages"),
    "comments": ("expression", "description"),
    "pubdate": ("manifestation", "publication_date"),
    "rights": ("manifestation", "rights"),
}
"""Fields moved as they are: calibre's name, to the level and name in WEMI.

``title``, ``series``, ``publisher``, ``identifiers`` and the creator fields
are reshaped as well as placed, so they are handled separately.
"""

CONSUMED_FIELDS = frozenset(
    {
        "title",
        "series",
        "series_index",
        "publisher",
        "identifiers",
        "authors",
        "author_sort_map",
        "author_link_map",
        "book_producer",
        *FIELD_LEVELS,
    }
)
"""Every calibre field the mapping places; the rest are kept as unplaced."""


def _texts(value: object) -> tuple[str, ...]:
    """
    Read a value as non-empty strings.

    Example:
        >>> _texts(("a", "", None, 3)), _texts("b"), _texts(None)
        (('a', '3'), ('b',), ())


    :param value: A string, a sequence, or nothing.
    :return: The non-empty strings it holds, in order.
    """
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,) if value else ()
    if isinstance(value, tuple | list):
        items = cast("Sequence[object]", value)
        return tuple(str(item) for item in items if item not in (None, ""))
    return (str(value),)


def _creators(
    values: Mapping[str, JSONValue], roles: Mapping[str, Sequence[str]] | None
) -> list[tuple[WEMILevel, dict[str, JSONValue]]]:
    """
    List the creators a reading names, each with its role and level.

    Names come from ``roles`` when the source gave creators by role, and
    otherwise from calibre's ``authors`` and ``book_producer`` fields. A sort
    name or link from calibre's author maps is attached where one is given.

    Example:
        >>> _creators({"authors": ("Ann",), "author_sort_map": {"Ann": "A, Ann"}}, None)
        [('work', {'name': 'Ann', 'role': 'authors', 'sort': 'A, Ann'})]


    :param values: calibre fields.
    :param roles: Creator names by role, or ``None``.
    :return: Each creator's level and record, in role then name order.
    """
    by_role: dict[str, tuple[str, ...]] = {}
    if roles is not None:
        for role, names in roles.items():
            by_role[role] = tuple(name for name in names if name)
    else:
        by_role["authors"] = _texts(values.get("authors"))
        by_role["book_producer"] = _texts(values.get("book_producer"))
    sorts = values.get("author_sort_map")
    links = values.get("author_link_map")
    found: list[tuple[WEMILevel, dict[str, JSONValue]]] = []
    for role, names in by_role.items():
        for name in names:
            record: dict[str, JSONValue] = {"name": name, "role": role}
            if role == "authors" and isinstance(sorts, Mapping) and sorts.get(name):
                record["sort"] = sorts[name]
            if isinstance(links, Mapping) and links.get(name):
                record["link"] = links[name]
            found.append((ROLE_LEVELS.get(role, "expression"), record))
    return found


def _identifiers(
    values: Mapping[str, JSONValue], extra: Mapping[str, Sequence[str]] | None
) -> dict[str, JSONValue]:
    """
    Merge calibre's identifiers with any the source gave as lists.

    Example:
        >>> _identifiers({"identifiers": {"isbn": "1"}}, {"isbn": ["1", "2"], "doi": []})
        {'isbn': ('1', '2')}


    :param values: calibre fields; ``identifiers`` maps scheme to one value.
    :param extra: More values by scheme, or ``None``.
    :return: Each scheme with at least one value, mapped to its values in
        order and without repeats.
    """
    merged: dict[str, list[str]] = {}
    given = values.get("identifiers")
    if isinstance(given, Mapping):
        for scheme, value in given.items():
            merged.setdefault(scheme, []).extend(_texts(value))
    for scheme, more in (extra or {}).items():
        merged.setdefault(scheme, []).extend(str(item) for item in more if item)
    return {
        scheme: tuple(dict.fromkeys(found)) for scheme, found in merged.items() if found
    }


def wemi_from_calibre(
    values: Mapping[str, JSONValue],
    *,
    creators: Mapping[str, Sequence[str]] | None = None,
    identifiers: Mapping[str, Sequence[str]] | None = None,
) -> WEMIFields:
    """
    Place calibre fields at their WEMI levels.

    Empty values are left out, as calibre's own serialiser leaves out empty
    fields. A source that knows more than calibre's fields can say may pass
    creators by role and identifiers as lists; these replace calibre's
    ``authors`` and ``book_producer`` and add to its ``identifiers``.

    Example:
        >>> wemi = wemi_from_calibre(
        ...     {"title": "A Book", "authors": ["Ann"], "languages": ["en"],
        ...      "publisher": "Pub Co", "identifiers": {"isbn": "978"},
        ...      "timestamp": "2026-11-17T00:00:00"},
        ...     creators={"authors": ["Ann"], "translators": ["Tom"]},
        ... )
        >>> wemi.levels["work"]["titles"]
        ('A Book',)
        >>> [dict(person) for person in wemi.levels["expression"]["creators"]]
        [{'name': 'Tom', 'role': 'translators'}]
        >>> dict(wemi.levels["manifestation"]["unplaced"])
        {'timestamp': '2026-11-17T00:00:00'}


    :param values: calibre fields, as calibre's serialiser writes them.
    :param creators: Creator names by role, if the source gave them.
    :param identifiers: Identifier values by scheme, if the source gave them
        as lists.
    :return: The same information, placed by level.
    """
    levels: dict[WEMILevel, dict[str, JSONValue]] = {
        "work": {},
        "expression": {},
        "manifestation": {},
    }
    titles = _texts(values.get("title"))
    if titles:
        levels["work"]["titles"] = titles
    for field, (level, name) in FIELD_LEVELS.items():
        value = values.get(field)
        if value not in (None, "", (), [], {}):
            levels[level][name] = value
    series = _texts(values.get("series"))
    if series:
        levels["work"]["series"] = (
            {"name": series[0], "index": values.get("series_index")},
        )
    publishers = _texts(values.get("publisher"))
    if publishers:
        levels["manifestation"]["publishers"] = publishers
    found_identifiers = _identifiers(values, identifiers)
    if found_identifiers:
        levels["manifestation"]["identifiers"] = found_identifiers
    people: dict[WEMILevel, list[JSONValue]] = {}
    for level, record in _creators(values, creators):
        people.setdefault(level, []).append(record)
    for level, records in people.items():
        levels.setdefault(level, {})["creators"] = tuple(records)
    unplaced = {
        field: value
        for field, value in values.items()
        if field not in CONSUMED_FIELDS and value not in (None, "", (), [], {})
    }
    if unplaced:
        levels["manifestation"]["unplaced"] = unplaced
    return WEMIFields({level: fields for level, fields in levels.items() if fields})
