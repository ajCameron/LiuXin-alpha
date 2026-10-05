"""
Define string enums shared by editable metadata containers and their APIs.

These labels describe titles, notes, relations, language/date/rating/resource
attachments, and identifier status. They do not enforce database constraints or
replace core WEMI/agent types. Construct enums from their exact stored string
values; invalid values raise ValueError.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/containers/test_relation_container_contracts.py
"""
from __future__ import annotations

from enum import StrEnum


class TitleKind(StrEnum):
    """
    Classify the role of a title string, such as main, subtitle, sort, or translated.

    Example:
        >>> str(TitleKind.MAIN)
        'main'
        >>> TitleKind('main') is TitleKind.MAIN
        True
    """

    MAIN = "main"
    SUBTITLE = "subtitle"
    ALTERNATIVE = "alternative"
    SHORT = "short"
    SORT = "sort"
    UNIFORM = "uniform"
    TRANSLATED = "translated"
    TRANSLITERATED = "transliterated"
    COVER = "cover"
    SPINE = "spine"
    RUNNING = "running"
    SUPPLIED = "supplied"


class NoteKind(StrEnum):
    """
    Classify the purpose of a long-form note, such as description, review, provenance, or internal.

    Example:
        >>> str(NoteKind.DESCRIPTION)
        'description'
        >>> NoteKind('description') is NoteKind.DESCRIPTION
        True
    """

    DESCRIPTION = "description"
    REVIEW = "review"
    ANNOTATION = "annotation"
    SUMMARY = "summary"
    TRANSCRIPTION = "transcription"
    PROVENANCE = "provenance"
    CONDITION = "condition"
    ACQUISITION = "acquisition"
    CONTENTS = "contents"
    CITATION = "citation"
    INTERNAL = "internal"


class NoteFormat(StrEnum):
    """
    Declare the stored text format of a note body: plain text, Markdown, or HTML.

    The enum records a format label; it does not parse or sanitize the body.

    Example:
        >>> str(NoteFormat.MARKDOWN)
        'markdown'
        >>> NoteFormat('markdown') is NoteFormat.MARKDOWN
        True
    """

    PLAIN_TEXT = "plain_text"
    MARKDOWN = "markdown"
    HTML = "html"


class NoteVisibility(StrEnum):
    """
    Label a note's intended audience as private, staff, or public.

    Access enforcement is the responsibility of consumers.

    Example:
        >>> str(NoteVisibility.STAFF)
        'staff'
        >>> NoteVisibility('staff') is NoteVisibility.STAFF
        True
    """

    PRIVATE = "private"
    STAFF = "staff"
    PUBLIC = "public"


class LabelKind(StrEnum):
    """
    Classify short labels and tag-like metadata by role, including topics, places, audiences, and awards.

    Example:
        >>> str(LabelKind.TAG)
        'tag'
        >>> LabelKind('tag') is LabelKind.TAG
        True
    """

    TAG = "tag"
    GENRE = "genre"
    FORM = "form"
    TOPIC = "topic"
    CHARACTER = "character"
    PLACE = "place"
    PERIOD = "period"
    AUDIENCE = "audience"
    AWARD = "award"
    COLLECTION = "collection"
    INTERNAL = "internal"


class GenreKind(StrEnum):
    """
    Distinguish genre, subgenre, form, mode, and movement terms.

    Example:
        >>> str(GenreKind.SUBGENRE)
        'subgenre'
        >>> GenreKind('subgenre') is GenreKind.SUBGENRE
        True
    """

    GENRE = "genre"
    SUBGENRE = "subgenre"
    FORM = "form"
    MODE = "mode"
    MOVEMENT = "movement"


class SubjectKind(StrEnum):
    """
    Classify subject attachments as topics, characters, places, or periods.

    Example:
        >>> str(SubjectKind.TOPIC)
        'topic'
        >>> SubjectKind('topic') is SubjectKind.TOPIC
        True
    """

    TOPIC = "topic"
    CHARACTER = "character"
    PLACE = "place"
    PERIOD = "period"


class LanguageKind(StrEnum):
    """
    Classify a language attachment by its relation to content, translation, subtitles, or interface.

    Example:
        >>> str(LanguageKind.ORIGINAL)
        'original'
        >>> LanguageKind('original') is LanguageKind.ORIGINAL
        True
    """

    CONTENT = "content"
    ORIGINAL = "original"
    SOURCE = "source"
    TARGET = "target"
    SUBTITLE = "subtitle"
    SUMMARY = "summary"
    INTERFACE = "interface"


class DateKind(StrEnum):
    """
    Identify the event described by a date attachment, such as creation, issue, acquisition, or copyright.

    Example:
        >>> str(DateKind.PUBLISHED)
        'published'
        >>> DateKind('published') is DateKind.PUBLISHED
        True
    """

    CREATED = "created"
    ISSUED = "issued"
    PUBLISHED = "published"
    RELEASED = "released"
    RECORDED = "recorded"
    PERFORMED = "performed"
    ACQUIRED = "acquired"
    MODIFIED = "modified"
    DIGITIZED = "digitized"
    COPYRIGHT = "copyright"


class RatingKind(StrEnum):
    """
    Classify a rating by its overall, user, critic, internal, or community role.

    These values do not specify a numeric rating scale.

    Example:
        >>> str(RatingKind.USER)
        'user'
        >>> RatingKind('user') is RatingKind.USER
        True
    """

    OVERALL = "overall"
    USER = "user"
    CRITIC = "critic"
    INTERNAL = "internal"
    COMMUNITY = "community"


class SeriesKind(StrEnum):
    """
    Distinguish a series, subseries, arc, or collection attachment.

    Example:
        >>> str(SeriesKind.ARC)
        'arc'
        >>> SeriesKind('arc') is SeriesKind.ARC
        True
    """

    SERIES = "series"
    SUBSERIES = "subseries"
    ARC = "arc"
    COLLECTION = "collection"


class ResourceKind(StrEnum):
    """
    Classify an external resource link by purpose, such as authority, full text, preview, or purchase.

    The kind does not fetch or validate a linked resource.

    Example:
        >>> str(ResourceKind.PREVIEW)
        'preview'
        >>> ResourceKind('preview') is ResourceKind.PREVIEW
        True
    """

    AUTHORITY = "authority"
    CATALOGUE = "catalogue"
    FULL_TEXT = "full_text"
    PREVIEW = "preview"
    DOWNLOAD = "download"
    COVER_IMAGE = "cover_image"
    MIRROR = "mirror"
    PUBLISHER = "publisher"
    PURCHASE = "purchase"


class IdentifierStatus(StrEnum):
    """
    Label the lifecycle or trust state of a bibliographic identifier.

    Status assignment does not itself validate an identifier or its scheme.

    Example:
        >>> str(IdentifierStatus.ACTIVE)
        'active'
        >>> IdentifierStatus('active') is IdentifierStatus.ACTIVE
        True
    """

    ACTIVE = "active"
    INVALID = "invalid"
    SUPERSEDED = "superseded"
    WITHDRAWN = "withdrawn"
    UNKNOWN = "unknown"


__all__ = [
    "TitleKind",
    "NoteKind",
    "NoteFormat",
    "NoteVisibility",
    "LabelKind",
    "GenreKind",
    "SubjectKind",
    "LanguageKind",
    "DateKind",
    "RatingKind",
    "SeriesKind",
    "ResourceKind",
    "IdentifierStatus",
]
