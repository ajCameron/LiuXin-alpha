"""Canonical controlled vocabularies for metadata container families.

These enums are shared by the editable metadata-container implementations and
their matching API modules. They are *not* database-enforced vocabularies; DB
constrained sets such as identifier schemes belong in ``LiuXin_alpha.databases``
and core WEMI / agent typing belongs in ``LiuXin_alpha.metadata.metadata_types``.
"""
from __future__ import annotations

from enum import StrEnum


class TitleKind(StrEnum):
    """
    Controlled kinds for title strings.
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
    Controlled kinds for long-form notes.
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
    Storage or rendering format for note body text.
    """

    PLAIN_TEXT = "plain_text"
    MARKDOWN = "markdown"
    HTML = "html"


class NoteVisibility(StrEnum):
    """
    Audience / exposure level for notes.
    """

    PRIVATE = "private"
    STAFF = "staff"
    PUBLIC = "public"


class LabelKind(StrEnum):
    """Controlled kinds for short-form labels and tag-like metadata."""

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
    """Controlled kinds for genre-style terms."""

    GENRE = "genre"
    SUBGENRE = "subgenre"
    FORM = "form"
    MODE = "mode"
    MOVEMENT = "movement"


class SubjectKind(StrEnum):
    """Controlled kinds for subject-style metadata."""

    TOPIC = "topic"
    CHARACTER = "character"
    PLACE = "place"
    PERIOD = "period"


class LanguageKind(StrEnum):
    """Controlled kinds for language attachments."""

    CONTENT = "content"
    ORIGINAL = "original"
    SOURCE = "source"
    TARGET = "target"
    SUBTITLE = "subtitle"
    SUMMARY = "summary"
    INTERFACE = "interface"


class DateKind(StrEnum):
    """Controlled kinds for date attachments."""

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
    """Controlled kinds for rating attachments."""

    OVERALL = "overall"
    USER = "user"
    CRITIC = "critic"
    INTERNAL = "internal"
    COMMUNITY = "community"


class SeriesKind(StrEnum):
    """Controlled kinds for series-style attachments."""

    SERIES = "series"
    SUBSERIES = "subseries"
    ARC = "arc"
    COLLECTION = "collection"


class ResourceKind(StrEnum):
    """Controlled kinds for external resource attachments."""

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
    """Lifecycle / trust state for a bibliographic identifier."""

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
