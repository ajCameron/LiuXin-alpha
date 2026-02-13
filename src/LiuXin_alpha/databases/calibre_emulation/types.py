"""Shared Calibre-reader types.

These objects are intentionally *data only* and JSON-serialisable (via
:meth:`to_dict`) so tests can snapshot outputs and import pipelines can log
cleanly.

Stage A1 scope:
- Library path container
- Schema-info container
- Book row containers (raw + normalised import payload)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Tuple


def _jsonify_path(p: Optional[Path]) -> Optional[str]:
    return None if p is None else str(p)


def _jsonify_seq(seq: Sequence[Any]) -> list[Any]:
    # Convert tuples to lists, and recursively jsonify paths / dataclasses.
    out: list[Any] = []
    for item in seq:
        if isinstance(item, Path):
            out.append(str(item))
        elif hasattr(item, "to_dict") and callable(getattr(item, "to_dict")):
            out.append(item.to_dict())
        else:
            out.append(item)
    return out


@dataclass(frozen=True, slots=True)
class CalibreLibraryPaths:
    """Filesystem paths for a Calibre library."""

    library_root: Path
    metadata_db_path: Path
    notes_db_path: Optional[Path] = None
    fts_db_path: Optional[Path] = None

    @classmethod
    def from_root(cls, library_root: Path) -> "CalibreLibraryPaths":
        root = Path(library_root)
        return cls(
            library_root=root,
            metadata_db_path=root / "metadata.db",
            notes_db_path=root / ".calnotes" / "notes.db",
            fts_db_path=root / "full-text-search.db",
        )

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "library_root": str(self.library_root),
            "metadata_db_path": str(self.metadata_db_path),
            "notes_db_path": _jsonify_path(self.notes_db_path),
            "fts_db_path": _jsonify_path(self.fts_db_path),
        }


@dataclass(frozen=True, slots=True)
class CalibreCustomColumnDef:
    """Definition of a Calibre custom column from the `custom_columns` table."""

    num: int
    label: str
    name: str
    datatype: str
    is_multiple: bool
    display: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "num": self.num,
            "label": self.label,
            "name": self.name,
            "datatype": self.datatype,
            "is_multiple": self.is_multiple,
            "display": dict(self.display),
        }


@dataclass(frozen=True, slots=True)
class CalibreVersionPlan:
    """A lightweight plan/report for handling a Calibre schema version.

    This is advisory only: it records what we observed and how it compares to
    the Calibre SQL snapshot vendored with LiuXin (if available).
    """

    application_id: int
    user_version: int
    target_user_version: Optional[int] = None
    expected_application_id: Optional[int] = None
    latest_supported_user_version: Optional[int] = None
    known_user_version_min: int = 0
    known_user_version_max: Optional[int] = None
    status: str = "ok"
    warnings: Tuple[str, ...] = ()

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "application_id": self.application_id,
            "user_version": self.user_version,
            "target_user_version": self.target_user_version,
            "expected_application_id": self.expected_application_id,
            "latest_supported_user_version": self.latest_supported_user_version,
            "known_user_version_min": self.known_user_version_min,
            "known_user_version_max": self.known_user_version_max,
            "status": self.status,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class CalibreSchemaInfo:
    """Observed schema information for a Calibre library."""

    application_id: int
    user_version: int
    tables: Tuple[str, ...] = ()
    triggers: Tuple[str, ...] = ()
    has_fts: bool = False
    has_notes: bool = False
    custom_columns: Tuple[CalibreCustomColumnDef, ...] = ()
    version_plan: Optional[CalibreVersionPlan] = None

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "application_id": self.application_id,
            "user_version": self.user_version,
            "tables": list(self.tables),
            "triggers": list(self.triggers),
            "has_fts": self.has_fts,
            "has_notes": self.has_notes,
            "custom_columns": [c.to_dict() for c in self.custom_columns],
            "version_plan": None if self.version_plan is None else self.version_plan.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class CalibreSeriesRef:
    """Series value (name + optional numeric index)."""

    name: str
    index: Optional[float] = None

    def to_dict(self) -> Mapping[str, Any]:
        return {"name": self.name, "index": self.index}


@dataclass(frozen=True, slots=True)
class CalibreFormatRef:
    """A reference to a format file on disk."""

    fmt: str
    file_path: Path
    size_bytes: Optional[int] = None

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "fmt": self.fmt,
            "file_path": str(self.file_path),
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class CalibreBookRow:
    """A *raw-ish* book row plus common pre-joined fields.

    This is intended for reader internals: it can retain the original DB row
    (as a mapping) while also carrying resolved/joined relationships.
    """

    book_id: int
    book_row: Mapping[str, Any]
    authors: Tuple[Mapping[str, Any], ...] = ()
    tags: Tuple[str, ...] = ()
    languages: Tuple[str, ...] = ()
    identifiers: Mapping[str, str] = field(default_factory=dict)
    series: Optional[CalibreSeriesRef] = None
    formats: Tuple[CalibreFormatRef, ...] = ()
    comments_html: Optional[str] = None
    cover_path: Optional[Path] = None
    custom_values: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "book_id": self.book_id,
            "book_row": dict(self.book_row),
            "authors": [dict(a) for a in self.authors],
            "tags": list(self.tags),
            "languages": list(self.languages),
            "identifiers": dict(self.identifiers),
            "series": None if self.series is None else self.series.to_dict(),
            "formats": [f.to_dict() for f in self.formats],
            "comments_html": self.comments_html,
            "cover_path": _jsonify_path(self.cover_path),
            "custom_values": dict(self.custom_values),
        }


@dataclass(frozen=True, slots=True)
class CalibreBookNormalized:
    """A normalised import payload derived from a Calibre library."""

    calibre_book_id: int
    title: str
    authors: Tuple[str, ...] = ()
    tags: Tuple[str, ...] = ()
    languages: Tuple[str, ...] = ()
    identifiers: Mapping[str, str] = field(default_factory=dict)
    series: Optional[CalibreSeriesRef] = None
    formats: Tuple[CalibreFormatRef, ...] = ()
    comments_html: Optional[str] = None
    cover_path: Optional[Path] = None
    custom_values: Mapping[str, Any] = field(default_factory=dict)
    warnings: Tuple[str, ...] = ()

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "calibre_book_id": self.calibre_book_id,
            "title": self.title,
            "authors": list(self.authors),
            "tags": list(self.tags),
            "languages": list(self.languages),
            "identifiers": dict(self.identifiers),
            "series": None if self.series is None else self.series.to_dict(),
            "formats": [f.to_dict() for f in self.formats],
            "comments_html": self.comments_html,
            "cover_path": _jsonify_path(self.cover_path),
            "custom_values": dict(self.custom_values),
            "warnings": list(self.warnings),
        }
