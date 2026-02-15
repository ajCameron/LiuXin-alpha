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
    """Definition of a Calibre custom column from the `custom_columns` table.

    Calibre creates dynamic tables per custom column id:

    - value table:   ``custom_column_{id}``
    - link table:    ``books_custom_column_{id}_link`` (only for normalised columns)

    We store both the *expected* table names and (when known) their presence in
    sqlite_master so higher-level readers can be robust in the face of partial
    or mangled libraries.
    """

    num: int
    label: str
    name: str
    datatype: str
    is_multiple: bool
    display: Mapping[str, Any] = field(default_factory=dict)

    # Extra flags from Calibre (may be missing in partial schemas).
    normalized: Optional[bool] = None
    editable: Optional[bool] = None
    mark_for_delete: bool = False

    # Derived table names (filled from `num` if not provided).
    value_table: Optional[str] = None
    link_table: Optional[str] = None

    # Expectations / observed presence (optional: may be unknown if schema_info
    # did not enumerate sqlite_master).
    expects_link_table: Optional[bool] = None
    has_value_table: Optional[bool] = None
    has_link_table: Optional[bool] = None
    link_has_extra: Optional[bool] = None  # series index stored in link.extra when normalised

    def __post_init__(self) -> None:
        # Fill defaults in a frozen dataclass.
        if self.normalized is None:
            object.__setattr__(
                self,
                "normalized",
                self.datatype not in ("datetime", "comments", "int", "bool", "float", "composite"),
            )
        if self.editable is None:
            object.__setattr__(self, "editable", True)

        if self.value_table is None:
            object.__setattr__(self, "value_table", f"custom_column_{self.num}")
        if self.link_table is None:
            object.__setattr__(self, "link_table", f"books_custom_column_{self.num}_link")

        if self.expects_link_table is None:
            object.__setattr__(self, "expects_link_table", bool(self.normalized))

        if self.link_has_extra is None:
            object.__setattr__(
                self,
                "link_has_extra",
                bool(self.expects_link_table) and self.datatype == "series",
            )

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "num": self.num,
            "label": self.label,
            "name": self.name,
            "datatype": self.datatype,
            "is_multiple": self.is_multiple,
            "normalized": self.normalized,
            "editable": self.editable,
            "mark_for_delete": self.mark_for_delete,
            "display": dict(self.display),
            "value_table": self.value_table,
            "link_table": self.link_table,
            "expects_link_table": self.expects_link_table,
            "has_value_table": self.has_value_table,
            "has_link_table": self.has_link_table,
            "link_has_extra": self.link_has_extra,
        }


@dataclass(frozen=True, slots=True)
class CalibreIssue:
    """Structured issues discovered while reading a Calibre library.

    These are intended for diagnostics and snapshot tests, not as a logging
    system. Keep messages short and stable.
    """

    severity: str  # "info" | "warning" | "error"
    code: str
    message: str
    context: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "context": dict(self.context),
        }


@dataclass(frozen=True, slots=True)
class CalibreDriftEvent:
    """Per-book filesystem drift events.

    These are derived from reconciling DB expectations with on-disk reality.
    Keep codes stable so callers can build ingestion policies.
    """

    severity: str  # "info" | "warning" | "error"
    code: str
    message: str
    context: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "context": dict(self.context),
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
    action: str = "continue"  # "continue" | "continue_with_warnings" | "refuse"
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
            "action": self.action,
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
    issues: Tuple[CalibreIssue, ...] = ()

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
            "issues": [i.to_dict() for i in self.issues],
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
    drift_events: Tuple[CalibreDriftEvent, ...] = ()
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
            "drift_events": [d.to_dict() for d in self.drift_events],
            "warnings": list(self.warnings),
        }
