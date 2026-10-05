"""
Define immutable Calibre emulation paths, records, issues, reports and import policies.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise types through a consuming regression::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Tuple


# Todo: These are not, in fact, types. They're adapters
def _jsonify_path(p: Optional[Path]) -> Optional[str]:
    """
    Take a path, and turn it into a json string.

    Example:
        Exercise  jsonify path through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


    :param p: Path-like value normalized or validated by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return None if p is None else str(p)


def _jsonify_seq(seq: Sequence[Any]) -> list[Any]:
    """
    Any sequence will come out as a json list.

    Example:
        Exercise  jsonify seq through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


    :param seq: Value supplied for seq under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Filesystem paths for a Calibre library.

    Example:
        Exercise CalibreLibraryPaths through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
    """

    library_root: Path
    metadata_db_path: Path
    notes_db_path: Optional[Path] = None
    fts_db_path: Optional[Path] = None

    @classmethod
    def from_root(cls, library_root: Path) -> "CalibreLibraryPaths":
        """
        Populate self with likely library paths.

        Example:
            Exercise CalibreLibraryPaths.from root through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :param library_root: Root directory of the Calibre library being inspected.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        root = Path(library_root)
        return cls(
            library_root=root,
            metadata_db_path=root / "metadata.db",
            notes_db_path=root / ".calnotes" / "notes.db",
            fts_db_path=root / "full-text-search.db",
        )

    def to_dict(self) -> Mapping[str, Any]:
        """
        Return the contents of this class as a dict.

        Example:
            Exercise CalibreLibraryPaths.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "library_root": str(self.library_root),
            "metadata_db_path": str(self.metadata_db_path),
            "notes_db_path": _jsonify_path(self.notes_db_path),
            "fts_db_path": _jsonify_path(self.fts_db_path),
        }


@dataclass(frozen=True, slots=True)
class CalibreCustomColumnDef:
    """
    Definition of a Calibre custom column from the `custom_columns` table.

    Example:
        Exercise CalibreCustomColumnDef through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
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
        """
        Initialize and validate the CalibreCustomColumnDef state.

        Example:
            Exercise CalibreCustomColumnDef.  post init   through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: None; validated state is stored on the receiving object.
        """
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
        """
        Serialize the normalized compatibility record into JSON-safe values.

        Example:
            Exercise CalibreCustomColumnDef.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    """
    Structured issues discovered while reading a Calibre library.

    Example:
        Exercise CalibreIssue through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
    """

    severity: str  # "info" | "warning" | "error"
    code: str
    message: str
    context: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Mapping[str, Any]:
        """
        Serialize the normalized compatibility record into JSON-safe values.

        Example:
            Exercise CalibreIssue.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "context": dict(self.context),
        }


@dataclass(frozen=True, slots=True)
class CalibreDriftEvent:
    """
    Per-book filesystem drift events.

    Example:
        Exercise CalibreDriftEvent through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
    """

    severity: str  # "info" | "warning" | "error"
    code: str
    message: str
    context: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Mapping[str, Any]:
        """
        Serialize the normalized compatibility record into JSON-safe values.

        Example:
            Exercise CalibreDriftEvent.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "context": dict(self.context),
        }


@dataclass(frozen=True, slots=True)
class CalibreVersionPlan:
    """
    A lightweight plan/report for handling a Calibre schema version.

    Example:
        Exercise CalibreVersionPlan through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
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
        """
        dict based representation of this plan.

        Example:
            Exercise CalibreVersionPlan.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    """
    Observed schema information for a Calibre library.

    Example:
        Exercise CalibreSchemaInfo through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
    """

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
        """
        Serialize the normalized compatibility record into JSON-safe values.

        Example:
            Exercise CalibreSchemaInfo.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    """
    Series value (name + optional numeric index).

    Example:
        Exercise CalibreSeriesRef through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
    """

    name: str
    index: Optional[float] = None

    def to_dict(self) -> Mapping[str, Any]:
        """
        Serialize the normalized compatibility record into JSON-safe values.

        Example:
            Exercise CalibreSeriesRef.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {"name": self.name, "index": self.index}


@dataclass(frozen=True, slots=True)
class CalibreFormatRef:
    """
    A reference to a format file on disk.

    Example:
        Exercise CalibreFormatRef through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
    """

    fmt: str
    file_path: Path
    size_bytes: Optional[int] = None

    def to_dict(self) -> Mapping[str, Any]:
        """
        Dict based rep of the format on disk.

        Example:
            Exercise CalibreFormatRef.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "fmt": self.fmt,
            "file_path": str(self.file_path),
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class CalibreBookRow:
    """
    A *raw-ish* book row plus common pre-joined fields.

    Example:
        Exercise CalibreBookRow through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
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
        """
        Dict based rep of the format on disk.

        Example:
            Exercise CalibreBookRow.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
    """
    A normalised import payload derived from a Calibre library.

    Example:
        Exercise CalibreBookNormalized through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
    """

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
        """
        Serialize the normalized compatibility record into JSON-safe values.

        Example:
            Exercise CalibreBookNormalized.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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


@dataclass(frozen=True, slots=True)
class CalibreScanCounts:
    """
    Aggregate counts produced by a best-effort scan.

    Example:
        Exercise CalibreScanCounts through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
    """

    books: int = 0
    formats_total: int = 0
    authors_unique: int = 0
    tags_unique: int = 0
    custom_columns: int = 0
    drift_events_total: int = 0

    def to_dict(self) -> Mapping[str, Any]:
        """
        Serialize the normalized compatibility record into JSON-safe values.

        Example:
            Exercise CalibreScanCounts.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "books": int(self.books),
            "formats_total": int(self.formats_total),
            "authors_unique": int(self.authors_unique),
            "tags_unique": int(self.tags_unique),
            "custom_columns": int(self.custom_columns),
            "drift_events_total": int(self.drift_events_total),
        }


@dataclass(frozen=True, slots=True)
class CalibreDriftSummary:
    """
    Summary of filesystem drift observations.

    Example:
        Exercise CalibreDriftSummary through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
    """

    by_code: Mapping[str, int] = field(default_factory=dict)
    by_severity: Mapping[str, int] = field(default_factory=dict)
    examples: Tuple[Mapping[str, Any], ...] = ()

    def to_dict(self) -> Mapping[str, Any]:
        """
        Serialize the normalized compatibility record into JSON-safe values.

        Example:
            Exercise CalibreDriftSummary.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "by_code": dict(self.by_code),
            "by_severity": dict(self.by_severity),
            "examples": [dict(e) for e in self.examples],
        }


@dataclass(frozen=True, slots=True)
class CalibreScanReport:
    """
    A best-effort scan report for a Calibre library root.

    Example:
        Exercise CalibreScanReport through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
    """

    library_root: Path
    mode: str  # "db" | "opf"
    schema: Optional[CalibreSchemaInfo] = None
    counts: CalibreScanCounts = field(default_factory=CalibreScanCounts)
    drift: CalibreDriftSummary = field(default_factory=CalibreDriftSummary)
    issues: Tuple[CalibreIssue, ...] = ()
    sample_books: Tuple[Mapping[str, Any], ...] = ()

    def to_dict(self) -> Mapping[str, Any]:
        """
        Serialize the normalized compatibility record into JSON-safe values.

        Example:
            Exercise CalibreScanReport.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "library_root": str(self.library_root),
            "mode": self.mode,
            "schema": None if self.schema is None else self.schema.to_dict(),
            "counts": self.counts.to_dict(),
            "drift": self.drift.to_dict(),
            "issues": [i.to_dict() for i in self.issues],
            "sample_books": [dict(b) for b in self.sample_books],
        }


@dataclass(frozen=True, slots=True)
class CalibreImportPolicy:
    """
    Policy for classifying Calibre books into ingestion jobs.

    Example:
        Exercise CalibreImportPolicy through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
    """

    action_default: str = "full"  # "full" | "metadata_only" | "skip"
    action_on_error_drift: str = "metadata_only"
    action_on_no_formats: str = "metadata_only"
    full_min_formats: int = 1
    require_safe_paths_for_full: bool = True

    # When a book is classified as metadata_only, optionally keep references.
    metadata_only_keep_cover_path: bool = False
    metadata_only_keep_formats: bool = False

    def to_dict(self) -> Mapping[str, Any]:
        """
        Serialize the normalized compatibility record into JSON-safe values.

        Example:
            Exercise CalibreImportPolicy.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "action_default": self.action_default,
            "action_on_error_drift": self.action_on_error_drift,
            "action_on_no_formats": self.action_on_no_formats,
            "full_min_formats": int(self.full_min_formats),
            "require_safe_paths_for_full": bool(self.require_safe_paths_for_full),
            "metadata_only_keep_cover_path": bool(self.metadata_only_keep_cover_path),
            "metadata_only_keep_formats": bool(self.metadata_only_keep_formats),
        }


@dataclass(frozen=True, slots=True)
class CalibreImportJob:
    """
    A single streaming ingestion job yielded by :func:`iter_import_jobs`.

    Example:
        Exercise CalibreImportJob through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py
    """

    library_root: Path
    source_mode: str  # "db" | "opf"
    action: str  # "full" | "metadata_only" | "skip"
    reasons: Tuple[str, ...] = ()
    payload: Optional[CalibreBookNormalized] = None

    def to_dict(self) -> Mapping[str, Any]:
        """
        Render the import job as a dict.

        Example:
            Exercise CalibreImportJob.to dict through a consuming regression::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_types.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return {
            "library_root": str(self.library_root),
            "source_mode": self.source_mode,
            "action": self.action,
            "reasons": list(self.reasons),
            "payload": None if self.payload is None else self.payload.to_dict(),
        }
