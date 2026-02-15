"""Bulk ingestion helpers (Stage F1).

These helpers sit *above* the DB/sidecar readers and are designed to:

* summarise a library quickly (schema + counts)
* classify filesystem drift at scale
* remain best-effort: extract something even from mangled libraries

The output is intentionally JSON-friendly (via :meth:`to_dict`) so callers can
store scan reports alongside ingestion logs.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .opf_sidecar import CalibreSidecarReader
from .readers import CalibreReader
from .types import (
    CalibreDriftEvent,
    CalibreDriftSummary,
    CalibreIssue,
    CalibreScanCounts,
    CalibreScanReport,
    CalibreSchemaInfo,
)


def scan_calibre_library(
    library_root: str | Path,
    *,
    best_effort: bool = True,
    filesystem_reconcile: bool = True,
    include_orphan_formats: bool = False,
    strict_paths: bool = False,
    sample_drift_events: int = 10,
    sample_books: int = 5,
    max_books: Optional[int] = None,
) -> CalibreScanReport:
    """Scan a Calibre library root and return an aggregate report.

    If ``metadata.db`` is missing, this falls back to OPF sidecar scanning.
    """

    root = Path(library_root)
    md = root / "metadata.db"

    mode: str
    schema: Optional[CalibreSchemaInfo]
    issues: List[CalibreIssue] = []

    if md.exists():
        mode = "db"
        reader = CalibreReader.from_root(root)
        schema = reader.db.schema_info(best_effort=bool(best_effort))
        issues.extend(list(schema.issues or ()))
        custom_columns_count = len(schema.custom_columns or ())
        it = reader.iter_book_payloads(
            include_formats=True,
            include_cover_path=True,
            include_custom_values=True,
            filesystem_reconcile=bool(filesystem_reconcile),
            include_orphan_formats=bool(include_orphan_formats),
            strict_paths=bool(strict_paths),
            best_effort=bool(best_effort),
        )
    else:
        mode = "opf"
        reader = CalibreSidecarReader.from_root(root)
        schema = None
        custom_columns_count = 0
        # Sidecar mode is inherently filesystem-based; "reconcile" and
        # orphan-format policy are not applicable here.
        it = reader.iter_book_payloads(
            include_formats=True,
            include_cover_path=True,
            strict_paths=bool(strict_paths),
            best_effort=bool(best_effort),
            max_books=max_books,
        )

    authors: set[str] = set()
    tags: set[str] = set()
    formats_total = 0
    drift_total = 0
    drift_by_code: Counter[str] = Counter()
    drift_by_severity: Counter[str] = Counter()
    drift_examples: List[Mapping[str, Any]] = []
    book_examples: List[Mapping[str, Any]] = []

    books_count = 0
    for b in it:
        books_count += 1
        if max_books is not None and books_count > int(max_books):
            break

        for a in b.authors or ():
            authors.add(str(a))
        for t in b.tags or ():
            tags.add(str(t))

        formats_total += len(b.formats or ())

        # Drift accounting.
        evs: Tuple[CalibreDriftEvent, ...] = tuple(b.drift_events or ())
        drift_total += len(evs)
        for e in evs:
            drift_by_code[str(e.code)] += 1
            drift_by_severity[str(e.severity)] += 1
            if len(drift_examples) < int(sample_drift_events):
                drift_examples.append(
                    {
                        "book_id": int(b.calibre_book_id),
                        "code": str(e.code),
                        "severity": str(e.severity),
                        "message": str(e.message),
                        "context": dict(e.context or {}),
                    }
                )

        if len(book_examples) < int(sample_books):
            book_examples.append(
                {
                    "book_id": int(b.calibre_book_id),
                    "title": str(b.title),
                    "authors": list(b.authors or ()),
                    "formats": [f.fmt for f in (b.formats or ())],
                    "warnings": list(b.warnings or ()),
                    "drift_codes": [d.code for d in evs],
                }
            )

    counts = CalibreScanCounts(
        books=int(books_count),
        formats_total=int(formats_total),
        authors_unique=int(len(authors)),
        tags_unique=int(len(tags)),
        custom_columns=int(custom_columns_count),
        drift_events_total=int(drift_total),
    )

    drift = CalibreDriftSummary(
        by_code=dict(drift_by_code),
        by_severity=dict(drift_by_severity),
        examples=tuple(drift_examples),
    )

    return CalibreScanReport(
        library_root=root,
        mode=mode,
        schema=schema,
        counts=counts,
        drift=drift,
        issues=tuple(issues),
        sample_books=tuple(book_examples),
    )
