"""
Scan Calibre library filesystems, reconcile database drift and produce import jobs.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise scan through a consuming regression::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_f1_scan_report.py
"""

# Todo: This should probably all be over in utils

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Iterator, List, Mapping, Optional, Tuple, TYPE_CHECKING

from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation.opf_sidecar import CalibreSidecarReader
from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation.readers import CalibreReader
from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation.types import (
    CalibreDriftEvent,
    CalibreDriftSummary,
    CalibreImportJob,
    CalibreImportPolicy,
    CalibreIssue,
    CalibreScanCounts,
    CalibreScanReport,
    CalibreSchemaInfo,
)

if TYPE_CHECKING:
    from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation.types import CalibreBookNormalized


def _trim_payload_for_metadata_only(
    payload,
    *,
    keep_cover_path: bool = False,
    keep_formats: bool = False,
) -> "CalibreBookNormalized":
    """
    Return a copy of a CalibreBookNormalized with heavy/IO-ish fields stripped.

    Example:
        Exercise  trim payload for metadata only through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_f1_scan_report.py


    :param payload: Value supplied for payload under the utility contract.
    :param keep_cover_path: Value supplied for keep cover path under the utility
        contract.
    :param keep_formats: Value supplied for keep formats under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation.types import CalibreBookNormalized

    if not isinstance(payload, CalibreBookNormalized):
        return payload

    return CalibreBookNormalized(
        calibre_book_id=int(payload.calibre_book_id),
        title=str(payload.title),
        authors=tuple(payload.authors or ()),
        tags=tuple(payload.tags or ()),
        languages=tuple(payload.languages or ()),
        identifiers=dict(payload.identifiers or {}),
        series=payload.series,
        formats=tuple(payload.formats or ()) if keep_formats else (),
        comments_html=payload.comments_html,
        cover_path=payload.cover_path if keep_cover_path else None,
        custom_values=dict(payload.custom_values or {}),
        drift_events=tuple(payload.drift_events or ()),
        warnings=tuple(payload.warnings or ()),
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
) -> "CalibreScanReport":
    """
    Scan a Calibre library root and return an aggregate report.

    Example:
        Exercise scan calibre library through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_f1_scan_report.py


    :param library_root: Root directory of the Calibre library being inspected.
    :param best_effort: Value supplied for best effort under the utility contract.
    :param filesystem_reconcile: Value supplied for filesystem reconcile under the
        utility contract.
    :param include_orphan_formats: Value supplied for include orphan formats under the
        utility contract.
    :param strict_paths: Value supplied for strict paths under the utility contract.
    :param sample_drift_events: Value supplied for sample drift events under the utility
        contract.
    :param sample_books: Value supplied for sample books under the utility contract.
    :param max_books: Value supplied for max books under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
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


def iter_import_jobs(
    library_root: str | Path,
    *,
    policy: Optional[CalibreImportPolicy] = None,
    best_effort: bool = True,
    filesystem_reconcile: bool = True,
    include_orphan_formats: bool = False,
    strict_paths: bool = False,
    batch_size: int = 500,
    max_books: Optional[int] = None,
) -> Iterator["CalibreImportJob"]:
    """
    Yield streaming import jobs for a Calibre library.

    Example:
        Exercise iter import jobs through a consuming regression::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_f1_scan_report.py


    :param library_root: Root directory of the Calibre library being inspected.
    :param policy: Value supplied for policy under the utility contract.
    :param best_effort: Value supplied for best effort under the utility contract.
    :param filesystem_reconcile: Value supplied for filesystem reconcile under the
        utility contract.
    :param include_orphan_formats: Value supplied for include orphan formats under the
        utility contract.
    :param strict_paths: Value supplied for strict paths under the utility contract.
    :param batch_size: Value supplied for batch size under the utility contract.
    :param max_books: Value supplied for max books under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """

    root = Path(library_root)
    md = root / "metadata.db"

    pol = policy or CalibreImportPolicy()

    if md.exists():
        mode = "db"
        reader = CalibreReader.from_root(root)
        it = reader.iter_book_payloads(
            batch_size=int(batch_size),
            include_custom_values=True,
            include_formats=True,
            include_cover_path=True,
            filesystem_reconcile=bool(filesystem_reconcile),
            include_orphan_formats=bool(include_orphan_formats),
            strict_paths=bool(strict_paths),
            best_effort=bool(best_effort),
        )
    else:
        mode = "opf"
        reader = CalibreSidecarReader.from_root(root)
        it = reader.iter_book_payloads(
            include_formats=True,
            include_cover_path=True,
            strict_paths=bool(strict_paths),
            best_effort=bool(best_effort),
            max_books=max_books,
        )

    yielded = 0
    for payload in it:
        yielded += 1
        if max_books is not None and yielded > int(max_books):
            break

        drift = tuple(payload.drift_events or ())
        has_error = any(d.severity == "error" for d in drift)
        has_formats = len(tuple(payload.formats or ())) >= int(pol.full_min_formats)

        reasons: List[str] = []
        for d in drift:
            reasons.append(f"drift:{d.severity}:{d.code}")

        # Baseline action.
        action = str(pol.action_default)

        if not has_formats:
            action = str(pol.action_on_no_formats)
            reasons.append("no_formats")

        if has_error:
            action = str(pol.action_on_error_drift)

        # Enforce safe/full constraints.
        if action == "full" and pol.require_safe_paths_for_full:
            unsafe_codes = {"unsafe_book_path", "unsafe_book_path_for_formats"}
            if any(d.code in unsafe_codes for d in drift):
                action = "metadata_only"
                reasons.append("requires_safe_paths")

        if action == "metadata_only":
            payload2 = _trim_payload_for_metadata_only(
                payload,
                keep_cover_path=bool(pol.metadata_only_keep_cover_path),
                keep_formats=bool(pol.metadata_only_keep_formats),
            )
            yield CalibreImportJob(
                library_root=root,
                source_mode=mode,
                action=action,
                reasons=tuple(reasons),
                payload=payload2,
            )
        elif action == "skip":
            yield CalibreImportJob(
                library_root=root,
                source_mode=mode,
                action=action,
                reasons=tuple(reasons),
                payload=None,
            )
        else:
            # "full" or any other future action that still includes payload
            yield CalibreImportJob(
                library_root=root,
                source_mode=mode,
                action=action,
                reasons=tuple(reasons),
                payload=payload,
            )
