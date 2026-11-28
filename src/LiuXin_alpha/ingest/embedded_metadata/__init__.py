"""
Plan, then write, the catalogue records embedded file metadata describes.

Ingest's "reading embedded metadata" step has two halves. The dry run reads the
files a Store sync registered through their Store, normalises their metadata,
matches it against the catalogue with Catalog's read-only matchers, groups it
into books and writes a reviewable plan. The apply step writes that plan, one
book per transaction, re-checking every decision first. See
``dev-docs/ingest/embedded-metadata.md``.
"""

from __future__ import annotations

from LiuXin_alpha.ingest.embedded_metadata.applying import (
    AppliedBook,
    ApplyReport,
    PlanError,
    apply_plan,
    apply_result,
    default_report_path,
    load_plan,
    write_report,
)
from LiuXin_alpha.ingest.embedded_metadata.models import (
    FileFacts,
    FileFailure,
    IngestFile,
    MetadataIngestPlan,
    PlannedBook,
    Resolution,
    WorkCheck,
)
from LiuXin_alpha.ingest.embedded_metadata.planning import plan_books
from LiuXin_alpha.ingest.embedded_metadata.reading import (
    DEFAULT_MAX_BYTES,
    normalise,
    read_file,
    select_files,
)
from LiuXin_alpha.ingest.embedded_metadata.runner import (
    default_plan_path,
    plan_result,
    plan_store_metadata,
    write_plan,
)

__all__ = [
    "AppliedBook",
    "ApplyReport",
    "DEFAULT_MAX_BYTES",
    "FileFacts",
    "FileFailure",
    "IngestFile",
    "MetadataIngestPlan",
    "PlanError",
    "PlannedBook",
    "Resolution",
    "WorkCheck",
    "apply_plan",
    "apply_result",
    "default_plan_path",
    "default_report_path",
    "load_plan",
    "normalise",
    "plan_books",
    "plan_result",
    "plan_store_metadata",
    "read_file",
    "select_files",
    "write_plan",
    "write_report",
]
