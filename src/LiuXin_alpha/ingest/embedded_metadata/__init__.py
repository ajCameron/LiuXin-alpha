"""
Plan, without writing, the catalogue records embedded file metadata would create.

This is the read half of ingest's "reading embedded metadata" step: files a
Store sync registered are read through their Store, normalised, matched against
the catalogue with Catalog's read-only matchers, grouped into books and written
out as a reviewable plan. The write phase, which will create the records, is not
built yet. See ``dev-docs/ingest/embedded-metadata.md``.
"""

from __future__ import annotations

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
    "DEFAULT_MAX_BYTES",
    "FileFacts",
    "FileFailure",
    "IngestFile",
    "MetadataIngestPlan",
    "PlannedBook",
    "Resolution",
    "WorkCheck",
    "default_plan_path",
    "normalise",
    "plan_books",
    "plan_result",
    "plan_store_metadata",
    "read_file",
    "select_files",
    "write_plan",
]
