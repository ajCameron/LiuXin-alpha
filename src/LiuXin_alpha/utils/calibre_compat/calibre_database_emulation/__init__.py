
"""
API for the calibre_emulation module.
"""



from .db import CalibreDB
from .errors import (
    CalibreCorruptError,
    CalibreLibraryNotFoundError,
    CalibreSchemaError,
    CalibreUnsupportedVersionError,
    CalibreUnsafePathError,
)
from .readers import CalibreReader
from .opf_sidecar import CalibreSidecarReader, ParsedOPF, parse_metadata_opf
from .scan import iter_import_jobs, scan_calibre_library
from .types import (
    CalibreBookNormalized,
    CalibreBookRow,
    CalibreCustomColumnDef,
    CalibreFormatRef,
    CalibreLibraryPaths,
    CalibreSchemaInfo,
    CalibreVersionPlan,
    CalibreSeriesRef,
    CalibreIssue,
    CalibreDriftEvent,
    CalibreScanCounts,
    CalibreDriftSummary,
    CalibreScanReport,
    CalibreImportPolicy,
    CalibreImportJob,
)
from .versioning import CalibreVersionPolicy

__all__ = [
    "CalibreCorruptError",
    "CalibreDB",
    "CalibreReader",
    "CalibreSidecarReader",
    "ParsedOPF",
    "parse_metadata_opf",
    "CalibreLibraryNotFoundError",
    "CalibreSchemaError",
    "CalibreUnsupportedVersionError",
    "CalibreUnsafePathError",
    "CalibreLibraryPaths",
    "CalibreSchemaInfo",
    "CalibreVersionPlan",
    "CalibreVersionPolicy",
    "CalibreIssue",
    "CalibreDriftEvent",
    "CalibreCustomColumnDef",
    "CalibreSeriesRef",
    "CalibreFormatRef",
    "CalibreBookRow",
    "CalibreBookNormalized",
    "CalibreScanCounts",
    "CalibreDriftSummary",
    "CalibreScanReport",
    "CalibreImportPolicy",
    "CalibreImportJob",
    "scan_calibre_library",
    "iter_import_jobs",
]
