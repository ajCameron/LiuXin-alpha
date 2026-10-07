"""
Compatibility exports for storage schema migration utilities.

The implementation lives in ``LiuXin_alpha.storage.utils.migrations``.  This
forwarding module keeps existing callers source-compatible while leaving the
storage package root focused on public boundaries.
"""

from LiuXin_alpha.storage.utils.migrations import (
    STORAGE_SCHEMA_VERSION,
    StorageMigrationReport,
    can_migrate_storage_schema,
    migrate_storage_schema,
    record_envelope_migration,
)

__all__ = [
    "STORAGE_SCHEMA_VERSION",
    "StorageMigrationReport",
    "can_migrate_storage_schema",
    "migrate_storage_schema",
    "record_envelope_migration",
]
