"""
Export local mixed-format and SquashFS-specific storage ingestion workflows.

The mixed coordinator adopts loose files and supported container members with
run-wide limits; the SquashFS workflow scans specifically for image candidates.
Both borrow a caller-owned manager and expose reports of incremental work. This
package eagerly imports their declarations without executing either workflow.

MemberMetadataFactory here is the mixed coordinator callback accepting a
ContainerMemberContext and inventory entry. SquashFS's module-local callback
instead receives the archive Path and entry; it is not re-exported under that name.

Example:
    >>> MixedIngestBudget(max_members=10).max_members
    10
"""

from LiuXin_alpha.storage.ingest.mixed_format import (
    ContainerHandler,
    ContainerIngestReport,
    ContainerMemberContext,
    MemberMetadataFactory,
    MixedFormatIngestCoordinator,
    MixedIngestBudget,
    MixedIngestIssue,
    MixedIngestReport,
    SourceMetadataFactory,
    default_container_handlers,
    ingest_mixed_local_tree,
)
from LiuXin_alpha.storage.ingest.squashfs_drive import (
    SquashfsArchiveIngestReport,
    SquashfsDriveIngestIssue,
    SquashfsDriveIngestReport,
    SquashfsDriveIngestWorkflow,
    ingest_squashfs_drive,
)


__all__ = [
    "ContainerHandler",
    "ContainerIngestReport",
    "ContainerMemberContext",
    "MemberMetadataFactory",
    "MixedFormatIngestCoordinator",
    "MixedIngestBudget",
    "MixedIngestIssue",
    "MixedIngestReport",
    "SourceMetadataFactory",
    "SquashfsArchiveIngestReport",
    "SquashfsDriveIngestIssue",
    "SquashfsDriveIngestReport",
    "SquashfsDriveIngestWorkflow",
    "default_container_handlers",
    "ingest_mixed_local_tree",
    "ingest_squashfs_drive",
]
