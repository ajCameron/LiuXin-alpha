"""Compose table-scoped SQL operations for the current stored schema.

This is the database layer for concrete query/write routines and SQL
accelerators over actual tables and columns. Helpers use portable macros,
preserving backend behavior and the caller's transaction. Vocabulary batches
share bounded key lookups; row updates/deletes share atomic batch machinery.
Normalization maintenance repairs declared comparison keys without schema edits
or a second set of value rules. Relationship readers batch destination rows and
retain every link; the mixin returning those rows owns the specialised entry point.
Foreign-key readers group rows for requested owners in bounded SQL batches.
Policy readers resolve explicit references; their callers interpret the results.

Catalog owns bibliographic interpretation, WEMI selection and compatibility
facades such as a singular title or Calibre book. Book/title/ray projections
stay outside this package. Helpers here use physical row IDs, with cover asset
selection and physical file changes delegated to their semantic/storage owners.
Do not import Catalog to forward a semantic operation from this package.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.databases.api.metadata_sql_api import MetadataSQLAPI

from .agents_mixin import AgentsSQLMixin
from .annotations_mixin import AnnotationsSQLMixin
from .asset_replicas_mixin import AssetReplicasSQLMixin
from .backup_policies_mixin import BackupPoliciesSQLMixin
from .backup_workflow_outputs_mixin import BackupWorkflowOutputsSQLMixin
from .backup_workflow_sources_mixin import BackupWorkflowSourcesSQLMixin
from .backup_workflow_state_mixin import BackupWorkflowStateSQLMixin
from .backup_workflows_mixin import BackupWorkflowsSQLMixin
from .comments_mixin import CommentsSQLMixin
from .composite_digital_assets_mixin import CompositeDigitalAssetsSQLMixin
from .devices_mixin import DevicesSQLMixin
from .digital_assets_mixin import DigitalAssetsSQLMixin
from .entity_identifiers_mixin import EntityIdentifiersSQLMixin
from .expressions_mixin import ExpressionsSQLMixin
from .feeds_mixin import FeedsSQLMixin
from .file_derivations_mixin import FileDerivationsSQLMixin
from .files_mixin import FilesSQLMixin
from .folders_mixin import FoldersSQLMixin
from .genres_mixin import GenresSQLMixin
from .human_agents_mixin import HumanAgentsSQLMixin
from .images_mixin import ImagesSQLMixin
from .item_identifiers_mixin import ItemIdentifiersSQLMixin
from .items_mixin import ItemsSQLMixin
from .labels_mixin import LabelsSQLMixin
from .languages_mixin import LanguagesSQLMixin
from .manifestations_mixin import ManifestationsSQLMixin
from .normalization_mixin import NormalizationSQLMixin
from .notes_mixin import NotesSQLMixin
from .org_agent_relations_mixin import OrgAgentRelationsSQLMixin
from .org_agents_mixin import OrgAgentsSQLMixin
from .ratings_mixin import RatingsSQLMixin
from .replication_policies_mixin import ReplicationPoliciesSQLMixin
from .series_mixin import SeriesSQLMixin
from .stores_mixin import StoresSQLMixin
from .subjects_mixin import SubjectsSQLMixin
from .synopses_mixin import SynopsesSQLMixin
from .tags_mixin import TagsSQLMixin
from .works_mixin import WorksSQLMixin

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class MetadataSQL(
    AgentsSQLMixin,
    AnnotationsSQLMixin,
    AssetReplicasSQLMixin,
    BackupPoliciesSQLMixin,
    BackupWorkflowOutputsSQLMixin,
    BackupWorkflowSourcesSQLMixin,
    BackupWorkflowStateSQLMixin,
    BackupWorkflowsSQLMixin,
    CommentsSQLMixin,
    CompositeDigitalAssetsSQLMixin,
    DevicesSQLMixin,
    DigitalAssetsSQLMixin,
    EntityIdentifiersSQLMixin,
    ExpressionsSQLMixin,
    FeedsSQLMixin,
    FileDerivationsSQLMixin,
    FilesSQLMixin,
    FoldersSQLMixin,
    GenresSQLMixin,
    HumanAgentsSQLMixin,
    ImagesSQLMixin,
    ItemIdentifiersSQLMixin,
    ItemsSQLMixin,
    LabelsSQLMixin,
    LanguagesSQLMixin,
    ManifestationsSQLMixin,
    NormalizationSQLMixin,
    NotesSQLMixin,
    OrgAgentRelationsSQLMixin,
    OrgAgentsSQLMixin,
    RatingsSQLMixin,
    ReplicationPoliciesSQLMixin,
    SeriesSQLMixin,
    StoresSQLMixin,
    SubjectsSQLMixin,
    SynopsesSQLMixin,
    TagsSQLMixin,
    WorksSQLMixin,
    MetadataSQLAPI,
):
    """Bind physical-table operations to a database's current macro provider.

    Constructing this object retains the owner only. Calls resolve its current
    macros and canonical-identity service; no connection or transaction is
    opened until an operation needs one. Database rebinding/close owns lifecycle.

    Example:
        >>> sql = MetadataSQL(None)
        >>> sql.db is None
        True
    """

    def __init__(self, db: DatabaseAPI) -> None:
        """Retain the database without opening a connection or executing SQL."""
        self.db = db


__all__ = [
    "AgentsSQLMixin",
    "AnnotationsSQLMixin",
    "AssetReplicasSQLMixin",
    "BackupPoliciesSQLMixin",
    "BackupWorkflowOutputsSQLMixin",
    "BackupWorkflowSourcesSQLMixin",
    "BackupWorkflowStateSQLMixin",
    "BackupWorkflowsSQLMixin",
    "CommentsSQLMixin",
    "CompositeDigitalAssetsSQLMixin",
    "DevicesSQLMixin",
    "DigitalAssetsSQLMixin",
    "EntityIdentifiersSQLMixin",
    "ExpressionsSQLMixin",
    "FeedsSQLMixin",
    "FileDerivationsSQLMixin",
    "FilesSQLMixin",
    "FoldersSQLMixin",
    "GenresSQLMixin",
    "HumanAgentsSQLMixin",
    "ImagesSQLMixin",
    "ItemIdentifiersSQLMixin",
    "ItemsSQLMixin",
    "LabelsSQLMixin",
    "LanguagesSQLMixin",
    "ManifestationsSQLMixin",
    "MetadataSQL",
    "NormalizationSQLMixin",
    "NotesSQLMixin",
    "OrgAgentRelationsSQLMixin",
    "OrgAgentsSQLMixin",
    "RatingsSQLMixin",
    "ReplicationPoliciesSQLMixin",
    "SeriesSQLMixin",
    "StoresSQLMixin",
    "SubjectsSQLMixin",
    "SynopsesSQLMixin",
    "TagsSQLMixin",
    "WorksSQLMixin",
]
