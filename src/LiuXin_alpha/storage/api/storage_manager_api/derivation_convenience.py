"""Responsibility-specific manager convenience adapter."""

from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import BinaryIO, cast
from uuid import UUID

from LiuXin_alpha.storage.api.errors import StorageIntegrityError
from LiuXin_alpha.storage.api.models import Digest, StoreUUID
from LiuXin_alpha.storage.api.placement_hints_api import (
    StorageHintSource,
    StoragePlacementHints,
    derive_storage_hints,
)
from LiuXin_alpha.storage.api.storage_manager_api.catalog_api import (
    DigitalAssetRegistryAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.composites_api import (
    CompositeDigitalAssetAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.derivations_api import (
    DigitalAssetDerivationRegistryAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.errors import (
    DigitalAssetNotFound,
)
from LiuXin_alpha.storage.api.storage_manager_api.ingest_api import (
    DigitalAssetIngestAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.item_links_api import (
    ItemDigitalAssetLinkAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    BackupPolicy,
    BackupPolicyID,
    BackupPolicyRecord,
    CompositeDigitalAssetDeclaration,
    CompositeDigitalAssetID,
    CompositeDigitalAssetMemberResolution,
    CompositeDigitalAssetMembership,
    CompositeDigitalAssetRecord,
    DigitalAssetDeclaration,
    DigitalAssetDerivationDeclaration,
    DigitalAssetDerivationKind,
    DigitalAssetDerivationRecord,
    DigitalAssetDerivationSourceReference,
    DigitalAssetID,
    DigitalAssetIngestResult,
    DigitalAssetLossAction,
    DigitalAssetMetadata,
    DigitalAssetRecord,
    DigitalAssetResolution,
    ItemID,
    ReplicaID,
    ReplicaMode,
    ReplicaRecord,
    ReplicaSeparationDimension,
    ReplicationPolicy,
    ReplicationPolicyID,
    ReplicationPolicyRecord,
    ReproductionRecipe,
    StoreConfiguration,
)
from LiuXin_alpha.storage.api.storage_manager_api.policies_api import (
    StoragePolicyAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.replicas_api import (
    ReplicaLifecycleAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.retrieval_api import (
    DigitalAssetRetrievalAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.router_api import (
    StorageRouterAPI,
)
from LiuXin_alpha.storage.api.store_api.facade_api import StoreAPI

from LiuXin_alpha.storage.api.storage_manager_api._convenience_support import *  # noqa: F403


class DerivationConvenienceMixin:
    """Build ordered provenance declarations over atomic and Composite sources.

    The host provides :class:`DigitalAssetDerivationRegistryAPI`. This mixin does
    not execute recipes or infer reproducibility from their presence.

    Example:
        >>> manager.record_derivation(result, {"source": source})  # doctest: +SKIP
    """

    def record_derivation(
        self,
        result: (
            DigitalAssetID
            | DigitalAssetRecord
            | DigitalAssetIngestResult
            | DigitalAssetResolution
        ),
        sources: (
            Mapping[
                str,
                DigitalAssetID
                | DigitalAssetRecord
                | DigitalAssetIngestResult
                | DigitalAssetResolution
                | CompositeDigitalAssetRecord,
            ]
            | Iterable[
                DigitalAssetID
                | DigitalAssetRecord
                | DigitalAssetIngestResult
                | DigitalAssetResolution
                | CompositeDigitalAssetRecord
            ]
        ),
        *,
        kind: DigitalAssetDerivationKind | str = (DigitalAssetDerivationKind.OTHER),
        recipe: ReproductionRecipe | None = None,
        output_role: str | None = None,
        created_at: datetime | None = None,
        operator: str | None = None,
        notes: str | None = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
    ) -> DigitalAssetDerivationRecord:
        """
        Collect ordered provenance sources and register one complete derivation declaration.

        Mapping keys become source roles; other iterables use None roles. Materialize the inputs,
        assign consecutive sequence numbers, and detect Composite records explicitly; raw nominal
        integers follow the atomic Asset path. Normalize kind through its enum constructor and
        retain the supplied recipe/workflow evidence. Value constructors and the manager own
        graph/reference checks. This wrapper does not execute a recipe, hash outputs, or prove exact
        recreation merely because recipe evidence was supplied.

        Example:
            >>> derivation = manager.record_derivation(  # doctest: +SKIP
            ...     cover, {"source": book}, kind="extract",
            ... )


        :param result: Positive integer Asset ID or an Asset record, ingest result, or resolution whose retained Asset ID is extracted.
        :param sources: Role-to-source mapping or ordered iterable of atomic inputs and Composite records.
        :param kind: Derivation-kind enum or exact value string; defaults to OTHER.
        :param recipe: Optional retained reproduction recipe describing supplied replay evidence.
        :param output_role: Optional role identifying this declaration's result within recipe outputs.
        :param created_at: Optional provenance timestamp forwarded for declaration validation.
        :param operator: Optional operator attribution retained in the declaration.
        :param notes: Optional provenance notes retained without wrapper normalization.
        :param workflow_id: Optional legacy workflow execution ID grouping this step.
        :param workflow_reference: Optional namespaced external workflow reference retained separately from workflow_id.
        :return: The record returned by record_digital_asset_derivation.
        """

        source_values: tuple[
            tuple[_DerivationSourceInput, str | None],
            ...,
        ]
        if isinstance(sources, Mapping):
            source_mapping = cast(
                Mapping[str, _DerivationSourceInput],
                sources,
            )
            source_values = tuple(
                (source, role) for role, source in source_mapping.items()
            )
        else:
            source_iterable = sources
            source_values = tuple((source, None) for source in source_iterable)
        references = tuple(
            _derivation_source(sequence_number, source, role)
            for sequence_number, (source, role) in enumerate(source_values)
        )
        declaration = DigitalAssetDerivationDeclaration(
            result_digital_asset_id=_asset_id(result),
            sources=references,
            kind=_derivation_kind(kind),
            recipe=recipe,
            output_role=output_role,
            created_at=created_at,
            operator=operator,
            notes=notes,
            workflow_id=workflow_id,
            workflow_reference=workflow_reference,
        )
        return cast(
            DigitalAssetDerivationRegistryAPI,
            cast(object, self),
        ).record_digital_asset_derivation(declaration)


__all__ = ["DerivationConvenienceMixin"]
