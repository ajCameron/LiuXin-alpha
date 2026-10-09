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


class ItemLinkConvenienceMixin:
    """Adapt ordinary Item and Asset identifiers to the explicit link API.

    Hosts provide :class:`ItemDigitalAssetLinkAPI`; this mixin owns no state and
    can be reused independently of ingest, policy, and Composite conveniences.

    Example:
        >>> manager.link(9, asset, role="cover")  # doctest: +SKIP
    """

    def link(
        self,
        item: ItemID | int,
        asset: (
            DigitalAssetID
            | DigitalAssetRecord
            | DigitalAssetIngestResult
            | DigitalAssetResolution
            | CompositeDigitalAssetID
            | CompositeDigitalAssetRecord
        ),
        *,
        role: str = "primary_payload",
        composite: bool = False,
    ) -> None:
        """
        Associate an Item role with an atomic or Composite Asset through the relevant manager API.

        Require a positive non-bool integer Item ID first. A Composite record selects the Composite
        path automatically; otherwise a truthy composite flag selects it. Nominal IDs are runtime
        integers, so a raw Composite ID needs that flag to avoid atomic interpretation. Exact role
        text is forwarded and no revision, reference lookup, or metadata transaction is added by
        this wrapper.

        Example:
            >>> manager.link(9, cover, role="cover")  # doctest: +SKIP


        :param item: Required positive non-bool integer Item identity.
        :param asset: Atomic ID/record/result/resolution, or Composite record/ID for the Composite path.
        :param role: Exact role text identifying the association; defaults to primary_payload.
        :param composite: Whether to interpret a raw identifier as Composite; Composite records select that path automatically.
        :return: None after the selected link operation returns.
        """

        item_id = _required_item_id(item)
        if composite or isinstance(asset, CompositeDigitalAssetRecord):
            cast(
                ItemDigitalAssetLinkAPI,
                cast(object, self),
            ).link_item_to_composite_digital_asset(
                item_id,
                _composite_id(cast(_CompositeInput, asset)),
                role=role,
            )
            return
        cast(
            ItemDigitalAssetLinkAPI,
            cast(object, self),
        ).link_item_to_digital_asset(
            item_id,
            _asset_id(cast(_AssetInput, asset)),
            role=role,
        )

    def unlink(
        self,
        item: ItemID | int,
        *,
        role: str = "primary_payload",
    ) -> bool:
        """
        Validate the Item ID and delegate removal of its exact role association. The underlying
        manager determines whether a link existed and applies persistence/reference behavior; the
        wrapper performs no physical-byte deletion.

        Example:
            >>> manager.unlink(9, role="cover")  # doctest: +SKIP
            True


        :param item: Required positive non-bool integer Item ID.
        :param role: Exact Item-link role to remove, defaulting to primary_payload.
        :return: The boolean reported by unlink_item_digital_asset for the requested association.
        """

        return cast(
            ItemDigitalAssetLinkAPI,
            cast(object, self),
        ).unlink_item_digital_asset(
            _required_item_id(item),
            role=role,
        )


__all__ = ["ItemLinkConvenienceMixin"]
