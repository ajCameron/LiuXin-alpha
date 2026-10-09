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


class StoragePolicyConvenienceMixin:
    """Construct and assign replication and backup policies from ordinary values.

    The host provides :class:`StoragePolicyAPI`; this adapter performs only input
    normalization and delegates all registration, revision, and policy checks.

    Example:
        >>> policy = manager.define_replication_policy("two copies", copies=2)  # doctest: +SKIP
    """

    def define_replication_policy(
        self,
        name: str,
        *,
        copies: int = 1,
        target: int | None = None,
        spread_by: Iterable[ReplicaSeparationDimension | str] = (
            ReplicaSeparationDimension.STORE,
        ),
        copies_per_location: int = 1,
        require_tags: Iterable[str] = (),
        prefer_tags: Iterable[str] = (),
        avoid_tags: Iterable[str] = (),
        synchronous_copies: int | None = None,
        auto_heal: bool = True,
        mode: ReplicaMode | str = ReplicaMode.ACTIVE,
        on_loss: DigitalAssetLossAction | str = (DigitalAssetLossAction.REQUIRE_COPY),
        priority: int = 100,
    ) -> ReplicationPolicyRecord:
        """
        Construct and register a replication policy from ordinary count and placement controls.

        Normalize enum-value strings, tuple-collect dimensions, and frozenset-collect tag iterables.
        If synchronous_copies is None, choose zero only when the effective target equals zero,
        otherwise one. Policy validation still applies: a zero target must permit recreation or
        loss. Counts and flags are not broadly coerced, and a bare string tag iterable becomes
        characters. Registration does not assign this policy to an Asset, reserve capacity, or
        perform replication/repair.

        Example:
            >>> policy = manager.define_replication_policy(  # doctest: +SKIP
            ...     "durable", copies=2, spread_by=("host",),
            ... )


        :param name: Policy name retained without stripping or case normalization by this wrapper.
        :param copies: Minimum desired copy count, passed unchanged to policy validation.
        :param target: Optional desired target count; None lets the policy use copies.
        :param spread_by: Ordered separation enums or exact value strings converted to a tuple without deduplication.
        :param copies_per_location: Maximum copies per separation bucket, not a byte capacity or Store reservation.
        :param require_tags: Iterable collected as required Store tags in a frozenset without per-tag normalization.
        :param prefer_tags: Iterable collected as preferred Store tags in a frozenset.
        :param avoid_tags: Iterable collected as forbidden Store tags in a frozenset.
        :param synchronous_copies: Required synchronous publications, or None to choose zero for a zero target and one otherwise.
        :param auto_heal: Supplied automatic-healing setting retained in the policy; defining it does not execute repair.
        :param mode: Replica mode enum or exact value string normalized before policy construction.
        :param on_loss: Loss-action enum or exact value string describing the permitted response to unavailable copies.
        :param priority: Retention priority forwarded unchanged to policy validation.
        :return: The policy record returned by create_replication_policy.
        """

        effective_target = copies if target is None else target
        if synchronous_copies is None:
            synchronous_copies = 0 if effective_target == 0 else 1
        policy = ReplicationPolicy(
            name=name,
            min_copies=copies,
            target_copies=target,
            distinct_by=tuple(_separation_dimension(value) for value in spread_by),
            max_copies_per_bucket=copies_per_location,
            required_store_tags=frozenset(require_tags),
            preferred_store_tags=frozenset(prefer_tags),
            forbidden_store_tags=frozenset(avoid_tags),
            synchronous_write_copies=synchronous_copies,
            auto_heal=auto_heal,
            mode=_replica_mode(mode),
            loss_action=_loss_action(on_loss),
            retention_priority=priority,
        )
        return cast(
            StoragePolicyAPI,
            cast(object, self),
        ).create_replication_policy(policy)

    def define_backup_policy(
        self,
        name: str,
        *,
        copies: int = 1,
        target: int | None = None,
        spread_by: Iterable[ReplicaSeparationDimension | str] = (
            ReplicaSeparationDimension.STORE,
        ),
        copies_per_location: int = 1,
        require_tags: Iterable[str] = (),
        prefer_tags: Iterable[str] = (),
        avoid_tags: Iterable[str] = (),
        auto_heal: bool = True,
        verify_after_write: bool = True,
        periodic_verification: bool = True,
        locked: bool = False,
        mode: ReplicaMode | str = ReplicaMode.BACKUP,
        priority: int = 100,
    ) -> BackupPolicyRecord:
        """
        Construct and register a backup policy without copying or scheduling content.

        Normalize mode/dimension strings and collect dimensions/tags before constructing the policy.
        Counts, flags, and name are retained subject to BackupPolicy's selected validation;
        backup/archive mode and zero-copy retention combinations are checked there. Tags are not
        individually stripped or validated by this wrapper. Creation adds no policy assignment or
        physical retention enforcement.

        Example:
            >>> policy = manager.define_backup_policy(  # doctest: +SKIP
            ...     "offsite", copies=2, require_tags={"offsite"},
            ... )


        :param name: Policy name retained without stripping or case normalization by this wrapper.
        :param copies: Minimum desired copy count, passed unchanged to policy validation.
        :param target: Optional desired target count; None lets the policy use copies.
        :param spread_by: Ordered separation enums or exact value strings converted to a tuple without deduplication.
        :param copies_per_location: Maximum copies per separation bucket, not a byte capacity or Store reservation.
        :param require_tags: Iterable collected as required Store tags in a frozenset without per-tag normalization.
        :param prefer_tags: Iterable collected as preferred Store tags in a frozenset.
        :param avoid_tags: Iterable collected as forbidden Store tags in a frozenset.
        :param auto_heal: Supplied automatic-healing setting retained in the policy; defining it does not execute repair.
        :param verify_after_write: Supplied setting for later backup verification after publication.
        :param periodic_verification: Supplied periodic-check setting recorded without scheduling a check here.
        :param locked: Supplied retention-lock setting; zero-target combinations are checked by BackupPolicy.
        :param mode: Replica mode enum or exact value string normalized before policy construction.
        :param priority: Retention priority forwarded unchanged to policy validation.
        :return: The policy record returned by create_backup_policy.
        """

        policy = BackupPolicy(
            name=name,
            min_copies=copies,
            target_copies=target,
            distinct_by=tuple(_separation_dimension(value) for value in spread_by),
            max_copies_per_bucket=copies_per_location,
            required_store_tags=frozenset(require_tags),
            preferred_store_tags=frozenset(prefer_tags),
            forbidden_store_tags=frozenset(avoid_tags),
            auto_heal=auto_heal,
            verify_after_write=verify_after_write,
            periodic_verification=periodic_verification,
            retention_locked=locked,
            mode=_replica_mode(mode),
            retention_priority=priority,
        )
        return cast(
            StoragePolicyAPI,
            cast(object, self),
        ).create_backup_policy(policy)

    def assign_replication_policy(
        self,
        asset: _AssetInput,
        policy: ReplicationPolicyID | ReplicationPolicyRecord | None,
        *,
        if_revision: str | None = None,
    ) -> DigitalAssetRecord:
        """
        Assign or clear one Asset's explicit replication policy from ordinary record-or-ID inputs.

        Record inputs contribute only their retained identities; their revision or policy contents
        are not re-registered. ``None`` clears the explicit assignment so effective resolution
        falls back through Store/default policy rules. The underlying manager preserves the current
        backup-policy reference atomically and owns registration and optimistic-revision checks.

        Example:
            >>> updated = manager.assign_replication_policy(  # doctest: +SKIP
            ...     asset, policy, if_revision=asset.revision,
            ... )

        :param asset: Positive Asset ID or Asset-bearing record/result/resolution.
        :param policy: Registered replication-policy ID or record, or None to clear the explicit assignment.
        :param if_revision: Expected current Asset revision, or None to omit the optimistic precondition.
        :return: Updated Asset record with its backup-policy reference preserved.
        """

        return cast(
            StoragePolicyAPI,
            cast(object, self),
        ).set_digital_asset_replication_policy(
            _asset_id(asset),
            _replication_policy_id(policy),
            if_revision=if_revision,
        )

    def assign_backup_policy(
        self,
        asset: _AssetInput,
        policy: BackupPolicyID | BackupPolicyRecord | None,
        *,
        if_revision: str | None = None,
    ) -> DigitalAssetRecord:
        """
        Assign or clear one Asset's explicit backup policy from ordinary record-or-ID inputs.

        Record inputs are identity carriers rather than policy updates. ``None`` clears the explicit
        assignment and restores fallback resolution. The underlying manager preserves the current
        replication-policy reference atomically and applies policy registration, recreation, and
        optional revision checks.

        Example:
            >>> updated = manager.assign_backup_policy(  # doctest: +SKIP
            ...     asset, policy, if_revision=asset.revision,
            ... )

        :param asset: Positive Asset ID or Asset-bearing record/result/resolution.
        :param policy: Registered backup-policy ID or record, or None to clear the explicit assignment.
        :param if_revision: Expected current Asset revision, or None to omit the optimistic precondition.
        :return: Updated Asset record with its replication-policy reference preserved.
        """

        return cast(
            StoragePolicyAPI,
            cast(object, self),
        ).set_digital_asset_backup_policy(
            _asset_id(asset),
            _backup_policy_id(policy),
            if_revision=if_revision,
        )


__all__ = ["StoragePolicyConvenienceMixin"]
