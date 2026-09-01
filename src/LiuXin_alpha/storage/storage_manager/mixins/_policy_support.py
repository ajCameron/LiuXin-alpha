"""
Cross-cutting placement and recoverability policy mechanics.
"""

from __future__ import annotations

import dataclasses
from collections import Counter
from collections.abc import Iterable

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState
from LiuXin_alpha.storage.storage_manager.mixins._types import (
    StoreFactory,
    _ItemTargetID,
    _ItemTargetKind,
    _RecreationBranch,
)


class _StorageManagerPolicySupportMixin(_StorageManagerState):
    """
    Share placement and recoverability rules across policy workflows.

    The public policy component owns registration, assessment, and planning.
    This private support slice supplies the common store-separation,
    derivation-cycle, and recoverability checks used by those workflows and by
    Store administration.  Its helpers assume callers have already selected
    the relevant records; they do not mutate policy state themselves.
    """

    def _require_store_factory(self) -> StoreFactory:
        """Return the configured constructor or reject lifecycle mutation."""

        if self._store_factory is None:
            raise api.StoreUnsupportedOperation(
                "manager has no Store factory; attach a Store instance explicitly."
            )
        return self._store_factory

    def _validate_declared_policy_ids(
        self,
        replication_policy_id: api.ReplicationPolicyID | None,
        backup_policy_id: api.BackupPolicyID | None,
    ) -> None:
        """Require every supplied policy identifier to be registered."""

        if replication_policy_id is not None:
            self.get_replication_policy_record(replication_policy_id)
        if backup_policy_id is not None:
            self.get_backup_policy_record(backup_policy_id)

    def _validate_store_policy_references(
        self,
        configuration: api.StoreConfiguration,
    ) -> None:
        """Require a Store configuration's default policy references."""

        self._validate_declared_policy_ids(
            configuration.store_default_replication_policy_id,
            configuration.store_default_backup_policy_id,
        )

    def _placement_policy_ids(
        self,
        store_ref: api.StoreUUID,
    ) -> tuple[
        api.ReplicationPolicyID | None,
        api.BackupPolicyID | None,
    ]:
        """Return policy identifiers captured for a new placement."""

        configuration = self.get_store_configuration(store_ref)
        return (
            configuration.store_default_replication_policy_id,
            configuration.store_default_backup_policy_id,
        )

    def _capture_first_placement_policies(
        self,
        asset: api.DigitalAssetRecord,
        replication_policy_id: api.ReplicationPolicyID | None,
        backup_policy_id: api.BackupPolicyID | None,
    ) -> api.DigitalAssetRecord:
        """Capture Store defaults on a declared but not yet placed Asset."""

        with self._lock:
            has_replica = any(
                replica.digital_asset_id == asset.digital_asset_id
                and replica.state is not api.ReplicaState.DELETED
                for replica in self._replicas.values()
            )
            if has_replica:
                return asset
            effective_replication_id = (
                asset.replication_policy_id
                if asset.replication_policy_id is not None
                else replication_policy_id
            )
            effective_backup_id = (
                asset.backup_policy_id
                if asset.backup_policy_id is not None
                else backup_policy_id
            )
            if (
                effective_replication_id == asset.replication_policy_id
                and effective_backup_id == asset.backup_policy_id
            ):
                return asset
            return self.set_digital_asset_policies(
                asset.digital_asset_id,
                replication_policy_id=effective_replication_id,
                backup_policy_id=effective_backup_id,
                if_revision=asset.revision,
            )

    def _validate_all_recreation_policies(self) -> None:
        """Require every effective recreate-on-loss policy to remain safe."""

        for asset in tuple(self.iter_digital_asset_records()):
            policies = self.resolve_effective_policies(asset.digital_asset_id)
            if policies.replication.loss_action is api.DigitalAssetLossAction.RECREATE:
                self._validate_recreation_policy(
                    asset.digital_asset_id,
                    set(),
                )

    def _set_item_target(
        self,
        item_id: api.ItemID,
        role: str,
        kind: _ItemTargetKind,
        target_id: _ItemTargetID,
    ) -> None:
        """Set one well-formed Item role link in reference metadata."""

        if int(item_id) <= 0:
            raise ValueError("item_id must be positive.")
        if not role.strip():
            raise ValueError("role must not be empty.")
        with self._lock, self._metadata_transaction():
            self._item_targets[(item_id, role)] = (kind, target_id)

    def _asset_has_derivation_reference_locked(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> bool:
        """Return whether an Asset participates in stored provenance."""

        for record in self._derivations.values():
            declaration = record.declaration
            if declaration.result_digital_asset_id == digital_asset_id:
                return True
            if any(
                source.digital_asset_id == digital_asset_id
                for source in declaration.sources
            ):
                return True
            recipe = declaration.recipe
            if recipe is None:
                continue
            if any(
                input_.digital_asset_id == digital_asset_id for input_ in recipe.inputs
            ):
                return True
            artifacts = (
                () if recipe.executor is None else (recipe.executor,)
            ) + recipe.dependencies
            if any(
                artifact.digital_asset_id == digital_asset_id for artifact in artifacts
            ):
                return True
        return False

    def _store_satisfies_policy(
        self,
        store_ref: api.StoreUUID,
        policy: api.ReplicationPolicy | api.BackupPolicy,
    ) -> bool:
        """Return whether Store tags and supported mode satisfy a policy."""

        try:
            configuration = self.get_store_configuration(store_ref)
        except api.StoreConfigurationNotFound:
            return False
        tags = set(configuration.store_tags)
        return (
            policy.mode in configuration.supported_replica_modes
            and policy.required_store_tags <= tags
            and not policy.forbidden_store_tags & tags
        )

    def _policy_bucket(
        self,
        store_ref: api.StoreUUID,
        dimension: api.ReplicaSeparationDimension,
    ) -> object:
        """Resolve a declared failure-domain bucket without inventing data."""

        configuration = self.get_store_configuration(store_ref)
        if dimension is api.ReplicaSeparationDimension.STORE:
            return configuration.store_uuid
        if dimension is api.ReplicaSeparationDimension.HOST:
            return configuration.store_host_uuid or ("unknown_host",)
        if dimension is api.ReplicaSeparationDimension.DEVICE:
            return configuration.store_device_uuid or ("unknown_device",)
        if dimension is api.ReplicaSeparationDimension.FAILURE_DOMAIN:
            return configuration.store_failure_domain or ("unknown_failure_domain",)
        return configuration.store_region or ("unknown_region",)

    def _separated_copy_capacity(
        self,
        records: Iterable[api.ReplicaRecord],
        policy: api.ReplicationPolicy | api.BackupPolicy,
    ) -> int:
        """Count policy-eligible copies after per-bucket copy limits."""

        records = tuple(records)
        if not records:
            return 0
        capacities = [len(records)]
        for dimension in policy.distinct_by:
            counts = Counter(
                self._policy_bucket(record.location.store_ref, dimension)
                for record in records
            )
            capacities.append(
                sum(
                    min(count, policy.max_copies_per_bucket)
                    for count in counts.values()
                )
            )
        return min(capacities)

    def _record_is_readable(self, record: api.ReplicaRecord) -> bool:
        """Return whether state and Store status currently permit reading."""

        if record.state not in {
            api.ReplicaState.PRESENT,
            api.ReplicaState.UNVERIFIED,
            api.ReplicaState.VERIFIED,
        }:
            return False
        try:
            if not self.status(record.location.store_ref).available:
                return False
            asset = self.get_digital_asset_record(record.digital_asset_id)
            return self.stat(record.location).size == asset.size_bytes
        except api.StorageError:
            return False

    def _assess_policy(
        self,
        digital_asset_id: api.DigitalAssetID,
        policy: api.ReplicationPolicy | api.BackupPolicy,
    ) -> api.StoragePolicyAssessment:
        """Assess one policy against eligible, separated Replica claims."""

        self.get_digital_asset_record(digital_asset_id)
        records = tuple(
            self.iter_replica_records(
                digital_asset_id=digital_asset_id,
                mode=policy.mode,
            )
        )
        present = tuple(
            record for record in records if self._record_is_readable(record)
        )
        healthy = tuple(
            record
            for record in present
            if record.state is api.ReplicaState.VERIFIED
            and self._store_satisfies_policy(record.location.store_ref, policy)
        )
        capacity = self._separated_copy_capacity(healthy, policy)
        errors: list[str] = []
        ineligible = tuple(
            record.replica_id
            for record in present
            if not self._store_satisfies_policy(record.location.store_ref, policy)
        )
        if ineligible:
            errors.append(
                "Replicas fail Store policy constraints: "
                + ", ".join(str(value) for value in ineligible)
            )
        return api.StoragePolicyAssessment(
            digital_asset_id,
            policy.name,
            policy.mode,
            present_replica_ids=tuple(record.replica_id for record in present),
            healthy_replica_ids=tuple(record.replica_id for record in healthy),
            meets_minimum=capacity >= policy.min_copies,
            meets_target=capacity >= policy.effective_target_copies,
            errors=tuple(errors),
        )

    def _plan_destination_stores(
        self,
        policy: api.ReplicationPolicy | api.BackupPolicy,
        existing: tuple[api.ReplicaRecord, ...],
        needed: int,
        *,
        expected_size: int | None = None,
        excluded_store_refs: set[api.StoreUUID] | None = None,
    ) -> tuple[api.StoreUUID, ...]:
        """Select writable policy-compliant Stores without mutating state."""

        if needed <= 0:
            return ()
        occupied = {record.location.store_ref for record in existing}
        occupied.update(excluded_store_refs or ())
        configurations = list(self.iter_store_configurations())
        configurations.sort(
            key=lambda configuration: (
                -len(policy.preferred_store_tags & set(configuration.store_tags)),
                configuration.store_uuid != self._default_store_ref,
                configuration.store_name,
                str(configuration.store_uuid),
            )
        )
        selected_store_refs = [record.location.store_ref for record in existing]
        selected_refs: list[api.StoreUUID] = []
        for configuration in configurations:
            store_ref = configuration.store_uuid
            if store_ref in occupied or not self._store_satisfies_policy(
                store_ref,
                policy,
            ):
                continue
            try:
                characteristics = self.characteristics(store_ref)
                if (
                    characteristics.recommended_write_usage
                    is api.StorageWriteUsage.ARCHIVAL_SNAPSHOT
                    and policy.mode is not api.ReplicaMode.ARCHIVE
                ):
                    continue
                self._require_writable_destination(
                    store_ref,
                    policy.mode,
                    expected_size=expected_size,
                )
            except api.StorageError:
                continue
            if any(
                sum(
                    self._policy_bucket(selected_store_ref, dimension)
                    == self._policy_bucket(store_ref, dimension)
                    for selected_store_ref in selected_store_refs
                )
                >= policy.max_copies_per_bucket
                for dimension in policy.distinct_by
            ):
                continue
            selected_refs.append(store_ref)
            occupied.add(store_ref)
            selected_store_refs.append(store_ref)
            if len(selected_refs) == needed:
                break
        return tuple(selected_refs)

    def _plan_recreation_branch(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        visiting: frozenset[api.DigitalAssetID],
        memo: dict[api.DigitalAssetID, _RecreationBranch],
    ) -> _RecreationBranch:
        """Select an exact route for one currently unavailable Asset."""

        if digital_asset_id in visiting:
            return _RecreationBranch(
                False,
                unavailable_digital_asset_ids=frozenset({digital_asset_id}),
                warnings=(
                    f"recreation planning encountered a cycle at Asset "
                    f"{digital_asset_id}",
                ),
            )
        cached = memo.get(digital_asset_id)
        if cached is not None:
            return cached
        if self._asset_has_readable_replica(digital_asset_id):
            branch = _RecreationBranch(
                True,
                available_digital_asset_ids=frozenset({digital_asset_id}),
            )
            memo[digital_asset_id] = branch
            return branch

        candidates = tuple(
            self.iter_digital_asset_derivation_records(
                result_digital_asset_id=digital_asset_id,
                exact_only=True,
            )
        )
        if not candidates:
            branch = _RecreationBranch(
                False,
                unavailable_digital_asset_ids=frozenset({digital_asset_id}),
                warnings=(
                    f"Asset {digital_asset_id} has no complete exact derivation recipe",
                ),
            )
            memo[digital_asset_id] = branch
            return branch

        next_visiting = visiting | {digital_asset_id}
        viable: list[tuple[api.DigitalAssetDerivationRecord, _RecreationBranch]] = []
        unavailable_ids: set[api.DigitalAssetID] = {digital_asset_id}
        failed_warnings: list[str] = []
        for candidate in candidates:
            attempt = self._plan_recreation_derivation(
                candidate,
                visiting=next_visiting,
                memo=memo,
            )
            if attempt.viable:
                viable.append((candidate, attempt))
            else:
                unavailable_ids.update(attempt.unavailable_digital_asset_ids)
                failed_warnings.extend(attempt.warnings)

        if not viable:
            branch = _RecreationBranch(
                False,
                unavailable_digital_asset_ids=frozenset(unavailable_ids),
                warnings=tuple(failed_warnings),
            )
            memo[digital_asset_id] = branch
            return branch

        viable.sort(
            key=lambda item: (
                len(item[1].steps),
                item[0].digital_asset_derivation_id,
            )
        )
        selected_record, selected = viable[0]
        alternatives = list(selected.alternative_derivation_ids)
        alternatives.extend(
            record.digital_asset_derivation_id for record, _ in viable[1:]
        )
        branch = dataclasses.replace(
            selected,
            selected_derivation_id=(selected_record.digital_asset_derivation_id),
            alternative_derivation_ids=tuple(dict.fromkeys(alternatives)),
            warnings=tuple(dict.fromkeys(selected.warnings + tuple(failed_warnings))),
        )
        memo[digital_asset_id] = branch
        return branch

    def _plan_recreation_derivation(
        self,
        record: api.DigitalAssetDerivationRecord,
        *,
        visiting: frozenset[api.DigitalAssetID],
        memo: dict[api.DigitalAssetID, _RecreationBranch],
    ) -> _RecreationBranch:
        """Plan every managed prerequisite of one exact derivation recipe."""

        recipe = record.declaration.recipe
        if recipe is None or not record.can_recreate_exactly:
            return _RecreationBranch(
                False,
                warnings=(
                    f"derivation {record.digital_asset_derivation_id} is not "
                    "a complete exact recipe",
                ),
            )

        prerequisite_branches: list[_RecreationBranch] = []
        unavailable_ids: set[api.DigitalAssetID] = set()
        warnings: list[str] = []
        for source_id in sorted(
            self._source_asset_ids(
                record,
                include_recipe_artifacts=False,
            )
        ):
            source_branch = self._plan_recreation_branch(
                source_id,
                visiting=visiting,
                memo=memo,
            )
            if source_branch.viable:
                prerequisite_branches.append(source_branch)
            else:
                unavailable_ids.update(source_branch.unavailable_digital_asset_ids)
                warnings.extend(source_branch.warnings)

        artifacts = (
            () if recipe.executor is None else (recipe.executor,)
        ) + recipe.dependencies
        for artifact in artifacts:
            managed_branch: _RecreationBranch | None = None
            if artifact.digital_asset_id is not None:
                managed_branch = self._plan_recreation_branch(
                    artifact.digital_asset_id,
                    visiting=visiting,
                    memo=memo,
                )
                if managed_branch.viable:
                    prerequisite_branches.append(managed_branch)
                    continue
            if self._external_recipe_artifact_is_available(artifact):
                if managed_branch is not None:
                    warnings.append(
                        f"derivation {record.digital_asset_derivation_id} "
                        f"will retrieve artefact {artifact.name!r} by URI "
                        "because its managed Asset is unavailable"
                    )
                continue
            if managed_branch is not None:
                unavailable_ids.update(managed_branch.unavailable_digital_asset_ids)
                warnings.extend(managed_branch.warnings)
            warnings.append(
                f"derivation {record.digital_asset_derivation_id} requires "
                f"unavailable artefact {artifact.name!r}"
            )

        if unavailable_ids or any(
            "requires unavailable artefact" in warning for warning in warnings
        ):
            return _RecreationBranch(
                False,
                unavailable_digital_asset_ids=frozenset(unavailable_ids),
                warnings=tuple(dict.fromkeys(warnings)),
            )

        steps: list[api.DigitalAssetDerivationRecord] = []
        seen_steps: set[api.DigitalAssetDerivationID] = set()
        available_ids: set[api.DigitalAssetID] = set()
        alternatives: list[api.DigitalAssetDerivationID] = []
        for prerequisite in prerequisite_branches:
            available_ids.update(prerequisite.available_digital_asset_ids)
            warnings.extend(prerequisite.warnings)
            alternatives.extend(prerequisite.alternative_derivation_ids)
            for step in prerequisite.steps:
                if step.digital_asset_derivation_id in seen_steps:
                    continue
                steps.append(step)
                seen_steps.add(step.digital_asset_derivation_id)
        if record.digital_asset_derivation_id not in seen_steps:
            steps.append(record)

        return _RecreationBranch(
            True,
            steps=tuple(steps),
            available_digital_asset_ids=frozenset(available_ids),
            selected_derivation_id=record.digital_asset_derivation_id,
            alternative_derivation_ids=tuple(dict.fromkeys(alternatives)),
            warnings=tuple(dict.fromkeys(warnings)),
        )

    def _external_recipe_artifact_is_available(
        self,
        artifact: api.ReproductionRecipeArtifactReference,
    ) -> bool:
        """Check a pinned URI artefact through the configured resolver."""

        resolver = self._artifact_resolver
        if artifact.uri is None or resolver is None:
            return False
        try:
            return resolver.is_available(artifact)
        except Exception:
            return False

    def _source_asset_ids(
        self,
        record: api.DigitalAssetDerivationRecord,
        *,
        include_recipe_artifacts: bool = True,
    ) -> set[api.DigitalAssetID]:
        """Expand derivation sources and pinned recipe Assets."""

        source_ids: set[api.DigitalAssetID] = set()
        for source in record.declaration.sources:
            if source.digital_asset_id is not None:
                source_ids.add(source.digital_asset_id)
            elif source.composite_digital_asset_id is not None:
                composite = self.get_composite_digital_asset_record(
                    source.composite_digital_asset_id
                )
                source_ids.update(
                    member.digital_asset_id for member in composite.members
                )
        recipe = record.declaration.recipe
        if recipe is not None:
            source_ids.update(input_.digital_asset_id for input_ in recipe.inputs)
            if include_recipe_artifacts:
                artifacts = (
                    () if recipe.executor is None else (recipe.executor,)
                ) + recipe.dependencies
                source_ids.update(
                    artifact.digital_asset_id
                    for artifact in artifacts
                    if artifact.digital_asset_id is not None
                )
        return source_ids

    def _reject_derivation_cycle(
        self,
        result_digital_asset_id: api.DigitalAssetID,
        source_asset_ids: set[api.DigitalAssetID],
    ) -> None:
        """Reject a result-to-source edge that closes a provenance cycle."""

        adjacency: dict[api.DigitalAssetID, set[api.DigitalAssetID]] = {}
        for record in self.iter_digital_asset_derivation_records():
            adjacency.setdefault(
                record.declaration.result_digital_asset_id,
                set(),
            ).update(self._source_asset_ids(record))
        adjacency.setdefault(result_digital_asset_id, set()).update(source_asset_ids)

        def reaches_result(
            current: api.DigitalAssetID,
            visited: set[api.DigitalAssetID],
        ) -> bool:
            """Walk result-to-source edges looking for the proposed result."""

            if current == result_digital_asset_id:
                return True
            if current in visited:
                return False
            visited.add(current)
            return any(
                reaches_result(child, visited) for child in adjacency.get(current, ())
            )

        if any(reaches_result(source_id, set()) for source_id in source_asset_ids):
            raise api.StoragePreconditionFailed(
                "Digital Asset derivation would create a provenance cycle."
            )

    def _asset_has_readable_replica(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> bool:
        """Return whether any mode contains a currently readable Replica."""

        return any(
            self._record_is_readable(record)
            for record in self.iter_replica_records(digital_asset_id=digital_asset_id)
        )

    def _asset_is_recoverable_now(
        self,
        digital_asset_id: api.DigitalAssetID,
        visiting: set[api.DigitalAssetID],
    ) -> bool:
        """Return whether bytes are readable or exactly recreatable now."""

        if self._asset_has_readable_replica(digital_asset_id):
            return True
        if digital_asset_id in visiting:
            return False
        return any(
            self._derivation_is_recoverable(
                derivation,
                visiting | {digital_asset_id},
            )
            for derivation in self.iter_digital_asset_derivation_records(
                result_digital_asset_id=digital_asset_id,
                exact_only=True,
            )
        )

    def _derivation_is_recoverable(
        self,
        record: api.DigitalAssetDerivationRecord,
        visiting: set[api.DigitalAssetID],
    ) -> bool:
        """Return whether an exact recipe and all pinned inputs are reachable."""

        if not record.can_recreate_exactly:
            return False
        if not self._recipe_artifacts_are_recoverable(
            record,
            visiting,
            for_policy=False,
        ):
            return False
        return all(
            self._asset_is_recoverable_now(source_id, visiting)
            for source_id in self._source_asset_ids(
                record,
                include_recipe_artifacts=False,
            )
        )

    def _asset_policy_recoverable(
        self,
        digital_asset_id: api.DigitalAssetID,
        visiting: set[api.DigitalAssetID],
    ) -> bool:
        """Return whether policy retains or exactly recreates one input Asset."""

        if digital_asset_id in visiting:
            return False
        policies = self.resolve_effective_policies(digital_asset_id)
        if policies.replication.min_copies > 0 or policies.backup.min_copies > 0:
            return True
        if policies.replication.loss_action is not api.DigitalAssetLossAction.RECREATE:
            return False
        return any(
            self._recipe_artifacts_are_recoverable(
                record,
                visiting | {digital_asset_id},
                for_policy=True,
            )
            and all(
                self._asset_policy_recoverable(
                    source_id,
                    visiting | {digital_asset_id},
                )
                for source_id in self._source_asset_ids(
                    record,
                    include_recipe_artifacts=False,
                )
            )
            for record in self.iter_digital_asset_derivation_records(
                result_digital_asset_id=digital_asset_id,
                exact_only=True,
            )
        )

    def _validate_recreation_policy(
        self,
        digital_asset_id: api.DigitalAssetID,
        visiting: set[api.DigitalAssetID],
    ) -> None:
        """Require an exact recipe with policy-recoverable pinned inputs."""

        candidates = tuple(
            self.iter_digital_asset_derivation_records(
                result_digital_asset_id=digital_asset_id,
                exact_only=True,
            )
        )
        if not candidates or not any(
            self._recipe_artifacts_are_recoverable(
                record,
                visiting | {digital_asset_id},
                for_policy=True,
            )
            and all(
                self._asset_policy_recoverable(
                    source_id,
                    visiting | {digital_asset_id},
                )
                for source_id in self._source_asset_ids(
                    record,
                    include_recipe_artifacts=False,
                )
            )
            for record in candidates
        ):
            raise api.StoragePolicyUnsatisfied(
                "recreate-on-loss requires an exact complete derivation whose "
                "pinned inputs and artefacts remain recoverable."
            )

    def _recipe_artifacts_are_recoverable(
        self,
        record: api.DigitalAssetDerivationRecord,
        visiting: set[api.DigitalAssetID],
        *,
        for_policy: bool,
    ) -> bool:
        """Require a managed or resolver-verified route for every artefact."""

        recipe = record.declaration.recipe
        if recipe is None:
            return False
        artifacts = (
            () if recipe.executor is None else (recipe.executor,)
        ) + recipe.dependencies
        for artifact in artifacts:
            managed_available = False
            if artifact.digital_asset_id is not None:
                if for_policy:
                    managed_available = self._asset_policy_recoverable(
                        artifact.digital_asset_id,
                        visiting,
                    )
                else:
                    managed_available = self._asset_is_recoverable_now(
                        artifact.digital_asset_id,
                        visiting,
                    )
            if managed_available:
                continue
            if artifact.uri is None:
                return False
            resolver = self._artifact_resolver
            if resolver is None:
                return False
            try:
                if not resolver.is_available(artifact):
                    return False
            except Exception:
                return False
        return True


__all__ = ["_StorageManagerPolicySupportMixin"]
