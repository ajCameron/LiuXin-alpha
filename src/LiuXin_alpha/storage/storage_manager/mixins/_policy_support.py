"""
Provide shared policy, placement, reference, and recursive recreation mechanics.

Current readability and policy-retention feasibility are separate checks. Planning
builds branch values and caller-owned memo entries without executing recipes; first-
placement policy capture and Item-target assignment are explicit metadata mutations.
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
    Share placement, reference, and recreation mechanics across manager workflows.

    Helpers distinguish current state/status/size readability from policy-based retention
    feasibility. Most inspection and planning methods do not mutate domain records, but
    first-placement policy capture and Item-target assignment do. Recursive planning also fills
    caller-provided memo state; callers own broader locking and transaction scope.

    Example:
        >>> assessment = manager._assess_policy(asset_id, policy)  # doctest: +SKIP
    """

    def _require_store_factory(self) -> StoreFactory:
        """
        Return the configured Store constructor without invoking it. Missing configuration raises
        StoreUnsupportedOperation so callers can attach an already constructed facade instead.

        Example:
            >>> factory = manager._require_store_factory()  # doctest: +SKIP


        :return: Retained StoreFactory reference; None configuration raises.
        """

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
        """
        Resolve each non-None policy ID in replication-then-backup order. Missing references and
        other lookup failures propagate; definitions are not otherwise inspected.

        Example:
            >>> manager._validate_declared_policy_ids(replication_id, backup_id)  # doctest: +SKIP


        :param replication_policy_id: Optional registered replication identity to require.
        :param backup_policy_id: Optional registered backup identity to require.
        :return: None after all supplied references resolve.
        """

        if replication_policy_id is not None:
            self.get_replication_policy_record(replication_policy_id)
        if backup_policy_id is not None:
            self.get_backup_policy_record(backup_policy_id)

    def _validate_store_policy_references(
        self,
        configuration: api.StoreConfiguration,
    ) -> None:
        """
        Delegate validation of the two default policy IDs retained by a Store configuration. This
        does not probe the Store or assign policies to an Asset.

        Example:
            >>> manager._validate_store_policy_references(configuration)  # doctest: +SKIP


        :param configuration: Store configuration carrying optional replication and backup default IDs.
        :return: None after delegated reference validation succeeds.
        """

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
        """
        Resolve one Store configuration and project its default replication/backup IDs without
        capturing them or validating the referenced definitions here.

        Example:
            >>> replication_id, backup_id = manager._placement_policy_ids(store_uuid)  # doctest: +SKIP


        :param store_ref: Configured Store UUID whose first-placement defaults are requested.
        :return: Pair of retained replication and backup IDs, each possibly None.
        """

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
        """
        Fill absent Asset policy references from supplied placement defaults only when no nondeleted
        Replica claim exists.

        The lock protects the claim scan and delegation. Existing explicit references take
        precedence, and unchanged choices return the original supplied Asset object. A change calls
        set_digital_asset_policies with the supplied revision, so that method owns
        reference/recreation validation and persistence. Tombstones alone do not prevent capture;
        this helper does not refresh the supplied Asset before comparison.

        Example:
            >>> asset = manager._capture_first_placement_policies(asset, replication_id, backup_id)  # doctest: +SKIP


        :param asset: Existing Asset record whose references and revision form the capture precondition.
        :param replication_policy_id: Placement default used only when the Asset reference is None.
        :param backup_policy_id: Backup placement default used only when the Asset reference is None.
        :return: Original Asset record if capture is unnecessary, otherwise the updated record from policy assignment.
        """

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
        """
        Capture the Asset iteration, resolve effective policies, and validate each RECREATE enum
        assignment with a fresh visiting set.

        The helper adds no lock or transaction of its own. Validation failures propagate to the
        caller, which may be inspecting a temporarily installed candidate policy or Asset
        assignment.

        Example:
            >>> manager._validate_all_recreation_policies()  # doctest: +SKIP


        :return: None if every effective RECREATE policy passes recursive feasibility validation.
        """

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
        """
        Require positive int-convertibility of the Item ID and nonblank role text, then replace the
        exact reference-map entry under the metadata lock/transaction.

        Original ID and role values are retained rather than converted or stripped. The helper does
        not validate target kind, target existence, or Item catalogue existence, and has no revision
        precondition.

        Example:
            >>> manager._set_item_target(item_id, "cover", "digital_asset", asset_id)  # doctest: +SKIP


        :param item_id: Item key checked with int(item_id) <= 0, then retained unchanged.
        :param role: Nonblank exact role key retained with its original spelling.
        :param kind: Target-kind discriminator stored without additional validation.
        :param target_id: Atomic or Composite target identity stored without lookup here.
        :return: None after replacing the target tuple; conversion, validation, mapping, and transaction errors propagate.
        """

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
        """
        Scan derivation results, atomic sources, recipe inputs, and managed executor/dependency IDs
        for the requested Asset.

        Return at the first match without validating recipes or expanding Composite-source
        membership. The caller owns the lock implied by the name; this method does not acquire it.

        Example:
            >>> referenced = manager._asset_has_derivation_reference_locked(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Atomic Asset identity being considered.
        :return: True when any directly inspected provenance field references the Asset, otherwise False.
        """

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
        """
        Check configured mode support, required tags, and absence of forbidden tags.

        Only StoreConfigurationNotFound becomes False. Preferred tags, physical availability,
        writability, byte health, and separation are not checked here; other lookup or
        value-operation errors propagate.

        Example:
            >>> eligible = manager._store_satisfies_policy(store_uuid, policy)  # doctest: +SKIP


        :param store_ref: Configured Store UUID to examine.
        :param policy: Replication or backup definition supplying mode and tag constraints.
        :return: True when configuration satisfies these mode/tag constraints.
        """

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
        """
        Project one configured separation value, using a shared sentinel for missing topology data.

        STORE, HOST, DEVICE, and FAILURE_DOMAIN are matched by enum identity. All other values
        follow the region branch. False host/device/domain/region values map to their common unknown
        bucket, so missing information is not treated as independent placement. No physical topology
        probe occurs.

        Example:
            >>> bucket = manager._policy_bucket(store_uuid, api.ReplicaSeparationDimension.HOST)  # doctest: +SKIP


        :param store_ref: Configured Store UUID whose topology declaration is read.
        :param dimension: Expected separation enum; unmatched values use the region branch without coercion.
        :return: Declared bucket value or a shared unknown sentinel; configuration lookup errors propagate.
        """

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
        """
        Materialize records and return the smallest of their count and each independently capped
        dimension total.

        For each dimension, count records per bucket and sum min(bucket count,
        max_copies_per_bucket). The minimum across these totals is returned; this does not solve for
        a jointly compatible subset across all dimensions. Inputs are not deduplicated or checked
        for policy eligibility/readability here.

        Example:
            >>> capacity = manager._separated_copy_capacity(records, policy)  # doctest: +SKIP


        :param records: Claims already chosen by the caller; consumed once into a tuple.
        :param policy: Separation dimensions and per-bucket count limit to apply.
        :return: Zero for no records, otherwise the minimum of the record count and capped per-dimension totals.
        """

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
        """
        Require PRESENT/UNVERIFIED/VERIFIED state, an available Store, and stat size equal to the
        owning Asset's expected size.

        StorageError from status, Asset lookup, or stat becomes False; other errors propagate. State
        membership uses equality. The helper does not open a reader, compare digests, require a
        writable Store, or update the observation.

        Example:
            >>> readable = manager._record_is_readable(replica)  # doctest: +SKIP


        :param record: Replica claim whose recorded state and current status/size are inspected.
        :return: True when the selected readability checks pass, otherwise False.
        """

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
        """
        Resolve the Asset and capture claims in the policy mode, then classify current readability
        and recorded policy eligibility.

        Present claims pass _record_is_readable. Healthy claims additionally carry the VERIFIED enum
        singleton and satisfy configuration mode/tags. Thresholds use _separated_copy_capacity,
        while diagnostics list present claims that fail configuration rules. Missing or unverified
        copies do not automatically produce diagnostics, and errors do not independently override
        threshold flags.

        Example:
            >>> assessment = manager._assess_policy(asset_id, policy)  # doctest: +SKIP


        :param digital_asset_id: Atomic Asset identity being considered.
        :param policy: Resolved definition supplying mode, tags, separation, and count thresholds.
        :return: Assessment with present/healthy IDs, count-threshold results, and configuration diagnostics.
        """

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
        """
        Choose unoccupied policy-eligible destinations in preferred-tag, default-Store, name, then
        UUID order.

        Nonpositive needed returns immediately. Existing and explicitly excluded Store UUIDs are
        occupied. Candidate checks enforce configuration constraints, archival-snapshot mode
        suitability, and writable/size support through shared hooks; StorageError in
        characteristics/writability checks skips a candidate. Other configuration, sorting, or
        bucket failures propagate.

        Each selected destination must stay below every bucket limit when combined with existing and
        already selected Store references. The method returns as soon as needed destinations are
        selected, or the available subset if exhausted. It reserves nothing and executes no
        publication.

        Example:
            >>> destinations = manager._plan_destination_stores(policy, healthy, 2, expected_size=asset.size_bytes)  # doctest: +SKIP


        :param policy: Mode/tag/separation constraints and preferred labels used to rank candidates.
        :param existing: Existing claims contributing occupied Stores and bucket counts.
        :param needed: Requested additional destination count; values at or below zero return empty.
        :param expected_size: Optional expected object size for the writable-destination preflight.
        :param excluded_store_refs: Additional occupied/excluded Store UUIDs, or None.
        :return: Ordered tuple of selected Store UUIDs, possibly shorter than requested.
        """

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
        """
        Plan a currently readable or recursively recreatable route for one Asset, memoizing by Asset
        ID.

        Path-cycle detection precedes memo reuse and readability testing. Readable Assets produce
        viable branches without recreation steps. Otherwise all exact derivations are attempted;
        viable routes rank by step count then derivation ID. Alternatives and selected warnings are
        deduplicated, including warnings from failed alternatives.

        Unavailable branches collect missing IDs and diagnostics. The supplied memo is mutated for
        completed outcomes and is keyed only by Asset ID, so the caller owns its scope. No bytes are
        created, and repository/helper errors propagate.

        Example:
            >>> branch = manager._plan_recreation_branch(asset_id, visiting=frozenset(), memo={})  # doctest: +SKIP


        :param digital_asset_id: Atomic Asset identity being considered.
        :param visiting: Immutable Asset IDs already on the active recursion path.
        :param memo: Caller-owned mapping of Asset IDs to previously planned branch outcomes.
        :return: Selected viable branch or an unavailable/cyclic outcome with supporting diagnostics.
        """

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
        """
        Plan managed source and artifact prerequisites for a declared complete exact recipe.

        Source IDs are expanded and sorted before recursive planning. A managed artifact route takes
        precedence; an available URI can substitute when the managed route is unavailable, with a
        warning. Any unresolved prerequisite or unavailable-artifact diagnostic makes the branch
        nonviable.

        For a viable recipe, prerequisite steps are combined in encounter order and deduplicated by
        derivation ID, then this recipe is appended. Available source IDs, alternatives, and
        warnings are collected; the memo is shared with recursive calls. Nothing is executed or
        materialized.

        Example:
            >>> branch = manager._plan_recreation_derivation(derivation, visiting=frozenset(), memo={})  # doctest: +SKIP


        :param record: Candidate derivation whose recipe and prerequisites are inspected.
        :param visiting: Active recursion-path IDs forwarded to prerequisite branch planning.
        :param memo: Shared caller-owned cache populated by recursive Asset planning.
        :return: Viable ordered prerequisite/recipe branch, or a nonviable outcome with missing IDs and warnings.
        """

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
        """
        Ask the configured resolver about an artifact with a supplied URI.

        Missing URI or resolver returns False. Resolver Exception failures become False while
        BaseException subclasses propagate; the resolver's normal result is returned without bool
        coercion. No independent URI or digest check is performed here.

        Example:
            >>> available = manager._external_recipe_artifact_is_available(artifact)  # doctest: +SKIP


        :param artifact: Pinned recipe artifact passed unchanged to the configured availability resolver.
        :return: Resolver availability result, or False for missing prerequisites or caught resolver failure.
        """

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
        """
        Collect atomic source IDs, every Composite-source member, and pinned recipe inputs into a
        set.

        Managed executor/dependency IDs are also included unless disabled. Composite expansion
        includes optional memberships and requires the Composite record; errors propagate. The set
        removes repeated identities without preserving order and performs no current-readability
        check.

        Example:
            >>> sources = manager._source_asset_ids(derivation, include_recipe_artifacts=False)  # doctest: +SKIP


        :param record: Derivation whose source relationships and recipe references are expanded.
        :param include_recipe_artifacts: Whether managed executor and dependency IDs join the source set.
        :return: New set of atomic Asset IDs referenced by the requested source categories.
        """

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
        """
        Build result-to-source adjacency from all derivations, add proposed edges, and reject any
        source route back to the result.

        Source expansion includes Composite members and managed recipe artifacts. Each proposed
        source starts a fresh depth-first visited set; existing unrelated cycles terminate through
        visited checks. The method does not store the proposed derivation, and expansion/repository
        errors propagate.

        Example:
            >>> manager._reject_derivation_cycle(result_id, source_ids)  # doctest: +SKIP


        :param result_digital_asset_id: Proposed result Asset whose reachability must not close a cycle.
        :param source_asset_ids: Proposed atomic prerequisites added to the temporary adjacency map.
        :return: None when proposed edges do not reach the result; a cycle raises StoragePreconditionFailed.
        """

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
            """
            Walk the captured adjacency toward the proposed result, mutating the supplied visited
            set.

            Result equality returns True before the visited check. Previously visited nodes return
            False, and child traversal stops at the first route reaching the result.

            Example:
                >>> reaches_result(source_id, set())  # doctest: +SKIP


            :param current: Asset node being explored in the enclosing temporary graph.
            :param visited: Mutable nodes already explored for this root-source traversal.
            :return: True if a result-to-source path reaches the enclosing proposed result ID.
            """

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
        """
        Return whether any claim in any mode passes the current state/status/size readability
        helper.

        Iteration stops at the first success. This method does not separately require the Asset to
        exist, verify digests, or suppress repository iteration errors.

        Example:
            >>> available = manager._asset_has_readable_replica(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Atomic Asset identity being considered.
        :return: True for at least one readable claim, otherwise False.
        """

        return any(
            self._record_is_readable(record)
            for record in self.iter_replica_records(digital_asset_id=digital_asset_id)
        )

    def _asset_is_recoverable_now(
        self,
        digital_asset_id: api.DigitalAssetID,
        visiting: set[api.DigitalAssetID],
    ) -> bool:
        """
        Accept a currently readable claim or recursively find an exact derivation with reachable
        prerequisites.

        Readability is checked before cycle detection, so retained readable bytes can terminate a
        path even when the Asset is already visiting. Otherwise visited IDs fail and exact
        derivations are tried with a copied path set. Policy minimum counts are not used as proof of
        current availability here.

        Example:
            >>> recoverable = manager._asset_is_recoverable_now(asset_id, set())  # doctest: +SKIP


        :param digital_asset_id: Atomic Asset identity being considered.
        :param visiting: Active Asset path used to terminate recursive recipe searches; not mutated here.
        :return: True for current readable bytes or a reachable exact route, otherwise False.
        """

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
        """
        Require declared exact recreatability, reachable recipe artifacts, and current
        recoverability of every expanded source.

        Artifact routes are checked before source recursion. Managed artifacts use current
        recoverability, with configured URI resolution as fallback. An empty source set passes all
        once artifact checks succeed; no recipe execution or fresh source hashing occurs.

        Example:
            >>> recoverable = manager._derivation_is_recoverable(derivation, set())  # doctest: +SKIP


        :param record: Derivation whose declared exact recipe and prerequisites are inspected.
        :param visiting: Active Asset path forwarded to artifact and source recovery checks.
        :return: True when the declaration and all required recovery routes pass, otherwise False.
        """

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
        """
        Check whether policy requires retaining an input or provides a recursively feasible exact
        recreation route.

        Visited IDs fail before any policy lookup. A positive replication or backup minimum succeeds
        without checking current bytes. Zero-minimum policies require the RECREATE enum action and
        at least one exact derivation whose artifacts and sources remain policy-recoverable. The
        path set is extended through copies rather than mutated.

        Example:
            >>> retained = manager._asset_policy_recoverable(asset_id, set())  # doctest: +SKIP


        :param digital_asset_id: Atomic Asset identity being considered.
        :param visiting: Asset IDs already on the policy-dependency path.
        :return: True for a retaining minimum or feasible recursive recreation policy, otherwise False.
        """

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
        """
        Require at least one exact derivation with policy-recoverable artifacts and expanded
        sources.

        Candidate derivations are captured before checking routes. The Asset joins the visiting path
        for each attempt. Feasibility relies on prerequisite policies and resolver evidence rather
        than proving all bytes are currently present. No suitable route raises
        StoragePolicyUnsatisfied; other lookup failures propagate.

        Example:
            >>> manager._validate_recreation_policy(asset_id, set())  # doctest: +SKIP


        :param digital_asset_id: Asset whose recreate-on-loss assignment is being validated.
        :param visiting: Existing policy recursion path, extended with this Asset for route checks.
        :return: None when a suitable exact route exists; otherwise raises StoragePolicyUnsatisfied.
        """

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
        """
        Require a managed recovery route or resolver-reported URI availability for each
        executor/dependency artifact.

        The flag selects policy-retention feasibility or current recoverability for managed IDs. A
        successful managed route skips URI checks. Otherwise missing URI/resolver or a
        false/caught-Exception resolver result rejects immediately. Managed lookup failures and
        resolver BaseException failures propagate. A missing recipe rejects; an existing recipe with
        no artifacts passes.

        Example:
            >>> available = manager._recipe_artifacts_are_recoverable(derivation, set(), for_policy=True)  # doctest: +SKIP


        :param record: Derivation supplying an optional recipe, executor, and dependencies.
        :param visiting: Active Asset path forwarded to managed recovery checks.
        :param for_policy: True for policy-based retention feasibility, False for current managed recoverability.
        :return: True when every artifact has an accepted route, otherwise False.
        """

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
