"""
Register provenance, build directed graph inventories, and propose exact Asset recreation.

The mixin composes manager state and policy-support mechanics without executing
recipes. Graph queries retain stable first-encounter ordering; registration and
forgetting mutate metadata through the supplied transaction boundary.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator, Mapping
from typing import override

from LiuXin_alpha.storage.api import errors as storage_errors
from LiuXin_alpha.storage.api import storage_manager_api as manager_api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class _DerivationGraphTraversal:
    """
    Accumulate ordered provenance nodes and records across independent breadth-first walks.

    Each walk starts at the same root with its own visited set. Inventories, deduplication sets, and
    the truncation flag persist across walks, allowing ancestor results to precede descendants.
    Input indexes are retained references and must stay coherent while traversing.

    Example:
        >>> traversal = _DerivationGraphTraversal(  # doctest: +SKIP
        ...     root_id, max_depth=2, sources_by_derivation=sources,
        ...     by_result=results, by_source=inputs,
        ... )
    """

    def __init__(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
        *,
        max_depth: int | None,
        sources_by_derivation: Mapping[
            manager_api.DigitalAssetDerivationID,
            tuple[manager_api.DigitalAssetID, ...],
        ],
        by_result: Mapping[
            manager_api.DigitalAssetID,
            list[manager_api.DigitalAssetDerivationRecord],
        ],
        by_source: Mapping[
            manager_api.DigitalAssetID,
            list[manager_api.DigitalAssetDerivationRecord],
        ],
    ) -> None:
        """
        Retain the traversal indexes and initialize inventories with the root Asset only.

        No identity, depth, direction, or index-consistency checks occur. The three mappings are not
        copied, while mutable result lists and deduplication sets are newly allocated.

        Example:
            >>> traversal = _DerivationGraphTraversal(  # doctest: +SKIP
            ...     root_id, max_depth=None, sources_by_derivation=sources,
            ...     by_result=results, by_source=inputs,
            ... )


        :param digital_asset_id: Initial atomic root placed first in the node inventory; not validated by this helper.
        :param max_depth: Optional depth cutoff retained for comparison during each walk.
        :param sources_by_derivation: Retained mapping from derivation ID to ordered expanded atomic source IDs.
        :param by_result: Retained adjacency mapping from result Asset ID to ordered provenance records.
        :param by_source: Retained adjacency mapping from source Asset ID to ordered provenance records.
        :return: None after initializing mutable traversal state.
        """

        self.digital_asset_id = digital_asset_id
        self.max_depth = max_depth
        self.sources_by_derivation = sources_by_derivation
        self.by_result = by_result
        self.by_source = by_source
        self.asset_ids = [digital_asset_id]
        self.seen_asset_ids = {digital_asset_id}
        self.composite_ids: list[manager_api.CompositeDigitalAssetID] = []
        self.seen_composite_ids: set[manager_api.CompositeDigitalAssetID] = set()
        self.records: list[manager_api.DigitalAssetDerivationRecord] = []
        self.seen_derivation_ids: set[manager_api.DigitalAssetDerivationID] = set()
        self.truncated = False

    def walk(
        self,
        direction: manager_api.DigitalAssetDerivationGraphDirection,
    ) -> None:
        """
        Traverse one direction from the root and append first-encounter evidence to shared
        inventories.

        Queue order and supplied adjacency order determine breadth-first discovery. Only the
        ANCESTORS enum singleton selects result-to-source edges; callers must supply a concrete
        direction, as other values take the descendant branch. Each call creates a fresh queue and
        visited set while retaining earlier inventories.

        Any adjacency at or beyond the depth cutoff marks truncation, including previously recorded
        edges. Remembered descendant records retain all their provenance sources even though only
        their result IDs are added to the atomic inventory. Inconsistent source indexes can raise
        after partial state accumulation.

        Example:
            >>> traversal.walk(manager_api.DigitalAssetDerivationGraphDirection.ANCESTORS)  # doctest: +SKIP


        :param direction: Single ancestor or descendant direction; this helper does not expand BOTH or coerce strings.
        :return: None after mutating node/record inventories and possibly setting truncated.
        """

        queue: deque[tuple[manager_api.DigitalAssetID, int]] = deque(
            ((self.digital_asset_id, 0),)
        )
        walked: set[manager_api.DigitalAssetID] = set()
        while queue:
            current_id, depth = queue.popleft()
            if current_id in walked:
                continue
            walked.add(current_id)
            ancestors = (
                direction is manager_api.DigitalAssetDerivationGraphDirection.ANCESTORS
            )
            adjacent = (
                self.by_result.get(current_id, ())
                if ancestors
                else self.by_source.get(current_id, ())
            )
            if self.max_depth is not None and depth >= self.max_depth:
                if adjacent:
                    self.truncated = True
                continue
            for record in adjacent:
                self._remember_record(record)
                derivation_id = record.digital_asset_derivation_id
                next_ids = (
                    self.sources_by_derivation[derivation_id]
                    if ancestors
                    else (record.declaration.result_digital_asset_id,)
                )
                for next_id in next_ids:
                    if next_id not in self.seen_asset_ids:
                        self.asset_ids.append(next_id)
                        self.seen_asset_ids.add(next_id)
                    if next_id not in walked:
                        queue.append((next_id, depth + 1))

    def _remember_record(
        self,
        record: manager_api.DigitalAssetDerivationRecord,
    ) -> None:
        """
        Append a previously unseen derivation and its first-encounter Composite source IDs.

        Deduplication uses the derivation ID, not record equality. Repeated IDs return before
        inspecting sources. This method does not expand Composite membership or add atomic
        source/result IDs; it retains the record reference.

        Example:
            >>> traversal._remember_record(record)  # doctest: +SKIP


        :param record: Provenance record whose ID and directly referenced Composites should be remembered.
        :return: None after extending inventories, or immediately when the derivation ID was already seen.
        """

        derivation_id = record.digital_asset_derivation_id
        if derivation_id in self.seen_derivation_ids:
            return
        self.records.append(record)
        self.seen_derivation_ids.add(derivation_id)
        for source in record.declaration.sources:
            composite_id = source.composite_digital_asset_id
            if composite_id is not None and composite_id not in self.seen_composite_ids:
                self.composite_ids.append(composite_id)
                self.seen_composite_ids.add(composite_id)


class DigitalAssetDerivationRegistryMixin(_StorageManagerState):
    """
    Implement provenance registration, filtered snapshots, graph traversal, and replay proposals.

    References, identity evidence, and cycles are checked without executing recipes or proving
    current readability. Registration validates before its metadata transaction; graph queries
    combine a record snapshot with later source expansion. Broader state/persistence and recursive
    policy mechanics come from the composed manager.

    Example:
        >>> graph = manager.get_derivation_graph(asset_id, direction="ancestors")  # doctest: +SKIP
    """

    @override
    def record_digital_asset_derivation(
        self,
        declaration: manager_api.DigitalAssetDerivationDeclaration,
    ) -> manager_api.DigitalAssetDerivationRecord:
        """
        Validate provenance and allocate a fresh metadata record for an existing result Asset.

        Read the result first, resolve atomic/Composite sources, and require complete recipes to pin
        every expanded provenance member. Recipe input sizes must match, with at least one matching
        digest algorithm and no disagreement on any overlap. Managed artefact digests receive the
        same overlap check. Exact recipes additionally require the result size and every stated
        output digest to match; extra registered digests are allowed.

        Cycle checks include recipe inputs and managed artefact Assets. These lookups/checks precede
        the lock and metadata transaction and are not revalidated inside it. Registration allocates
        an ID and revision without declaration deduplication, physical reads, URI probing, or recipe
        execution. Persistence/failure rollback follows the supplied metadata adapter.

        Example:
            >>> record = manager.record_digital_asset_derivation(declaration)  # doctest: +SKIP


        :param declaration: Retained result/source assertion and optional recipe, validated against current manager records.
        :return: Freshly stored derivation record; missing references, identity mismatch, cycles, and adapter failures propagate.
        """

        result = self.get_digital_asset_record(declaration.result_digital_asset_id)
        source_asset_ids: set[manager_api.DigitalAssetID] = set()
        for source in declaration.sources:
            if source.digital_asset_id is not None:
                self.get_digital_asset_record(source.digital_asset_id)
                source_asset_ids.add(source.digital_asset_id)
                continue
            if source.composite_digital_asset_id is None:
                raise storage_errors.StoragePreconditionFailed(
                    "derivation source has no Asset identity."
                )
            composite = self.get_composite_digital_asset_record(
                source.composite_digital_asset_id
            )
            source_asset_ids.update(
                member.digital_asset_id for member in composite.members
            )

        recipe = declaration.recipe
        if recipe is not None:
            recipe_asset_ids = {input_.digital_asset_id for input_ in recipe.inputs}
            if recipe.complete and not source_asset_ids <= recipe_asset_ids:
                missing = sorted(source_asset_ids - recipe_asset_ids)
                raise storage_errors.StoragePreconditionFailed(
                    "complete recipe does not pin every provenance source: "
                    + ", ".join(str(value) for value in missing)
                )
            for input_ in recipe.inputs:
                input_record = self.get_digital_asset_record(input_.digital_asset_id)
                self._require_same_identity(
                    input_record,
                    input_.size_bytes,
                    input_.digests,
                )
            artifacts = (
                () if recipe.executor is None else (recipe.executor,)
            ) + recipe.dependencies
            for artifact in artifacts:
                if artifact.digital_asset_id is None:
                    continue
                artifact_record = self.get_digital_asset_record(
                    artifact.digital_asset_id
                )
                self._require_same_identity(
                    artifact_record,
                    artifact_record.size_bytes,
                    (artifact.digest,),
                )
                recipe_asset_ids.add(artifact.digital_asset_id)
            source_asset_ids.update(recipe_asset_ids)
            if recipe.can_recreate_exactly:
                if recipe.expected_output_size != result.size_bytes:
                    raise storage_errors.StorageIntegrityError(
                        "exact recipe output size differs from the result Asset."
                    )
                self._require_expected_digests(
                    recipe.expected_output_digests,
                    result.digests,
                )

        self._reject_derivation_cycle(
            declaration.result_digital_asset_id,
            source_asset_ids,
        )
        with self._lock, self._metadata_transaction():
            derivation_id = manager_api.DigitalAssetDerivationID(
                self._allocate_metadata_id_locked("derivation")
            )
            record = manager_api.DigitalAssetDerivationRecord(
                derivation_id,
                declaration,
                self._new_revision_locked(),
            )
            self._derivations[derivation_id] = record
            return record

    @override
    def get_digital_asset_derivation_record(
        self,
        digital_asset_derivation_id: manager_api.DigitalAssetDerivationID,
    ) -> manager_api.DigitalAssetDerivationRecord:
        """
        Read the retained derivation mapping under the manager lock.

        A missing key becomes DigitalAssetDerivationNotFound chained from KeyError. The record is
        not copied or checked for current source availability.

        Example:
            >>> record = manager.get_digital_asset_derivation_record(derivation_id)  # doctest: +SKIP


        :param digital_asset_derivation_id: Key identifying the registered provenance assertion.
        :return: Stored record reference; DigitalAssetDerivationNotFound when the key is absent.
        """

        with self._lock:
            try:
                return self._derivations[digital_asset_derivation_id]
            except KeyError as error:
                raise manager_api.DigitalAssetDerivationNotFound(
                    "Digital Asset derivation "
                    f"{digital_asset_derivation_id} is not registered."
                ) from error

    @override
    def iter_digital_asset_derivation_records(
        self,
        *,
        result_digital_asset_id: manager_api.DigitalAssetID | None = None,
        source_digital_asset_id: manager_api.DigitalAssetID | None = None,
        source_composite_digital_asset_id: (
            manager_api.CompositeDigitalAssetID | None
        ) = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        exact_only: bool = False,
    ) -> Iterator[manager_api.DigitalAssetDerivationRecord]:
        """
        Validate workflow filters and capture matching records under the lock in sorted mapping-key
        order.

        All filters are conjunctive. Atomic and Composite source IDs match only direct declaration
        references, independently; neither Composite members nor recipe-only inputs are expanded.
        Asset IDs are not resolved, and exact_only tests the stored recipe claim rather than current
        recoverability. The tuple captures record references at call time, without holding the lock
        during subsequent iteration.

        Example:
            >>> records = tuple(manager.iter_digital_asset_derivation_records(exact_only=True))  # doctest: +SKIP


        :param result_digital_asset_id: Optional result identity matched directly; None leaves this dimension unrestricted.
        :param source_digital_asset_id: Optional direct atomic provenance source; Composite members and recipe-only inputs are not expanded for this filter.
        :param source_composite_digital_asset_id: Optional directly referenced Composite identity, matched independently of the atomic-source filter.
        :param workflow_id: Optional positive workflow ID; None includes all workflow IDs.
        :param workflow_reference: Optional nonblank workflow label compared exactly, without stripping retained text.
        :param exact_only: Whether to retain only records whose recipes claim complete EXACT enum reproducibility.
        :return: Iterator over the eager tuple snapshot, possibly empty; invalid workflow filters raise before capture.
        """

        if workflow_id is not None and workflow_id <= 0:
            raise ValueError("workflow_id must be positive when supplied.")
        if workflow_reference is not None and not workflow_reference.strip():
            raise ValueError("workflow_reference must not be empty when supplied.")

        with self._lock:
            records = tuple(
                record
                for _, record in sorted(self._derivations.items())
                if (
                    result_digital_asset_id is None
                    or record.declaration.result_digital_asset_id
                    == result_digital_asset_id
                )
                and (
                    source_digital_asset_id is None
                    or any(
                        source.digital_asset_id == source_digital_asset_id
                        for source in record.declaration.sources
                    )
                )
                and (
                    source_composite_digital_asset_id is None
                    or any(
                        source.composite_digital_asset_id
                        == source_composite_digital_asset_id
                        for source in record.declaration.sources
                    )
                )
                and (
                    workflow_id is None or record.declaration.workflow_id == workflow_id
                )
                and (
                    workflow_reference is None
                    or record.declaration.workflow_reference == workflow_reference
                )
                and (not exact_only or record.can_recreate_exactly)
            )
        return iter(records)

    @override
    def get_derivation_graph(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
        *,
        direction: (
            manager_api.DigitalAssetDerivationGraphDirection | str
        ) = manager_api.DigitalAssetDerivationGraphDirection.BOTH,
        max_depth: int | None = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        exact_only: bool = False,
    ) -> manager_api.DigitalAssetDerivationGraph:
        """
        Index filtered provenance and combine stable breadth-first walks rooted at one registered
        Asset.

        Root lookup precedes the nonnegative-depth check, direction coercion, and workflow-filter
        validation. Every matching record is indexed before walking; Composite expansion errors can
        therefore arise from records outside the requested neighbourhood. Recipe inputs and all
        current Composite members participate, while managed executor/dependency Assets are
        excluded.

        BOTH walks ancestors first and descendants second, each starting at the root with a fresh
        visited set. Shared inventories deduplicate first encounters; co-inputs in descendant
        records need not appear in the atomic inventory. Adjacency at the depth cutoff sets
        truncated even when already encountered elsewhere. No lock spans record capture, member
        expansion, and traversal.

        Example:
            >>> graph = manager.get_derivation_graph(asset_id, direction="both", max_depth=2)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic root, resolved before direction/depth/filter validation.
        :param direction: Ancestors, descendants, or both, accepted as an enum or its exact string value.
        :param max_depth: Optional nonnegative edge-depth limit; zero retains the root only and None imposes no limit.
        :param workflow_id: Optional positive workflow ID; None includes all workflow IDs.
        :param workflow_reference: Optional nonblank workflow label compared exactly, without stripping retained text.
        :param exact_only: Whether to retain only records whose recipes claim complete EXACT enum reproducibility.
        :return: New graph value with ordered node/record tuples; lookup, filter, direction, and expansion failures propagate.
        """

        self.get_digital_asset_record(digital_asset_id)
        if max_depth is not None and max_depth < 0:
            raise ValueError("max_depth must not be negative.")
        try:
            graph_direction = manager_api.DigitalAssetDerivationGraphDirection(
                direction
            )
        except ValueError as error:
            raise ValueError(
                "direction must be 'ancestors', 'descendants', or 'both'."
            ) from error

        records = tuple(
            self.iter_digital_asset_derivation_records(
                workflow_id=workflow_id,
                workflow_reference=workflow_reference,
                exact_only=exact_only,
            )
        )
        sources_by_derivation, by_result, by_source = self._index_derivation_graph(
            records
        )
        traversal = _DerivationGraphTraversal(
            digital_asset_id,
            max_depth=max_depth,
            sources_by_derivation=sources_by_derivation,
            by_result=by_result,
            by_source=by_source,
        )
        directions = {
            manager_api.DigitalAssetDerivationGraphDirection.ANCESTORS: (
                manager_api.DigitalAssetDerivationGraphDirection.ANCESTORS,
            ),
            manager_api.DigitalAssetDerivationGraphDirection.DESCENDANTS: (
                manager_api.DigitalAssetDerivationGraphDirection.DESCENDANTS,
            ),
            manager_api.DigitalAssetDerivationGraphDirection.BOTH: (
                manager_api.DigitalAssetDerivationGraphDirection.ANCESTORS,
                manager_api.DigitalAssetDerivationGraphDirection.DESCENDANTS,
            ),
        }[graph_direction]
        for walk_direction in directions:
            traversal.walk(walk_direction)

        return manager_api.DigitalAssetDerivationGraph(
            digital_asset_id,
            graph_direction,
            tuple(traversal.asset_ids),
            tuple(traversal.composite_ids),
            tuple(traversal.records),
            traversal.truncated,
        )

    @override
    def find_digital_asset_derivation_path(
        self,
        source_digital_asset_id: manager_api.DigitalAssetID,
        result_digital_asset_id: manager_api.DigitalAssetID,
        *,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        exact_only: bool = False,
    ) -> manager_api.DigitalAssetDerivationGraph | None:
        """
        Breadth-first search filtered, expanded provenance edges and reconstruct one shortest path.

        Resolve both endpoints before validating workflow filters through derivation iteration. Each
        record becomes an edge from every expanded atomic source to its single result. Adjacency
        retains sorted derivation-record order, making predecessor choice deterministic among equal
        length routes. Visited Assets prevent cycle/redundant traversal even if imported repository
        state violates ordinary registration constraints.

        Reconstructed node order runs source to result. Composite IDs are collected in first
        appearance order from direct Composite references on selected records. Co-input Assets are
        intentionally not added to the node path. No storage availability or recipe execution is
        considered, and no metadata is mutated.

        :param source_digital_asset_id: Registered atomic start identity.
        :param result_digital_asset_id: Registered atomic destination identity.
        :param workflow_id: Optional positive workflow filter forwarded to record iteration.
        :param workflow_reference: Optional nonblank workflow-reference filter forwarded unchanged.
        :param exact_only: Whether traversal is restricted to complete exact recipe claims.
        :return: Selected shortest descendant graph, a zero-step graph for equal endpoints, or None.
        """

        self.get_digital_asset_record(source_digital_asset_id)
        self.get_digital_asset_record(result_digital_asset_id)
        if source_digital_asset_id == result_digital_asset_id:
            return manager_api.DigitalAssetDerivationGraph(
                source_digital_asset_id,
                manager_api.DigitalAssetDerivationGraphDirection.DESCENDANTS,
                (source_digital_asset_id,),
            )

        records = tuple(
            self.iter_digital_asset_derivation_records(
                workflow_id=workflow_id,
                workflow_reference=workflow_reference,
                exact_only=exact_only,
            )
        )
        _, _, by_source = self._index_derivation_graph(records)
        predecessors: dict[
            manager_api.DigitalAssetID,
            tuple[manager_api.DigitalAssetID, manager_api.DigitalAssetDerivationRecord],
        ] = {}
        visited = {source_digital_asset_id}
        pending = deque((source_digital_asset_id,))
        while pending:
            current = pending.popleft()
            for record in by_source.get(current, ()):
                result = record.declaration.result_digital_asset_id
                if result in visited:
                    continue
                visited.add(result)
                predecessors[result] = (current, record)
                if result == result_digital_asset_id:
                    pending.clear()
                    break
                pending.append(result)

        if result_digital_asset_id not in predecessors:
            return None

        reverse_nodes = [result_digital_asset_id]
        reverse_records: list[manager_api.DigitalAssetDerivationRecord] = []
        current = result_digital_asset_id
        while current != source_digital_asset_id:
            previous, record = predecessors[current]
            reverse_records.append(record)
            reverse_nodes.append(previous)
            current = previous
        path_records = tuple(reversed(reverse_records))
        composite_ids = tuple(
            dict.fromkeys(
                source.composite_digital_asset_id
                for record in path_records
                for source in record.declaration.sources
                if source.composite_digital_asset_id is not None
            )
        )
        return manager_api.DigitalAssetDerivationGraph(
            source_digital_asset_id,
            manager_api.DigitalAssetDerivationGraphDirection.DESCENDANTS,
            tuple(reversed(reverse_nodes)),
            composite_ids,
            path_records,
        )

    def _index_derivation_graph(
        self,
        records: tuple[manager_api.DigitalAssetDerivationRecord, ...],
    ) -> tuple[
        dict[
            manager_api.DigitalAssetDerivationID, tuple[manager_api.DigitalAssetID, ...]
        ],
        dict[
            manager_api.DigitalAssetID, list[manager_api.DigitalAssetDerivationRecord]
        ],
        dict[
            manager_api.DigitalAssetID, list[manager_api.DigitalAssetDerivationRecord]
        ],
    ]:
        """
        Build result/source adjacency indexes from supplied records and expanded atomic inputs.

        Source IDs are deduplicated and sorted after adding current Composite members and recipe
        inputs, excluding managed executables/dependencies. Adjacency lists retain the input record
        order. Every record is expanded, including disconnected ones; failures propagate and no
        whole-graph snapshot lock is acquired here.

        Example:
            >>> sources, results, inputs = manager._index_derivation_graph(records)  # doctest: +SKIP


        :param records: Ordered provenance snapshot; normal callers provide unique derivation IDs.
        :return: Three new dictionaries: sources per derivation, records per result, and records per atomic source.
        """

        sources_by_derivation = {
            record.digital_asset_derivation_id: tuple(
                sorted(
                    self._source_asset_ids(
                        record,
                        include_recipe_artifacts=False,
                    )
                )
            )
            for record in records
        }
        by_result: dict[
            manager_api.DigitalAssetID,
            list[manager_api.DigitalAssetDerivationRecord],
        ] = {}
        by_source: dict[
            manager_api.DigitalAssetID,
            list[manager_api.DigitalAssetDerivationRecord],
        ] = {}
        for record in records:
            by_result.setdefault(
                record.declaration.result_digital_asset_id,
                [],
            ).append(record)
            for source_id in sources_by_derivation[record.digital_asset_derivation_id]:
                by_source.setdefault(source_id, []).append(record)
        return sources_by_derivation, by_result, by_source

    @override
    def plan_digital_asset_recreation(
        self,
        digital_asset_id: manager_api.DigitalAssetID,
    ) -> manager_api.DigitalAssetRecreationPlan:
        """
        Validate the requested Asset and project a recursively selected branch into a public replay
        plan.

        A fresh memo and empty visiting set scope the recursive search to this call. Readable roots
        need no replay; other branches compare viable proposals by step count and derivation ID.
        Prerequisites precede consumers, without claiming a globally optimized shared-work schedule
        or reserving bytes/tools.

        The projection sorts availability IDs and deduplicates alternatives and warnings in
        first-occurrence order, removing the selected derivation from alternatives. Ordinary
        unavailability returns diagnostic evidence; lookup and unexpected helper failures remain
        visible.

        Example:
            >>> plan = manager.plan_digital_asset_recreation(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic result whose current readability or exact replay route is requested.
        :return: New recreation plan retaining branch steps, selected/alternative recipes, availability IDs, and warnings.
        """

        self.get_digital_asset_record(digital_asset_id)
        branch = self._plan_recreation_branch(
            digital_asset_id,
            visiting=frozenset(),
            memo={},
        )
        alternatives = tuple(
            derivation_id
            for derivation_id in dict.fromkeys(branch.alternative_derivation_ids)
            if derivation_id != branch.selected_derivation_id
        )
        return manager_api.DigitalAssetRecreationPlan(
            digital_asset_id,
            steps=branch.steps,
            available_digital_asset_ids=tuple(
                sorted(branch.available_digital_asset_ids)
            ),
            unavailable_digital_asset_ids=tuple(
                sorted(branch.unavailable_digital_asset_ids)
            ),
            selected_derivation_id=branch.selected_derivation_id,
            alternative_derivation_ids=alternatives,
            warnings=tuple(dict.fromkeys(branch.warnings)),
        )

    @override
    def forget_digital_asset_derivation(
        self,
        digital_asset_derivation_id: manager_api.DigitalAssetDerivationID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """
        Delete one provenance mapping entry under the lock and metadata transaction.

        An absent ID returns False before revision validation. Existing entries require any supplied
        revision to match, then are deleted without checking recreation-policy dependence or
        cascading to Asset bytes, other derivations, or workflow records. Adapter transaction
        behavior governs failure persistence.

        Example:
            >>> removed = manager.forget_digital_asset_derivation(derivation_id, if_revision=revision)  # doctest: +SKIP


        :param digital_asset_derivation_id: Provenance identity to remove from the registry.
        :param if_revision: Optional expected revision; None disables this precondition for an existing record.
        :return: True after deletion, False for absence; stale revisions raise StoragePreconditionFailed.
        """

        with self._lock, self._metadata_transaction():
            record = self._derivations.get(digital_asset_derivation_id)
            if record is None:
                return False
            self._check_revision(record.revision, if_revision)
            del self._derivations[digital_asset_derivation_id]
            return True


__all__ = ["DigitalAssetDerivationRegistryMixin"]
