"""
Digital Asset provenance graphs and recreation planning.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator, Mapping
from typing import override

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class _DerivationGraphTraversal:
    """
    Mutable breadth-first traversal state for one provenance graph.
    """

    def __init__(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        max_depth: int | None,
        sources_by_derivation: Mapping[
            api.DigitalAssetDerivationID,
            tuple[api.DigitalAssetID, ...],
        ],
        by_result: Mapping[
            api.DigitalAssetID,
            list[api.DigitalAssetDerivationRecord],
        ],
        by_source: Mapping[
            api.DigitalAssetID,
            list[api.DigitalAssetDerivationRecord],
        ],
    ) -> None:
        """
        Initialise stable, de-duplicated state shared by graph walks.


        :param digital_asset_id:
        :param max_depth:
        :param sources_by_derivation:
        :param by_result:
        :param by_source:
        :return:
        """

        self.digital_asset_id = digital_asset_id
        self.max_depth = max_depth
        self.sources_by_derivation = sources_by_derivation
        self.by_result = by_result
        self.by_source = by_source
        self.asset_ids = [digital_asset_id]
        self.seen_asset_ids = {digital_asset_id}
        self.composite_ids: list[api.CompositeDigitalAssetID] = []
        self.seen_composite_ids: set[api.CompositeDigitalAssetID] = set()
        self.records: list[api.DigitalAssetDerivationRecord] = []
        self.seen_derivation_ids: set[api.DigitalAssetDerivationID] = set()
        self.truncated = False

    def walk(
        self,
        direction: api.DigitalAssetDerivationGraphDirection,
    ) -> None:
        """
        Walk in one direction while retaining every encountered branch.


        :param direction:
        :return:
        """

        queue: deque[tuple[api.DigitalAssetID, int]] = deque(
            ((self.digital_asset_id, 0),)
        )
        walked: set[api.DigitalAssetID] = set()
        while queue:
            current_id, depth = queue.popleft()
            if current_id in walked:
                continue
            walked.add(current_id)
            ancestors = direction is api.DigitalAssetDerivationGraphDirection.ANCESTORS
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
        record: api.DigitalAssetDerivationRecord,
    ) -> None:
        """
        Append one derivation and its Composite sources at most once.


        :param record:
        :return:
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
    Record immutable provenance and reason over derivation graphs.

    A derivation links atomic or Composite sources to one result Asset and may
    carry an exact, replayable recipe.  This component validates references and
    cycles, traverses provenance in stable breadth-first order, and plans
    recreation; it records recipes but does not execute converters.
    """

    @override
    def record_digital_asset_derivation(
        self,
        declaration: api.DigitalAssetDerivationDeclaration,
    ) -> api.DigitalAssetDerivationRecord:
        """
        Validate and record immutable provenance for a derived Asset.


        :param declaration:
        :return:
        """

        result = self.get_digital_asset_record(declaration.result_digital_asset_id)
        source_asset_ids: set[api.DigitalAssetID] = set()
        for source in declaration.sources:
            if source.digital_asset_id is not None:
                self.get_digital_asset_record(source.digital_asset_id)
                source_asset_ids.add(source.digital_asset_id)
                continue
            if source.composite_digital_asset_id is None:
                raise api.StoragePreconditionFailed(
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
                raise api.StoragePreconditionFailed(
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
                    raise api.StorageIntegrityError(
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
            derivation_id = api.DigitalAssetDerivationID(
                self._allocate_metadata_id_locked("derivation")
            )
            record = api.DigitalAssetDerivationRecord(
                derivation_id,
                declaration,
                self._new_revision_locked(),
            )
            self._derivations[derivation_id] = record
            return record

    @override
    def get_digital_asset_derivation_record(
        self,
        digital_asset_derivation_id: api.DigitalAssetDerivationID,
    ) -> api.DigitalAssetDerivationRecord:
        """
        Return one registered derivation record.


        :param digital_asset_derivation_id:
        :return:
        """

        with self._lock:
            try:
                return self._derivations[digital_asset_derivation_id]
            except KeyError as error:
                raise api.DigitalAssetDerivationNotFound(
                    "Digital Asset derivation "
                    f"{digital_asset_derivation_id} is not registered."
                ) from error

    @override
    def iter_digital_asset_derivation_records(
        self,
        *,
        result_digital_asset_id: api.DigitalAssetID | None = None,
        source_digital_asset_id: api.DigitalAssetID | None = None,
        source_composite_digital_asset_id: (api.CompositeDigitalAssetID | None) = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        exact_only: bool = False,
    ) -> Iterator[api.DigitalAssetDerivationRecord]:
        """
        Iterate over an ID-ordered, provenance-filtered snapshot.


        :param result_digital_asset_id:
        :param source_digital_asset_id:
        :param source_composite_digital_asset_id:
        :param workflow_id:
        :param workflow_reference:
        :param exact_only:
        :return:
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
        digital_asset_id: api.DigitalAssetID,
        *,
        direction: (
            api.DigitalAssetDerivationGraphDirection | str
        ) = api.DigitalAssetDerivationGraphDirection.BOTH,
        max_depth: int | None = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        exact_only: bool = False,
    ) -> api.DigitalAssetDerivationGraph:
        """
        Return a stable breadth-first provenance graph around one Asset.


        :param digital_asset_id:
        :param direction:
        :param max_depth:
        :param workflow_id:
        :param workflow_reference:
        :param exact_only:
        :return:
        """

        self.get_digital_asset_record(digital_asset_id)
        if max_depth is not None and max_depth < 0:
            raise ValueError("max_depth must not be negative.")
        try:
            graph_direction = api.DigitalAssetDerivationGraphDirection(direction)
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
            api.DigitalAssetDerivationGraphDirection.ANCESTORS: (
                api.DigitalAssetDerivationGraphDirection.ANCESTORS,
            ),
            api.DigitalAssetDerivationGraphDirection.DESCENDANTS: (
                api.DigitalAssetDerivationGraphDirection.DESCENDANTS,
            ),
            api.DigitalAssetDerivationGraphDirection.BOTH: (
                api.DigitalAssetDerivationGraphDirection.ANCESTORS,
                api.DigitalAssetDerivationGraphDirection.DESCENDANTS,
            ),
        }[graph_direction]
        for walk_direction in directions:
            traversal.walk(walk_direction)

        return api.DigitalAssetDerivationGraph(
            digital_asset_id,
            graph_direction,
            tuple(traversal.asset_ids),
            tuple(traversal.composite_ids),
            tuple(traversal.records),
            traversal.truncated,
        )

    def _index_derivation_graph(
        self,
        records: tuple[api.DigitalAssetDerivationRecord, ...],
    ) -> tuple[
        dict[api.DigitalAssetDerivationID, tuple[api.DigitalAssetID, ...]],
        dict[api.DigitalAssetID, list[api.DigitalAssetDerivationRecord]],
        dict[api.DigitalAssetID, list[api.DigitalAssetDerivationRecord]],
    ]:
        """
        Index records by result and expanded atomic source for traversal.


        :param records:
        :return:
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
            api.DigitalAssetID,
            list[api.DigitalAssetDerivationRecord],
        ] = {}
        by_source: dict[
            api.DigitalAssetID,
            list[api.DigitalAssetDerivationRecord],
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
        digital_asset_id: api.DigitalAssetID,
    ) -> api.DigitalAssetRecreationPlan:
        """
        Select a shortest viable, topologically ordered exact replay route.


        :param digital_asset_id:
        :return:
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
        return api.DigitalAssetRecreationPlan(
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
        digital_asset_derivation_id: api.DigitalAssetDerivationID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """
        Forget one provenance assertion under revision control.


        :param digital_asset_derivation_id:
        :param if_revision:
        :return:
        """

        with self._lock, self._metadata_transaction():
            record = self._derivations.get(digital_asset_derivation_id)
            if record is None:
                return False
            self._check_revision(record.revision, if_revision)
            del self._derivations[digital_asset_derivation_id]
            return True


__all__ = ["DigitalAssetDerivationRegistryMixin"]
