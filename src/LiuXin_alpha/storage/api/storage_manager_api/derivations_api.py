"""
Define provenance, graph traversal, replay planning, and external artefact resolution contracts.

The registry separates recorded identity/recipe evidence from current availability
and execution. Its concrete graph conveniences expose ordered records from an eager
graph result; providers own persistence and external content-verification behavior.
"""

import abc

from collections.abc import Iterator
from typing import Protocol, runtime_checkable

from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetDerivationDeclaration,
    DigitalAssetDerivationGraph,
    DigitalAssetDerivationGraphDirection,
    DigitalAssetDerivationID,
    DigitalAssetDerivationRecord,
    DigitalAssetRecreationPlan,
    CompositeDigitalAssetID,
    DigitalAssetID,
    ReproductionRecipeArtifactReference,
)


@runtime_checkable
class ReproductionRecipeArtifactResolverAPI(Protocol):
    """
    Describe a provider that can check retrieval of externally pinned artefact bytes.

    Implementations own retrieval and digest-verification semantics. Runtime protocol checks only
    establish the structural method surface, not that a provider verifies content or obeys its
    return annotation.

    Example:
        >>> isinstance(resolver, ReproductionRecipeArtifactResolverAPI)  # doctest: +SKIP
        True
    """

    def is_available(
        self,
        reference: ReproductionRecipeArtifactReference,
    ) -> bool:
        """
        Report whether bytes matching the pinned digest can currently be retrieved.

        This protocol supplies no retrieval implementation. The manager uses a configured resolver
        for external hints when a managed route is unavailable; its policy/recreation helpers treat
        ordinary provider exceptions as unavailable while allowing BaseException to propagate.

        Example:
            >>> resolver.is_available(reference)  # doctest: +SKIP
            True


        :param reference: Pinned artefact identity and retrieval hints the provider must assess.
        :return: True for retrievable matching bytes, False when the provider cannot establish availability.
        """
        ...


class DigitalAssetDerivationRegistryAPI(abc.ABC):
    """
    Define provenance registration, filtered traversal, and exact-recreation planning for atomic
    Assets.

    Derivations retain how an existing result was produced; recipes are evidence for proposed
    replay. This API does not run converters. Ancestor/descendant conveniences eagerly obtain a
    graph from the concrete implementation and then expose its ordered records.

    Example:
        >>> record = manager.record_digital_asset_derivation(declaration)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def record_digital_asset_derivation(
        self,
        declaration: DigitalAssetDerivationDeclaration,
    ) -> DigitalAssetDerivationRecord:
        """
        Register a provenance assertion after validating source references, pinned identities, and
        cycles.

        A complete recipe must pin every expanded provenance source. Input identities must agree
        with registered sizes and comparable digests; managed artefacts must match their pinned
        digest. Complete exact output evidence must match the result. Validation does not establish
        current readability or execute the recipe. The composed manager allocates a fresh record
        even for an equivalent declaration.

        Example:
            >>> record = manager.record_digital_asset_derivation(declaration)  # doctest: +SKIP


        :param declaration: Provenance for an existing result, with ordered atomic/Composite sources and an optional recipe.
        :return: New registered derivation record; missing references, inconsistent identities, or cycles raise their domain errors.
        """
        ...

    @abc.abstractmethod
    def get_digital_asset_derivation_record(
        self,
        digital_asset_derivation_id: DigitalAssetDerivationID,
    ) -> DigitalAssetDerivationRecord:
        """
        Look up one registered provenance record without probing its bytes or replay route.

        Example:
            >>> record = manager.get_digital_asset_derivation_record(DigitalAssetDerivationID(11))  # doctest: +SKIP


        :param digital_asset_derivation_id: Registered derivation identity, distinct from its result Asset ID.
        :return: Retained provenance record; DigitalAssetDerivationNotFound when absent and other repository errors propagate.
        """
        ...

    @abc.abstractmethod
    def iter_digital_asset_derivation_records(
        self,
        *,
        result_digital_asset_id: DigitalAssetID | None = None,
        source_digital_asset_id: DigitalAssetID | None = None,
        source_composite_digital_asset_id: CompositeDigitalAssetID | None = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        exact_only: bool = False,
    ) -> Iterator[DigitalAssetDerivationRecord]:
        """
        Return an ID-ordered snapshot matching all supplied provenance and workflow filters.

        Atomic-source filtering examines direct provenance references; it does not expand Composite
        members or recipe inputs. The atomic and Composite filters can both be supplied and must
        each match. Exactness is a recipe claim, independent of current readability. The composed
        manager validates workflow filters and captures records when this method is called, before
        iteration.

        Example:
            >>> records = tuple(manager.iter_digital_asset_derivation_records(  # doctest: +SKIP
            ...     result_digital_asset_id=DigitalAssetID(8), exact_only=True,
            ... ))


        :param result_digital_asset_id: Optional result identity matched directly; None leaves this dimension unrestricted.
        :param source_digital_asset_id: Optional direct atomic provenance source; Composite members and recipe-only inputs are not expanded for this filter.
        :param source_composite_digital_asset_id: Optional directly referenced Composite identity, matched independently of the atomic-source filter.
        :param workflow_id: Optional positive workflow ID; None includes all workflow IDs.
        :param workflow_reference: Optional nonblank workflow label compared exactly, without stripping retained text.
        :param exact_only: Whether to retain only records whose recipes claim complete EXACT enum reproducibility.
        :return: Iterator over the captured matching records, possibly empty; invalid workflow filters and repository failures raise.
        """
        ...

    def iter_derivation_ancestors(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        max_depth: int | None = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        exact_only: bool = False,
    ) -> Iterator[DigitalAssetDerivationRecord]:
        """
        Obtain an ancestor graph eagerly and iterate its nearest-first provenance records.

        All matching alternatives remain, rather than selecting a recreation route. Graph/filter
        errors occur during this call. Returning only records discards the graph node inventory and
        truncation indicator; use get_derivation_graph when that evidence is needed.

        Example:
            >>> chain = tuple(manager.iter_derivation_ancestors(DigitalAssetID(9)))  # doctest: +SKIP


        :param digital_asset_id: Registered atomic root, resolved before direction/depth/filter validation.
        :param max_depth: Optional nonnegative edge-depth limit; zero retains the root only and None imposes no limit.
        :param workflow_id: Optional positive workflow ID; None includes all workflow IDs.
        :param workflow_reference: Optional nonblank workflow label compared exactly, without stripping retained text.
        :param exact_only: Whether to retain only records whose recipes claim complete EXACT enum reproducibility.
        :return: Iterator over the already materialized ancestor graph records, preserving their order.
        """

        graph = self.get_derivation_graph(
            digital_asset_id,
            direction=DigitalAssetDerivationGraphDirection.ANCESTORS,
            max_depth=max_depth,
            workflow_id=workflow_id,
            workflow_reference=workflow_reference,
            exact_only=exact_only,
        )
        return iter(graph.derivation_records)

    def iter_derivation_descendants(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        max_depth: int | None = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        exact_only: bool = False,
    ) -> Iterator[DigitalAssetDerivationRecord]:
        """
        Obtain a descendant graph eagerly and iterate its nearest-first provenance records.

        Branches are retained. Errors propagate from graph construction before an iterator is
        returned; the convenience does not expose the graph truncation flag or node inventory.

        Example:
            >>> outputs = tuple(manager.iter_derivation_descendants(  # doctest: +SKIP
            ...     DigitalAssetID(7), max_depth=2,
            ... ))


        :param digital_asset_id: Registered atomic root, resolved before direction/depth/filter validation.
        :param max_depth: Optional nonnegative edge-depth limit; zero retains the root only and None imposes no limit.
        :param workflow_id: Optional positive workflow ID; None includes all workflow IDs.
        :param workflow_reference: Optional nonblank workflow label compared exactly, without stripping retained text.
        :param exact_only: Whether to retain only records whose recipes claim complete EXACT enum reproducibility.
        :return: Iterator over the already materialized descendant graph records, preserving their order.
        """

        graph = self.get_derivation_graph(
            digital_asset_id,
            direction=DigitalAssetDerivationGraphDirection.DESCENDANTS,
            max_depth=max_depth,
            workflow_id=workflow_id,
            workflow_reference=workflow_reference,
            exact_only=exact_only,
        )
        return iter(graph.derivation_records)

    @abc.abstractmethod
    def get_derivation_graph(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        direction: (
            DigitalAssetDerivationGraphDirection | str
        ) = DigitalAssetDerivationGraphDirection.BOTH,
        max_depth: int | None = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        exact_only: bool = False,
    ) -> DigitalAssetDerivationGraph:
        """
        Build a bounded provenance inventory using breadth-first walks from one Asset.

        Recipe inputs and current Composite members participate as atomic sources; managed
        executables/dependencies do not. Alternatives remain visible. BOTH combines an ancestor walk
        followed by a separate descendant walk from the root, rather than exploring every undirected
        connection. Records can retain co-inputs absent from the traversed atomic node inventory.

        Workflow/exactness filters apply before traversal. In the composed manager every matching
        record is indexed first, so an unrelated matching record with a broken Composite reference
        can still fail this call. A depth limit marks truncation when adjacency exists at the
        cutoff, even if some adjacent records were already seen.

        Example:
            >>> graph = manager.get_derivation_graph(  # doctest: +SKIP
            ...     DigitalAssetID(9), direction="ancestors", max_depth=2,
            ... )


        :param digital_asset_id: Registered atomic root, resolved before direction/depth/filter validation.
        :param direction: Ancestors, descendants, or both, accepted as an enum or its exact string value.
        :param max_depth: Optional nonnegative edge-depth limit; zero retains the root only and None imposes no limit.
        :param workflow_id: Optional positive workflow ID; None includes all workflow IDs.
        :param workflow_reference: Optional nonblank workflow label compared exactly, without stripping retained text.
        :param exact_only: Whether to retain only records whose recipes claim complete EXACT enum reproducibility.
        :return: Graph with stable first-encounter node/record order and a truncation flag; validation or reference failures propagate.
        """
        ...

    @abc.abstractmethod
    def plan_digital_asset_recreation(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> DigitalAssetRecreationPlan:
        """
        Select an exact-replay proposal from currently readable bytes and recursively viable
        recipes.

        A readable root yields no steps. Otherwise the composed manager compares viable branch
        proposals by step count and then derivation ID, retaining alternatives and diagnostics.
        Prerequisite recipes precede consumers. This is a recursive selection, not a global cost
        optimizer or reservation; current source/tool availability can change before execution.

        Example:
            >>> plan = manager.plan_digital_asset_recreation(DigitalAssetID(9))  # doctest: +SKIP
            >>> plan.can_recreate_exactly  # doctest: +SKIP
            True


        :param digital_asset_id: Registered atomic result whose availability or exact recreation route should be assessed.
        :return: Plan containing selected steps, availability evidence, alternatives, and warnings; an unavailable plan is a valid result.
        """
        ...

    @abc.abstractmethod
    def forget_digital_asset_derivation(
        self,
        digital_asset_derivation_id: DigitalAssetDerivationID,
        *,
        if_revision: str | None = None,
    ) -> bool:
        """
        Remove a provenance assertion while retaining its result and source Assets.

        An optional revision guards an existing record. The composed manager returns False for
        absence before checking that token; it does not revalidate policies that relied on the
        removed recipe. Correction requires a separately coordinated removal and new registration,
        without an atomic replacement guarantee from this API.

        Example:
            >>> forgotten = manager.forget_digital_asset_derivation(  # doctest: +SKIP
            ...     DigitalAssetDerivationID(11), if_revision="v1",
            ... )


        :param digital_asset_derivation_id: Provenance identity to remove.
        :param if_revision: Optional expected revision; None omits the optimistic-lock precondition.
        :return: True when a record is removed, False when absent; a stale supplied revision raises StoragePreconditionFailed.
        """
        ...


__all__ = [
    "DigitalAssetDerivationRegistryAPI",
    "ReproductionRecipeArtifactResolverAPI",
]
