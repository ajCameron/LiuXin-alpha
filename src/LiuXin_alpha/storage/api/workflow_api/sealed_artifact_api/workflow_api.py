"""
Define cataloguing of existing sealed images as derived atomic Assets.

The manager owns byte identity and provenance persistence. This facade records ordered
member inputs and replay intent; it neither builds an archive nor verifies membership by
opening it. Local-image ingest or Location adoption can precede a later provenance error.
"""

from __future__ import annotations

import abc
import os

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING
from uuid import UUID

from LiuXin_alpha.storage.api.models import Location, StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetID,
    DigitalAssetIngestResult,
    DigitalAssetMetadata,
    DigitalAssetRecord,
    DigitalAssetResolution,
    ReplicaMode,
    ReproductionRecipeArtifactReference,
    Reproducibility,
)
from LiuXin_alpha.storage.api.workflow_api.backup_api.models import (
    BackupWorkflowResult,
)
from LiuXin_alpha.storage.api.workflow_api.sealed_artifact_api.models import (
    SealedArtifactFormat,
    SealedArtifactRegistration,
)

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.storage_manager_api import StorageManagerAPI


SealedArtifactAssetInput = (
    DigitalAssetID
    | DigitalAssetRecord
    | DigitalAssetIngestResult
    | DigitalAssetResolution
)
SealedArtifactSources = (
    Mapping[str, SealedArtifactAssetInput]
    | Iterable[tuple[str, SealedArtifactAssetInput]]
)


class SealedArtifactWorkflowAPI(abc.ABC):
    """
    Catalogue a completed container as an atomic Asset with ordered member provenance.

    The concrete workflow adopts or ingests existing image bytes and records a package derivation
    and reproduction recipe. It does not build the archive, execute a replay command, inspect
    archive members to prove correspondence, or register the archive contents as a separate Store.
    Those operations have other owners.

    SealedArtifactSources accepts an ordered path-to-Asset mapping or iterable of path/Asset pairs.
    Asset inputs identify existing atomic catalogue records; the implementation refreshes them
    through the bound manager. The supplied manager remains caller-owned.

    Example:
        >>> workflow.record_backup_result(result, executor=tool)  # doctest: +SKIP
    """

    storage_manager: StorageManagerAPI

    def __init__(self, storage_manager: StorageManagerAPI) -> None:
        """
        Retain the manager used for byte ingest/adoption and provenance writes.

        Assignment performs no runtime conformance check, startup, transaction, or ownership
        transfer. The caller remains responsible for manager lifetime.

        Example:
            >>> workflow = ConcreteSealedArtifactWorkflow(manager)  # doctest: +SKIP


        :param storage_manager: Catalogue-owning manager instance borrowed for subsequent workflow operations.
        :return: None after storing the original manager reference.
        """

        self.storage_manager = storage_manager

    @abc.abstractmethod
    def record_artifact(
        self,
        artifact: str | os.PathLike[str] | Location,
        sources: SealedArtifactSources,
        *,
        artifact_format: SealedArtifactFormat | str,
        executor: ReproductionRecipeArtifactReference | None,
        command: Iterable[str],
        parameters: Mapping[str, object] | None = None,
        environment: Mapping[str, object] | None = None,
        dependencies: Iterable[ReproductionRecipeArtifactReference] = (),
        reproducibility: Reproducibility | str = Reproducibility.BEST_EFFORT,
        complete: bool = True,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
        operation_id: UUID | None = None,
        preferred_store_ref: StoreUUID | None = None,
        replica_mode: ReplicaMode = ReplicaMode.ARCHIVE,
        metadata: DigitalAssetMetadata | None = None,
        operator: str | None = None,
        notes: str | None = None,
        verify: bool = True,
    ) -> SealedArtifactRegistration:
        """
        Catalogue an already-sealed image and record its ordered package recipe.

        The concrete implementation refreshes all source records, validates unique logical paths,
        and captures their size/digests as recipe inputs. Complete recipes require a retrievable
        pinned executor, retrievable dependencies, a nonempty command, and a reproducibility class
        other than NOT_REPRODUCIBLE. These are recorded claims and references, not proof that replay
        succeeds or that the image contains those members.

        A Location is adopted in its current Store; a local path or file URI is ingested. Byte
        cataloguing precedes final recipe/declaration construction and provenance recording, so a
        later failure can leave the Asset and Replica committed. An identical existing derivation is
        reused; conflicting provenance for the same output and supplied workflow identity is
        rejected. No transaction spans byte ingest and derivation registration.

        Example:
            >>> registration = workflow.record_artifact(  # doctest: +SKIP
            ...     output, {"book.epub": book},
            ...     artifact_format="squashfs", executor=tool,
            ...     command=("mksquashfs", ".", "artifact.squashfs"),
            ... )


        :param artifact: Existing local path/PathLike or file URI to ingest, or managed Location to adopt; remote URI strings are unsupported by the concrete implementation.
        :param sources: Ordered mapping or iterable of logical member paths to existing atomic Asset inputs; must be nonempty with unique valid paths.
        :param artifact_format: Container format enum or exact enum value used for metadata and recipe naming.
        :param executor: Pinned tool reference used by replay; None is allowed only for an incomplete recipe.
        :param command: Replay argument iterable, materialized as strings; nonempty arguments are required and complete recipes need at least one.
        :param parameters: Optional JSON-compatible build settings; artifact_format is inserted and a conflicting supplied value is rejected.
        :param environment: Optional JSON-compatible replay environment description; None records an empty object.
        :param dependencies: Additional pinned recipe artifacts; complete recipes require retrieval sources for each.
        :param reproducibility: Declared reproducibility enum or value; does not run a reproducibility check.
        :param complete: Whether the recipe is claimed complete and must satisfy the complete-reference preconditions.
        :param workflow_id: Optional catalogue provenance workflow ID used in derivation identity/conflict checks.
        :param workflow_reference: Optional external workflow identity string recorded with provenance.
        :param operation_id: Optional ingest/adoption operation token forwarded to the manager; not a transaction for the whole workflow.
        :param preferred_store_ref: Preferred ingest destination; for a Location it must be omitted or equal that Location's Store.
        :param replica_mode: Mode forwarded for the container copy, default ARCHIVE.
        :param metadata: Optional image description; None derives name, media type, basename, and format/kind attributes.
        :param operator: Optional attribution recorded on the package derivation.
        :param notes: Optional human-readable provenance notes.
        :param verify: Verification request forwarded to manager ingest/adoption; does not inspect archive members or test replay.
        :return: Bundle containing declared format, manager ingest outcome, registered or reused derivation, and recipe.
        """

        ...

    @abc.abstractmethod
    def record_backup_result(
        self,
        result: BackupWorkflowResult,
        *,
        executor: ReproductionRecipeArtifactReference,
        source_assets: SealedArtifactSources | None = None,
        environment: Mapping[str, object] | None = None,
        dependencies: Iterable[ReproductionRecipeArtifactReference] = (),
        operation_id: UUID | None = None,
        preferred_store_ref: StoreUUID | None = None,
        operator: str | None = None,
        notes: str | None = None,
        verify: bool = True,
    ) -> SealedArtifactRegistration:
        """
        Adapt a successful backup outcome into sealed-image Asset provenance.

        The concrete adapter supports SQUASHFS_PACK. It uses catalogue IDs from designated sources
        or an explicit override whose paths and order exactly match the declaration. It derives the
        replay command from compression/deterministic options and the supplied executor, claims
        EXACT when deterministic is "1" and BEST_EFFORT otherwise, and records a complete recipe in
        ARCHIVE mode. No archive build or replay is executed.

        A backup repository ID becomes workflow_reference="backup:<id>" rather than a catalogue
        workflow_id. Byte ingest/adoption and provenance have the same partial-failure boundary as
        record_artifact. Successful result fields do not independently establish that source reports
        or final_checkpoint agree with the declaration.

        Example:
            >>> registration = workflow.record_backup_result(  # doctest: +SKIP
            ...     result, executor=tool,
            ... )


        :param result: Successful SquashFS backup result containing a non-None output reference and intended source order.
        :param executor: Retrievable pinned tool reference supplying the replay command name; not automatically inferred from the local builder executable.
        :param source_assets: Optional complete ordered path/Asset override; None requires an Asset ID on every declared source.
        :param environment: Optional JSON-compatible replay environment description forwarded to recipe recording.
        :param dependencies: Pinned additional artifacts required for the complete recipe.
        :param operation_id: Optional manager ingest/adoption token for cataloguing the output image.
        :param preferred_store_ref: Preferred destination for local-image ingest; cannot redirect adoption of a Location.
        :param operator: Optional attribution for the derived image.
        :param notes: Optional provenance notes attached to the derivation.
        :param verify: Manager byte-verification request, independent of backup verification milestones or recipe replay.
        :return: SealedArtifactRegistration for the catalogued backup image and its recorded SquashFS recipe.
        """

        ...


__all__ = [
    "SealedArtifactAssetInput",
    "SealedArtifactSources",
    "SealedArtifactWorkflowAPI",
]
