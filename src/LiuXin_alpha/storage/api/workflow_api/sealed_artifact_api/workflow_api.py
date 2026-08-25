"""Workflow contract for cataloguing immutable container images."""

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
    """Catalogue a sealed image as an Asset derived from its members.

    Stores and archive drivers remain responsible only for physical build and
    read mechanics. This workflow owns the catalogue operation that connects
    the finished image to its ordered, path-pinned inputs and replay recipe.

    Example:
        >>> workflow.record_backup_result(result, executor=tool)  # doctest: +SKIP
    """

    storage_manager: StorageManagerAPI

    def __init__(self, storage_manager: StorageManagerAPI) -> None:
        """Bind the workflow to one catalogue-owning manager.

        Example:
            >>> workflow = ConcreteSealedArtifactWorkflow(manager)  # doctest: +SKIP
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
        """Record one already-sealed image and a replayable package recipe.

        Example:
            >>> registration = workflow.record_artifact(  # doctest: +SKIP
            ...     output, {"book.epub": book},
            ...     artifact_format="squashfs", executor=tool,
            ...     command=("mksquashfs", ".", "artifact.squashfs"),
            ... )
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
        """Catalogue a completed backup output using its durable intent.

        Example:
            >>> registration = workflow.record_backup_result(  # doctest: +SKIP
            ...     result, executor=tool,
            ... )
        """

        ...


__all__ = [
    "SealedArtifactAssetInput",
    "SealedArtifactSources",
    "SealedArtifactWorkflowAPI",
]
