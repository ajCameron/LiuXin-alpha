"""Domain values returned when a sealed container is catalogued."""

from __future__ import annotations

import dataclasses

from enum import StrEnum

from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetDerivationRecord,
    DigitalAssetIngestResult,
    DigitalAssetRecord,
    ReplicaRecord,
    ReproductionRecipe,
)


class SealedArtifactFormat(StrEnum):
    """Container formats whose completed image is an atomic Digital Asset.

    Example:
        >>> SealedArtifactFormat.SQUASHFS.value
        'squashfs'
    """

    SQUASHFS = "squashfs"
    RAR = "rar"
    SEVEN_ZIP = "7z"
    ISO_9660 = "iso9660"
    UDF = "udf"
    ZIP = "zip"
    TAR = "tar"
    OTHER = "other"

    @property
    def media_type(self) -> str:
        """Return a conservative media type for the container image.

        Example:
            >>> SealedArtifactFormat.ZIP.media_type
            'application/zip'
        """

        return {
            self.SQUASHFS: "application/vnd.squashfs",
            self.RAR: "application/vnd.rar",
            self.SEVEN_ZIP: "application/x-7z-compressed",
            self.ISO_9660: "application/x-iso9660-image",
            self.UDF: "application/x-udf",
            self.ZIP: "application/zip",
            self.TAR: "application/x-tar",
            self.OTHER: "application/octet-stream",
        }[self]

    @property
    def default_output_name(self) -> str:
        """Return the stable recipe output name used during replay.

        Example:
            >>> SealedArtifactFormat.RAR.default_output_name
            'artifact.rar'
        """

        suffix = {
            self.SQUASHFS: "squashfs",
            self.RAR: "rar",
            self.SEVEN_ZIP: "7z",
            self.ISO_9660: "iso",
            self.UDF: "udf",
            self.ZIP: "zip",
            self.TAR: "tar",
            self.OTHER: "bin",
        }[self]
        return f"artifact.{suffix}"


@dataclasses.dataclass(slots=True, frozen=True)
class SealedArtifactRegistration:
    """A container image, its managed bytes, and its provenance record.

    Example:
        >>> registration.asset_record.digital_asset_id  # doctest: +SKIP
        8
    """

    artifact_format: SealedArtifactFormat
    ingest_result: DigitalAssetIngestResult
    derivation_record: DigitalAssetDerivationRecord
    recipe: ReproductionRecipe

    def __post_init__(self) -> None:
        """Require the derivation and ingest result to name the same Asset.

        Example:
            >>> registration.__post_init__()  # doctest: +SKIP
        """

        if (
            self.derivation_record.declaration.result_digital_asset_id
            != self.ingest_result.asset_record.digital_asset_id
        ):
            raise ValueError(
                "sealed artifact derivation does not describe the ingested Asset."
            )
        if self.derivation_record.declaration.recipe != self.recipe:
            raise ValueError(
                "sealed artifact registration recipe differs from its derivation."
            )

    @property
    def asset_record(self) -> DigitalAssetRecord:
        """Return the atomic Digital Asset record for the container bytes.

        Example:
            >>> registration.asset_record  # doctest: +SKIP
            DigitalAssetRecord(...)
        """

        return self.ingest_result.asset_record

    @property
    def replica_record(self) -> ReplicaRecord:
        """Return the concrete Replica created or adopted for the container.

        Example:
            >>> registration.replica_record  # doctest: +SKIP
            ReplicaRecord(...)
        """

        return self.ingest_result.replica_record


__all__ = ["SealedArtifactFormat", "SealedArtifactRegistration"]
