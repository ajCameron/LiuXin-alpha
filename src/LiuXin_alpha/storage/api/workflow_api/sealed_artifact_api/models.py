"""
Label sealed container formats and bundle ingestion with package provenance.

Format properties provide metadata and replay-name defaults. Registration validates only
cross-record Asset identity and recipe equality; it is not a physical verification report.
"""

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
    """
    Label the format of a completed container image treated as one atomic Asset.

    Values select conservative media types and replay output filenames. They do not inspect a file
    signature, make an archive backend available, or prove a supplied image has the named format.
    OTHER uses generic binary metadata.

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
        """
        Map the selected format to a conservative container-image media type.

        The value is a static metadata default. It is not inferred from content or checked against
        installed archive tools.

        Example:
            >>> SealedArtifactFormat.ZIP.media_type
            'application/zip'


        :return: Media-type string for this enum member, or application/octet-stream for OTHER.
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
        """
        Map the selected format to its conventional replay output filename.

        Names use artifact with the format suffix; ISO_9660 uses .iso and OTHER uses .bin. This does
        not preserve an input basename or allocate an output file.

        Example:
            >>> SealedArtifactFormat.RAR.default_output_name
            'artifact.rar'


        :return: Stable relative filename used by the recorded recipe to identify its expected output.
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
    """
    Bundle the managed container bytes with their recorded package provenance.

    Construction requires the derivation result Asset ID to match the ingest result and its recipe
    to compare equal to recipe. It does not verify the format, inspect bytes, execute the recipe, or
    compare recipe output expectations to the actual image. Property shortcuts return the retained
    ingest records rather than fetching current catalogue state.

    Example:
        >>> registration.asset_record.digital_asset_id  # doctest: +SKIP
        8


    :ivar artifact_format: Declared image format; not coerced or validated against content here.
    :ivar ingest_result: Original adopt/ingest outcome carrying the container Asset and Replica records.
    :ivar derivation_record: Recorded provenance whose result ID and recipe must agree with this bundle.
    :ivar recipe: Replay description equal to the derivation declaration recipe; equality, not object identity, is required.
    """

    artifact_format: SealedArtifactFormat
    ingest_result: DigitalAssetIngestResult
    derivation_record: DigitalAssetDerivationRecord
    recipe: ReproductionRecipe

    def __post_init__(self) -> None:
        """
        Cross-check the derivation's result Asset ID and recipe against this bundle.

        Asset identity is compared before recipe equality. The checks compare retained values only
        and do not call the manager or validate physical/replay evidence.

        Example:
            >>> registration.__post_init__()  # doctest: +SKIP


        :return: None when both comparisons agree; otherwise ValueError describing the mismatched Asset or recipe.
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
        """
        Expose the container Asset record retained in the ingest outcome.

        The returned record is the same object and can describe earlier catalogue state; this
        property performs no refresh.

        Example:
            >>> registration.asset_record  # doctest: +SKIP
            DigitalAssetRecord(...)


        :return: Original ingest_result.asset_record for the whole container byte sequence.
        """

        return self.ingest_result.asset_record

    @property
    def replica_record(self) -> ReplicaRecord:
        """
        Expose the container Replica record retained in the ingest outcome.

        This can describe a reused or adopted copy, not necessarily a newly created one. Access
        neither verifies its bytes nor checks current availability.

        Example:
            >>> registration.replica_record  # doctest: +SKIP
            ReplicaRecord(...)


        :return: Original ingest_result.replica_record identifying the copy reported by ingest or adoption.
        """

        return self.ingest_result.replica_record


__all__ = ["SealedArtifactFormat", "SealedArtifactRegistration"]
