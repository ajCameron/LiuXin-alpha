"""
Asset-derivation provenance and reproducibility values.
"""

from __future__ import annotations

import dataclasses

from datetime import datetime
from enum import StrEnum
from pathlib import PurePosixPath

from LiuXin_alpha.storage.api.models import Digest
from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    DigitalAssetDerivationID,
    CompositeDigitalAssetID,
    DigitalAssetID,
)


class DigitalAssetDerivationKind(StrEnum):
    """
    Broad semantic operation that produced a new Asset.

    Example:
        >>> DigitalAssetDerivationKind.EXTRACT.value
        'extract'
    """

    EXTRACT = "extract"
    CONVERT = "convert"
    TRANSCODE = "transcode"
    OCR = "ocr"
    PACKAGE = "package"
    GENERATE = "generate"
    NORMALIZE = "normalize"
    REPAIR = "repair"
    OTHER = "other"


class Reproducibility(StrEnum):
    """
    Strength of the claim made by a reproduction recipe.

    ``EXACT`` means the recipe is expected to recreate the same bytes and may
    therefore justify recreate-on-loss storage policy. ``BEST_EFFORT`` can
    rerun the process but does not promise byte-identical output.

    Example:
        >>> Reproducibility.EXACT.value
        'exact'
    """

    EXACT = "exact"
    BEST_EFFORT = "best_effort"
    NOT_REPRODUCIBLE = "not_reproducible"


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetDerivationSourceReference:
    """
    One ordered atomic or Composite source of a derivation.

    Exactly one source identifier is set. ``role`` distinguishes inputs such
    as the primary document, cover source, stylesheet, or dictionary.

    Example:
        >>> source = DigitalAssetDerivationSourceReference(
        ...     sequence_number=0, digital_asset_id=DigitalAssetID(7),
        ...     role="primary",
        ... )
        >>> source.digital_asset_id
        7
    """

    sequence_number: int
    digital_asset_id: DigitalAssetID | None = None
    composite_digital_asset_id: CompositeDigitalAssetID | None = None
    role: str | None = None

    def __post_init__(self) -> None:
        """
        Require one positive source identity and a valid ordered position.

        Example:
            >>> DigitalAssetDerivationSourceReference(0)
            Traceback (most recent call last):
            ...
            ValueError: exactly one derivation source identity is required.


        :return:
        """

        identities = (self.digital_asset_id, self.composite_digital_asset_id)
        if sum(identity is not None for identity in identities) != 1:
            raise ValueError("exactly one derivation source identity is required.")
        if any(identity is not None and identity <= 0 for identity in identities):
            raise ValueError("derivation source identifiers must be positive.")
        if self.sequence_number < 0:
            raise ValueError("sequence_number must not be negative.")
        _require_optional_text(self.role, "role")


@dataclasses.dataclass(slots=True, frozen=True)
class ReproductionRecipeInputReference:
    """
    One exact atomic input required to replay a transformation.

    Recipe inputs are atomic even when provenance names a Composite source:
    replay requires the Composite's member byte identities and their logical
    names, not merely the mutable Composite membership record.

    Example:
        >>> input_ = ReproductionRecipeInputReference(
        ...     0, DigitalAssetID(7), 4, (Digest("sha256", "abcd"),),
        ...     logical_path="book.epub", role="primary",
        ... )
        >>> input_.logical_path
        'book.epub'
    """

    sequence_number: int
    digital_asset_id: DigitalAssetID
    size_bytes: int
    digests: tuple[Digest, ...]
    logical_path: str
    role: str | None = None

    def __post_init__(self) -> None:
        """
        Require a pinned input identity, digest, and logical path.

        Example:
            >>> ReproductionRecipeInputReference(
            ...     0, DigitalAssetID(7), 4, (), "book.epub",
            ... )
            Traceback (most recent call last):
            ...
            ValueError: a recipe input requires at least one digest.


        :return:
        """

        if self.sequence_number < 0:
            raise ValueError("sequence_number must not be negative.")
        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")
        if self.size_bytes < 0:
            raise ValueError("size_bytes must not be negative.")
        if not self.digests:
            raise ValueError("a recipe input requires at least one digest.")
        _require_unique_digests(self.digests)
        _require_relative_path(self.logical_path, "logical_path")
        _require_optional_text(self.role, "role")


@dataclasses.dataclass(slots=True, frozen=True)
class ReproductionRecipeArtifactReference:
    """
    One immutable executable, dependency bundle, or auxiliary artefact.

    A content digest pins what was used even when a package registry or tool
    name later changes. ``uri`` is a retrieval hint, not the identity.

    Example:
        >>> artifact_reference = ReproductionRecipeArtifactReference(
        ...     "calibre-ebook-convert", Digest("sha256", "abcd"),
        ...     version="7.20.0", digital_asset_id=DigitalAssetID(20),
        ... )
        >>> artifact_reference.version
        '7.20.0'
    """

    name: str
    digest: Digest
    version: str | None = None
    uri: str | None = None
    digital_asset_id: DigitalAssetID | None = None

    def __post_init__(self) -> None:
        """
        Reject missing names and empty optional artefact metadata.

        Example:
            >>> ReproductionRecipeArtifactReference("", Digest("sha256", "abcd"))
            Traceback (most recent call last):
            ...
            ValueError: name must not be empty.


        :return:
        """

        _require_text(self.name, "name")
        _require_optional_text(self.version, "version")
        _require_optional_text(self.uri, "uri")
        if self.digital_asset_id is not None and self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive when supplied.")

    @property
    def has_retrieval_source(self) -> bool:
        """
        Return whether the pinned artefact has a stated retrieval source.

        Example:
            >>> ReproductionRecipeArtifactReference(
            ...     "tool", Digest("sha256", "abcd"), uri="oci://tool@sha256:abcd",
            ... ).has_retrieval_source
            True


        :return:
        """

        return self.digital_asset_id is not None or self.uri is not None


@dataclasses.dataclass(slots=True, frozen=True)
class ReproductionRecipe:
    """
    Self-contained recipe for recreating one derivation result.

    Canonical JSON strings keep structured parameters and environment data
    portable without weakening their deterministic serialization contract.
    An exact, complete recipe pins inputs and executor artefacts and declares
    the expected output identity.

    Example:
        >>> recipe = ReproductionRecipe(
        ...     recipe_type="extract_cover",
        ...     reproducibility=Reproducibility.EXACT,
        ...     complete=True,
        ...     inputs=(ReproductionRecipeInputReference(
        ...         0, DigitalAssetID(7), 4, (Digest("sha256", "source"),),
        ...         "book.epub",
        ...     ),),
        ...     executor=ReproductionRecipeArtifactReference(
        ...         "cover-extractor", Digest("sha256", "tool"),
        ...         digital_asset_id=DigitalAssetID(20),
        ...     ),
        ...     parameters_json='{"index":0}',
        ...     environment_json='{"locale":"C"}',
        ...     command=("cover-extractor", "book.epub", "cover.jpg"),
        ...     output_path="cover.jpg",
        ...     expected_output_size=5,
        ...     expected_output_digests=(Digest("sha256", "cover"),),
        ... )
        >>> recipe.can_recreate_exactly
        True
    """

    recipe_type: str
    reproducibility: Reproducibility
    complete: bool
    inputs: tuple[ReproductionRecipeInputReference, ...]
    executor: ReproductionRecipeArtifactReference | None = None
    dependencies: tuple[ReproductionRecipeArtifactReference, ...] = ()
    parameters_json: str = "{}"
    environment_json: str = "{}"
    command: tuple[str, ...] = ()
    working_directory: str = "."
    output_path: str | None = None
    instructions: str | None = None
    expected_output_size: int | None = None
    expected_output_digests: tuple[Digest, ...] = ()
    recipe_version: int = 1

    def __post_init__(self) -> None:
        """
        Validate replay completeness, ordered inputs, and canonical documents.

        Example:
            >>> ReproductionRecipe(
            ...     "extract", Reproducibility.EXACT, True, (),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: a complete recipe requires pinned inputs.


        :return:
        """

        _require_text(self.recipe_type, "recipe_type")
        if self.recipe_version < 1:
            raise ValueError("recipe_version must be positive.")
        _require_contiguous_positions(
            tuple(input_.sequence_number for input_ in self.inputs),
            "recipe input",
        )
        _require_json_object(self.parameters_json, "parameters_json")
        _require_json_object(self.environment_json, "environment_json")
        if any(not argument for argument in self.command):
            raise ValueError("recipe command arguments must not be empty.")
        _require_relative_path(
            self.working_directory,
            "working_directory",
            allow_current_directory=True,
        )
        if self.output_path is not None:
            _require_relative_path(self.output_path, "output_path")
        _require_optional_text(self.instructions, "instructions")
        if self.expected_output_size is not None and self.expected_output_size < 0:
            raise ValueError("expected_output_size must not be negative.")
        _require_unique_digests(self.expected_output_digests)
        artifact_names = [artifact.name for artifact in self.dependencies]
        if len(artifact_names) != len(set(artifact_names)):
            raise ValueError("recipe dependency names must be unique.")
        if self.complete:
            if self.reproducibility is Reproducibility.NOT_REPRODUCIBLE:
                raise ValueError("a non-reproducible recipe cannot be complete.")
            if not self.inputs:
                raise ValueError("a complete recipe requires pinned inputs.")
            if self.executor is None:
                raise ValueError("a complete recipe requires a pinned executor.")
            if not self.executor.has_retrieval_source:
                raise ValueError(
                    "a complete recipe requires a retrievable executor artefact."
                )
            if any(
                not dependency.has_retrieval_source
                for dependency in self.dependencies
            ):
                raise ValueError(
                    "a complete recipe requires retrievable dependency artefacts."
                )
            if not self.command:
                raise ValueError("a complete recipe requires a replay command.")
            if self.output_path is None:
                raise ValueError("a complete recipe requires an output path.")
        if self.complete and self.reproducibility is Reproducibility.EXACT:
            if self.expected_output_size is None:
                raise ValueError(
                    "an exact complete recipe requires expected output size."
                )
            if not self.expected_output_digests:
                raise ValueError(
                    "an exact complete recipe requires expected output digests."
                )

    @property
    def can_recreate_exactly(self) -> bool:
        """
        Return whether this recipe claims complete byte-identical recreation.

        Example:
            >>> recipe.can_recreate_exactly  # doctest: +SKIP
            True


        :return:
        """

        return self.complete and self.reproducibility is Reproducibility.EXACT


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetDerivationDeclaration:
    """
    Input for recording how one atomic Asset was produced.

    The result remains an ordinary Digital Asset. This value records its
    provenance, ordered sources, and optional replay recipe.

    Example:
        >>> declaration = DigitalAssetDerivationDeclaration(
        ...     result_digital_asset_id=DigitalAssetID(8),
        ...     sources=(DigitalAssetDerivationSourceReference(
        ...         0, digital_asset_id=DigitalAssetID(7), role="primary",
        ...     ),),
        ...     kind=DigitalAssetDerivationKind.EXTRACT,
        ...     recipe=None,
        ... )
        >>> declaration.kind is DigitalAssetDerivationKind.EXTRACT
        True
    """

    result_digital_asset_id: DigitalAssetID
    sources: tuple[DigitalAssetDerivationSourceReference, ...]
    kind: DigitalAssetDerivationKind
    recipe: ReproductionRecipe | None = None
    output_role: str | None = None
    created_at: datetime | None = None
    operator: str | None = None
    notes: str | None = None

    def __post_init__(self) -> None:
        """
        Require valid result identity, source ordering, and timestamps.

        Example:
            >>> DigitalAssetDerivationDeclaration(
            ...     DigitalAssetID(8), (), DigitalAssetDerivationKind.EXTRACT,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: an Asset derivation requires at least one source.


        :return:
        """

        if self.result_digital_asset_id <= 0:
            raise ValueError("result_digital_asset_id must be positive.")
        if not self.sources:
            raise ValueError("an Asset derivation requires at least one source.")
        _require_contiguous_positions(
            tuple(source.sequence_number for source in self.sources),
            "derivation source",
        )
        if any(
            source.digital_asset_id == self.result_digital_asset_id
            for source in self.sources
        ):
            raise ValueError("an Asset cannot be derived directly from itself.")
        _require_optional_text(self.output_role, "output_role")
        _require_optional_text(self.operator, "operator")
        _require_optional_text(self.notes, "notes")
        if self.created_at is not None:
            if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
                raise ValueError("created_at must be timezone-aware.")
        if self.recipe is not None and self.recipe.can_recreate_exactly:
            expected_algorithms = {
                digest.algorithm for digest in self.recipe.expected_output_digests
            }
            if not expected_algorithms:
                raise ValueError("an exact recipe must identify its expected output.")


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetDerivationRecord:
    """
    Manager-maintained provenance facts for one Asset derivation.

    The record is a public domain value, not the repository adapter's database
    row or document representation.

    Example:
        >>> record = DigitalAssetDerivationRecord(  # doctest: +SKIP
        ...     DigitalAssetDerivationID(11), declaration,
        ... )
        >>> record.digital_asset_derivation_id  # doctest: +SKIP
        11
    """

    digital_asset_derivation_id: DigitalAssetDerivationID
    declaration: DigitalAssetDerivationDeclaration
    revision: str | None = None

    def __post_init__(self) -> None:
        """
        Require positive identity and a non-empty optional revision.

        Example:
            >>> DigitalAssetDerivationRecord(  # doctest: +SKIP
            ...     DigitalAssetDerivationID(0), declaration,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: digital_asset_derivation_id must be positive.


        :return:
        """

        if self.digital_asset_derivation_id <= 0:
            raise ValueError("digital_asset_derivation_id must be positive.")
        _require_optional_text(self.revision, "revision")

    @property
    def can_recreate_exactly(self) -> bool:
        """
        Return whether the attached recipe supports exact recreation.

        Example:
            >>> record.can_recreate_exactly  # doctest: +SKIP
            True


        :return:
        """

        return (
            self.declaration.recipe is not None
            and self.declaration.recipe.can_recreate_exactly
        )


def _require_text(value: str, field_name: str) -> None:
    """
    Reject an empty required text value.

    Example:
        >>> _require_text("", "name")
        Traceback (most recent call last):
        ...
        ValueError: name must not be empty.


    :param value:
    :param field_name:
    :return:
    """

    if not value.strip():
        raise ValueError(f"{field_name} must not be empty.")


def _require_optional_text(value: str | None, field_name: str) -> None:
    """
    Reject an empty optional text value when supplied.

    Example:
        >>> _require_optional_text(None, "role")


    :param value:
    :param field_name:
    :return:
    """

    if value is not None:
        _require_text(value, field_name)


def _require_unique_digests(digests: tuple[Digest, ...]) -> None:
    """
    Require no duplicate digest algorithms.

    Example:
        >>> _require_unique_digests((Digest("sha256", "abcd"),))


    :param digests:
    :return:
    """

    algorithms = [digest.algorithm for digest in digests]
    if len(algorithms) != len(set(algorithms)):
        raise ValueError("digest algorithms must be unique.")


def _require_contiguous_positions(positions: tuple[int, ...], label: str) -> None:
    """
    Require unique, contiguous zero-based sequence numbers.

    Example:
        >>> _require_contiguous_positions((0, 1), "input")


    :param positions:
    :param label:
    :return:
    """

    if sorted(positions) != list(range(len(positions))):
        raise ValueError(f"{label} sequence numbers must be unique and contiguous.")


def _require_relative_path(
    value: str,
    field_name: str,
    *,
    allow_current_directory: bool = False,
) -> None:
    """
    Require a canonical portable path confined to a recipe workspace.

    Example:
        >>> _require_relative_path("disc-1/track.mp3", "logical_path")


    :param value:
    :param field_name:
    :param allow_current_directory:
    :return:
    """

    _require_text(value, field_name)
    if "\\" in value or "\x00" in value:
        raise ValueError(f"{field_name} must be a portable POSIX path.")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{field_name} must remain inside the recipe workspace.")
    if value != str(path):
        raise ValueError(f"{field_name} must be a canonical POSIX path.")
    if path == PurePosixPath(".") and not allow_current_directory:
        raise ValueError(f"{field_name} must identify a file path.")


def _require_json_object(document: str, field_name: str) -> None:
    """
    Require a canonical JSON object string with no insignificant whitespace.

    Example:
        >>> _require_json_object('{"index":0}', "parameters_json")


    :param document:
    :param field_name:
    :return:
    """

    import json

    try:
        value: object = json.loads(document)  # pyright: ignore[reportAny]
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError(f"{field_name} must be valid JSON.") from error
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must contain a JSON object.")
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
    if document != canonical:
        raise ValueError(f"{field_name} must be canonical JSON.")


__all__ = [
    "DigitalAssetDerivationDeclaration",
    "DigitalAssetDerivationRecord",
    "DigitalAssetDerivationKind",
    "DigitalAssetDerivationSourceReference",
    "ReproductionRecipeArtifactReference",
    "ReproductionRecipeInputReference",
    "Reproducibility",
    "ReproductionRecipe",
]
