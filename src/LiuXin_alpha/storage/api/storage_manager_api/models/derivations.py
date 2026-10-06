"""
Represent provenance sources, pinned replay recipes, graph inventories, and recreation plans.

These values validate selected local relationships without reading Asset bytes or
executing recipes. Manager operations own reference/identity checks and traversal;
availability predicates interpret supplied evidence. Private validators preserve
original values while checking text, digest algorithms, numbering, paths, and JSON.
"""

from __future__ import annotations

import dataclasses
import json
import shutil

from collections.abc import Callable
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
    Name the broad operation recorded as an Asset's provenance.

    These labels classify an assertion about how bytes were produced. Choosing a kind neither
    selects an executor nor establishes replayability or byte identity.

    Example:
        >>> DigitalAssetDerivationKind.EXTRACT.value
        'extract'
    """

    EXTRACT = "extract"
    CONVERT = "convert"
    TRANSCODE = "transcode"
    COMPRESS = "compress"
    DENOISE = "denoise"
    OCR = "ocr"
    PACKAGE = "package"
    GENERATE = "generate"
    NORMALIZE = "normalize"
    REPAIR = "repair"
    OTHER = "other"


class Reproducibility(StrEnum):
    """
    State how closely a recipe claims it can reproduce its original output.

    EXACT claims identical bytes, BEST_EFFORT allows a rerun without that promise, and
    NOT_REPRODUCIBLE records the absence of replay support. Recipe completeness and current
    input/tool availability are separate requirements. The claim itself is not an execution or
    verification result.

    Example:
        >>> Reproducibility.EXACT.value
        'exact'
    """

    EXACT = "exact"
    BEST_EFFORT = "best_effort"
    NOT_REPRODUCIBLE = "not_reproducible"


class DigitalAssetDerivationGraphDirection(StrEnum):
    """
    Select which provenance edges to follow from a root Asset.

    ANCESTORS follows results to inputs; DESCENDANTS follows inputs to results. The composed manager
    implements BOTH as an ancestor walk followed by a separate descendant walk from the same root.
    It does not repeatedly switch direction at every discovered node to find the entire undirected
    component.

    Example:
        >>> DigitalAssetDerivationGraphDirection.ANCESTORS.value
        'ancestors'
    """

    ANCESTORS = "ancestors"
    DESCENDANTS = "descendants"
    BOTH = "both"


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetDerivationSourceReference:
    """
    Retain one positioned atomic or Composite provenance source.

    Construction requires exactly one positive source ID, a nonnegative sequence number, and a
    nonblank role when supplied. It does not resolve the source, enforce integer types, or normalize
    retained text. A Composite reference identifies membership to expand later, rather than pinning
    each member's bytes.

    Example:
        >>> source = DigitalAssetDerivationSourceReference(
        ...     sequence_number=0, digital_asset_id=DigitalAssetID(7),
        ...     role="primary",
        ... )
        >>> source.digital_asset_id
        7


    :ivar sequence_number: Zero-based provenance position; checked for negativity here and contiguity by the declaration.
    :ivar digital_asset_id: Atomic source identity, mutually exclusive with composite_digital_asset_id.
    :ivar composite_digital_asset_id: Composite source identity whose members the manager expands.
    :ivar role: Optional nonblank source role retained with its original whitespace.
    """

    sequence_number: int
    digital_asset_id: DigitalAssetID | None = None
    composite_digital_asset_id: CompositeDigitalAssetID | None = None
    role: str | None = None

    def __post_init__(self) -> None:
        """
        Reject missing or simultaneous source IDs, nonpositive identities, negative positions, and
        blank supplied roles.

        Numeric comparisons do not enforce integer types. No Asset lookup, member expansion, or text
        reassignment occurs.

        Example:
            >>> DigitalAssetDerivationSourceReference(0)
            Traceback (most recent call last):
            ...
            ValueError: exactly one derivation source identity is required.


        :return: None when these identity/position/text checks pass; ValueError or malformed-input errors otherwise propagate.
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
    Pin an atomic recipe input by registered ID, expected size/digests, and workspace path.

    Even Composite provenance uses atomic recipe inputs so member byte identities can be checked
    independently. Construction validates local value structure; it does not query the manager, read
    bytes, or ensure another input uses a different Asset or path. Retained containers are not
    copied.

    Example:
        >>> input_ = ReproductionRecipeInputReference(
        ...     0, DigitalAssetID(7), 4, (Digest("sha256", "abcd"),),
        ...     logical_path="book.epub", role="primary",
        ... )
        >>> input_.logical_path
        'book.epub'


    :ivar sequence_number: Nonnegative input position; recipe-level validation checks contiguous numbering.
    :ivar digital_asset_id: Positive atomic Asset identity, resolved only when a manager records the recipe.
    :ivar size_bytes: Expected input byte count, rejected when it compares below zero.
    :ivar digests: Nonempty expected digest collection with distinct algorithm attributes.
    :ivar logical_path: Canonical relative POSIX path for the input in a replay workspace; checked lexically.
    :ivar role: Optional nonblank input role, retained without stripping.
    """

    sequence_number: int
    digital_asset_id: DigitalAssetID
    size_bytes: int
    digests: tuple[Digest, ...]
    logical_path: str
    role: str | None = None

    def __post_init__(self) -> None:
        """
        Check position, ID, size, digest-algorithm uniqueness, lexical workspace path, and optional
        role.

        Numeric types and digest contents are not independently validated. Path checks do not
        inspect an actual workspace or symlink targets, and no content hash is computed.

        Example:
            >>> ReproductionRecipeInputReference(
            ...     0, DigitalAssetID(7), 4, (), "book.epub",
            ... )
            Traceback (most recent call last):
            ...
            ValueError: a recipe input requires at least one digest.


        :return: None after the selected identity/path constraints pass; validation and malformed-input errors propagate.
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
    Describe an executor or dependency using a digest and optional retrieval hints.

    A managed Asset ID and an external URI may coexist or both be absent. Construction checks
    name/version/URI text and a supplied ID, without validating the digest object, URI scheme, or
    availability. A frozen reference records evidence and does not make the referenced bytes
    immutable.

    Example:
        >>> artifact_reference = ReproductionRecipeArtifactReference(
        ...     "calibre-ebook-convert", Digest("sha256", "abcd"),
        ...     version="7.20.0", digital_asset_id=DigitalAssetID(20),
        ... )
        >>> artifact_reference.version
        '7.20.0'

    :ivar name: Nonblank artefact label retained without whitespace normalization.
    :ivar digest: Expected content digest; this constructor does not compute or independently validate it.
    :ivar version: Optional nonblank version label, separate from content identity.
    :ivar uri: Optional nonblank external retrieval hint; URI syntax and reachability are not checked here.
    :ivar digital_asset_id: Optional positive managed Asset identity; may be supplied together with uri.
    """

    name: str
    digest: Digest
    version: str | None = None
    uri: str | None = None
    digital_asset_id: DigitalAssetID | None = None

    def __post_init__(self) -> None:
        """
        Require a nonblank name, nonblank supplied version/URI, and a positive supplied Asset ID.

        The digest and actual retrieval routes remain unchecked. Original text and values are
        retained.

        Example:
            >>> ReproductionRecipeArtifactReference("", Digest("sha256", "abcd"))
            Traceback (most recent call last):
            ...
            ValueError: name must not be empty.


        :return: None when text and optional-ID comparisons pass; invalid values or malformed inputs raise.
        """

        _require_text(self.name, "name")
        _require_optional_text(self.version, "version")
        _require_optional_text(self.uri, "uri")
        if self.digital_asset_id is not None and self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive when supplied.")

    @property
    def has_retrieval_source(self) -> bool:
        """
        Report whether either a managed Asset ID or external URI is stated.

        This tests presence only. It does not consult a Store, resolver, registry, or the expected
        digest.

        Example:
            >>> ReproductionRecipeArtifactReference(
            ...     "tool", Digest("sha256", "abcd"), uri="oci://tool@sha256:abcd",
            ... ).has_retrieval_source
            True


        :return: True if digital_asset_id or uri is not None; False when both are absent.
        """

        return self.digital_asset_id is not None or self.uri is not None


@dataclasses.dataclass(slots=True, frozen=True)
class ExternalReproductionCommand:
    """
    Represent one argument-vector invocation of an external executable.

    The value is shell-neutral: arguments are retained separately and never joined or evaluated by
    a shell. Canonical JSON supports durable interchange, while executable discovery is an explicit
    environment observation that can use an injected resolver in tests or sandboxed runtimes.

    Example:
        >>> command = ExternalReproductionCommand(("converter", "in.epub", "out.pdf"))
        >>> ExternalReproductionCommand.from_json(command.to_json()) == command
        True


    :ivar arguments: Nonempty argument vector whose first entry names or paths the executable.
    """

    arguments: tuple[str, ...]

    def __post_init__(self) -> None:
        """Require a nonempty tuple of nonempty string arguments."""
        if not isinstance(self.arguments, tuple):
            raise TypeError("external command arguments must be a tuple.")
        if not self.arguments:
            raise ValueError("an external command requires an executable argument.")
        if any(not isinstance(argument, str) for argument in self.arguments):
            raise TypeError("external command arguments must be strings.")
        if any(not argument for argument in self.arguments):
            raise ValueError("external command arguments must not be empty.")

    @property
    def executable(self) -> str:
        """Return the first argument without resolving or executing it."""
        return self.arguments[0]

    def resolve_executable(
        self,
        resolver: Callable[[str], str | None] = shutil.which,
    ) -> str | None:
        """
        Ask a caller-controlled resolver whether the executable is currently available.

        :param resolver: Callable receiving the executable text and returning a resolved path or None.
        :return: Resolver result unchanged; no command is executed.
        """
        return resolver(self.executable)

    def to_json(self) -> str:
        """Serialize the versioned external-command document in canonical JSON spelling."""
        return json.dumps(
            {
                "arguments": self.arguments,
                "kind": "external",
                "version": 1,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, document: str) -> ExternalReproductionCommand:
        """
        Parse the exact supported external-command document schema.

        The version must be a JSON integer exactly equal to one. JSON booleans are rejected
        explicitly even though Python's decoded ``True`` compares equal to integer one.

        Example:
            >>> ExternalReproductionCommand.from_json(
            ...     '{"arguments":["tool"],"kind":"external","version":1}'
            ... ).arguments
            ('tool',)

        :param document: JSON object containing integer version 1, kind external, and a string argument list.
        :return: Validated command; malformed JSON, unknown fields/kinds/versions, and bad arguments raise ValueError or TypeError.
        """
        try:
            value: object = json.loads(document)  # pyright: ignore[reportAny]
        except (TypeError, json.JSONDecodeError) as error:
            raise ValueError("external command must be valid JSON.") from error
        if not isinstance(value, dict):
            raise ValueError("external command JSON must contain an object.")
        if set(value) != {"arguments", "kind", "version"}:
            raise ValueError("external command JSON has unsupported fields.")
        version = value["version"]
        if (
            value["kind"] != "external"
            or isinstance(version, bool)
            or not isinstance(version, int)
            or version != 1
        ):
            raise ValueError("unsupported external command kind or version.")
        arguments = value["arguments"]
        if not isinstance(arguments, list):
            raise TypeError("external command arguments must be a JSON array.")
        if any(not isinstance(argument, str) for argument in arguments):
            raise TypeError("external command arguments must be strings.")
        return cls(tuple(arguments))


@dataclasses.dataclass(slots=True, frozen=True)
class ReproductionNormalizationDigest:
    """
    Pin a digest calculated after a named deterministic normalization profile.

    This records comparison evidence for outputs whose irrelevant representation details may vary.
    It does not implement the normalizer, claim byte identity, or make an otherwise incomplete
    recipe replayable.

    Example:
        >>> evidence = ReproductionNormalizationDigest(
        ...     "epub-metadata-v1", Digest("sha256", "abcd"),
        ... )
        >>> evidence.normalizer
        'epub-metadata-v1'


    :ivar normalizer: Nonblank stable profile or implementation identifier.
    :ivar digest: Expected digest of the normalized output bytes.
    :ivar normalizer_version: Optional nonblank version independent of the profile identifier.
    :ivar parameters_json: Canonical JSON object configuring normalization, defaulting to empty.
    """

    normalizer: str
    digest: Digest
    normalizer_version: str | None = None
    parameters_json: str = "{}"

    def __post_init__(self) -> None:
        """Validate profile text, Digest type, optional version, and canonical parameters."""
        _require_text(self.normalizer, "normalizer")
        if not isinstance(self.digest, Digest):
            raise TypeError("normalization digest must be a Digest.")
        _require_optional_text(self.normalizer_version, "normalizer_version")
        _require_json_object(self.parameters_json, "parameters_json")


@dataclasses.dataclass(slots=True, frozen=True)
class ReproductionRecipe:
    """
    Retain replay instructions, pinned inputs/artefacts, and an output-identity claim.

    Construction checks document/path structure and stronger evidence requirements for a truthy
    complete recipe. It neither executes commands nor proves determinism, input/tool availability,
    or output identity. Canonical JSON follows Python json serialization defaults, and supplied
    sequences remain shared despite the frozen dataclass.

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


    :ivar recipe_type: Nonblank descriptive recipe label; no executor registry lookup occurs.
    :ivar reproducibility: Replay claim; exact/non-reproducible validation branches use enum identity without coercion.
    :ivar complete: Declared completeness; truthiness activates required input/executor/command/output checks.
    :ivar inputs: Pinned atomic inputs with contiguous zero-based positions; supplied sequence order is retained.
    :ivar executor: Optional pinned executable; a complete recipe requires a stated retrieval source.
    :ivar dependencies: Pinned dependency artefacts with exactly unique names; complete recipes require retrieval hints.
    :ivar parameters_json: JSON object text equal to sorted, compact json.dumps output with default escaping.
    :ivar environment_json: Environment object text checked by the same canonical JSON rule.
    :ivar command: Retained argument sequence; false entries reject, but whitespace-only arguments are allowed.
    :ivar working_directory: Canonical relative POSIX workspace directory; the literal current directory is allowed.
    :ivar output_path: Optional canonical relative POSIX output path, required when complete is truthy.
    :ivar instructions: Optional nonblank explanatory text retained without normalization.
    :ivar expected_output_size: Optional nonnegative output byte count, required for a complete EXACT enum claim.
    :ivar expected_output_digests: Expected output digests with distinct algorithms; nonempty for complete EXACT recipes.
    :ivar recipe_version: Schema/version value rejected when it compares below one, without integer coercion.
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
    normalized_output_digests: tuple[ReproductionNormalizationDigest, ...] = ()

    def __post_init__(self) -> None:
        """
        Validate recipe structure and the additional evidence required by completeness and
        exactness.

        Checks include contiguous input positions, canonical JSON objects, truthy command entries,
        lexical paths, optional text/size, digest-algorithm uniqueness, and unique dependency names.
        Position validation retains an unsorted supplied sequence, and does not require unique Asset
        IDs or logical paths.

        A complete recipe requires inputs, executor and dependency retrieval hints, a command, and
        an output path; the NOT_REPRODUCIBLE enum singleton rejects. Complete EXACT enum recipes
        also require output size and digests. No enum coercion, command execution, URI probing, or
        comparison with registered Asset identities occurs.

        Example:
            >>> ReproductionRecipe(
            ...     "extract", Reproducibility.EXACT, True, (),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: a complete recipe requires pinned inputs.


        :return: None when the selected structure/completeness checks pass; ValueError and malformed-input errors propagate.
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
        normalization_keys = tuple(
            (
                evidence.normalizer,
                evidence.normalizer_version,
                evidence.parameters_json,
                evidence.digest.algorithm,
            )
            for evidence in self.normalized_output_digests
        )
        if len(normalization_keys) != len(set(normalization_keys)):
            raise ValueError(
                "normalized output digests must be unique by profile and algorithm."
            )
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
                not dependency.has_retrieval_source for dependency in self.dependencies
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
        Evaluate the declared completeness and exact-reproducibility flags.

        The reproducibility check uses the EXACT enum singleton; an equal plain string is not
        coerced. This property does not check current inputs, tools, or actual replay output.

        Example:
            >>> recipe.can_recreate_exactly  # doctest: +SKIP
            True


        :return: The supplied complete value when false, otherwise whether reproducibility is the EXACT enum singleton.
        """

        return self.complete and self.reproducibility is Reproducibility.EXACT

    @property
    def external_command(self) -> ExternalReproductionCommand | None:
        """Return a typed external command for a nonempty argument vector, otherwise None."""
        if not self.command:
            return None
        return ExternalReproductionCommand(self.command)

    @property
    def can_verify_normalized_equivalence(self) -> bool:
        """
        Report complete replay evidence with exact or normalization-based output identity.

        Exact recipes already provide stronger byte identity. For other complete recipes, at least
        one normalization digest permits a caller with the named normalizer to compare semantic or
        representation-insensitive output. No normalizer is run here.
        """
        return self.complete and (
            self.reproducibility is Reproducibility.EXACT
            or bool(self.normalized_output_digests)
        )


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetDerivationDeclaration:
    """
    Describe how an existing atomic result was produced from positioned provenance sources.

    Construction checks selected IDs, source numbering, direct atomic self-reference, optional text,
    and timestamp awareness. It does not resolve sources, expand Composites, detect indirect cycles,
    validate the kind, or compare recipe identities with registered bytes. The manager performs
    those reference and identity checks when recording the assertion.

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


    :ivar result_digital_asset_id: Positive identity of the atomic result, not a newly allocated identity.
    :ivar sources: Nonempty source sequence with contiguous positions; original order and repeated source IDs remain.
    :ivar kind: Semantic operation label, retained without enum validation or coercion.
    :ivar recipe: Optional replay evidence; an exact claim must expose expected output digest algorithms.
    :ivar output_role: Optional nonblank role of the result within a broader operation.
    :ivar created_at: Optional timezone-aware provenance timestamp, retained without timezone conversion.
    :ivar operator: Optional nonblank operator label; no account lookup is performed.
    :ivar notes: Optional nonblank provenance explanation, retained verbatim.
    :ivar workflow_id: Optional workflow identifier rejected when it compares at or below zero.
    :ivar workflow_reference: Optional nonblank namespaced workflow label; syntax and existence are not checked.
    """

    result_digital_asset_id: DigitalAssetID
    sources: tuple[DigitalAssetDerivationSourceReference, ...]
    kind: DigitalAssetDerivationKind
    recipe: ReproductionRecipe | None = None
    output_role: str | None = None
    created_at: datetime | None = None
    operator: str | None = None
    notes: str | None = None
    workflow_id: int | None = None
    workflow_reference: str | None = None

    def __post_init__(self) -> None:
        """
        Reject invalid result/source structure, direct atomic self-reference, blank optional text,
        and naive timestamps.

        Sources must have contiguous zero-based positions but are not sorted or deduplicated by
        identity. Workflow IDs receive a positive comparison. An attached exact recipe must name an
        expected output digest algorithm; no recursive graph or repository validation occurs.

        Example:
            >>> DigitalAssetDerivationDeclaration(
            ...     DigitalAssetID(8), (), DigitalAssetDerivationKind.EXTRACT,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: an Asset derivation requires at least one source.


        :return: None when declaration-level value constraints pass; validation and malformed-input errors propagate.
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
        _require_optional_text(self.workflow_reference, "workflow_reference")
        if self.workflow_id is not None and self.workflow_id <= 0:
            raise ValueError("workflow_id must be positive when supplied.")
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
    Pair a manager-assigned derivation ID with its provenance declaration and revision.

    Direct construction validates only the positive ID and nonblank supplied revision. It does not
    revalidate the declaration or prove persistence, replayability, or current byte availability.
    The declaration and nested values remain retained references.

    Example:
        >>> record = DigitalAssetDerivationRecord(  # doctest: +SKIP
        ...     DigitalAssetDerivationID(11), declaration,
        ... )
        >>> record.digital_asset_derivation_id  # doctest: +SKIP
        11


    :ivar digital_asset_derivation_id: Positive registered provenance identity, distinct from the result Asset ID.
    :ivar declaration: Retained provenance and optional recipe, not revalidated by record construction.
    :ivar revision: Optional nonblank optimistic-lock token, retained with its original whitespace.
    """

    digital_asset_derivation_id: DigitalAssetDerivationID
    declaration: DigitalAssetDerivationDeclaration
    revision: str | None = None

    def __post_init__(self) -> None:
        """
        Check a positive derivation ID and a nonblank optional revision without inspecting the
        declaration.

        Example:
            >>> DigitalAssetDerivationRecord(  # doctest: +SKIP
            ...     DigitalAssetDerivationID(0), declaration,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: digital_asset_derivation_id must be positive.


        :return: None when identity/revision checks pass; invalid comparisons or revision text raise.
        """

        if self.digital_asset_derivation_id <= 0:
            raise ValueError("digital_asset_derivation_id must be positive.")
        _require_optional_text(self.revision, "revision")

    @property
    def can_recreate_exactly(self) -> bool:
        """
        Delegate exactness to the attached recipe when one exists.

        This reports declared replay evidence, without probing sources or executing the recipe. A
        malformed declaration can fail during attribute access.

        Example:
            >>> record.can_recreate_exactly  # doctest: +SKIP
            True


        :return: False when no recipe is attached; otherwise the recipe can_recreate_exactly value.
        """

        return (
            self.declaration.recipe is not None
            and self.declaration.recipe.can_recreate_exactly
        )


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetDerivationGraph:
    """
    Retain a rooted provenance inventory and its ordered derivation records.

    The manager supplies traversal order and a depth-truncation flag. Direct construction checks
    root inclusion and identity uniqueness, without validating direction, positive non-root IDs,
    connectivity, edge coverage, or the truth of truncation. Frozen fields do not copy supplied
    sequences.

    Example:
        >>> graph = DigitalAssetDerivationGraph(
        ...     DigitalAssetID(8),
        ...     DigitalAssetDerivationGraphDirection.ANCESTORS,
        ...     (DigitalAssetID(8),),
        ... )
        >>> graph.digital_asset_ids
        (8,)


    :ivar root_digital_asset_id: Positive atomic starting identity, required in digital_asset_ids.
    :ivar direction: Requested traversal direction, retained without constructor-level validation.
    :ivar digital_asset_ids: Unique atomic node inventory in supplied traversal order.
    :ivar composite_digital_asset_ids: Unique Composite provenance identities encountered in supplied records.
    :ivar derivation_records: Ordered records with distinct derivation IDs; graph consistency is not rechecked.
    :ivar truncated: Supplied indication that a traversal encountered adjacency at its depth limit.
    """

    root_digital_asset_id: DigitalAssetID
    direction: DigitalAssetDerivationGraphDirection
    digital_asset_ids: tuple[DigitalAssetID, ...]
    composite_digital_asset_ids: tuple[CompositeDigitalAssetID, ...] = ()
    derivation_records: tuple[DigitalAssetDerivationRecord, ...] = ()
    truncated: bool = False

    def __post_init__(self) -> None:
        """
        Require a positive included root and unique atomic, Composite, and derivation identities.

        The check does not validate non-root identity positivity, direction, connectivity, or
        consistency between node inventories and record endpoints.

        Example:
            >>> DigitalAssetDerivationGraph(
            ...     DigitalAssetID(0),
            ...     DigitalAssetDerivationGraphDirection.ANCESTORS,
            ...     (DigitalAssetID(0),),
            ... )
            Traceback (most recent call last):
            ...
            ValueError: root_digital_asset_id must be positive.


        :return: None if root membership and identity uniqueness hold; invalid values or malformed collections raise.
        """

        if self.root_digital_asset_id <= 0:
            raise ValueError("root_digital_asset_id must be positive.")
        if self.root_digital_asset_id not in self.digital_asset_ids:
            raise ValueError("digital_asset_ids must contain the graph root.")
        if len(self.digital_asset_ids) != len(set(self.digital_asset_ids)):
            raise ValueError("digital_asset_ids must be unique.")
        if len(self.composite_digital_asset_ids) != len(
            set(self.composite_digital_asset_ids)
        ):
            raise ValueError("composite_digital_asset_ids must be unique.")
        derivation_ids = tuple(
            record.digital_asset_derivation_id for record in self.derivation_records
        )
        if len(derivation_ids) != len(set(derivation_ids)):
            raise ValueError("derivation_records must be unique.")

    def derivations_producing(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        exact_only: bool = False,
    ) -> tuple[DigitalAssetDerivationRecord, ...]:
        """Return ordered records whose declared result is the selected Asset.

        ``exact_only`` filters on recorded complete exact recipe evidence; it does not check current
        source or executor availability.

        :param digital_asset_id: Result identity to match exactly.
        :param exact_only: Whether to retain only records claiming exact recreation.
        :return: Matching records in graph traversal order.
        """
        return tuple(
            record
            for record in self.derivation_records
            if record.declaration.result_digital_asset_id == digital_asset_id
            and (not exact_only or record.can_recreate_exactly)
        )

    def derivations_using(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> tuple[DigitalAssetDerivationRecord, ...]:
        """Return ordered records directly naming the selected atomic Asset as a source."""
        return tuple(
            record
            for record in self.derivation_records
            if any(
                source.digital_asset_id == digital_asset_id
                for source in record.declaration.sources
            )
        )

    def direct_predecessor_ids(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> tuple[DigitalAssetID, ...]:
        """Return unique direct atomic inputs of records producing the selected Asset."""
        return tuple(
            dict.fromkeys(
                source.digital_asset_id
                for record in self.derivations_producing(digital_asset_id)
                for source in record.declaration.sources
                if source.digital_asset_id is not None
            )
        )

    def direct_successor_ids(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> tuple[DigitalAssetID, ...]:
        """Return unique direct results of records using the selected atomic Asset."""
        return tuple(
            dict.fromkeys(
                record.declaration.result_digital_asset_id
                for record in self.derivations_using(digital_asset_id)
            )
        )

    def to_dot(self) -> str:
        """Render a deterministic Graphviz DOT description without executing Graphviz.

        Atomic and Composite nodes use distinct shapes. Every retained provenance source produces
        one labelled directed edge to its result. The returned text contains no Store locations,
        recipe commands, or other potentially sensitive execution details.

        :return: Complete DOT document suitable for a CLI or UI renderer.
        """
        lines = ["digraph derivations {"]
        for digital_asset_id in self.digital_asset_ids:
            lines.append(
                f'  "asset:{digital_asset_id}" [label="Asset {digital_asset_id}", shape=ellipse];'
            )
        for composite_id in self.composite_digital_asset_ids:
            lines.append(
                f'  "composite:{composite_id}" [label="Composite {composite_id}", shape=box];'
            )
        for record in self.derivation_records:
            result = record.declaration.result_digital_asset_id
            label = _dot_quote(
                f"Derivation {record.digital_asset_derivation_id}: "
                f"{record.declaration.kind.value}"
            )
            for source in record.declaration.sources:
                if source.digital_asset_id is not None:
                    source_node = f"asset:{source.digital_asset_id}"
                else:
                    source_node = f"composite:{source.composite_digital_asset_id}"
                lines.append(
                    f'  "{source_node}" -> "asset:{result}" [label="{label}"];'
                )
        lines.append("}")
        return "\n".join(lines)


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetRecreationPlan:
    """
    Retain a proposed replay route and the availability evidence used to select it.

    Manager-produced steps place prerequisites before consumers and prefer fewer replay steps.
    Direct construction only checks selected identity relationships; it does not prove that steps
    are exact, topologically ordered, sufficient, or still executable. Availability and warnings are
    supplied evidence, and nothing is executed or reserved.

    Example:
        >>> plan = DigitalAssetRecreationPlan(
        ...     DigitalAssetID(8),
        ...     available_digital_asset_ids=(DigitalAssetID(8),),
        ... )
        >>> (plan.already_available, plan.can_recreate_exactly)
        (True, True)


    :ivar digital_asset_id: Positive atomic identity requested by the plan.
    :ivar steps: Proposed derivation records with unique IDs; constructor validation does not establish execution order.
    :ivar available_digital_asset_ids: Unique supplied available-Asset evidence, disjoint from unavailable IDs.
    :ivar unavailable_digital_asset_ids: Unique supplied missing-Asset evidence; identities are not resolved here.
    :ivar selected_derivation_id: Optional selected step ID, required to identify a step producing the requested Asset.
    :ivar alternative_derivation_ids: Unique alternative IDs excluding the selected ID; other step overlap is allowed.
    :ivar warnings: Retained diagnostics, not validated or used by the availability predicates.
    """

    digital_asset_id: DigitalAssetID
    steps: tuple[DigitalAssetDerivationRecord, ...] = ()
    available_digital_asset_ids: tuple[DigitalAssetID, ...] = ()
    unavailable_digital_asset_ids: tuple[DigitalAssetID, ...] = ()
    selected_derivation_id: DigitalAssetDerivationID | None = None
    alternative_derivation_ids: tuple[DigitalAssetDerivationID, ...] = ()
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """
        Check root positivity, disjoint unique availability IDs, unique step/alternative IDs, and
        the selected result.

        A selected derivation must be present among the steps and produce the requested Asset. Other
        IDs need not be positive; no graph ordering, recipe exactness, warning consistency, or
        source availability is checked.

        Example:
            >>> DigitalAssetRecreationPlan(DigitalAssetID(0))
            Traceback (most recent call last):
            ...
            ValueError: digital_asset_id must be positive.


        :return: None when these identity relationships hold; ValueError or malformed-record errors otherwise propagate.
        """

        if self.digital_asset_id <= 0:
            raise ValueError("digital_asset_id must be positive.")
        available = set(self.available_digital_asset_ids)
        unavailable = set(self.unavailable_digital_asset_ids)
        if len(available) != len(self.available_digital_asset_ids):
            raise ValueError("available_digital_asset_ids must be unique.")
        if len(unavailable) != len(self.unavailable_digital_asset_ids):
            raise ValueError("unavailable_digital_asset_ids must be unique.")
        if available & unavailable:
            raise ValueError("available and unavailable Asset IDs must be disjoint.")
        step_ids = tuple(step.digital_asset_derivation_id for step in self.steps)
        if len(step_ids) != len(set(step_ids)):
            raise ValueError("recreation steps must be unique.")
        if self.selected_derivation_id is not None:
            if self.selected_derivation_id not in step_ids:
                raise ValueError("selected_derivation_id must identify a plan step.")
            selected = next(
                step
                for step in self.steps
                if step.digital_asset_derivation_id == self.selected_derivation_id
            )
            if selected.declaration.result_digital_asset_id != self.digital_asset_id:
                raise ValueError(
                    "selected_derivation_id must produce the requested Asset."
                )
        if len(self.alternative_derivation_ids) != len(
            set(self.alternative_derivation_ids)
        ):
            raise ValueError("alternative_derivation_ids must be unique.")
        if self.selected_derivation_id in self.alternative_derivation_ids:
            raise ValueError("the selected derivation cannot be an alternative.")

    @property
    def already_available(self) -> bool:
        """
        Test whether supplied available-ID evidence contains the requested Asset.

        This does not perform a current read, and it does not require an empty replay-step list.

        Example:
            >>> DigitalAssetRecreationPlan(
            ...     DigitalAssetID(8),
            ...     available_digital_asset_ids=(DigitalAssetID(8),),
            ... ).already_available
            True


        :return: True when digital_asset_id belongs to available_digital_asset_ids; False otherwise.
        """

        return self.digital_asset_id in self.available_digital_asset_ids

    @property
    def requires_replay(self) -> bool:
        """
        Report whether any steps are supplied, independently of availability and selected-route
        evidence.

        Example:
            >>> DigitalAssetRecreationPlan(DigitalAssetID(8)).requires_replay
            False


        :return: True for a nonempty steps collection; False otherwise.
        """

        return bool(self.steps)

    @property
    def can_recreate_exactly(self) -> bool:
        """
        Evaluate availability evidence or a selected route with no reported unavailable Assets.

        Warnings and individual recipe exactness are not inspected. Directly constructed evidence
        can therefore satisfy this predicate without establishing that replay will produce the
        expected bytes.

        Example:
            >>> DigitalAssetRecreationPlan(
            ...     DigitalAssetID(8),
            ...     available_digital_asset_ids=(DigitalAssetID(8),),
            ... ).can_recreate_exactly
            True


        :return: True if already_available, or if a selected derivation exists and unavailable_digital_asset_ids is empty.
        """

        return self.already_available or (
            self.selected_derivation_id is not None
            and not self.unavailable_digital_asset_ids
        )


def _dot_quote(value: str) -> str:
    """Escape text for one double-quoted Graphviz DOT label."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _require_text(value: str, field_name: str) -> None:
    """
    Require text that remains nonempty after stripping for the check.

    The original value is not stripped, returned, or reassigned. Non-string values can fail when
    strip is accessed; NUL and other text contents are not rejected here.

    Example:
        >>> _require_text("", "name")
        Traceback (most recent call last):
        ...
        ValueError: name must not be empty.


    :param value: Required text to check without normalization.
    :param field_name: Label interpolated into the empty-text ValueError.
    :return: None for nonblank text; ValueError for blank text or an underlying attribute error for malformed input.
    """

    if not value.strip():
        raise ValueError(f"{field_name} must not be empty.")


def _require_optional_text(value: str | None, field_name: str) -> None:
    """
    Accept None or delegate the nonblank-text check without changing supplied text.

    Example:
        >>> _require_optional_text(None, "role")


    :param value: Optional text; None bypasses validation.
    :param field_name: Field label passed to the required-text validator.
    :return: None when omitted or nonblank; delegated validation errors propagate.
    """

    if value is not None:
        _require_text(value, field_name)


def _require_unique_digests(digests: tuple[Digest, ...]) -> None:
    """
    Reject repeated algorithm attributes without hashing bytes or validating digest values.

    Empty collections are allowed. Entries are inspected as supplied; ordinary Digest construction
    performs its own field normalization.

    Example:
        >>> _require_unique_digests((Digest("sha256", "abcd"),))


    :param digests: Digest entries whose algorithm attributes must be unique and hashable.
    :return: None for unique algorithms, including an empty collection; duplicates raise ValueError.
    """

    algorithms = [digest.algorithm for digest in digests]
    if len(algorithms) != len(set(algorithms)):
        raise ValueError("digest algorithms must be unique.")


def _require_contiguous_positions(positions: tuple[int, ...], label: str) -> None:
    """
    Compare sorted positions with the zero-based range of the same length.

    This accepts an empty or unsorted contiguous collection without reordering it. Numeric equality
    can admit values such as integral floats; type coercion and non-position identity checks are
    absent.

    Example:
        >>> _require_contiguous_positions((0, 1), "input")


    :param positions: Position values checked through sorting and equality without mutation.
    :param label: Description prefixed to the sequence-number validation error.
    :return: None for unique contiguous zero-based values; ValueError for a mismatch and comparison errors for malformed values.
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
    Require canonical relative POSIX spelling with no parent traversal, backslashes, or NUL.

    PurePosixPath normalization must leave the spelling unchanged. The literal current directory is
    accepted only when requested. This is a lexical rule: it does not resolve symlinks, inspect a
    workspace, or reject every platform-specific filename such as a drive-like POSIX segment.

    Example:
        >>> _require_relative_path("disc-1/track.mp3", "logical_path")


    :param value: Nonblank path text to check without rewriting.
    :param field_name: Label included in validation errors.
    :param allow_current_directory: Whether the canonical literal current directory may stand for a workspace directory.
    :return: None when the lexical path constraints pass; ValueError or malformed-input errors otherwise propagate.
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
    Require object text matching sorted, compact Python JSON serialization.

    Parsing must yield a dict and serializing it with sort_keys=True and compact separators must
    reproduce the supplied document exactly. Default ASCII escaping and Python handling of nonfinite
    numeric constants apply; this is not a separate strict-JSON schema or standards validator. No
    parsed value is retained.

    Example:
        >>> _require_json_object('{"index":0}', "parameters_json")


    :param document: JSON object text compared exactly with the canonical serialization.
    :param field_name: Field label included in parse, object-kind, or canonical-spelling errors.
    :return: None for matching canonical object text; TypeError/JSONDecodeError are wrapped as ValueError and other failures propagate.
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
    "DigitalAssetDerivationGraph",
    "DigitalAssetDerivationGraphDirection",
    "DigitalAssetDerivationRecord",
    "DigitalAssetDerivationKind",
    "DigitalAssetDerivationSourceReference",
    "DigitalAssetRecreationPlan",
    "ExternalReproductionCommand",
    "ReproductionNormalizationDigest",
    "ReproductionRecipeArtifactReference",
    "ReproductionRecipeInputReference",
    "Reproducibility",
    "ReproductionRecipe",
]
