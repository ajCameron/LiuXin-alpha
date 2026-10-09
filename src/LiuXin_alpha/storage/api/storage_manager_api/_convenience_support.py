"""Shared coercion and delivery helpers for manager convenience adapters.

This private module owns value normalization only. It performs no persistence or
physical storage work; responsibility-specific mixins remain in adjacent modules.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import BinaryIO, cast
from uuid import UUID

from LiuXin_alpha.storage.api.errors import StorageIntegrityError
from LiuXin_alpha.storage.api.models import Digest, StoreUUID
from LiuXin_alpha.storage.api.placement_hints_api import (
    StorageHintSource,
    StoragePlacementHints,
    derive_storage_hints,
)
from LiuXin_alpha.storage.api.storage_manager_api.catalog_api import (
    DigitalAssetRegistryAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.composites_api import (
    CompositeDigitalAssetAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.derivations_api import (
    DigitalAssetDerivationRegistryAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.errors import (
    DigitalAssetNotFound,
)
from LiuXin_alpha.storage.api.storage_manager_api.ingest_api import (
    DigitalAssetIngestAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.item_links_api import (
    ItemDigitalAssetLinkAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    BackupPolicy,
    BackupPolicyID,
    BackupPolicyRecord,
    CompositeDigitalAssetDeclaration,
    CompositeDigitalAssetID,
    CompositeDigitalAssetMemberResolution,
    CompositeDigitalAssetMembership,
    CompositeDigitalAssetRecord,
    DigitalAssetDeclaration,
    DigitalAssetDerivationDeclaration,
    DigitalAssetDerivationKind,
    DigitalAssetDerivationRecord,
    DigitalAssetDerivationSourceReference,
    DigitalAssetID,
    DigitalAssetIngestResult,
    DigitalAssetLossAction,
    DigitalAssetMetadata,
    DigitalAssetRecord,
    DigitalAssetResolution,
    ItemID,
    ReplicaID,
    ReplicaMode,
    ReplicaRecord,
    ReplicaSeparationDimension,
    ReplicationPolicy,
    ReplicationPolicyID,
    ReplicationPolicyRecord,
    ReproductionRecipe,
    StoreConfiguration,
)
from LiuXin_alpha.storage.api.storage_manager_api.policies_api import (
    StoragePolicyAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.replicas_api import (
    ReplicaLifecycleAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.retrieval_api import (
    DigitalAssetRetrievalAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.router_api import (
    StorageRouterAPI,
)
from LiuXin_alpha.storage.api.store_api.facade_api import StoreAPI

type _AssetInput = (
    DigitalAssetID
    | DigitalAssetRecord
    | DigitalAssetIngestResult
    | DigitalAssetResolution
)
type DigitalAssetFileIdentifier = _AssetInput | int | Digest | str
type _CompositeInput = CompositeDigitalAssetID | CompositeDigitalAssetRecord
type _CompositeMembersInput = Mapping[str, _AssetInput] | Iterable[_AssetInput]
type _StoreInput = StoreUUID | StoreConfiguration | StoreAPI
type _ReplicaInput = ReplicaID | ReplicaRecord
type _AttributeInput = Mapping[str, str] | Iterable[tuple[str, str]]
type _DigestInput = Mapping[str, str] | Iterable[Digest]
type _DerivationSourceInput = _AssetInput | CompositeDigitalAssetRecord
type _StorableSource = (
    bytes | bytearray | memoryview | BinaryIO | str | os.PathLike[str]
)


def _file_asset_id(
    manager: object,
    identifier: DigitalAssetFileIdentifier,
    *,
    algorithm: str,
    size: int | None,
) -> DigitalAssetID:
    """
    Resolve a digest through the registry or extract a direct Asset identity.

    Non-string/non-Digest inputs go directly to _asset_id and ignore algorithm and size. A string
    constructs Digest(algorithm, identifier), applying the Digest value's text normalization; an
    existing Digest is retained. Query find_digital_asset_record_by_digest with the optional size
    and raise DigitalAssetNotFound only for a None result. Other lookup errors propagate, and
    returned record IDs are retained without a second validation or byte read.

    Example:
        >>> asset_id = _file_asset_id(  # doctest: +SKIP
        ...     manager, "a" * 64, algorithm="sha256", size=None,
        ... )


    :param manager: Host exposing the digest lookup contract; no runtime cast validation is performed.
    :param identifier: Asset ID/record/result/resolution, a Digest, or digest text; strings are never local paths here.
    :param algorithm: Digest algorithm used only for a string identifier; Digest values retain their own algorithm.
    :param size: Optional exact Asset size used only with digest lookup; ignored for direct ID/record inputs.
    :return: The extracted or digest-matched Asset ID.
    """

    if not isinstance(identifier, (str, Digest)):
        return _asset_id(identifier)
    digest = (
        identifier if isinstance(identifier, Digest) else Digest(algorithm, identifier)
    )
    record = cast(
        DigitalAssetRegistryAPI,
        manager,
    ).find_digital_asset_record_by_digest(
        digest,
        size_bytes=size,
    )
    if record is None:
        size_detail = "" if size is None else f" with size {size}"
        raise DigitalAssetNotFound(
            f"No Digital Asset is registered for {digest.algorithm}:"
            + f"{digest.value}{size_detail}."
        )
    return record.digital_asset_id


def _positive_integer(value: object) -> int | None:
    """
    Accept positive int instances except bool, returning the original value. Numeric strings,
    fractional numbers, zero, and negatives return None; no int coercion occurs. Accepted integer
    subclasses are retained rather than converted to plain int.

    Example:
        >>> _positive_integer(7)
        7
        >>> _positive_integer(True) is None
        True


    :param value: Candidate scalar to check using isinstance and a positive comparison.
    :return: The original positive non-bool integer, or None.
    """

    return (
        value
        if isinstance(value, int) and not isinstance(value, bool) and value > 0
        else None
    )


def _asset_id(value: _AssetInput | int) -> DigitalAssetID:
    """
    Extract a retained Asset ID from a record, ingest result, or resolution before checking scalar
    input. Record IDs are not revalidated. Otherwise require a positive non-bool int and apply the
    nominal ID constructor; other forms raise TypeError without a registry lookup.

    Example:
        >>> _asset_id(DigitalAssetID(7))
        7


    :param value: Atomic record/result/resolution or positive integer identity; a Composite record is not an atomic input.
    :return: The retained or nominally wrapped Asset identity.
    """

    if isinstance(value, DigitalAssetRecord):
        return value.digital_asset_id
    if isinstance(value, DigitalAssetIngestResult):
        return value.asset_record.digital_asset_id
    if isinstance(value, DigitalAssetResolution):
        return value.asset_record.digital_asset_id
    identifier = _positive_integer(value)
    if identifier is not None:
        return DigitalAssetID(identifier)
    raise TypeError("asset must be a positive ID or an atomic Asset result/record.")


def _composite_id(value: _CompositeInput) -> CompositeDigitalAssetID:
    """
    Return a Composite record's retained ID without revalidation, otherwise require a positive
    non-bool int. Nominal ID types are not distinguished at runtime, so an integer from another
    identity family is accepted here; no catalogue lookup occurs.

    Example:
        >>> _composite_id(CompositeDigitalAssetID(3))
        3


    :param value: Composite record or positive integer identity.
    :return: The retained or nominally wrapped Composite identity.
    """

    if isinstance(value, CompositeDigitalAssetRecord):
        return value.composite_digital_asset_id
    identifier = _positive_integer(value)
    if identifier is not None:
        return CompositeDigitalAssetID(identifier)
    raise TypeError("composite must be a positive ID or Composite record.")


def _composite_memberships(
    members: _CompositeMembersInput,
) -> tuple[CompositeDigitalAssetMembership, ...]:
    """
    Convert mapping or iterable Asset inputs into ordered required memberships.

    Mapping insertion order supplies sequence numbers and keys become logical paths. Other
    iterables produce unnamed members. Asset values are normalized through _asset_id and the
    membership constructor retains its own validation; this helper does not check catalogue
    existence or filesystem delivery safety.

    Example:
        >>> memberships = _composite_memberships({"book.epub": DigitalAssetID(7)})
        >>> memberships[0].logical_path
        'book.epub'


    :param members: Logical-path mapping or ordered iterable of atomic Asset inputs.
    :return: Materialized zero-based membership values in caller order.
    """

    if isinstance(members, Mapping):
        member_mapping = cast(Mapping[str, _AssetInput], members)
        return tuple(
            CompositeDigitalAssetMembership(
                _asset_id(asset),
                sequence_number,
                logical_path=logical_path,
            )
            for sequence_number, (logical_path, asset) in enumerate(
                member_mapping.items()
            )
        )
    return tuple(
        CompositeDigitalAssetMembership(
            _asset_id(asset),
            sequence_number,
        )
        for sequence_number, asset in enumerate(members)
    )


def _store_ref(value: _StoreInput | None) -> StoreUUID | None:
    """
    Return None or a UUID unchanged; otherwise prefer a UUID-valued store_uuid attribute, then a
    UUID-valued store_ref. Structural attribute access accepts objects beyond the annotated classes
    and does not inspect configuration/availability. Other getter failures propagate, and no usable
    UUID raises TypeError.

    Example:
        >>> _store_ref(UUID(int=1))
        UUID('00000000-0000-0000-0000-000000000001')


    :param value: Optional UUID, configuration, facade, or object exposing one of the recognized UUID attributes.
    :return: The selected UUID, or None when no Store preference/destination was supplied.
    """

    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    configured = getattr(value, "store_uuid", None)
    if isinstance(configured, UUID):
        return configured
    live = getattr(value, "store_ref", None)
    if isinstance(live, UUID):
        return live
    raise TypeError("store must be a Store UUID, configuration, or Store facade.")


def _replica_id(value: _ReplicaInput | None) -> ReplicaID | None:
    """
    Return None unchanged or extract a Replica record's retained ID without revalidation. Other
    inputs must be positive non-bool integers; no existence, Asset-ownership, state, or revision
    check is performed.

    Example:
        >>> _replica_id(ReplicaID(2))
        2


    :param value: Optional Replica record or positive integer identity.
    :return: The retained/nominal Replica ID, or None for omitted source selection.
    """

    if value is None:
        return None
    if isinstance(value, ReplicaRecord):
        return value.replica_id
    identifier = _positive_integer(value)
    if identifier is not None:
        return ReplicaID(identifier)
    raise TypeError("replica must be a positive ID or Replica record.")


def _item_id(value: ItemID | int | None) -> ItemID | None:
    """
    Preserve None for omitted Item linkage; otherwise delegate to the required positive integer
    check. No Item catalogue lookup or coercion from strings is performed.

    Example:
        >>> _item_id(None) is None
        True


    :param value: Optional positive non-bool integer Item identity.
    :return: The nominal Item ID, or None.
    """

    return None if value is None else _required_item_id(value)


def _required_item_id(value: ItemID | int) -> ItemID:
    """
    Accept a positive int instance other than bool and apply the nominal ItemID constructor. Invalid
    types and nonpositive values raise TypeError rather than being int-coerced;
    registration/existence is left to the manager.

    Example:
        >>> _required_item_id(9)
        9


    :param value: Required Item identity checked before link/ingest delegation.
    :return: The positive nominal Item ID, retaining an accepted integer value.
    """

    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return ItemID(value)
    raise TypeError("item must be a positive integer ID.")


def _attributes(value: _AttributeInput) -> tuple[tuple[str, str], ...]:
    """
    Collect mapping items or an iterable, then require each entry to unpack into two strings.

    Preserve order, whitespace, duplicate names, and the original non-mapping entry objects; only
    the outer sequence becomes a tuple. A two-character string or two-string list can therefore pass
    without becoming a canonical tuple pair. Wrong element types raise TypeError, while malformed
    unpacking can raise its own error. Blank/duplicate-name rejection, where required, belongs to
    the later metadata constructor.

    Example:
        >>> _attributes({"language": "en"})
        (('language', 'en'),)


    :param value: Attribute mapping or iterable of entries unpacking into string name/value pairs.
    :return: The collected tuple of original items after the two-string check.
    """

    if isinstance(value, Mapping):
        attribute_mapping = cast(Mapping[str, str], value)
        normalized = tuple(attribute_mapping.items())
    else:
        normalized = tuple(value)
    if any(
        not isinstance(name, str) or not isinstance(item, str)
        for name, item in normalized
    ):
        raise TypeError("attribute names and values must be strings.")
    return normalized


def _metadata(
    name: str | None,
    media_type: str | None,
    original_name: str | None,
    attributes: _AttributeInput,
) -> DigitalAssetMetadata:
    """
    Construct flat DigitalAssetMetadata using the supplied labels and collected string attributes.
    Its constructor validates selected nonblank/unique names; this helper adds no MIME inference,
    filename sanitization, rich-hint projection, or deep copying.

    Example:
        >>> _metadata("book", None, None, ()).name
        'book'


    :param name: Optional Asset display name, retained without stripping; None leaves it unspecified.
    :param media_type: Optional Asset media-type text; this wrapper does not infer it from bytes or a filename.
    :param original_name: Optional original filename/source label for Asset metadata, separate from a physical Store key.
    :param attributes: Ordered string name/value pairs or a mapping for Asset metadata; pairs are collected before delegation.
    :return: A new Asset metadata record containing the retained labels and normalized outer attribute sequence.
    """

    return DigitalAssetMetadata(
        name=name,
        media_type=media_type,
        original_name=original_name,
        attributes=_attributes(attributes),
    )


def _placement_hints(
    metadata: StorageHintSource | None,
) -> StoragePlacementHints | None:
    """
    Return None for omitted rich metadata, otherwise delegate to derive_storage_hints. Provider
    projection and validation errors propagate; the result is advisory placement information, not
    Asset identity metadata or proof that a Store will honor it.

    Example:
        >>> _placement_hints({"title": "Book"})["title"]
        'Book'


    :param metadata: Optional library metadata container/mapping or existing placement-hints value.
    :return: The derived placement-hints value, or None for omitted input.
    """

    return None if metadata is None else derive_storage_hints(metadata)


def _digests(value: _DigestInput) -> tuple[Digest, ...]:
    """
    Convert mapping entries to Digest objects in mapping order, or tuple-collect an iterable and
    require every value to be a Digest instance. Mapping construction applies Digest text
    normalization. No hashing, nonempty requirement, or duplicate-algorithm rejection is performed
    by this helper; later declaration validation handles those constraints.

    Example:
        >>> _digests({"sha256": "abcd"})[0].algorithm
        'sha256'


    :param value: Algorithm/value mapping or iterable of Digest instances.
    :return: A tuple of normalized mapping-derived or retained supplied Digest objects.
    """

    if isinstance(value, Mapping):
        digest_mapping = cast(Mapping[str, str], value)
        return tuple(
            Digest(algorithm, digest) for algorithm, digest in digest_mapping.items()
        )
    digests = tuple(value)
    if any(not isinstance(digest, Digest) for digest in digests):
        raise TypeError("digests must contain Digest values.")
    return digests


def _replica_mode(value: ReplicaMode | str) -> ReplicaMode:
    """
    Return an existing ReplicaMode unchanged, otherwise invoke its enum constructor with the
    original input. No stripping, case folding, or synonym conversion is added; unsupported values
    propagate the constructor error.

    Example:
        >>> _replica_mode("active") is ReplicaMode.ACTIVE
        True


    :param value: Replica mode enum or exact enum-value string to normalize.
    :return: The corresponding ReplicaMode value.
    """

    return value if isinstance(value, ReplicaMode) else ReplicaMode(value)


def _replica_mode_argument(
    replica_mode: ReplicaMode | str | None,
    mode: ReplicaMode | str | None,
) -> ReplicaMode:
    """
    Reject simultaneous non-None replica_mode and mode values even if they agree, then choose the
    supplied spelling. If both are None, return ACTIVE; otherwise normalize the selected enum/value
    string. This alias selects a Replica role, never a read/write file-open mode.

    Example:
        >>> _replica_mode_argument("backup", None) is ReplicaMode.BACKUP
        True


    :param replica_mode: Preferred parameter name for a requested Replica mode, or None.
    :param mode: Historical alias, mutually exclusive with a non-None replica_mode.
    :return: The selected normalized mode, defaulting to ReplicaMode.ACTIVE.
    """

    if replica_mode is not None and mode is not None:
        raise TypeError("use replica_mode or mode, not both.")
    selected = replica_mode if replica_mode is not None else mode
    return ReplicaMode.ACTIVE if selected is None else _replica_mode(selected)


def _separation_dimension(
    value: ReplicaSeparationDimension | str,
) -> ReplicaSeparationDimension:
    """
    Return an existing ReplicaSeparationDimension unchanged, otherwise invoke its enum constructor
    with the original input. No stripping, case folding, or synonym conversion is added; unsupported
    values propagate the constructor error.

    Example:
        >>> _separation_dimension("host") is ReplicaSeparationDimension.HOST
        True


    :param value: Separation dimension enum or exact enum-value string to normalize.
    :return: The corresponding ReplicaSeparationDimension value.
    """

    return (
        value
        if isinstance(value, ReplicaSeparationDimension)
        else ReplicaSeparationDimension(value)
    )


def _loss_action(
    value: DigitalAssetLossAction | str,
) -> DigitalAssetLossAction:
    """
    Return an existing DigitalAssetLossAction unchanged, otherwise invoke its enum constructor with
    the original input. No stripping, case folding, or synonym conversion is added; unsupported
    values propagate the constructor error.

    Example:
        >>> _loss_action("accept_loss") is DigitalAssetLossAction.ACCEPT_LOSS
        True


    :param value: Loss action enum or exact enum-value string to normalize.
    :return: The corresponding DigitalAssetLossAction value.
    """

    return (
        value
        if isinstance(value, DigitalAssetLossAction)
        else DigitalAssetLossAction(value)
    )


def _derivation_kind(
    value: DigitalAssetDerivationKind | str,
) -> DigitalAssetDerivationKind:
    """
    Return an existing DigitalAssetDerivationKind unchanged, otherwise invoke its enum constructor
    with the original input. No stripping, case folding, or synonym conversion is added; unsupported
    values propagate the constructor error.

    Example:
        >>> _derivation_kind("extract") is DigitalAssetDerivationKind.EXTRACT
        True


    :param value: Derivation kind enum or exact enum-value string to normalize.
    :return: The corresponding DigitalAssetDerivationKind value.
    """

    return (
        value
        if isinstance(value, DigitalAssetDerivationKind)
        else DigitalAssetDerivationKind(value)
    )


def _replication_policy_id(
    value: ReplicationPolicyID | ReplicationPolicyRecord | None,
) -> ReplicationPolicyID | None:
    """
    Preserve None and extract a ReplicationPolicyRecord's retained ID without rechecking positivity.
    Otherwise require a positive non-bool integer; unlike some internal policy helpers, this
    function does not use int conversion on strings or fractional values. No policy lookup is
    performed.

    Example:
        >>> _replication_policy_id(ReplicationPolicyID(4))
        4


    :param value: Optional replication-policy record or positive integer ID.
    :return: The retained/nominal replication-policy ID, or None.
    """

    if value is None:
        return None
    if isinstance(value, ReplicationPolicyRecord):
        return value.replication_policy_id
    identifier = _positive_integer(value)
    if identifier is not None:
        return ReplicationPolicyID(identifier)
    raise TypeError("replication must be a positive policy ID or policy record.")


def _backup_policy_id(
    value: BackupPolicyID | BackupPolicyRecord | None,
) -> BackupPolicyID | None:
    """
    Preserve None and extract a BackupPolicyRecord's retained ID without rechecking positivity.
    Otherwise require a positive non-bool integer; unlike some internal policy helpers, this
    function does not use int conversion on strings or fractional values. No policy lookup is
    performed.

    Example:
        >>> _backup_policy_id(BackupPolicyID(5))
        5


    :param value: Optional backup-policy record or positive integer ID.
    :return: The retained/nominal backup-policy ID, or None.
    """

    if value is None:
        return None
    if isinstance(value, BackupPolicyRecord):
        return value.backup_policy_id
    identifier = _positive_integer(value)
    if identifier is not None:
        return BackupPolicyID(identifier)
    raise TypeError("backup must be a positive policy ID or policy record.")


def _derivation_source(
    sequence_number: int,
    value: _DerivationSourceInput,
    role: str | None,
) -> DigitalAssetDerivationSourceReference:
    """
    Construct one ordered provenance source, recognizing Composite records explicitly. Other
    supported inputs contribute an atomic ID through _asset_id, so raw nominal Composite integers
    are not distinguished. Pass sequence/role through source-reference validation without resolving
    content or pinning a Composite revision.

    Example:
        >>> _derivation_source(0, DigitalAssetID(7), "source").role
        'source'


    :param sequence_number: Supplied source position forwarded to the reference constructor.
    :param value: Atomic ID/record/result/resolution or a Composite record for an explicit Composite reference.
    :param role: Optional source role retained subject to the reference value's validation.
    :return: A new atomic or Composite derivation-source reference.
    """

    if isinstance(value, CompositeDigitalAssetRecord):
        return DigitalAssetDerivationSourceReference(
            sequence_number,
            composite_digital_asset_id=value.composite_digital_asset_id,
            role=role,
        )
    return DigitalAssetDerivationSourceReference(
        sequence_number,
        digital_asset_id=_asset_id(value),
        role=role,
    )


def _composite_record(
    manager: object,
    value: CompositeDigitalAssetID | CompositeDigitalAssetRecord,
) -> CompositeDigitalAssetRecord:
    """
    Return an existing Composite record unchanged without consulting the manager. Otherwise
    validate/extract its integer ID and call get_composite_digital_asset_record. This helper does
    not refresh a supplied record or assess member availability; callers can subsequently resolve by
    its ID.

    Example:
        >>> _composite_record(manager, record) is record  # doctest: +SKIP
        True


    :param manager: Host providing Composite-record lookup for ID input.
    :param value: Existing Composite record or positive integer identity.
    :return: The original supplied record, or the manager lookup result.
    """

    if isinstance(value, CompositeDigitalAssetRecord):
        return value
    return cast(
        CompositeDigitalAssetAPI,
        manager,
    ).get_composite_digital_asset_record(_composite_id(value))


def _composite_logical_path(value: str) -> str:
    """
    Require canonical relative POSIX path syntax and return the original string.

    Reject nonstrings with TypeError. Empty text, NUL, backslashes, absolute paths, empty/dot/parent
    components, or spelling changed by PurePosixPath raise ValueError. No whitespace stripping,
    Unicode-encoding check, host-specific reserved-name validation, or filesystem/symlink inspection
    is performed. Other control characters and surrogate code points are not rejected by this syntax
    check.

    Example:
        >>> _composite_logical_path("images/cover.jpg")
        'images/cover.jpg'


    :param value: Logical member path to validate as exact relative POSIX text.
    :return: The unchanged path string after syntax validation.
    """

    if not isinstance(value, str):
        raise TypeError("Composite logical paths must be strings.")
    path = PurePosixPath(value)
    parts = value.split("/")
    if (
        not value
        or "\x00" in value
        or "\\" in value
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in parts)
        or path.as_posix() != value
    ):
        raise ValueError(f"invalid relative Composite logical path: {value!r}")
    return value


def _member_delivery_path(
    member: CompositeDigitalAssetMemberResolution,
) -> str:
    """
    Choose the first truthy logical_path, logical_name, or Asset original_name; otherwise use
    member- followed by the sequence number. Validate only that chosen value with
    _composite_logical_path. A truthy invalid higher-priority name raises rather than trying a later
    fallback; role and Store key are not used.

    Example:
        >>> _member_delivery_path(member)  # doctest: +SKIP
        'images/cover.jpg'


    :param member: Resolved membership carrying relationship labels and an Asset description.
    :return: The selected and syntax-validated relative delivery name.
    """

    membership = member.membership
    asset = member.resolution.asset_record
    candidate = (
        membership.logical_path
        or membership.logical_name
        or asset.metadata.original_name
        or f"member-{membership.sequence_number}"
    )
    return _composite_logical_path(candidate)


def _resolved_composite_targets(
    root: Path,
    resolutions: Iterable[CompositeDigitalAssetMemberResolution],
) -> tuple[Path, ...]:
    """
    Preflight delivery names and current resolved containment, returning lexical target Paths.

    Resolve root once, validate each selected member name, join its POSIX components, and require
    the currently resolved target to be under the resolved root. Current symlink escape raises
    StorageIntegrityError. Duplicate lexical Paths also raise, but distinct in-root aliases to the
    same file are not compared by resolved identity. No directories are created, existing files
    checked, or handles/locks retained; the check does not prevent filesystem changes before later
    writes.

    Example:
        >>> targets = _resolved_composite_targets(root, members)  # doctest: +SKIP


    :param root: Destination root Path whose current resolution bounds the target preflight.
    :param resolutions: Ordered resolved members whose selected names become targets.
    :return: A tuple of joined target Paths in input order, without replacing them by resolved paths.
    """

    targets: list[Path] = []
    root_resolved = root.resolve(strict=False)
    for resolution in resolutions:
        relative = _member_delivery_path(resolution)
        target = root.joinpath(*PurePosixPath(relative).parts)
        resolved = target.resolve(strict=False)
        try:
            resolved.relative_to(root_resolved)
        except ValueError as error:
            raise StorageIntegrityError(
                f"Composite member path escapes destination: {relative!r}"
            ) from error
        targets.append(target)
    if len(targets) != len(set(targets)):
        raise StorageIntegrityError(
            "Composite members resolve to duplicate delivery paths."
        )
    return tuple(targets)


__all__ = [
    "_AssetInput",
    "DigitalAssetFileIdentifier",
    "_CompositeInput",
    "_CompositeMembersInput",
    "_StoreInput",
    "_ReplicaInput",
    "_AttributeInput",
    "_DigestInput",
    "_DerivationSourceInput",
    "_StorableSource",
    "_file_asset_id",
    "_positive_integer",
    "_asset_id",
    "_composite_id",
    "_composite_memberships",
    "_store_ref",
    "_replica_id",
    "_item_id",
    "_required_item_id",
    "_attributes",
    "_metadata",
    "_placement_hints",
    "_digests",
    "_replica_mode",
    "_replica_mode_argument",
    "_separation_dimension",
    "_loss_action",
    "_derivation_kind",
    "_replication_policy_id",
    "_backup_policy_id",
    "_derivation_source",
    "_composite_record",
    "_composite_logical_path",
    "_member_delivery_path",
    "_resolved_composite_targets",
]
