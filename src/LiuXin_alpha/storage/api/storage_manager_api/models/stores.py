"""
Represent configured Store endpoints and bootstrap/reconciliation observations.

These passive values apply selected local validation and factory normalization.
They do not construct Stores, persist configuration, establish inventory truth,
or perform recovery; manager implementations own those operations.
"""

from __future__ import annotations

import dataclasses
import os

from collections.abc import Iterable, Mapping
from enum import StrEnum
from pathlib import Path
from typing import Self, cast
from urllib.parse import urlparse
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api.models import (
    EnumerationCompleteness,
    Location,
    StoreStatus,
    StoreUUID,
)
from LiuXin_alpha.storage.api.storage_manager_api.models.replicas import ReplicaMode
from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import (
    BackupPolicyID,
    DigitalAssetID,
    ReplicationPolicyID,
    ReplicaID,
)


class TopologyRelation(StrEnum):
    """
    Classify equality of declared Store host or device identities.

    UNKNOWN represents missing topology evidence and must not be interpreted as physical separation.
    Values describe configuration comparisons rather than a hardware probe.

    Example:
        >>> TopologyRelation.SAME.value
        'same'
    """

    SAME = "same"
    DIFFERENT = "different"
    UNKNOWN = "unknown"


@dataclasses.dataclass(slots=True, frozen=True)
class StoreBackingReference:
    """
    Identify the catalogue Asset whose container bytes back a read-only Store view. The preferred
    Replica is a replaceable routing hint; a materialization Store can supply local bytes for a
    driver that cannot read the selected representation directly. Construction validates selected
    identifier/reference shapes without resolving any Asset or opening bytes.

    Example:
        >>> backing = StoreBackingReference(
        ...     DigitalAssetID(7), preferred_replica_id=ReplicaID(12),
        ... )
        >>> int(backing.digital_asset_id)
        7


    :ivar digital_asset_id: Backing Asset identity; positive int conversion is checked but the original value is retained.
    :ivar preferred_replica_id: Optional preferred Replica identity, validated by the same positive-conversion rule.
    :ivar materialization_store_ref: Optional UUID of the Store used for local materialization; no availability/capability check occurs here.
    """

    digital_asset_id: DigitalAssetID
    preferred_replica_id: ReplicaID | None = None
    materialization_store_ref: StoreUUID | None = None

    def __post_init__(self) -> None:
        """
        Reject boolean or nonpositive int-convertible Asset/Replica IDs and non-UUID materialization
        references. Positive numeric strings and truncatable floats can pass without being assigned
        converted integer values. Conversion/attribute errors propagate; repository existence is not
        checked.

        Example:
            >>> StoreBackingReference(DigitalAssetID(0))
            Traceback (most recent call last):
            ...
            ValueError: digital_asset_id must be a positive integer.


        :return: None after these validation checks; invalid IDs raise through validation/conversion and an invalid Store reference raises TypeError.
        """

        raw_digital_asset_id: object = self.digital_asset_id
        if isinstance(raw_digital_asset_id, bool) or int(
            self.digital_asset_id
        ) <= 0:
            raise ValueError("digital_asset_id must be a positive integer.")
        raw_preferred_replica_id: object = self.preferred_replica_id
        if self.preferred_replica_id is not None and (
            isinstance(raw_preferred_replica_id, bool)
            or int(self.preferred_replica_id) <= 0
        ):
            raise ValueError(
                "preferred_replica_id must be a positive integer or None."
            )
        if (
            self.materialization_store_ref is not None
            and not isinstance(self.materialization_store_ref, UUID)
        ):
            raise TypeError("materialization_store_ref must be a UUID or None.")


@dataclasses.dataclass(slots=True, frozen=True)
class StoreConfiguration:
    """
    Retain portable configuration for one Store endpoint. Manager first-placement logic can capture
    Store default policy IDs on a new Asset; these values are not dynamically inherited by every
    Asset later copied here. Construction neither registers a Store nor enforces backend
    capabilities.

    Direct construction validates selected identities, required text, backing consistency, and
    option shapes. It does not recursively freeze fields or generally coerce modes, tags, policy
    IDs, optional strings, or boolean values. Convenience factories perform their own additional
    normalization.

    Example:
        >>> configuration = StoreConfiguration(
        ...     store_uuid=UUID(int=1),
        ...     store_name="primary", store_kind="filesystem",
        ...     store_root_uri="file:///srv/liuxin",
        ... )
        >>> configuration.supports_folders
        True


    :ivar store_uuid: Durable configured Store UUID used for routing.
    :ivar store_name: Required nonblank display name, retained without stripping.
    :ivar store_kind: Required nonblank backend-kind text; direct construction does not normalize or validate registry membership.
    :ivar store_root_uri: Required nonblank root/endpoint text, without URI parsing at direct construction.
    :ivar store_url: Optional operator-facing URL, retained without validation.
    :ivar store_access_protocol: Optional access-protocol label or override.
    :ivar store_failure_domain: Optional fault-isolation label for placement policy.
    :ivar store_region: Optional geographic or administrative placement region.
    :ivar store_host_uuid: Optional declared host UUID; no host discovery is performed.
    :ivar store_device_uuid: Optional declared physical-device UUID; no device discovery is performed.
    :ivar store_tags: Placement labels retained as supplied by direct construction.
    :ivar store_default_replication_policy_id: Optional policy identity captured by manager first-placement behavior, not resolved here.
    :ivar store_default_backup_policy_id: Optional backup policy identity for first placement, not resolved here.
    :ivar supported_replica_modes: Permitted mode declarations; direct construction does not coerce their container or members.
    :ivar operational_role: Optional operator-facing role such as archive.
    :ivar read_only: Requested read-only policy; truthiness is required for backed configurations, but no runtime Store is changed here.
    :ivar supports_folders: Declared folder semantics, retained without boolean validation.
    :ivar backend_options: Ordered unique-name scalar or string-tuple options; supplied values remain shared.
    :ivar backing: Optional validated StoreBackingReference requiring a read-only view and preventing direct self-materialization.
    """

    store_uuid: StoreUUID
    store_name: str
    store_kind: str
    store_root_uri: str
    store_url: str | None = None
    store_access_protocol: str | None = None
    store_failure_domain: str | None = None
    store_region: str | None = None
    store_host_uuid: UUID | None = None
    store_device_uuid: UUID | None = None
    store_tags: tuple[str, ...] = ()
    store_default_replication_policy_id: ReplicationPolicyID | None = None
    store_default_backup_policy_id: BackupPolicyID | None = None
    supported_replica_modes: frozenset[ReplicaMode] = dataclasses.field(
        default_factory=lambda: frozenset(
            {ReplicaMode.ACTIVE, ReplicaMode.BACKUP, ReplicaMode.ARCHIVE}
        )
    )
    operational_role: str | None = None
    read_only: bool = False
    supports_folders: bool = True
    backend_options: tuple[tuple[str, object], ...] = ()
    backing: StoreBackingReference | None = None

    @classmethod
    def for_backend(
        cls,
        name: str,
        kind: str,
        root: str | os.PathLike[str],
        *,
        store_uuid: StoreUUID | None = None,
        url: str | None = None,
        protocol: str | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        host: UUID | None = None,
        device: UUID | None = None,
        tags: Iterable[str] = (),
        replication_policy: ReplicationPolicyID | None = None,
        backup_policy: BackupPolicyID | None = None,
        modes: Iterable[ReplicaMode | str] = (
            ReplicaMode.ACTIVE,
            ReplicaMode.BACKUP,
            ReplicaMode.ARCHIVE,
        ),
        operational_role: str | None = None,
        read_only: bool = False,
        folders: bool = True,
        options: (
            Mapping[str, object] | Iterable[tuple[str, object]]
        ) = (),
        backing: StoreBackingReference | None = None,
    ) -> Self:
        """
        Build configuration through cls after normalizing endpoint text and collection arguments.
        PathLike roots expand the user directory, resolve without requiring existence, and become
        file URIs. Other roots are stringified and stripped. Tags/options retain iteration order;
        modes are converted to enums and deduplicated. None UUID generates a random uuid4.

        No backend registry lookup, Store creation, persistence, or capacity probe occurs. Iterable,
        path-resolution, enum-conversion, and constructor errors propagate.

        Example:
            >>> configuration = StoreConfiguration.for_backend(
            ...     "archive", "s3", "s3://books/archive",
            ...     tags={"offsite"},
            ... )
            >>> configuration.store_kind
            's3'


        :param name: Required nonblank display name retained in store_name.
        :param kind: Backend-kind text retained in store_kind.
        :param root: PathLike root resolved to a file URI, or endpoint text stringified and stripped.
        :param store_uuid: Optional routing UUID; None generates uuid4.
        :param url: Optional operator-facing URL.
        :param protocol: Optional access-protocol declaration.
        :param failure_domain: Optional placement fault-isolation label.
        :param region: Optional geographic or administrative region.
        :param host: Optional declared host UUID.
        :param device: Optional declared device UUID.
        :param tags: Iterable collected into a tuple without sorting or deduplication.
        :param replication_policy: Optional default replication-policy identity retained without repository lookup.
        :param backup_policy: Optional default backup-policy identity retained without repository lookup.
        :param modes: Iterable converted to ReplicaMode members and collected into a frozenset.
        :param operational_role: Optional operator-facing Store role.
        :param read_only: Requested read-only flag forwarded to configuration validation.
        :param folders: Declared folder-semantics flag.
        :param options: Mapping or pair iterable shallowly collected into an ordered tuple before option validation.
        :param backing: Optional backing Asset reference, validated by the constructed configuration.
        :return: New configuration of cls with normalized factory inputs.
        """

        option_pairs = _option_pairs(options)
        return cls(
            store_uuid=uuid4() if store_uuid is None else store_uuid,
            store_name=name,
            store_kind=kind,
            store_root_uri=_endpoint_text(root),
            store_url=url,
            store_access_protocol=protocol,
            store_failure_domain=failure_domain,
            store_region=region,
            store_host_uuid=host,
            store_device_uuid=device,
            store_tags=tuple(tags),
            store_default_replication_policy_id=replication_policy,
            store_default_backup_policy_id=backup_policy,
            supported_replica_modes=frozenset(
                mode if isinstance(mode, ReplicaMode) else ReplicaMode(mode)
                for mode in modes
            ),
            operational_role=operational_role,
            read_only=read_only,
            supports_folders=folders,
            backend_options=option_pairs,
            backing=backing,
        )

    @classmethod
    def for_backed_backend(
        cls,
        name: str,
        kind: str,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_replica_id: ReplicaID | None = None,
        materialization_store_ref: StoreUUID | None = None,
        store_uuid: StoreUUID | None = None,
        protocol: str | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        tags: Iterable[str] = (),
        modes: Iterable[ReplicaMode | str] = (ReplicaMode.ARCHIVE,),
        operational_role: str | None = "archive",
        folders: bool = True,
        options: (
            Mapping[str, object] | Iterable[tuple[str, object]]
        ) = (),
    ) -> Self:
        """
        Create a read-only backed configuration with an asset://digital-asset/ID root.
        StoreBackingReference validation occurs first, then this factory calls cls.for_backend with
        read_only=True. Default modes/role describe an archive view. No Asset/Replica lookup, local
        materialization, or stable content-derived UUID occurs here; None UUID follows for_backend's
        random convention.

        Example:
            >>> configuration = StoreConfiguration.for_backed_backend(
            ...     "book pack", "zip_readonly", DigitalAssetID(7),
            ...     preferred_replica_id=ReplicaID(12),
            ... )
            >>> configuration.store_root_uri
            'asset://digital-asset/7'


        :param name: Required nonblank display name retained in store_name.
        :param kind: Backend-kind text retained in store_kind.
        :param digital_asset_id: Backing Asset identifier checked by StoreBackingReference and rendered into the asset URI.
        :param preferred_replica_id: Optional preferred Replica identifier retained as a routing hint.
        :param materialization_store_ref: Optional UUID for local materialization of the backing bytes.
        :param store_uuid: Optional routing UUID; None generates uuid4.
        :param protocol: Optional access-protocol declaration.
        :param failure_domain: Optional placement fault-isolation label.
        :param region: Optional geographic or administrative region.
        :param tags: Iterable collected into a tuple without sorting or deduplication.
        :param modes: Iterable converted to ReplicaMode members and collected into a frozenset.
        :param operational_role: Optional operator-facing Store role.
        :param folders: Declared folder-semantics flag.
        :param options: Mapping or pair iterable shallowly collected into an ordered tuple before option validation.
        :return: New read-only backed configuration, without constructing its runtime Store.
        """

        backing = StoreBackingReference(
            digital_asset_id,
            preferred_replica_id=preferred_replica_id,
            materialization_store_ref=materialization_store_ref,
        )
        return cls.for_backend(
            name,
            kind,
            f"asset://digital-asset/{int(digital_asset_id)}",
            store_uuid=store_uuid,
            protocol=protocol,
            failure_domain=failure_domain,
            region=region,
            tags=tags,
            modes=modes,
            operational_role=operational_role,
            read_only=True,
            folders=folders,
            options=options,
            backing=backing,
        )

    @classmethod
    def filesystem(
        cls,
        name: str,
        root: str | os.PathLike[str],
        *,
        store_uuid: StoreUUID | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        host: UUID | None = None,
        device: UUID | None = None,
        tags: Iterable[str] = (),
        replication_policy: ReplicationPolicyID | None = None,
        backup_policy: BackupPolicyID | None = None,
        modes: Iterable[ReplicaMode | str] = (
            ReplicaMode.ACTIVE,
            ReplicaMode.BACKUP,
            ReplicaMode.ARCHIVE,
        ),
        operational_role: str | None = None,
        read_only: bool = False,
        options: (
            Mapping[str, object] | Iterable[tuple[str, object]]
        ) = (),
    ) -> Self:
        """
        Build configuration with kind filesystem, protocol file, and folders enabled. Local paths
        are expanded/resolved without requiring existence; a parsed non-file scheme rejects.
        Existing file URI text is retained after stripping without checking authority, host, or path
        usability. No root directory or Store is created.

        Example:
            >>> configuration = StoreConfiguration.filesystem(
            ...     "primary", Path("/srv/liuxin"),
            ... )
            >>> configuration.store_kind
            'filesystem'


        :param name: Required nonblank display name retained in store_name.
        :param root: PathLike/local path resolved to a file URI, or stripped file URI accepted by the helper.
        :param store_uuid: Optional routing UUID; None generates uuid4.
        :param failure_domain: Optional placement fault-isolation label.
        :param region: Optional geographic or administrative region.
        :param host: Optional declared host UUID.
        :param device: Optional declared device UUID.
        :param tags: Iterable collected into a tuple without sorting or deduplication.
        :param replication_policy: Optional default replication-policy identity retained without repository lookup.
        :param backup_policy: Optional default backup-policy identity retained without repository lookup.
        :param modes: Iterable converted to ReplicaMode members and collected into a frozenset.
        :param operational_role: Optional operator-facing Store role.
        :param read_only: Requested read-only flag forwarded to configuration validation.
        :param options: Mapping or pair iterable shallowly collected into an ordered tuple before option validation.
        :return: New filesystem configuration produced through cls.for_backend.
        """

        return cls.for_backend(
            name,
            "filesystem",
            _filesystem_root_uri(root),
            store_uuid=store_uuid,
            protocol="file",
            failure_domain=failure_domain,
            region=region,
            host=host,
            device=device,
            tags=tags,
            replication_policy=replication_policy,
            backup_policy=backup_policy,
            modes=modes,
            operational_role=operational_role,
            read_only=read_only,
            folders=True,
            options=options,
        )

    def __post_init__(self) -> None:
        """
        Validate Store/host/device UUIDs, backed-view restrictions, nonblank required text, and
        option key/value shapes. Backing requires the expected reference type, truthy read_only, and
        a materialization UUID different from this Store. Required text is tested with strip but
        retained unchanged.

        Option names must be nonblank strings unique by exact spelling. Values permit None,
        str/int/float/bool, or tuples of strings; nonfinite floats are not rejected, so accepted
        shapes do not guarantee strict JSON serialization. Other collections, policy IDs, optional
        strings, and booleans are not generally validated or coerced.

        Example:
            >>> StoreConfiguration(
            ...     UUID(int=1), "", "filesystem", "file:///srv",
            ... )
            Traceback (most recent call last):
            ...
            ValueError: store_name must not be empty.


        :return: None when these checks pass; validation and delegated string/iteration operations may raise.
        """

        if not isinstance(self.store_uuid, UUID):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("store_uuid must be a UUID.")
        if self.backing is not None:
            if not isinstance(self.backing, StoreBackingReference):
                raise TypeError("backing must be a StoreBackingReference or None.")
            if not self.read_only:
                raise ValueError(
                    "a Store backed by a catalogued Asset must be read-only."
                )
            if self.backing.materialization_store_ref == self.store_uuid:
                raise ValueError("a backed Store cannot materialize into itself.")
        for name, uuid_value in (
            ("store_host_uuid", self.store_host_uuid),
            ("store_device_uuid", self.store_device_uuid),
        ):
            if uuid_value is not None and not isinstance(uuid_value, UUID):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise TypeError(f"{name} must be a UUID or None.")
        for name, text_value in (
            ("store_name", self.store_name),
            ("store_kind", self.store_kind),
            ("store_root_uri", self.store_root_uri),
        ):
            if not text_value.strip():
                raise ValueError(f"{name} must not be empty.")
        option_names: set[str] = set()
        for key, option_value in self.backend_options:
            if not isinstance(key, str) or not key.strip():
                raise ValueError("backend option names must be non-empty strings.")
            if key in option_names:
                raise ValueError(f"duplicate backend option: {key!r}.")
            option_names.add(key)
            if not (
                option_value is None
                or isinstance(option_value, (str, int, float, bool))
                or (
                    isinstance(option_value, tuple)
                    and all(isinstance(item, str) for item in option_value)
                )
            ):
                raise TypeError(
                    "backend option values must be JSON scalars or string tuples."
                )


def _endpoint_text(root: str | os.PathLike[str]) -> str:
    """
    Render PathLike values as expanded, resolved file URIs; stringify and strip other inputs.
    Resolution uses strict=False but can still inspect filesystem path components and raise.
    Endpoint strings are not otherwise parsed or required to use a recognized scheme.

    Example:
        >>> _endpoint_text("s3://books/archive")
        's3://books/archive'


    :param root: PathLike root or value expected to provide endpoint text.
    :return: Absolute file URI for PathLike input, otherwise nonempty stripped text.
    """

    if isinstance(root, os.PathLike):
        return Path(root).expanduser().resolve(strict=False).as_uri()
    text = str(root).strip()
    if not text:
        raise ValueError("store root must not be empty.")
    return text


def _filesystem_root_uri(root: str | os.PathLike[str]) -> str:
    """
    Resolve PathLike/plain-path roots to file URIs and retain stripped file URI strings. Parsed
    schemes other than file reject, including drive-like strings interpreted as schemes on this
    host. A file URI is not checked for local authority, nonempty path, or existence.

    Example:
        >>> _filesystem_root_uri("/srv/liuxin").startswith("file:")
        True


    :param root: Local path, PathLike value, or file URI to normalize.
    :return: Resolved local file URI or supplied stripped file URI; unsupported schemes and empty text raise ValueError.
    """

    if isinstance(root, os.PathLike):
        return Path(root).expanduser().resolve(strict=False).as_uri()
    text = str(root).strip()
    if not text:
        raise ValueError("filesystem Store root must not be empty.")
    parsed = urlparse(text)
    if parsed.scheme:
        if parsed.scheme != "file":
            raise ValueError(
                "filesystem Store root must be a local path or file URI."
            )
        return text
    return Path(text).expanduser().resolve(strict=False).as_uri()


def _option_pairs(
    options: Mapping[str, object] | Iterable[tuple[str, object]],
) -> tuple[tuple[str, object], ...]:
    """
    Shallowly collect mapping items or an iterable of pairs into a tuple. Pair validation, duplicate
    detection, and allowed-value checks belong to StoreConfiguration construction. Inner objects are
    neither copied nor frozen, and iteration failures propagate.

    Example:
        >>> _option_pairs({"region": "local"})
        (('region', 'local'),)


    :param options: Mapping or iterable expected to yield option-name/value pairs.
    :return: Tuple preserving supplied item order and references.
    """

    if isinstance(options, Mapping):
        mapping = cast(Mapping[str, object], options)
        return tuple(mapping.items())
    return tuple(options)


@dataclasses.dataclass(slots=True, frozen=True)
class StorageBootstrapIssue:
    """
    Retain attribution and an explanation for a skipped or failed configuration load.

    The record performs no validation and can describe a skipped offline Store as well as a failed
    construction.

    Example:
        >>> issue = StorageBootstrapIssue(
        ...     store_ref=UUID(int=1), store_name="archive", reason="offline",
        ... )
        >>> issue.reason
        'offline'


    :ivar store_ref: Optional configured Store UUID supplied by the producer.
    :ivar store_name: Optional display name supplied by the producer.
    :ivar reason: Producer explanation, retained without nonempty/type checks.
    """

    store_ref: StoreUUID | None
    store_name: str | None
    reason: str


@dataclasses.dataclass(slots=True, frozen=True)
class StorageBootstrapReport:
    """
    Summarize attempted Store-registry reconstruction using supplied counters and issues. Counter
    checks reject negativity and handled totals above discovered; they do not require every
    configuration to be handled or each issue to match a counter. ok describes only the absence of
    counted failures.

    Example:
        >>> report = StorageBootstrapReport(
        ...     discovered_configurations=2, loaded_stores=2,
        ... )
        >>> report.ok
        True


    :ivar discovered_configurations: Number of configurations discovered by the producer.
    :ivar loaded_stores: Number counted as loaded; this does not independently establish availability.
    :ivar skipped_configurations: Number intentionally skipped, including existing or offline cases.
    :ivar failed_configurations: Number counted as failed by the producer.
    :ivar issues: Ordered explanations retained without count or type validation.
    """

    discovered_configurations: int = 0
    loaded_stores: int = 0
    skipped_configurations: int = 0
    failed_configurations: int = 0
    issues: tuple[StorageBootstrapIssue, ...] = ()

    def __post_init__(self) -> None:
        """
        Reject negative counters and a loaded-plus-skipped-plus-failed sum exceeding discovered.
        Integer/finiteness checks, exact total equality, and issue-count consistency are not
        enforced.

        Example:
            >>> StorageBootstrapReport(discovered_configurations=-1)
            Traceback (most recent call last):
            ...
            ValueError: bootstrap counts must not be negative.


        :return: None after count comparisons pass; inconsistent counts raise ValueError.
        """

        counts = (
            self.discovered_configurations,
            self.loaded_stores,
            self.skipped_configurations,
            self.failed_configurations,
        )
        if any(count < 0 for count in counts):
            raise ValueError("bootstrap counts must not be negative.")
        handled = (
            self.loaded_stores
            + self.skipped_configurations
            + self.failed_configurations
        )
        if handled > self.discovered_configurations:
            raise ValueError(
                "handled Store configurations exceed those discovered."
            )

    @property
    def ok(self) -> bool:
        """
        Test only whether failed_configurations equals zero. Skips, unhandled discovered
        configurations, and populated issues do not make this property false.

        Example:
            >>> StorageBootstrapReport(discovered_configurations=2, skipped_configurations=1).ok
            True


        :return: True when no failures were counted, otherwise False.
        """

        return self.failed_configurations == 0


@dataclasses.dataclass(slots=True, frozen=True)
class StoreStatusObservation:
    """
    Pair a configured Store identity with a supplied dynamic status value. The wrapper preserves
    attribution during enumeration; it validates the UUID but neither probes a Store nor
    validates/copies the status object.

    Example:
        >>> observation = StoreStatusObservation(
        ...     UUID(int=1), StoreStatus(available=True, writable=False),
        ... )
        >>> observation.store_ref
        UUID('00000000-0000-0000-0000-000000000001')


    :ivar store_ref: Configured Store UUID identifying the observation.
    :ivar status: Supplied StoreStatus, retained by reference without validation.
    """

    store_ref: StoreUUID
    status: StoreStatus

    def __post_init__(self) -> None:
        """
        Require store_ref to be a UUID instance without checking status shape or current Store
        registration.

        Example:
            >>> StoreStatusObservation(
            ...     "archive", StoreStatus(available=True, writable=False),
            ... )
            Traceback (most recent call last):
            ...
            TypeError: store_ref must be a UUID.


        :return: None for a UUID reference; other reference types raise TypeError.
        """

        if not isinstance(self.store_ref, UUID):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("store_ref must be a UUID.")


@dataclasses.dataclass(slots=True, frozen=True)
class StoreReconciliationPlan:
    """
    Retain a comparison of Replica claims and observed Store inventory for later application.
    UUID/count checks do not establish snapshot freshness, ownership of every member, complete
    verification, or consistency among classification lists. The applying manager owns revision
    checks and mutations.

    Example:
        >>> plan = StoreReconciliationPlan(
        ...     UUID(int=2), UUID(int=1), True,
        ...     EnumerationCompleteness.COMPLETE,
        ... )
        >>> plan.conclusive
        True


    :ivar plan_id: UUID identifying this plan value.
    :ivar store_ref: UUID of the configured Store being compared.
    :ivar verify_digests: Producer flag describing requested digest comparison, not proof it occurred.
    :ivar enumeration: Enumeration claim retained without enum coercion.
    :ivar expected_replicas: Declared count of expected catalogue Replicas.
    :ivar observed_locations: Declared count of observed Store addresses.
    :ivar matched_replicas: Declared matched count, bounded by both totals.
    :ivar missing_replica_ids: Replica identities classified missing by the producer.
    :ivar unexpected_locations: Observed addresses without expected claims.
    :ivar corrupt_replica_ids: Replica identities classified corrupt.
    :ivar unavailable_replica_ids: Replica identities whose state could not be conclusively checked.
    :ivar repository_revision: Optional opaque revision used by the applying implementation.
    :ivar warnings: Producer warning strings, retained without validation.
    :ivar errors: Producer errors that make conclusive false.
    """

    plan_id: UUID
    store_ref: StoreUUID
    verify_digests: bool
    enumeration: EnumerationCompleteness
    expected_replicas: int = 0
    observed_locations: int = 0
    matched_replicas: int = 0
    missing_replica_ids: tuple[ReplicaID, ...] = ()
    unexpected_locations: tuple[Location, ...] = ()
    corrupt_replica_ids: tuple[ReplicaID, ...] = ()
    unavailable_replica_ids: tuple[ReplicaID, ...] = ()
    repository_revision: str | None = None
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """
        Require UUID plan/Store identities, nonnegative counts, and matched count no greater than
        either total. Counts are compared without integer/finiteness validation; enum values, lists,
        revisions, and their mutual consistency are not checked.

        Example:
            >>> StoreReconciliationPlan(
            ...     UUID(int=2), UUID(int=1), False,
            ...     EnumerationCompleteness.COMPLETE,
            ...     expected_replicas=1, matched_replicas=2,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: matched_replicas exceeds a reconciliation total.


        :return: None after the identity/count checks; invalid identities raise TypeError and rejected counts raise ValueError.
        """

        if not isinstance(self.plan_id, UUID):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("plan_id must be a UUID.")
        if not isinstance(self.store_ref, UUID):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("store_ref must be a UUID.")
        counts = (
            self.expected_replicas,
            self.observed_locations,
            self.matched_replicas,
        )
        if any(count < 0 for count in counts):
            raise ValueError("reconciliation counts must not be negative.")
        if self.matched_replicas > min(
            self.expected_replicas,
            self.observed_locations,
        ):
            raise ValueError(
                "matched_replicas exceeds a reconciliation total."
            )

    @property
    def conclusive(self) -> bool:
        """
        Require identity with the COMPLETE enumeration enum plus no unavailable Replica IDs and no
        errors. Missing/corrupt/unexpected evidence, warnings, and verify_digests do not affect this
        predicate.

        Example:
            >>> plan.conclusive  # doctest: +SKIP
            True


        :return: True only for the stated completeness and error conditions.
        """

        return (
            self.enumeration is EnumerationCompleteness.COMPLETE
            and not self.unavailable_replica_ids
            and not self.errors
        )


@dataclasses.dataclass(slots=True, frozen=True)
class StoreReconciliationReport:
    """
    Retain a preview or application result together with its original plan. Construction only
    forbids claimed updated IDs when applied is false. It does not independently verify mutations,
    plan freshness, or attribution; clean can be true for a preview.

    Example:
        >>> report = StoreReconciliationReport(
        ...     plan=StoreReconciliationPlan(
        ...         UUID(int=2), UUID(int=1), True,
        ...         EnumerationCompleteness.COMPLETE,
        ...         expected_replicas=2, observed_locations=2,
        ...         matched_replicas=2,
        ...     ),
        ...     applied=False,
        ... )
        >>> report.clean
        True


    :ivar plan: Original comparison plan retained by reference.
    :ivar applied: Producer flag indicating whether application was performed.
    :ivar updated_replica_ids: Identities reported updated; must be empty for a false applied flag.
    :ivar warnings: Additional producer warnings, not considered by clean.
    :ivar errors: Additional errors that make clean false.
    """

    plan: StoreReconciliationPlan
    applied: bool
    updated_replica_ids: tuple[ReplicaID, ...] = ()
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """
        Reject nonempty updated_replica_ids when applied is false. Other result types,
        relationships, and claims are retained without validation.

        Example:
            >>> StoreReconciliationReport(
            ...     plan, applied=False,
            ...     updated_replica_ids=(ReplicaID(1),),
            ... )  # doctest: +SKIP


        :return: None when the preview/update rule holds; conflicting claims raise ValueError.
        """

        if not self.applied and self.updated_replica_ids:
            raise ValueError(
                "an unapplied reconciliation cannot update Replicas."
            )

    @property
    def clean(self) -> bool:
        """
        Require a conclusive plan with no missing, unexpected, or corrupt classifications and no
        report errors. applied, updated IDs, and plan/report warnings do not affect this result.

        Example:
            >>> report.clean  # doctest: +SKIP
            True


        :return: True when the compared evidence meets the stated cleanliness conditions, including for a preview.
        """

        return self.plan.conclusive and not (
            self.plan.missing_replica_ids
            or self.plan.unexpected_locations
            or self.plan.corrupt_replica_ids
            or self.errors
        )


__all__ = [
    "StoreReconciliationPlan", "StoreReconciliationReport",
    "StorageBootstrapIssue", "StorageBootstrapReport",
    "StoreConfiguration", "StoreStatusObservation", "TopologyRelation",
]
