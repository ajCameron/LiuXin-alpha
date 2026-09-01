"""
Metadata-to-link-update writer foundation for the catalog layer.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any, cast

from LiuXin_alpha.catalog.write.base_writer import CatalogValueWriter
from LiuXin_alpha.catalog.write.host_api import CatalogWriterHostAPI
from LiuXin_alpha.catalog.write.link_update import LinkUpdate
from LiuXin_alpha.databases.db_types import DstTableID, SrcTableID
from LiuXin_alpha.databases.macro_types import (
    LINK_TYPE_UNSET,
    LinkRow,
    LinkValue,
    UnsetLinkType,
)
from LiuXin_alpha.databases.schema_specs import (
    LinkCardinality,
    StorageLinkSpec,
)

type CatalogLinkTypeScope = str | UnsetLinkType | None
type CatalogLinkValues[RawValueT] = (
    RawValueT
    | DstTableID
    | LinkValue
    | Iterable[RawValueT | DstTableID | LinkValue]
    | Mapping[str, CatalogLinkValues[RawValueT]]
    | None
)
type CatalogLinkMap[RawValueT] = Mapping[
    SrcTableID,
    CatalogLinkValues[RawValueT],
]


class CatalogLinkWriter[RawValueT, ValueT](
    CatalogValueWriter[
        RawValueT,
        ValueT,
        LinkUpdate,
        Mapping[SrcTableID, tuple[LinkRow, ...]],
    ],
    ABC,
):
    """
    Translate metadata link intent and apply one normalized catalog update.

    Concrete field writers implement :meth:`adapt` and
    :meth:`resolve_destination`; they may override :meth:`validate`. Existing
    integer ids and rich :class:`LinkValue` instructions already carry
    database-facing information and therefore bypass those methods.

    The writer does not issue SQL, acquire locks, mutate caches, discover link
    schemas, or remove unused destination rows. :class:`LinkUpdate` owns
    structural normalization and :class:`CatalogAPI` owns application.

    :param catalog: Catalog facade used to apply normalized link updates.
    :param link_spec: Declared storage route and capabilities for the link.
    """

    def __init__(
        self,
        catalog: CatalogWriterHostAPI,
        link_spec: StorageLinkSpec,
    ) -> None:
        """
        Validate and store the link-writer configuration.

        :param catalog: Catalog facade used to apply normalized link updates.
        :param link_spec: Declared storage route and capabilities for the link.
        :return: None.
        :raises TypeError: If the link specification or catalog does not
            provide the required interface.
        """

        if not isinstance(link_spec, StorageLinkSpec):
            raise TypeError("link_spec must be a StorageLinkSpec")
        if not callable(getattr(catalog, "write_link_update", None)):
            raise TypeError("catalog must provide write_link_update")

        super().__init__(catalog)
        self._link_spec = link_spec

    @property
    def link_spec(self) -> StorageLinkSpec:
        """
        Return the declared link storage route and capabilities.

        :return: Configured link specification.
        """

        return self._link_spec

    @abstractmethod
    def resolve_destination(self, value: ValueT) -> DstTableID:
        """
        Resolve one adapted value to a destination-table id.

        Matching and create-if-missing policy belong in the concrete field
        writer or a resolver used by it, not in the base link workflow.

        :param value: Adapted and validated destination value.
        :return: Resolved destination-table id.
        """

        raise NotImplementedError

    def _destination_id_for(self, raw_value: RawValueT) -> DstTableID:
        """
        Adapt, validate, and resolve one raw metadata value.

        :param raw_value: Raw metadata value to process.
        :return: Resolved destination-table id.
        """

        return self.resolve_destination(self.prepare_value(raw_value))

    def _live_allowed_link_types(self) -> tuple[str, ...] | None:
        """
        Read the optional allowed-type registry through the database wrapper.

        Registry values are deliberately not cached by the writer. Type
        registries are database data and may be extended while a writer
        instance remains in use.

        :return: Live allowed values, or ``None`` when no registry exists.
        :raises TypeError: If the catalog cannot expose the registry declared
            by the link specification.
        """

        if self.link_spec.allowed_types_table is None:
            return None

        database = getattr(self.catalog, "db", None)
        wrapper = getattr(database, "driver_wrapper", None)
        get_allowed_types = getattr(wrapper, "get_allowed_link_types", None)
        if not callable(get_allowed_types):
            raise TypeError(
                "catalog database driver wrapper must provide "
                "get_allowed_link_types for a link with an allowed-types "
                "table"
            )

        allowed_types = get_allowed_types(self.link_spec)
        if allowed_types is None:
            raise ValueError(
                "link spec declares allowed-types table "
                f"{self.link_spec.allowed_types_table!r}, but the driver "
                "wrapper returned no registry"
            )
        values = tuple(allowed_types)
        if any(
            not isinstance(value, str) or not value.strip()
            for value in values
        ):
            raise ValueError(
                "allowed-types registry contains a non-string or blank value"
            )
        return values

    @staticmethod
    def _display_allowed_link_types(allowed_types: tuple[str, ...]) -> str:
        """
        Format an allowed-type collection for a validation message.

        :param allowed_types: Values to display.
        :return: Deterministic human-readable value list.
        """

        if not allowed_types:
            return "<none>"
        return ", ".join(repr(value) for value in allowed_types)

    def _validate_link_type_value(
        self,
        link_type: Any,
        *,
        origin: str,
        live_allowed_types: tuple[str, ...] | None,
    ) -> None:
        """
        Validate one explicit link type against link capabilities and policy.

        ``None`` is the valid SQL-null type and is not an enumerated value.
        Every named type must satisfy both the specification tuple and a live
        database registry when either restriction is present.

        :param link_type: Explicit type value to validate.
        :param origin: Input location used in validation messages.
        :param live_allowed_types: Values read from the optional registry.
        :return: None.
        :raises TypeError: If a named type is not a string.
        :raises ValueError: If the link is untyped, the type is blank, or the
            value is outside an allowed set.
        """

        if not self.link_spec.typed:
            raise ValueError(
                f"{origin} cannot specify a type because link table "
                f"{self.link_spec.link_table!r} is untyped"
            )
        if link_type is None:
            return
        if not isinstance(link_type, str):
            raise TypeError(f"{origin} link type must be a string or None")
        if not link_type.strip():
            raise ValueError(f"{origin} link type cannot be blank")

        declared_allowed_types = self.link_spec.allowed_types
        if (
            declared_allowed_types
            and link_type not in declared_allowed_types
        ):
            allowed = self._display_allowed_link_types(declared_allowed_types)
            raise ValueError(
                f"{origin} link type {link_type!r} is not allowed by the "
                f"link spec; allowed types: {allowed}"
            )
        if (
            live_allowed_types is not None
            and link_type not in live_allowed_types
        ):
            allowed = self._display_allowed_link_types(live_allowed_types)
            raise ValueError(
                f"{origin} link type {link_type!r} does not exist in "
                f"allowed-types table "
                f"{self.link_spec.allowed_types_table!r}; allowed types: "
                f"{allowed}"
            )

    def _materialise_link_type_input(
        self,
        raw_links: object,
        *,
        origin: str,
    ) -> tuple[object, tuple[tuple[Any, str], ...]]:
        """
        Stabilize one compact input and collect its explicit link types.

        Iterables are materialized here so validation never consumes a
        one-shot input before :class:`LinkUpdate` normalizes it.

        :param raw_links: Compact link value, iterable, or typed mapping.
        :param origin: Input location used in validation messages.
        :return: Stable compact input and pairs of explicit type value and its
            input location.
        :raises ValueError: If typed-map syntax is used for an untyped link.
        """

        if isinstance(raw_links, Mapping):
            if not self.link_spec.typed:
                raise ValueError(
                    f"{origin} cannot use a typed mapping because link table "
                    f"{self.link_spec.link_table!r} is untyped"
                )
            found: list[tuple[Any, str]] = []
            stable_mapping: dict[Any, object] = {}
            for nested_type, nested_links in raw_links.items():
                nested_origin = f"{origin} key {nested_type!r}"
                found.append((nested_type, nested_origin))
                stable_links, nested_types = self._materialise_link_type_input(
                    nested_links,
                    origin=nested_origin,
                )
                stable_mapping[nested_type] = stable_links
                found.extend(nested_types)
            return stable_mapping, tuple(found)

        if isinstance(raw_links, LinkValue):
            if raw_links.link_type is None:
                return raw_links, ()
            return raw_links, ((raw_links.link_type, origin),)

        if isinstance(raw_links, Iterable) and not isinstance(
            raw_links,
            (str, bytes, bytearray),
        ):
            found = []
            stable_values: list[object] = []
            for index, raw_link in enumerate(raw_links):
                stable_link, nested_types = self._materialise_link_type_input(
                    raw_link,
                    origin=f"{origin}[{index}]",
                )
                stable_values.append(stable_link)
                found.extend(nested_types)
            return tuple(stable_values), tuple(found)
        return raw_links, ()

    def _validate_link_type_inputs(
        self,
        replacements: CatalogLinkMap[RawValueT] | None,
        *,
        additions: CatalogLinkMap[RawValueT] | None,
        deletions: CatalogLinkMap[RawValueT] | None,
        link_type: CatalogLinkTypeScope,
    ) -> tuple[
        CatalogLinkMap[RawValueT] | None,
        CatalogLinkMap[RawValueT] | None,
        CatalogLinkMap[RawValueT] | None,
    ]:
        """
        Validate all caller-supplied link types before resolving destinations.

        :param replacements: Authoritative replacement input.
        :param additions: Incremental addition input.
        :param deletions: Incremental deletion input.
        :param link_type: Optional update-wide type scope.
        :return: Stable replacement, addition, and deletion maps.
        """

        supplied_types: list[tuple[Any, str]] = []
        if link_type is not LINK_TYPE_UNSET:
            if not self.link_spec.type_part_of_identity:
                raise ValueError(
                    "a link-type scope requires a typed link spec whose type "
                    "is part of its identity"
                )
            supplied_types.append((link_type, "link_type scope"))

        stable_operations: list[CatalogLinkMap[RawValueT] | None] = []
        for operation_name, operation in (
            ("replacements", replacements),
            ("additions", additions),
            ("deletions", deletions),
        ):
            if not isinstance(operation, Mapping):
                stable_operations.append(operation)
                continue
            stable_operation: dict[SrcTableID, object] = {}
            for source_id, raw_links in operation.items():
                stable_links, nested_types = self._materialise_link_type_input(
                    raw_links,
                    origin=f"{operation_name}[{source_id!r}]",
                )
                stable_operation[source_id] = stable_links
                supplied_types.extend(nested_types)
            stable_operations.append(
                cast(CatalogLinkMap[RawValueT], stable_operation)
            )

        for supplied_type, origin in supplied_types:
            self._validate_link_type_value(
                supplied_type,
                origin=origin,
                live_allowed_types=None,
            )

        named_types = tuple(
            (supplied_type, origin)
            for supplied_type, origin in supplied_types
            if supplied_type is not None
        )
        if named_types and self.link_spec.allowed_types_table is not None:
            live_allowed_types = self._live_allowed_link_types()
            for supplied_type, origin in named_types:
                self._validate_link_type_value(
                    supplied_type,
                    origin=origin,
                    live_allowed_types=live_allowed_types,
                )
        return (
            stable_operations[0],
            stable_operations[1],
            stable_operations[2],
        )

    def _validate_cardinality(self, update: LinkUpdate) -> None:
        """
        Enforce cardinality constraints visible within one update request.

        A one-to-one or many-to-one relation permits at most one destination
        for each source. Reverse-side uniqueness for one-to-one and
        one-to-many relations is enforced atomically by the database schema,
        because an isolated update cannot determine all existing owners.

        :param update: Normalized link update to validate.
        :return: None.
        :raises ValueError: If a singular source is assigned multiple targets.
        """

        if self.link_spec.cardinality not in {
            LinkCardinality.ONE_TO_ONE,
            LinkCardinality.MANY_TO_ONE,
        }:
            return

        for operation_name in ("replacements", "additions"):
            operation = getattr(update, operation_name)
            for source_id, links in operation.items():
                if len(links) > 1:
                    raise ValueError(
                        f"{self.link_spec.cardinality.value} link permits at "
                        f"most one destination for source id {source_id!r}; "
                        f"{operation_name} supplied {len(links)}"
                    )

    def build_update(
        self,
        replacements: CatalogLinkMap[RawValueT] | None = None,
        *,
        additions: CatalogLinkMap[RawValueT] | None = None,
        deletions: CatalogLinkMap[RawValueT] | None = None,
        link_type: CatalogLinkTypeScope = LINK_TYPE_UNSET,
    ) -> LinkUpdate:
        """
        Build an immutable normalized update without applying its links.

        :param replacements: Authoritative desired link values keyed by source
            id.
        :param additions: Link values to add, keyed by source id.
        :param deletions: Link values to remove, keyed by source id.
        :param link_type: Optional link-type scope. ``LINK_TYPE_UNSET`` leaves
            the update unscoped.
        :return: Immutable normalized link update.
        """

        replacements, additions, deletions = self._validate_link_type_inputs(
            replacements,
            additions=additions,
            deletions=deletions,
            link_type=link_type,
        )
        update = LinkUpdate.from_legacy(
            self.link_spec,
            replacements,
            additions=additions,
            deletions=deletions,
            secondary_id_for=self._destination_id_for,
            link_type=link_type,
        )
        self._validate_cardinality(update)
        return update

    def build_one_update(
        self,
        src_id: SrcTableID,
        dst_value: Any,
        *,
        link_type: CatalogLinkTypeScope = LINK_TYPE_UNSET,
        **kwargs: Any,
    ) -> LinkUpdate:
        """
        Build one authoritative source-to-destination link update.

        This is equivalent to ``build_update({src_id: dst_value})``. On a
        plural link it replaces that source's complete link set with the one
        supplied destination. Use :meth:`build_update` with ``additions`` for
        a non-destructive incremental link.

        :param src_id: Source-table ID whose link set should change.
        :param dst_value: One raw, resolved, rich, or clear link value.
        :param link_type: Optional link-type scope.
        :param kwargs: Unsupported additional update options.
        :return: Immutable normalized link update.
        :raises TypeError: If additional update options are supplied.
        """

        if kwargs:
            names = ", ".join(sorted(kwargs))
            raise TypeError(
                f"link write_one received unexpected option(s): {names}"
            )
        return self.build_update(
            {src_id: dst_value},
            link_type=link_type,
        )

    def apply_update(
        self,
        update: LinkUpdate,
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """
        Apply one normalized link update through the catalog.

        :param update: Immutable normalized link update.
        :return: Complete written link rows keyed by source id.
        :raises TypeError: If ``update`` is not a :class:`LinkUpdate`.
        :raises ValueError: If the update targets a different link or violates
            the configured cardinality.
        """

        if not isinstance(update, LinkUpdate):
            raise TypeError("update must be a LinkUpdate")
        if update.link_spec != self.link_spec:
            raise ValueError("update link_spec does not match writer link_spec")
        self._validate_cardinality(update)
        return self.catalog.write_link_update(update)


__all__ = [
    "CatalogLinkMap",
    "CatalogLinkTypeScope",
    "CatalogLinkValues",
    "CatalogLinkWriter",
]
