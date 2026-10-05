"""
Resolve and apply catalog link-table updates with cardinality checks.

The module keeps validation, normalization and host mutation boundaries explicit.

Example:
    Exercise link writer through its owning regression module::

        python -m pytest -q tests/catalog/test_link_writer.py
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

    Example:
        Exercise CatalogLinkWriter through its owning regression module::

            python -m pytest -q tests/catalog/test_link_writer.py
    """

    def __init__(
        self,
        catalog: CatalogWriterHostAPI,
        link_spec: StorageLinkSpec,
    ) -> None:
        """
        Validate and store the link-writer configuration.

        Example:
            Exercise CatalogLinkWriter.init through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param catalog: Catalog host or facade supplying metadata and mutation services.
        :param link_spec: Value supplied for link spec under the catalog contract.
        :return: None; the function records state or raises through its assertions.
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

        Example:
            Exercise CatalogLinkWriter.link spec through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return self._link_spec

    @abstractmethod
    def resolve_destination(self, value: ValueT) -> DstTableID:
        """
        Resolve one adapted value to a destination-table id.

        Example:
            Exercise CatalogLinkWriter.resolve destination through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param value: Public or stored value to normalize, compare or write.
        :return: The deterministic value, row, identity or collection described above.
        """

        raise NotImplementedError

    def _destination_id_for(self, raw_value: RawValueT) -> DstTableID:
        """
        Adapt, validate, and resolve one raw metadata value.

        Example:
            Exercise CatalogLinkWriter.destination id for through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param raw_value: Value supplied for raw value under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return self.resolve_destination(self.prepare_value(raw_value))

    def _live_allowed_link_types(self) -> tuple[str, ...] | None:
        """
        Read the optional allowed-type registry through the database wrapper.

        Example:
            Exercise CatalogLinkWriter.live allowed link types through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise CatalogLinkWriter.display allowed link types through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param allowed_types: Value supplied for allowed types under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise CatalogLinkWriter.validate link type value through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param link_type: Optional typed relation value carried by the link.
        :param origin: Value supplied for origin under the catalog contract.
        :param live_allowed_types: Value supplied for live allowed types under the catalog
            contract.
        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise CatalogLinkWriter.materialise link type input through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param raw_links: Value supplied for raw links under the catalog contract.
        :param origin: Value supplied for origin under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise CatalogLinkWriter.validate link type inputs through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param replacements: Value supplied for replacements under the catalog contract.
        :param additions: Value supplied for additions under the catalog contract.
        :param deletions: Value supplied for deletions under the catalog contract.
        :param link_type: Optional typed relation value carried by the link.
        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise CatalogLinkWriter.validate cardinality through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise CatalogLinkWriter.build update through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param replacements: Value supplied for replacements under the catalog contract.
        :param additions: Value supplied for additions under the catalog contract.
        :param deletions: Value supplied for deletions under the catalog contract.
        :param link_type: Optional typed relation value carried by the link.
        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise CatalogLinkWriter.build one update through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param src_id: Value supplied for src id under the catalog contract.
        :param dst_value: Value supplied for dst value under the catalog contract.
        :param link_type: Optional typed relation value carried by the link.
        :param kwargs: Value supplied for kwargs under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
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

        Example:
            Exercise CatalogLinkWriter.apply update through its owning regression module::

                python -m pytest -q tests/catalog/test_link_writer.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
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
