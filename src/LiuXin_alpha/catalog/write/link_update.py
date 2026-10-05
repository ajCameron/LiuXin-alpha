"""
Normalize, validate, merge and serialize catalog relation updates.

The module keeps validation, normalization and host mutation boundaries explicit.

Example:
    Exercise link update through its owning regression module::

        python -m pytest -q tests/catalog/test_link_update.py
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from pprint import pformat as _pformat
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Self, TypeVar, cast

from LiuXin_alpha.databases.db_types import DstTableID, SrcTableID
from LiuXin_alpha.databases.macro_types import (
    LINK_TYPE_UNSET,
    LinkRow,
    LinkValue,
    UnsetLinkType,
)
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import PortableMacrosAPI


type _LinkType = str
type _LinkTypeScope = _LinkType | UnsetLinkType | None
type _CompactLinks[RawValueT] = (
    RawValueT
    | LinkValue
    | Iterable[RawValueT | LinkValue]
    | Mapping[_LinkType, _CompactLinks[RawValueT]]
    | None
)
type _CompactMap[RawValueT] = Mapping[SrcTableID, _CompactLinks[RawValueT]]

_RawValueT = TypeVar("_RawValueT")
_LINK_OPERATION_NAMES = ("replacements", "deletions", "additions")


def _identity(value: DstTableID) -> DstTableID:
    """
    Return the stable identity tuple used to compare and deduplicate links.

    Example:
        Exercise identity through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param value: Public or stored value to normalize, compare or write.
    :return: The deterministic value, row, identity or collection described above.
    """
    return value


def _empty_link_operation() -> dict[SrcTableID, tuple[LinkValue, ...]]:
    """
    Return whether a link-operation payload is semantically empty.

    Example:
        Exercise empty link operation through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :return: The deterministic value, row, identity or collection described above.
    """
    return {}


def _link_value_to_dict(link: LinkValue) -> dict[str, Any]:
    """
    Return a compact, plain representation used only for inspection.

    Example:
        Exercise link value to dict through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py


    :param link: Value supplied for link under the catalog contract.
    :return: The deterministic value, row, identity or collection described above.
    """

    rendered: dict[str, Any] = {"secondary_id": link.secondary_id}
    if link.link_type is not None:
        rendered["link_type"] = link.link_type
    if link.priority is not None:
        rendered["priority"] = link.priority
    if link.extra:
        rendered["extra"] = dict(link.extra)
    return rendered


@dataclass(frozen=True, slots=True)
class LinkUpdateLink(Mapping[str, Any]):
    """
    Read-only, display-friendly view of one link instruction.

    Example:
        Exercise LinkUpdateLink through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py
    """

    src_id: SrcTableID
    dst_id: DstTableID
    operation: str
    link_type: _LinkType | None = None
    priority: int | float | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)
    dst_value_for: Callable[[DstTableID], Any] | None = field(
        default=None,
        kw_only=True,
        repr=False,
        compare=False,
    )
    _dst_value: Any = field(default=None, init=False, repr=False, compare=False)
    _dst_value_loaded: bool = field(
        default=False,
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        """
        Initialize and validate the LinkUpdateLink state.

        Example:
            Exercise LinkUpdateLink.post init through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: None; the function records state or raises through its assertions.
        """
        if self.operation not in _LINK_OPERATION_NAMES:
            raise ValueError(
                "operation must be replacements, deletions, or additions"
            )
        if not isinstance(self.extra, Mapping):
            raise TypeError("link extras must be a mapping")
        if self.dst_value_for is not None and not callable(self.dst_value_for):
            raise TypeError("dst_value_for must be callable")
        object.__setattr__(self, "extra", MappingProxyType(dict(self.extra)))

    def __getitem__(self, key: str) -> Any:
        """
        Return one extra link-column value.

        Example:
            Exercise LinkUpdateLink.getitem through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param key: Value supplied for key under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return self.extra[key]

    def __iter__(self) -> Iterator[str]:
        """
        Iterate over extra link-column names in their supplied order.

        Example:
            Exercise LinkUpdateLink.iter through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return iter(self.extra)

    def __len__(self) -> int:
        """
        Return the number of extra link columns.

        Example:
            Exercise LinkUpdateLink.len through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return len(self.extra)

    @property
    def dst_value_loaded(self) -> bool:
        """
        Return whether this view has resolved its destination value.

        Example:
            Exercise LinkUpdateLink.dst value loaded through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return self._dst_value_loaded

    def get_dst_value(
        self,
        dst_value_for: Callable[[DstTableID], Any] | None = None,
    ) -> Any:
        """
        Resolve and cache the destination value on first access.

        Example:
            Exercise LinkUpdateLink.get dst value through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param dst_value_for: Value supplied for dst value for under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        if not self._dst_value_loaded:
            if dst_value_for is not None and not callable(dst_value_for):
                raise TypeError("dst_value_for must be callable")
            loader = (
                dst_value_for
                if dst_value_for is not None
                else self.dst_value_for
            )
            if loader is None:
                raise RuntimeError(
                    "no destination-value loader was supplied for this link"
                )
            value = loader(self.dst_id)
            object.__setattr__(self, "_dst_value", value)
            object.__setattr__(self, "_dst_value_loaded", True)
        return self._dst_value

    @property
    def dst_value(self) -> Any:
        """
        Lazily resolve and return the destination value.

        Example:
            Exercise LinkUpdateLink.dst value through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return self.get_dst_value()

    def to_dict(self) -> dict[str, Any]:
        """
        Return plain inspection data without forcing destination loading.

        Example:
            Exercise LinkUpdateLink.to dict through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        rendered: dict[str, Any] = {
            "src_id": self.src_id,
            "dst_id": self.dst_id,
            "operation": self.operation,
            "extra": dict(self.extra),
        }
        if self.link_type is not None:
            rendered["link_type"] = self.link_type
        if self.priority is not None:
            rendered["priority"] = self.priority
        if self.dst_value_loaded:
            rendered["dst_value"] = self._dst_value
        return rendered

    def pformat(self, *, indent: int = 2, width: int = 88) -> str:
        """
        Pretty-format this link without forcing destination loading.

        Example:
            Exercise LinkUpdateLink.pformat through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param indent: Value supplied for indent under the catalog contract.
        :param width: Value supplied for width under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return _pformat(
            self.to_dict(),
            indent=indent,
            width=width,
            sort_dicts=False,
        )

    def __str__(self) -> str:
        """
        Render the catalog update in a stable diagnostic representation.

        Example:
            Exercise LinkUpdateLink.str through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self.pformat()


@dataclass(frozen=True, slots=True)
class LinkUpdateEntry:
    """
    Read-only view of every effective operation for one primary id.

    Example:
        Exercise LinkUpdateEntry through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py
    """

    primary_id: SrcTableID
    replacements: tuple[LinkValue, ...] | None = None
    deletions: tuple[LinkValue, ...] = ()
    additions: tuple[LinkValue, ...] = ()

    @property
    def has_replacement(self) -> bool:
        """
        Return whether this id has an authoritative replacement.

        Example:
            Exercise LinkUpdateEntry.has replacement through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: True when the tested condition is satisfied; otherwise False.
        """

        return self.replacements is not None

    @property
    def clears_scope(self) -> bool:
        """
        Return whether the replacement intentionally starts from empty.

        Example:
            Exercise LinkUpdateEntry.clears scope through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return self.replacements == ()

    @property
    def is_incremental(self) -> bool:
        """
        Return whether this entry changes links without replacing them.

        Example:
            Exercise LinkUpdateEntry.is incremental through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: True when the tested condition is satisfied; otherwise False.
        """

        return not self.has_replacement and bool(self.deletions or self.additions)

    @property
    def operations(self) -> Mapping[str, tuple[LinkValue, ...]]:
        """
        Return effective operations in their database application order.

        Example:
            Exercise LinkUpdateEntry.operations through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        operations: dict[str, tuple[LinkValue, ...]] = {}
        if self.replacements is not None:
            operations["replacements"] = self.replacements
        if self.deletions:
            operations["deletions"] = self.deletions
        if self.additions:
            operations["additions"] = self.additions
        return MappingProxyType(operations)

    @property
    def operation_names(self) -> tuple[str, ...]:
        """
        Return the names of effective operations for this id.

        Example:
            Exercise LinkUpdateEntry.operation names through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return tuple(self.operations)

    def __bool__(self) -> bool:
        """
        Return whether at least one effective operation is present.

        Example:
            Exercise LinkUpdateEntry.bool through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return bool(self.operations)

    def iter_links(
        self,
        *,
        dst_value_for: Callable[[DstTableID], Any] | None = None,
    ) -> Iterator[LinkUpdateLink]:
        """
        Iterate over link instructions for this primary ID.

        Example:
            Exercise LinkUpdateEntry.iter links through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param dst_value_for: Value supplied for dst value for under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return (
            LinkUpdateLink(
                src_id=self.primary_id,
                dst_id=cast(DstTableID, link.secondary_id),
                operation=operation,
                link_type=cast(_LinkType | None, link.link_type),
                priority=link.priority,
                extra=link.extra,
                dst_value_for=dst_value_for,
            )
            for operation, links in self.operations.items()
            for link in links
        )

    def links(
        self,
        *,
        dst_value_for: Callable[[DstTableID], Any] | None = None,
    ) -> tuple[LinkUpdateLink, ...]:
        """
        Return all link instructions for this primary ID.

        Example:
            Exercise LinkUpdateEntry.links through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param dst_value_for: Value supplied for dst value for under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return tuple(self.iter_links(dst_value_for=dst_value_for))

    def to_dict(self) -> dict[str, Any]:
        """
        Return a plain, inspection-friendly representation of this view.

        Example:
            Exercise LinkUpdateEntry.to dict through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return {
            "primary_id": self.primary_id,
            "operations": {
                operation: [_link_value_to_dict(link) for link in links]
                for operation, links in self.operations.items()
            },
        }

    def pformat(self, *, indent: int = 2, width: int = 88) -> str:
        """
        Pretty-format this per-id view without printing as a side effect.

        Example:
            Exercise LinkUpdateEntry.pformat through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param indent: Value supplied for indent under the catalog contract.
        :param width: Value supplied for width under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return _pformat(
            self.to_dict(),
            indent=indent,
            width=width,
            sort_dicts=False,
        )

    def __str__(self) -> str:
        """
        Render the catalog update in a stable diagnostic representation.

        Example:
            Exercise LinkUpdateEntry.str through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self.pformat()


@dataclass(frozen=True, slots=True)
class LinkUpdate:
    """
    A collection of changes to links between two database tables.

    Example:
        Exercise LinkUpdate through its owning regression module::

            python -m pytest -q tests/catalog/test_link_update.py
    """

    link_spec: StorageLinkSpec
    replacements: Mapping[SrcTableID, Iterable[LinkValue]] = field(
        default_factory=_empty_link_operation
    )
    link_type: _LinkTypeScope = LINK_TYPE_UNSET
    additions: Mapping[SrcTableID, Iterable[LinkValue]] = field(
        default_factory=_empty_link_operation,
        kw_only=True,
    )
    deletions: Mapping[SrcTableID, Iterable[LinkValue]] = field(
        default_factory=_empty_link_operation,
        kw_only=True,
    )

    def __post_init__(self) -> None:
        """
        Materialise caller-owned containers into a stable update request.

        Example:
            Exercise LinkUpdate.post init through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: None; the function records state or raises through its assertions.
        """

        if not isinstance(self.link_spec, StorageLinkSpec):
            raise TypeError("link_spec must be a StorageLinkSpec")
        if self.link_type is not LINK_TYPE_UNSET and (
            not self.link_spec.typed or not self.link_spec.type_part_of_identity
        ):
            raise ValueError(
                "a link-type scope requires a typed link spec whose type is "
                "part of its identity"
            )
        if self.link_type is not LINK_TYPE_UNSET:
            self._validate_declared_link_type(
                self.link_type,
                origin="link_type scope",
            )

        object.__setattr__(
            self,
            "replacements",
            self._materialise_operation("replacements", self.replacements),
        )
        object.__setattr__(
            self,
            "additions",
            self._materialise_operation("additions", self.additions),
        )
        object.__setattr__(
            self,
            "deletions",
            self._materialise_operation("deletions", self.deletions),
        )

    def _validate_declared_link_type(
        self,
        link_type: Any,
        *,
        origin: str,
    ) -> None:
        """
        Validate a type against capabilities recorded in the link spec.

        Example:
            Exercise LinkUpdate.validate declared link type through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param link_type: Optional typed relation value carried by the link.
        :param origin: Value supplied for origin under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        if not self.link_spec.typed:
            if link_type is not None:
                raise ValueError(
                    f"{origin} cannot carry a type on an untyped link spec"
                )
            return
        if link_type is None:
            return
        if not isinstance(link_type, str):
            raise TypeError(f"{origin} must be a string or None")
        if not link_type.strip():
            raise ValueError(f"{origin} cannot be blank")
        if (
            self.link_spec.allowed_types
            and link_type not in self.link_spec.allowed_types
        ):
            allowed = ", ".join(
                repr(value) for value in self.link_spec.allowed_types
            )
            raise ValueError(
                f"{origin} {link_type!r} is not allowed by the link spec; "
                f"allowed types: {allowed}"
            )

    def _materialise_operation(
        self,
        name: str,
        supplied: object,
    ) -> Mapping[SrcTableID, tuple[LinkValue, ...]]:
        """
        Normalize materialise operation into the canonical link-update representation.

        Example:
            Exercise LinkUpdate.materialise operation through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param name: Value supplied for name under the catalog contract.
        :param supplied: Value supplied for supplied under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        if not isinstance(supplied, Mapping):
            raise TypeError(f"{name} must be a mapping")

        materialised: dict[SrcTableID, tuple[LinkValue, ...]] = {}
        singular = name.removesuffix("s")
        operation = cast(Mapping[SrcTableID, object], supplied)
        for primary_id, links in operation.items():
            if links is None:
                if name == "replacements":
                    raise TypeError(
                        "replacement links cannot be None; use an empty iterable to clear links"
                    )
                raise TypeError(f"{singular} links cannot be None")

            if not isinstance(links, Iterable):
                raise TypeError(f"{singular} links must be LinkValue instances")
            values = tuple(links)
            if not all(isinstance(link, LinkValue) for link in values):
                raise TypeError(f"{singular} links must be LinkValue instances")
            stable_values = tuple(
                self._materialise_link(cast(LinkValue, link), operation=singular)
                for link in values
            )
            self._reject_duplicate_identities(
                stable_values,
                operation=singular,
                primary_id=primary_id,
            )
            materialised[primary_id] = stable_values

        return MappingProxyType(materialised)

    def _materialise_link(self, link: LinkValue, *, operation: str) -> LinkValue:
        """
        Normalize materialise link into the canonical link-update representation.

        Example:
            Exercise LinkUpdate.materialise link through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param link: Value supplied for link under the catalog contract.
        :param operation: Value supplied for operation under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        if not isinstance(link.extra, Mapping):
            raise TypeError(f"{operation} link extras must be a mapping")

        supplied_type = cast(_LinkType | None, link.link_type)
        if not self.link_spec.typed and supplied_type is not None:
            raise ValueError(
                f"{operation} link cannot carry a type on an untyped link spec"
            )
        self._validate_declared_link_type(
            supplied_type,
            origin=f"{operation} link type",
        )
        if not self.link_spec.ordered and link.priority is not None:
            raise ValueError(
                f"{operation} link cannot carry a priority on an unordered link spec"
            )
        if self.link_type is not LINK_TYPE_UNSET:
            if supplied_type is None:
                link = replace(link, link_type=self.link_type)
            elif supplied_type != self.link_type:
                raise ValueError(
                    f"{operation} link type {supplied_type!r} does not match "
                    f"update scope {self.link_type!r}"
                )

        return replace(link, extra=MappingProxyType(dict(link.extra)))

    def _reject_duplicate_identities(
        self,
        links: tuple[LinkValue, ...],
        *,
        operation: str,
        primary_id: SrcTableID,
    ) -> None:
        """
        Perform the catalog reject duplicate identities operation under explicit validation and ordering rules.

        Example:
            Exercise LinkUpdate.reject duplicate identities through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param links: Value supplied for links under the catalog contract.
        :param operation: Value supplied for operation under the catalog contract.
        :param primary_id: Primary catalog row identity owning the link operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        seen: set[tuple[Any, ...]] = set()
        for link in links:
            identity = self._link_identity(self.link_spec, link)
            if identity in seen:
                raise ValueError(
                    f"{operation} links for primary id {primary_id!r} contain "
                    f"duplicate logical identity {identity!r}"
                )
            seen.add(identity)

    @property
    def mentioned_primary_ids(self) -> tuple[SrcTableID, ...]:
        """
        Return all ids named by any operation map, preserving first order.

        Example:
            Exercise LinkUpdate.mentioned primary ids through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return tuple(
            dict.fromkeys(
                (
                    *self.replacements,
                    *self.deletions,
                    *self.additions,
                )
            )
        )

    @property
    def primary_ids(self) -> tuple[SrcTableID, ...]:
        """
        Return ids with effective work, preserving application order.

        Example:
            Exercise LinkUpdate.primary ids through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return tuple(
            primary_id
            for primary_id in self.mentioned_primary_ids
            if self.for_primary_id(primary_id)
        )

    def for_primary_id(self, primary_id: SrcTableID) -> LinkUpdateEntry:
        """
        Return the complete operation view for ``primary_id``.

        Example:
            Exercise LinkUpdate.for primary id through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param primary_id: Primary catalog row identity owning the link operation.
        :return: The deterministic value, row, identity or collection described above.
        """

        replacements = (
            tuple(self.replacements[primary_id])
            if primary_id in self.replacements
            else None
        )
        return LinkUpdateEntry(
            primary_id=primary_id,
            replacements=replacements,
            deletions=tuple(self.deletions.get(primary_id, ())),
            additions=tuple(self.additions.get(primary_id, ())),
        )

    def __getitem__(self, primary_id: SrcTableID) -> LinkUpdateEntry:
        """
        Return or iterate getitem from the normalized catalog state.

        Example:
            Exercise LinkUpdate.getitem through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param primary_id: Primary catalog row identity owning the link operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        entry = self.for_primary_id(primary_id)
        if not entry:
            raise KeyError(primary_id)
        return entry

    def get(
        self,
        primary_id: SrcTableID,
        default: Any = None,
    ) -> LinkUpdateEntry | Any:
        """
        Return an effective per-id entry, or ``default`` when absent.

        Example:
            Exercise LinkUpdate.get through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param primary_id: Primary catalog row identity owning the link operation.
        :param default: Value supplied for default under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        entry = self.for_primary_id(primary_id)
        return entry if entry else default

    def keys(self) -> tuple[SrcTableID, ...]:
        """
        Return effective primary ids in stable order.

        Example:
            Exercise LinkUpdate.keys through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return self.primary_ids

    def values(self) -> Iterator[LinkUpdateEntry]:
        """
        Iterate over effective per-id views in stable order.

        Example:
            Exercise LinkUpdate.values through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return (self[primary_id] for primary_id in self.primary_ids)

    def items(self) -> Iterator[tuple[SrcTableID, LinkUpdateEntry]]:
        """
        Iterate over effective ``(primary_id, entry)`` pairs.

        Example:
            Exercise LinkUpdate.items through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return ((primary_id, self[primary_id]) for primary_id in self.primary_ids)

    def __iter__(self) -> Iterator[SrcTableID]:
        """
        Return or iterate iter from the normalized catalog state.

        Example:
            Exercise LinkUpdate.iter through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return iter(self.primary_ids)

    def __len__(self) -> int:
        """
        Expose the normalized update's len behavior.

        Example:
            Exercise LinkUpdate.len through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return len(self.primary_ids)

    def __bool__(self) -> bool:
        """
        Expose the normalized update's bool behavior.

        Example:
            Exercise LinkUpdate.bool through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return bool(self.primary_ids)

    def __contains__(self, primary_id: object) -> bool:
        """
        Expose the normalized update's contains behavior.

        Example:
            Exercise LinkUpdate.contains through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param primary_id: Primary catalog row identity owning the link operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return primary_id in self.primary_ids

    def links_for_primary_id(
        self,
        primary_id: SrcTableID,
        *,
        dst_value_for: Callable[[DstTableID], Any] | None = None,
    ) -> tuple[LinkUpdateLink, ...]:
        """
        Return display-friendly link dataclasses for one primary id.

        Example:
            Exercise LinkUpdate.links for primary id through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param primary_id: Primary catalog row identity owning the link operation.
        :param dst_value_for: Value supplied for dst value for under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return self.for_primary_id(primary_id).links(
            dst_value_for=dst_value_for,
        )

    def iter_links(
        self,
        *,
        dst_value_for: Callable[[DstTableID], Any] | None = None,
    ) -> Iterator[LinkUpdateLink]:
        """
        Iterate over every effective link instruction in this update.

        Example:
            Exercise LinkUpdate.iter links through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param dst_value_for: Value supplied for dst value for under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return (
            link
            for entry in self.values()
            for link in entry.iter_links(dst_value_for=dst_value_for)
        )

    def links(
        self,
        *,
        dst_value_for: Callable[[DstTableID], Any] | None = None,
    ) -> tuple[LinkUpdateLink, ...]:
        """
        Return one dataclass per effective link instruction.

        Example:
            Exercise LinkUpdate.links through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param dst_value_for: Value supplied for dst value for under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return tuple(self.iter_links(dst_value_for=dst_value_for))

    def to_dict(self) -> dict[str, Any]:
        """
        Return a plain, deterministic representation for inspection.

        Example:
            Exercise LinkUpdate.to dict through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """

        return {
            "primary_table": self.link_spec.primary_table,
            "secondary_table": self.link_spec.secondary_table,
            "link_table": self.link_spec.link_table,
            "link_type": self.link_type,
            "updates": {
                primary_id: {
                    operation: [_link_value_to_dict(link) for link in links]
                    for operation, links in entry.operations.items()
                }
                for primary_id, entry in self.items()
            },
        }

    def pformat(self, *, indent: int = 2, width: int = 88) -> str:
        """
        Pretty-format this update without printing as a side effect.

        Example:
            Exercise LinkUpdate.pformat through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param indent: Value supplied for indent under the catalog contract.
        :param width: Value supplied for width under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return _pformat(
            self.to_dict(),
            indent=indent,
            width=width,
            sort_dicts=False,
        )

    def __str__(self) -> str:
        """
        Render the catalog update in a stable diagnostic representation.

        Example:
            Exercise LinkUpdate.str through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self.pformat()

    @classmethod
    def from_ids(
        cls,
        link_spec: StorageLinkSpec,
        replacements: _CompactMap[DstTableID] | None = None,
        *,
        additions: _CompactMap[DstTableID] | None = None,
        deletions: _CompactMap[DstTableID] | None = None,
        link_type: _LinkTypeScope = LINK_TYPE_UNSET,
    ) -> Self:
        """
        Build an update from compact maps whose values are secondary ids.

        Example:
            Exercise LinkUpdate.from ids through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param link_spec: Value supplied for link spec under the catalog contract.
        :param replacements: Value supplied for replacements under the catalog contract.
        :param additions: Value supplied for additions under the catalog contract.
        :param deletions: Value supplied for deletions under the catalog contract.
        :param link_type: Optional typed relation value carried by the link.
        :return: The deterministic value, row, identity or collection described above.
        """

        return cls._from_compact_maps(
            link_spec,
            replacements=replacements,
            additions=additions,
            deletions=deletions,
            link_type=link_type,
            secondary_id_for=_identity,
        )

    @classmethod
    def from_values(
        cls,
        link_spec: StorageLinkSpec,
        replacements: _CompactMap[_RawValueT] | None = None,
        *,
        secondary_id_for: Callable[[_RawValueT], DstTableID],
        additions: _CompactMap[_RawValueT] | None = None,
        deletions: _CompactMap[_RawValueT] | None = None,
        link_type: _LinkTypeScope = LINK_TYPE_UNSET,
    ) -> Self:
        """
        Build an update from compact maps of secondary-table values.

        Example:
            Exercise LinkUpdate.from values through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param link_spec: Value supplied for link spec under the catalog contract.
        :param replacements: Value supplied for replacements under the catalog contract.
        :param secondary_id_for: Value supplied for secondary id for under the catalog
            contract.
        :param additions: Value supplied for additions under the catalog contract.
        :param deletions: Value supplied for deletions under the catalog contract.
        :param link_type: Optional typed relation value carried by the link.
        :return: The deterministic value, row, identity or collection described above.
        """

        if not callable(secondary_id_for):
            raise TypeError("secondary_id_for must be callable")
        return cls._from_compact_maps(
            link_spec,
            replacements=replacements,
            additions=additions,
            deletions=deletions,
            link_type=link_type,
            secondary_id_for=secondary_id_for,
        )

    @classmethod
    def from_legacy(
        cls,
        link_spec: StorageLinkSpec,
        replacements: _CompactMap[_RawValueT | DstTableID] | None = None,
        *,
        secondary_id_for: Callable[[_RawValueT], DstTableID],
        additions: _CompactMap[_RawValueT | DstTableID] | None = None,
        deletions: _CompactMap[_RawValueT | DstTableID] | None = None,
        link_type: _LinkTypeScope = LINK_TYPE_UNSET,
    ) -> Self:
        """
        Build an update from the mixed legacy writer value convention.

        Example:
            Exercise LinkUpdate.from legacy through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param link_spec: Value supplied for link spec under the catalog contract.
        :param replacements: Value supplied for replacements under the catalog contract.
        :param secondary_id_for: Value supplied for secondary id for under the catalog
            contract.
        :param additions: Value supplied for additions under the catalog contract.
        :param deletions: Value supplied for deletions under the catalog contract.
        :param link_type: Optional typed relation value carried by the link.
        :return: The deterministic value, row, identity or collection described above.
        """

        if not callable(secondary_id_for):
            raise TypeError("secondary_id_for must be callable")

        def resolve_legacy(value: _RawValueT | DstTableID) -> DstTableID:
            # This deliberately mirrors the established cache/catalog writer
            # contract, where an int denotes an already-matched database id.
            """
            Resolve legacy under destination-table identity rules.

            Example:
                Exercise LinkUpdate.from legacy.resolve legacy through its owning regression module::

                    python -m pytest -q tests/catalog/test_link_update.py


            :param value: Public or stored value to normalize, compare or write.
            :return: The deterministic value, row, identity or collection described above.
            """
            if isinstance(value, int):
                return cast(DstTableID, value)
            return secondary_id_for(cast(_RawValueT, value))

        return cls._from_compact_maps(
            link_spec,
            replacements=replacements,
            additions=additions,
            deletions=deletions,
            link_type=link_type,
            secondary_id_for=resolve_legacy,
        )

    @classmethod
    def _from_compact_maps(
        cls,
        link_spec: StorageLinkSpec,
        *,
        replacements: _CompactMap[_RawValueT] | None,
        additions: _CompactMap[_RawValueT] | None,
        deletions: _CompactMap[_RawValueT] | None,
        link_type: _LinkTypeScope,
        secondary_id_for: Callable[[_RawValueT], DstTableID],
    ) -> Self:
        """
        Construct a canonical link update from compact maps.

        Example:
            Exercise LinkUpdate.from compact maps through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param link_spec: Value supplied for link spec under the catalog contract.
        :param replacements: Value supplied for replacements under the catalog contract.
        :param additions: Value supplied for additions under the catalog contract.
        :param deletions: Value supplied for deletions under the catalog contract.
        :param link_type: Optional typed relation value carried by the link.
        :param secondary_id_for: Value supplied for secondary id for under the catalog
            contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        if not isinstance(link_spec, StorageLinkSpec):
            raise TypeError("link_spec must be a StorageLinkSpec")

        resolved_ids: dict[tuple[type, Any], DstTableID] = {}

        def resolve_once(value: _RawValueT) -> DstTableID:
            """
            Avoid repeating metadata matching for duplicate legacy values.

            Example:
                Exercise LinkUpdate.from compact maps.resolve once through its owning regression module::

                    python -m pytest -q tests/catalog/test_link_update.py


            :param value: Public or stored value to normalize, compare or write.
            :return: The deterministic value, row, identity or collection described above.
            """

            try:
                key = (type(value), value)
                return resolved_ids[key]
            except TypeError:
                # A caller may deliberately support an unhashable metadata
                # value. It cannot be cached, but it can still be resolved.
                return secondary_id_for(value)
            except KeyError:
                resolved = secondary_id_for(value)
                resolved_ids[key] = resolved
                return resolved

        return cls(
            link_spec=link_spec,
            replacements=cls._normalise_compact_map(
                link_spec,
                replacements,
                link_type=link_type,
                secondary_id_for=resolve_once,
                name="replacements",
            ),
            additions=cls._normalise_compact_map(
                link_spec,
                additions,
                link_type=link_type,
                secondary_id_for=resolve_once,
                name="additions",
            ),
            deletions=cls._normalise_compact_map(
                link_spec,
                deletions,
                link_type=link_type,
                secondary_id_for=resolve_once,
                name="deletions",
            ),
            link_type=link_type,
        )

    @classmethod
    def _normalise_compact_map(
        cls,
        link_spec: StorageLinkSpec,
        supplied: _CompactMap[_RawValueT] | None,
        *,
        link_type: _LinkTypeScope,
        secondary_id_for: Callable[[_RawValueT], DstTableID],
        name: str,
    ) -> dict[SrcTableID, tuple[LinkValue, ...]]:
        """
        Normalize normalise compact map into the canonical link-update representation.

        Example:
            Exercise LinkUpdate.normalise compact map through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param link_spec: Value supplied for link spec under the catalog contract.
        :param supplied: Value supplied for supplied under the catalog contract.
        :param link_type: Optional typed relation value carried by the link.
        :param secondary_id_for: Value supplied for secondary id for under the catalog
            contract.
        :param name: Value supplied for name under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        if supplied is None:
            return {}
        if not isinstance(supplied, Mapping):
            raise TypeError(f"{name} must be a mapping")

        normalised: dict[SrcTableID, tuple[LinkValue, ...]] = {}
        for primary_id, raw_links in supplied.items():
            # Legacy writers discard repeated values while preserving the
            # first occurrence and its order. Do that before strict direct
            # construction checks the final database logical identities.
            normalised[primary_id] = cls._deduplicate_links(
                link_spec,
                cls._normalise_compact_links(
                    link_spec,
                    raw_links,
                    link_type=link_type,
                    secondary_id_for=secondary_id_for,
                ),
            )
        return normalised

    @classmethod
    def _deduplicate_links(
        cls,
        link_spec: StorageLinkSpec,
        links: tuple[LinkValue, ...],
    ) -> tuple[LinkValue, ...]:
        """
        Normalize deduplicate links into the canonical link-update representation.

        Example:
            Exercise LinkUpdate.deduplicate links through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param link_spec: Value supplied for link spec under the catalog contract.
        :param links: Value supplied for links under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        seen: set[tuple[Any, ...]] = set()
        deduplicated: list[LinkValue] = []
        for link in links:
            identity = cls._link_identity(link_spec, link)
            if identity not in seen:
                seen.add(identity)
                deduplicated.append(link)
        return tuple(deduplicated)

    @classmethod
    def _normalise_compact_links(
        cls,
        link_spec: StorageLinkSpec,
        raw_links: _CompactLinks[_RawValueT],
        *,
        link_type: _LinkTypeScope,
        secondary_id_for: Callable[[_RawValueT], DstTableID],
    ) -> tuple[LinkValue, ...]:
        """
        Normalize normalise compact links into the canonical link-update representation.

        Example:
            Exercise LinkUpdate.normalise compact links through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param link_spec: Value supplied for link spec under the catalog contract.
        :param raw_links: Value supplied for raw links under the catalog contract.
        :param link_type: Optional typed relation value carried by the link.
        :param secondary_id_for: Value supplied for secondary id for under the catalog
            contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        if raw_links is None:
            return ()

        if isinstance(raw_links, Mapping):
            if not link_spec.typed:
                raise TypeError("nested link-type mappings require a typed link spec")
            typed_links = cast(
                Mapping[_LinkType, _CompactLinks[_RawValueT]],
                raw_links,
            )
            return tuple(
                link
                for nested_type, nested_links in typed_links.items()
                for link in cls._normalise_compact_links(
                    link_spec,
                    nested_links,
                    link_type=nested_type,
                    secondary_id_for=secondary_id_for,
                )
            )

        raw_values: tuple[_RawValueT | LinkValue, ...]
        if isinstance(raw_links, LinkValue):
            raw_values = (raw_links,)
        elif isinstance(raw_links, Iterable) and not isinstance(
            raw_links,
            (str, bytes, bytearray),
        ):
            raw_values = tuple(
                cast(Iterable[_RawValueT | LinkValue], raw_links)
            )
        else:
            raw_values = (cast(_RawValueT, raw_links),)

        return tuple(
            cls._link_value_from_compact_value(
                raw_value,
                link_type=link_type,
                secondary_id_for=secondary_id_for,
            )
            for raw_value in raw_values
        )

    @staticmethod
    def _link_value_from_compact_value(
        value: _RawValueT | LinkValue,
        *,
        link_type: _LinkTypeScope,
        secondary_id_for: Callable[[_RawValueT], DstTableID],
    ) -> LinkValue:
        """
        Perform the catalog link value from compact value operation under explicit validation and ordering rules.

        Example:
            Exercise LinkUpdate.link value from compact value through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param value: Public or stored value to normalize, compare or write.
        :param link_type: Optional typed relation value carried by the link.
        :param secondary_id_for: Value supplied for secondary id for under the catalog
            contract.
        :return: The deterministic value, row, identity or collection described above.
        """
        assigned_type = None if link_type is LINK_TYPE_UNSET else link_type

        if isinstance(value, LinkValue):
            supplied_type = cast(_LinkType | None, value.link_type)
            if assigned_type is None or supplied_type is not None:
                return value
            return replace(value, link_type=assigned_type)

        if value is None:
            raise TypeError(
                "None is only valid as the complete value for a primary id or link type"
            )

        return LinkValue(
            secondary_id=secondary_id_for(cast(_RawValueT, value)),
            link_type=assigned_type,
        )

    @staticmethod
    def _link_identity(
        link_spec: StorageLinkSpec,
        link: LinkValue | LinkRow,
    ) -> tuple[Any, ...]:
        """
        Return the same logical identity used by portable DB macros.

        Example:
            Exercise LinkUpdate.link identity through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param link_spec: Value supplied for link spec under the catalog contract.
        :param link: Value supplied for link under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        identity = [link.secondary_id]
        if link_spec.type_part_of_identity:
            identity.append(link.link_type)
        return tuple(identity)

    @staticmethod
    def _value_from_row(row: LinkRow) -> LinkValue:
        """
        Preserve every writable property while composing incrementals.

        Example:
            Exercise LinkUpdate.value from row through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param row: Value supplied for row under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return LinkValue(
            secondary_id=row.secondary_id,
            link_type=row.link_type,
            priority=row.priority,
            extra=row.extra,
        )

    @staticmethod
    def _merge_upsert(base: LinkValue, addition: LinkValue) -> LinkValue:
        """
        Compose an addition with the properties of an existing link.

        Example:
            Exercise LinkUpdate.merge upsert through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param base: Value supplied for base under the catalog contract.
        :param addition: Value supplied for addition under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        return LinkValue(
            secondary_id=addition.secondary_id,
            link_type=addition.link_type,
            priority=(
                base.priority if addition.priority is None else addition.priority
            ),
            extra={**base.extra, **addition.extra},
        )

    def as_replacement_update(self, macros: PortableMacrosAPI) -> Self:
        """
        Resolve incremental operations into authoritative replacements.

        Example:
            Exercise LinkUpdate.as replacement update through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param macros: Value supplied for macros under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        touched_ids = self.primary_ids
        read_ids = tuple(
            primary_id
            for primary_id in touched_ids
            if primary_id not in self.replacements
        )
        current_rows = (
            macros.get_link_rows_bulk(
                self.link_spec,
                read_ids,
                link_type=self.link_type,
            )
            if read_ids
            else {}
        )

        replacements: dict[SrcTableID, tuple[LinkValue, ...]] = {}
        for primary_id in touched_ids:
            base_links = self.replacements.get(primary_id)
            if base_links is None:
                base_links = tuple(
                    self._value_from_row(row)
                    for row in current_rows.get(primary_id, ())
                )

            order: list[tuple[Any, ...]] = []
            links_by_identity: dict[tuple[Any, ...], LinkValue] = {}
            for link in base_links:
                identity = self._link_identity(self.link_spec, link)
                if identity not in links_by_identity:
                    order.append(identity)
                links_by_identity[identity] = link

            for link in self.deletions.get(primary_id, ()):
                identity = self._link_identity(self.link_spec, link)
                links_by_identity.pop(identity, None)

            for link in self.additions.get(primary_id, ()):
                identity = self._link_identity(self.link_spec, link)
                existing = links_by_identity.get(identity)
                if existing is None:
                    if identity not in order:
                        order.append(identity)
                    links_by_identity[identity] = link
                else:
                    links_by_identity[identity] = self._merge_upsert(existing, link)

            replacements[primary_id] = tuple(
                links_by_identity[identity]
                for identity in order
                if identity in links_by_identity
            )

        return type(self)(
            link_spec=self.link_spec,
            replacements=replacements,
            link_type=self.link_type,
        )

    def write(
        self,
        macros: PortableMacrosAPI,
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """
        Apply this update through the portable database macro surface.

        Example:
            Exercise LinkUpdate.write through its owning regression module::

                python -m pytest -q tests/catalog/test_link_update.py


        :param macros: Value supplied for macros under the catalog contract.
        :return: The deterministic value, row, identity or collection described above.
        """

        replacement_update = self.as_replacement_update(macros)
        if not replacement_update.replacements:
            return MappingProxyType({})
        return cast(
            Mapping[SrcTableID, tuple[LinkRow, ...]],
            macros.replace_links_bulk(
                self.link_spec,
                replacement_update.replacements,
                link_type=self.link_type,
            ),
        )


__all__ = ["LinkUpdate", "LinkUpdateEntry", "LinkUpdateLink"]
