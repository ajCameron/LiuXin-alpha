"""
Containers for identifiers attached to W/E/M/I entities.

These are metadata value objects and editing containers.
They are not row or database proxies.
"""

from __future__ import annotations

import abc

from dataclasses import dataclass, field
from enum import StrEnum
from typing import ClassVar, Generic, Iterator, TypeVar, Literal

from LiuXin_alpha.databases.db_types import (
    IdentifierScheme,
    WORK_IDENTIFIER_SCHEMES,
    EXPRESSION_IDENTIFIER_SCHEMES,
    MANIFESTATION_IDENTIFIER_SCHEMES,
    ITEM_IDENTIFIER_SCHEMES,
)
from LiuXin_alpha.metadata.metadata_types import (
    WorkID,
    ExpressionID,
    ManifestationID,
    ItemID,
    LanguageID,
)


IdentifierT = TypeVar("IdentifierT", bound="IdentifierBase")
SchemeContainerT = TypeVar("SchemeContainerT", bound="SchemeIdentifiersContainer")


class IdentifierStatus(StrEnum):
    """Lifecycle / trust state for an identifier."""

    ACTIVE = "active"
    INVALID = "invalid"
    SUPERSEDED = "superseded"
    WITHDRAWN = "withdrawn"
    UNKNOWN = "unknown"


@dataclass(slots=True, kw_only=True)
class IdentifierBase(abc.ABC):
    """
    Shared relation data for an identifier attached to a bibliographic entity.

    This models the identifier-link, not a database row proxy.
    """

    scheme: IdentifierScheme
    value: str
    normalized_value: str | None = None

    qualifier: str | None = None
    assigning_body: str | None = None

    position: int | None = None
    is_primary: bool = False

    status: IdentifierStatus = IdentifierStatus.ACTIVE
    is_validated: bool = False
    source: str = "user_set"
    notes: str | None = None

    association_start_ep_k: int | None = None
    association_end_ep_k: int | None = None

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        ID of the W/E/M/I entity this identifier attaches to.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        work / expression / manifestation / item.
        """

    @property
    def scheme_key(self) -> "IdentifierScheme":
        """
        The type of identifier this class represents.

        :return:
        """
        return self.scheme

    def validate(self) -> None:
        """
        Validate that the identifiers are valid.

        :return:
        """
        if not str(self.scheme).strip():
            raise ValueError("scheme cannot be blank")

        if not self.value.strip():
            raise ValueError("value cannot be blank")

        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

        if (
            self.association_start_ep_k is not None
            and self.association_end_ep_k is not None
            and self.association_end_ep_k < self.association_start_ep_k
        ):
            raise ValueError(
                "association_end_ep_k cannot be earlier than association_start_ep_k"
            )

    def _common_write_payload(self) -> dict[str, object]:
        return {
            "scheme": self.scheme,
            "value": self.value,
            "normalized_value": self.normalized_value,
            "qualifier": self.qualifier,
            "assigning_body": self.assigning_body,
            "position": self.position,
            "is_primary": self.is_primary,
            "status": self.status,
            "is_validated": self.is_validated,
            "source": self.source,
            "notes": self.notes,
            "association_start_ep_k": self.association_start_ep_k,
            "association_end_ep_k": self.association_end_ep_k,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """
        Serialise to a write-layer payload.
        """


@dataclass(slots=True, kw_only=True)
class WorkIdentifier(IdentifierBase):
    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> str:
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update(
            {
                "work_id": self.work_id,
                "canonical_for_work": self.canonical_for_work,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ExpressionIdentifier(IdentifierBase):
    expression_id: ExpressionID
    language_id: LanguageID | None = None

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> str:
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update(
            {
                "expression_id": self.expression_id,
                "language_id": self.language_id,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationIdentifier(IdentifierBase):
    manifestation_id: ManifestationID
    edition_note: str | None = None

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> str:
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update(
            {
                "manifestation_id": self.manifestation_id,
                "edition_note": self.edition_note,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ItemIdentifier(IdentifierBase):
    item_id: ItemID
    copy_specific: bool = True
    physical_marking: str | None = None

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> str:
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update(
            {
                "item_id": self.item_id,
                "copy_specific": self.copy_specific,
                "physical_marking": self.physical_marking,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class SchemeIdentifiersContainer(Generic[IdentifierT], abc.ABC):
    """
    Ordered editable container for all identifiers of one scheme on one target entity.
    """

    scheme: IdentifierScheme
    target_id: int
    _identifiers: list[IdentifierT] = field(default_factory=list)

    target_kind: ClassVar[str]

    def __iter__(self) -> Iterator[IdentifierT]:
        return iter(self._identifiers)

    def __len__(self) -> int:
        return len(self._identifiers)

    def __getitem__(self, index: int) -> IdentifierT:
        return self._identifiers[index]

    def identifiers(self) -> tuple[IdentifierT, ...]:
        return tuple(self._identifiers)

    def values(self) -> tuple[str, ...]:
        return tuple(identifier.value for identifier in self._identifiers)

    def normalized_values(self) -> tuple[str, ...]:
        return tuple(
            identifier.normalized_value or identifier.value
            for identifier in self._identifiers
        )

    def to_display_string(self, sep: str = " / ") -> str:
        return sep.join(self.values())

    def add_identifier(self, identifier: IdentifierT) -> None:
        self._validate_identifier_shape(identifier)
        self._identifiers.append(identifier)
        self.normalize_positions()

    def replace_identifier(self, index: int, identifier: IdentifierT) -> None:
        self._validate_identifier_shape(identifier)
        self._identifiers[index] = identifier
        self.normalize_positions()

    def remove_identifier_at(self, index: int) -> IdentifierT:
        removed = self._identifiers.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        self._identifiers.clear()

    def move_identifier(self, old_index: int, new_index: int) -> None:
        identifier = self._identifiers.pop(old_index)
        self._identifiers.insert(new_index, identifier)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        for i, identifier in enumerate(self._identifiers):
            identifier.is_primary = (i == index)

    def normalize_positions(self) -> None:
        for index, identifier in enumerate(self._identifiers):
            identifier.position = index

    def primary_identifier(self) -> IdentifierT | None:
        for identifier in self._identifiers:
            if identifier.is_primary:
                return identifier
        return self._identifiers[0] if self._identifiers else None

    def validate(self) -> None:
        primary_count = 0

        for expected_index, identifier in enumerate(self._identifiers):
            self._validate_identifier_shape(identifier)
            identifier.validate()

            if identifier.position != expected_index:
                raise ValueError(
                    f"Identifier position mismatch for {self.target_kind} "
                    f"{self.target_id}: expected {expected_index}, got {identifier.position}"
                )

            if identifier.is_primary:
                primary_count += 1

        if primary_count > 1:
            raise ValueError(
                f"Only one primary identifier is allowed for "
                f"{self.target_kind} {self.target_id} scheme {self.scheme}"
            )

    def as_write_payload(self) -> list[dict[str, object]]:
        return [identifier.as_write_payload() for identifier in self._identifiers]

    def _validate_identifier_shape(self, identifier: IdentifierT) -> None:
        if identifier.target_kind != self.target_kind:
            raise ValueError(
                f"Cannot add {identifier.target_kind} identifier to "
                f"{self.target_kind} container"
            )

        if identifier.target_id != self.target_id:
            raise ValueError(
                f"Identifier target_id {identifier.target_id} does not match "
                f"container target_id {self.target_id}"
            )

        if identifier.scheme_key != self.scheme:
            raise ValueError(
                f"Identifier scheme {identifier.scheme_key} does not match "
                f"container scheme {self.scheme}"
            )


@dataclass(slots=True, kw_only=True)
class WorkSchemeIdentifiersContainer(SchemeIdentifiersContainer[WorkIdentifier]):
    target_kind: ClassVar[str] = "work"

    @property
    def work_id(self) -> WorkID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ExpressionSchemeIdentifiersContainer(
    SchemeIdentifiersContainer[ExpressionIdentifier]
):
    target_kind: ClassVar[str] = "expression"

    @property
    def expression_id(self) -> ExpressionID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ManifestationSchemeIdentifiersContainer(
    SchemeIdentifiersContainer[ManifestationIdentifier]
):
    target_kind: ClassVar[str] = "manifestation"

    @property
    def manifestation_id(self) -> ManifestationID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ItemSchemeIdentifiersContainer(SchemeIdentifiersContainer[ItemIdentifier]):
    target_kind: ClassVar[str] = "item"

    @property
    def item_id(self) -> ItemID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class BaseTargetIdentifiersContainer(
    Generic[IdentifierT, SchemeContainerT],
    abc.ABC,
):
    """
    Top-level editable identifier container for one target entity.
    Holds one SchemeIdentifiersContainer per scheme.
    """

    _by_scheme: dict[IdentifierScheme, SchemeContainerT] = field(default_factory=dict)

    ALLOWED_SCHEMES: ClassVar[frozenset[IdentifierScheme]] = frozenset()

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the target object."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> str:
        """work / expression / manifestation / item"""

    @abc.abstractmethod
    def _make_scheme_container(self, scheme: IdentifierScheme) -> SchemeContainerT:
        """Build the correct per-scheme container for this target type."""

    def _validate_scheme_allowed(self, scheme: IdentifierScheme) -> None:
        if scheme not in self.ALLOWED_SCHEMES:
            raise ValueError(
                f"Identifier scheme {scheme} is not allowed for {self.target_kind}"
            )

    def schemes(self) -> tuple[IdentifierScheme, ...]:
        return tuple(self._by_scheme.keys())

    def has_scheme(self, scheme: IdentifierScheme) -> bool:
        return scheme in self._by_scheme

    def get_scheme(self, scheme: IdentifierScheme) -> SchemeContainerT | None:
        return self._by_scheme.get(scheme)

    def ensure_scheme(self, scheme: IdentifierScheme) -> SchemeContainerT:
        self._validate_scheme_allowed(scheme)
        container = self._by_scheme.get(scheme)
        if container is None:
            container = self._make_scheme_container(scheme)
            self._by_scheme[scheme] = container
        return container

    def add_identifier(self, identifier: IdentifierT) -> None:
        if identifier.target_id != self.target_id:
            raise ValueError(
                f"Identifier target_id {identifier.target_id} does not match "
                f"{self.target_kind} target_id {self.target_id}"
            )

        self.ensure_scheme(identifier.scheme_key).add_identifier(identifier)

    def iter_all_identifiers(self) -> Iterator[IdentifierT]:
        for container in self._by_scheme.values():
            yield from container

    def all_values(self) -> tuple[str, ...]:
        return tuple(identifier.value for identifier in self.iter_all_identifiers())

    def scheme_values(self, scheme: IdentifierScheme) -> tuple[str, ...]:
        container = self.get_scheme(scheme)
        if container is None:
            return tuple()
        return container.values()

    def scheme_normalized_values(self, scheme: IdentifierScheme) -> tuple[str, ...]:
        container = self.get_scheme(scheme)
        if container is None:
            return tuple()
        return container.normalized_values()

    def scheme_to_display_string(
        self,
        scheme: IdentifierScheme,
        sep: str = " / ",
    ) -> str:
        container = self.get_scheme(scheme)
        if container is None:
            return ""
        return container.to_display_string(sep=sep)

    def primary_identifier_for_scheme(
        self,
        scheme: IdentifierScheme,
    ) -> IdentifierT | None:
        container = self.get_scheme(scheme)
        if container is None:
            return None
        return container.primary_identifier()

    def primary_identifiers(self) -> dict[IdentifierScheme, IdentifierT]:
        result: dict[IdentifierScheme, IdentifierT] = {}
        for scheme, container in self._by_scheme.items():
            primary = container.primary_identifier()
            if primary is not None:
                result[scheme] = primary
        return result

    def validate(self) -> None:
        for scheme, container in self._by_scheme.items():
            self._validate_scheme_allowed(scheme)
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        payload: list[dict[str, object]] = []
        for container in self._by_scheme.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkIdentifiersContainer(
    BaseTargetIdentifiersContainer[
        WorkIdentifier,
        WorkSchemeIdentifiersContainer,
    ]
):
    work_id: WorkID
    ALLOWED_SCHEMES: ClassVar[frozenset[IdentifierScheme]] = WORK_IDENTIFIER_SCHEMES

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> str:
        return "work"

    def _make_scheme_container(self, scheme: IdentifierScheme) -> WorkSchemeIdentifiersContainer:
        return WorkSchemeIdentifiersContainer(scheme=scheme, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionIdentifiersContainer(
    BaseTargetIdentifiersContainer[
        ExpressionIdentifier,
        ExpressionSchemeIdentifiersContainer,
    ]
):
    expression_id: ExpressionID
    ALLOWED_SCHEMES: ClassVar[frozenset[IdentifierScheme]] = EXPRESSION_IDENTIFIER_SCHEMES

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> str:
        return "expression"

    def _make_scheme_container(
        self,
        scheme: IdentifierScheme,
    ) -> ExpressionSchemeIdentifiersContainer:
        return ExpressionSchemeIdentifiersContainer(
            scheme=scheme,
            target_id=self.expression_id,
        )


@dataclass(slots=True, kw_only=True)
class ManifestationIdentifiersContainer(
    BaseTargetIdentifiersContainer[
        ManifestationIdentifier,
        ManifestationSchemeIdentifiersContainer,
    ]
):
    manifestation_id: ManifestationID
    ALLOWED_SCHEMES: ClassVar[frozenset[IdentifierScheme]] = MANIFESTATION_IDENTIFIER_SCHEMES

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> str:
        return "manifestation"

    def _make_scheme_container(
        self,
        scheme: IdentifierScheme,
    ) -> ManifestationSchemeIdentifiersContainer:
        return ManifestationSchemeIdentifiersContainer(
            scheme=scheme,
            target_id=self.manifestation_id,
        )


@dataclass(slots=True, kw_only=True)
class ItemIdentifiersContainer(
    BaseTargetIdentifiersContainer[
        ItemIdentifier,
        ItemSchemeIdentifiersContainer,
    ]
):
    item_id: ItemID
    ALLOWED_SCHEMES: ClassVar[frozenset[IdentifierScheme]] = ITEM_IDENTIFIER_SCHEMES

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> str:
        return "item"

    def _make_scheme_container(self, scheme: IdentifierScheme) -> ItemSchemeIdentifiersContainer:
        return ItemSchemeIdentifiersContainer(scheme=scheme, target_id=self.item_id)


# ---------------------------------------------------------------------------
# Identifier convenience layer
# ---------------------------------------------------------------------------


def _install_scheme_convenience_properties(
    cls: type[BaseTargetIdentifiersContainer],
    schemes: frozenset[IdentifierScheme],
) -> None:
    """
    Install per-scheme convenience properties and methods on a container class.

    For a scheme stem of 'isbn_13', this creates:
    - .isbn_13               -> SchemeIdentifiersContainer
    - .isbn_13_values        -> tuple[str, ...]
    - .isbn_13_normalized_values -> tuple[str, ...]
    - .isbn_13_str           -> str   (default " / " separator)
    - .isbn_13_to_string(sep=" / ") -> str
    - .isbn_13_primary       -> IdentifierBase | None
    """
    for scheme in sorted(schemes, key=lambda s: s.value):
        stem = scheme.value

        def scheme_container_getter(self, _scheme=scheme):
            return self.ensure_scheme(_scheme)

        def scheme_values_getter(self, _scheme=scheme):
            return self.scheme_values(_scheme)

        def scheme_normalized_values_getter(self, _scheme=scheme):
            return self.scheme_normalized_values(_scheme)

        def scheme_string_getter(self, _scheme=scheme):
            return self.scheme_to_display_string(_scheme)

        def scheme_string_method(self, sep: str = " / ", _scheme=scheme) -> str:
            return self.scheme_to_display_string(_scheme, sep=sep)

        def scheme_primary_getter(self, _scheme=scheme):
            return self.primary_identifier_for_scheme(_scheme)

        setattr(cls, stem, property(scheme_container_getter))
        setattr(cls, f"{stem}_values", property(scheme_values_getter))
        setattr(cls, f"{stem}_normalized_values", property(scheme_normalized_values_getter))
        setattr(cls, f"{stem}_str", property(scheme_string_getter))
        setattr(cls, f"{stem}_to_string", scheme_string_method)
        setattr(cls, f"{stem}_primary", property(scheme_primary_getter))


_install_scheme_convenience_properties(WorkIdentifiersContainer, WORK_IDENTIFIER_SCHEMES)
_install_scheme_convenience_properties(ExpressionIdentifiersContainer, EXPRESSION_IDENTIFIER_SCHEMES)
_install_scheme_convenience_properties(
    ManifestationIdentifiersContainer,
    MANIFESTATION_IDENTIFIER_SCHEMES,
)
_install_scheme_convenience_properties(ItemIdentifiersContainer, ITEM_IDENTIFIER_SCHEMES)
