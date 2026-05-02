"""Language metadata containers attached to W/E/M/I entities.

Category: additional metadata family.
These classes are editable metadata value objects and helper containers, not
independent identity objects and not joined read-side views.
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import Iterator, Literal, Generic, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import LanguageKind
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    MetadataSequenceStringMixin,
    MetadataValueStringMixin,
)
from LiuXin_alpha.metadata.metadata_types import WorkID, ExpressionID, ManifestationID, ItemID, LanguageID


LanguageT = TypeVar("LanguageT", bound="LanguageBase")
KindContainerT = TypeVar("KindContainerT", bound="KindLanguagesContainer")


@dataclass(slots=True, kw_only=True)
class LanguageBase(MetadataValueStringMixin, abc.ABC):
    """Shared relation data for one language record attached to a bibliographic entity."""

    language_kind: LanguageKind
    language_id: LanguageID | None = None
    language_code: str | None = None
    language_name: str | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    notes: str | None = None
    STRING_DISPLAY_KEYS = ("language_name", "language_code", "display_text")

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the W/E/M/I entity this language attaches to."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """work / expression / manifestation / item."""

    @property
    def display_text(self) -> str:
        if self.language_name:
            return self.language_name
        if self.language_code:
            return self.language_code
        return f"language:{self.language_id}"

    def validate(self) -> None:
        if self.language_id is None and not (self.language_code or self.language_name):
            raise ValueError("language record must provide at least one of language_id, language_code, or language_name")
        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

    def _common_write_payload(self) -> dict[str, object]:
        return {
            "language_kind": self.language_kind,
            "language_id": self.language_id,
            "language_code": self.language_code,
            "language_name": self.language_name,
            "position": self.position,
            "is_primary": self.is_primary,
            "source": self.source,
            "notes": self.notes,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """Serialise to a write-layer payload."""


@dataclass(slots=True, kw_only=True)
class WorkLanguage(LanguageBase):
    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update({"work_id": self.work_id, "canonical_for_work": self.canonical_for_work})
        return payload


@dataclass(slots=True, kw_only=True)
class ExpressionLanguage(LanguageBase):
    expression_id: ExpressionID
    applies_to_language_id: LanguageID | None = None

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update({"expression_id": self.expression_id, "applies_to_language_id": self.applies_to_language_id})
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationLanguage(LanguageBase):
    manifestation_id: ManifestationID
    edition_specific: bool = True

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update({"manifestation_id": self.manifestation_id, "edition_specific": self.edition_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class ItemLanguage(LanguageBase):
    item_id: ItemID
    copy_specific: bool = True

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        payload = self._common_write_payload()
        payload.update({"item_id": self.item_id, "copy_specific": self.copy_specific})
        return payload


@dataclass(slots=True, kw_only=True)
class KindLanguagesContainer(MetadataSequenceStringMixin, Generic[LanguageT], abc.ABC):
    """Ordered editable container for all languages of one kind on one target entity."""

    language_kind: LanguageKind
    target_id: int
    _languages: list[LanguageT] = field(default_factory=list)

    target_kind: Literal["work", "expression", "manifestation", "item"]
    STRING_COUNT_LABEL = "languages"

    def __iter__(self) -> Iterator[LanguageT]:
        return iter(self._languages)

    def __len__(self) -> int:
        return len(self._languages)

    def __getitem__(self, index: int) -> LanguageT:
        return self._languages[index]

    def languages(self) -> tuple[LanguageT, ...]:
        return tuple(self._languages)

    def texts(self) -> tuple[str, ...]:
        return tuple(language.display_text for language in self._languages)

    def to_text(self, sep: str = ", ") -> str:
        return sep.join(self.texts())

    def add_language(self, language: LanguageT) -> None:
        self._validate_language_shape(language)
        self._languages.append(language)
        self.normalize_positions()

    def replace_language(self, index: int, language: LanguageT) -> None:
        self._validate_language_shape(language)
        self._languages[index] = language
        self.normalize_positions()

    def remove_language_at(self, index: int) -> LanguageT:
        removed = self._languages.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        self._languages.clear()

    def move_language(self, old_index: int, new_index: int) -> None:
        language = self._languages.pop(old_index)
        self._languages.insert(new_index, language)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        for i, language in enumerate(self._languages):
            language.is_primary = (i == index)

    def normalize_positions(self) -> None:
        for index, language in enumerate(self._languages):
            language.position = index

    def validate(self) -> None:
        primary_count = 0
        for expected_index, language in enumerate(self._languages):
            self._validate_language_shape(language)
            language.validate()
            if language.position != expected_index:
                raise ValueError(f"Language position mismatch for {self.target_kind} {self.target_id}: expected {expected_index}, got {language.position}")
            if language.is_primary:
                primary_count += 1
        if primary_count > 1:
            raise ValueError(f"Only one primary language is allowed for {self.target_kind} {self.target_id} kind {self.language_kind}")

    def as_write_payload(self) -> list[dict[str, object]]:
        return [language.as_write_payload() for language in self._languages]

    def _validate_language_shape(self, language: LanguageT) -> None:
        if language.target_kind != self.target_kind:
            raise ValueError(f"Cannot add {language.target_kind} language to {self.target_kind} container")
        if language.target_id != self.target_id:
            raise ValueError(f"Language target_id {language.target_id} does not match container target_id {self.target_id}")
        if language.language_kind != self.language_kind:
            raise ValueError(f"Language kind {language.language_kind} does not match container kind {self.language_kind}")


@dataclass(slots=True, kw_only=True)
class WorkKindLanguagesContainer(KindLanguagesContainer[WorkLanguage]):
    target_kind: Literal["work"] = "work"


@dataclass(slots=True, kw_only=True)
class ExpressionKindLanguagesContainer(KindLanguagesContainer[ExpressionLanguage]):
    target_kind: Literal["expression"] = "expression"


@dataclass(slots=True, kw_only=True)
class ManifestationKindLanguagesContainer(KindLanguagesContainer[ManifestationLanguage]):
    target_kind: Literal["manifestation"] = "manifestation"


@dataclass(slots=True, kw_only=True)
class ItemKindLanguagesContainer(KindLanguagesContainer[ItemLanguage]):
    target_kind: Literal["item"] = "item"


@dataclass(slots=True, kw_only=True)
class BaseTargetLanguagesContainer(
    MetadataSequenceStringMixin,
    Generic[LanguageT, KindContainerT],
    abc.ABC,
):
    """Top-level editable language container for one target entity."""

    _by_kind: dict[LanguageKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL = "languages"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the target object."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """work / expression / manifestation / item."""

    @abc.abstractmethod
    def _make_kind_container(self, language_kind: LanguageKind) -> KindContainerT:
        """Build the correct per-kind container for this target type."""

    def kinds(self) -> tuple[LanguageKind, ...]:
        return tuple(self._by_kind.keys())

    def has_kind(self, language_kind: LanguageKind) -> bool:
        return language_kind in self._by_kind

    def get_kind(self, language_kind: LanguageKind) -> KindContainerT | None:
        return self._by_kind.get(language_kind)

    def ensure_kind(self, language_kind: LanguageKind) -> KindContainerT:
        container = self._by_kind.get(language_kind)
        if container is None:
            container = self._make_kind_container(language_kind)
            self._by_kind[language_kind] = container
        return container

    def add_language(self, language: LanguageT) -> None:
        if language.target_id != self.target_id:
            raise ValueError(f"Language target_id {language.target_id} does not match {self.target_kind} target_id {self.target_id}")
        self.ensure_kind(language.language_kind).add_language(language)

    def iter_all_languages(self) -> Iterator[LanguageT]:
        for container in self._by_kind.values():
            yield from container

    def kind_text(self, language_kind: LanguageKind, sep: str = ", ") -> str:
        container = self.get_kind(language_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def validate(self) -> None:
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkLanguagesContainer(BaseTargetLanguagesContainer[WorkLanguage, WorkKindLanguagesContainer]):
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        return "work"

    def _make_kind_container(self, language_kind: LanguageKind) -> WorkKindLanguagesContainer:
        return WorkKindLanguagesContainer(language_kind=language_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionLanguagesContainer(BaseTargetLanguagesContainer[ExpressionLanguage, ExpressionKindLanguagesContainer]):
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        return "expression"

    def _make_kind_container(self, language_kind: LanguageKind) -> ExpressionKindLanguagesContainer:
        return ExpressionKindLanguagesContainer(language_kind=language_kind, target_id=self.expression_id)


@dataclass(slots=True, kw_only=True)
class ManifestationLanguagesContainer(BaseTargetLanguagesContainer[ManifestationLanguage, ManifestationKindLanguagesContainer]):
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        return "manifestation"

    def _make_kind_container(self, language_kind: LanguageKind) -> ManifestationKindLanguagesContainer:
        return ManifestationKindLanguagesContainer(language_kind=language_kind, target_id=self.manifestation_id)


@dataclass(slots=True, kw_only=True)
class ItemLanguagesContainer(BaseTargetLanguagesContainer[ItemLanguage, ItemKindLanguagesContainer]):
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        return "item"

    def _make_kind_container(self, language_kind: LanguageKind) -> ItemKindLanguagesContainer:
        return ItemKindLanguagesContainer(language_kind=language_kind, target_id=self.item_id)


def _kind_property_stem(language_kind: LanguageKind) -> str:
    stems = {
        LanguageKind.CONTENT: "content_languages",
        LanguageKind.ORIGINAL: "original_languages",
        LanguageKind.SOURCE: "source_languages",
        LanguageKind.TARGET: "target_languages",
        LanguageKind.SUBTITLE: "subtitle_languages",
        LanguageKind.SUMMARY: "summary_languages",
        LanguageKind.INTERFACE: "interface_languages",
    }
    return stems[language_kind]


def _install_kind_convenience_properties(cls: type[BaseTargetLanguagesContainer]) -> None:
    """Install per-kind convenience properties and methods on a languages container class.

    This is deliberate runtime sugar, not the load-bearing core API. The
    explicit generic methods on the container remain the canonical surface. See
    `metadata_container_dynamic_convenience_policy.md`.
    """
    for language_kind in LanguageKind:
        stem = _kind_property_stem(language_kind)

        def kind_container_getter(self, _kind=language_kind):
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=language_kind):
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = ", ", _kind=language_kind) -> str:
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkLanguagesContainer)
_install_kind_convenience_properties(ExpressionLanguagesContainer)
_install_kind_convenience_properties(ManifestationLanguagesContainer)
_install_kind_convenience_properties(ItemLanguagesContainer)


__all__ = [
    "LanguageKind",
    "LanguageBase",
    "WorkLanguage",
    "ExpressionLanguage",
    "ManifestationLanguage",
    "ItemLanguage",
    "KindLanguagesContainer",
    "WorkKindLanguagesContainer",
    "ExpressionKindLanguagesContainer",
    "ManifestationKindLanguagesContainer",
    "ItemKindLanguagesContainer",
    "BaseTargetLanguagesContainer",
    "WorkLanguagesContainer",
    "ExpressionLanguagesContainer",
    "ManifestationLanguagesContainer",
    "ItemLanguagesContainer",
]
