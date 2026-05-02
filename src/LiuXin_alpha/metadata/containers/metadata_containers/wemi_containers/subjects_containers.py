"""Subject metadata containers attached to W/E/M/I entities.

Category: additional metadata family.
These classes are editable metadata value objects and helper containers, not
independent identity objects and not joined read-side views.
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import ClassVar, Generic, Iterator, Literal, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import SubjectKind
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    MetadataSequenceStringMixin,
    MetadataValueStringMixin,
)
from LiuXin_alpha.metadata.metadata_types import (
    WorkID,
    ExpressionID,
    ManifestationID,
    ItemID,
    LanguageID,
)


SubjectT = TypeVar("SubjectT", bound="SubjectBase")
KindContainerT = TypeVar("KindContainerT", bound="KindSubjectsContainer")



@dataclass(slots=True, kw_only=True)
class SubjectBase(MetadataValueStringMixin, abc.ABC):
    """
    Shared relation data for a subject attached to a bibliographic entity.

    Subjects are intentionally narrower than general labels. They are for
    descriptive access points such as topics, places, periods, and named
    characters rather than for every possible short-form tag-like metadata
    value.
    """

    subject_kind: SubjectKind
    text: str
    normalized_text: str | None = None
    sort_text: str | None = None
    language_id: LanguageID | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    notes: str | None = None
    STRING_DISPLAY_KEYS = ("text", "subject_kind", "source")

    authority_record_id: int | None = None
    external_key: str | None = None

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the W/E/M/I entity this subject attaches to."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """work / expression / manifestation / item."""

    @property
    def kind_key(self) -> SubjectKind:
        """The subject kind this subject is grouped by."""
        return self.subject_kind

    def validate(self) -> None:
        """Validate that the subject is internally consistent."""
        if not str(self.subject_kind).strip():
            raise ValueError("subject_kind cannot be blank")

        if not self.text.strip():
            raise ValueError("text cannot be blank")

        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

    def _common_write_payload(self) -> dict[str, object]:
        return {
            "subject_kind": self.subject_kind,
            "text": self.text,
            "normalized_text": self.normalized_text,
            "sort_text": self.sort_text,
            "language_id": self.language_id,
            "position": self.position,
            "is_primary": self.is_primary,
            "source": self.source,
            "notes": self.notes,
            "authority_record_id": self.authority_record_id,
            "external_key": self.external_key,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """Serialise to a write-layer payload."""


@dataclass(slots=True, kw_only=True)
class WorkSubject(SubjectBase):
    """Subject attached directly to a work."""

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
        payload.update(
            {
                "work_id": self.work_id,
                "canonical_for_work": self.canonical_for_work,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ExpressionSubject(SubjectBase):
    """Subject attached directly to an expression."""

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
        payload.update(
            {
                "expression_id": self.expression_id,
                "applies_to_language_id": self.applies_to_language_id,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationSubject(SubjectBase):
    """Subject attached directly to a manifestation."""

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
        payload.update(
            {
                "manifestation_id": self.manifestation_id,
                "edition_specific": self.edition_specific,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ItemSubject(SubjectBase):
    """Subject attached directly to an individual item / copy."""

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
        payload.update(
            {
                "item_id": self.item_id,
                "copy_specific": self.copy_specific,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class KindSubjectsContainer(MetadataSequenceStringMixin, Generic[SubjectT], abc.ABC):
    """Ordered editable container for all subjects of one kind on one target entity."""

    subject_kind: SubjectKind
    target_id: int
    _subjects: list[SubjectT] = field(default_factory=list)

    target_kind: ClassVar[str]
    STRING_COUNT_LABEL = "subjects"

    def __iter__(self) -> Iterator[SubjectT]:
        return iter(self._subjects)

    def __len__(self) -> int:
        return len(self._subjects)

    def __getitem__(self, index: int) -> SubjectT:
        return self._subjects[index]

    def subjects(self) -> tuple[SubjectT, ...]:
        return tuple(self._subjects)

    def texts(self) -> tuple[str, ...]:
        return tuple(subject.text for subject in self._subjects)

    def sort_texts(self) -> tuple[str, ...]:
        return tuple(subject.sort_text or subject.text for subject in self._subjects)

    def to_text(self, sep: str = ", ") -> str:
        return sep.join(self.texts())

    def add_subject(self, subject: SubjectT) -> None:
        self._validate_subject_shape(subject)
        self._subjects.append(subject)
        self.normalize_positions()

    def replace_subject(self, index: int, subject: SubjectT) -> None:
        self._validate_subject_shape(subject)
        self._subjects[index] = subject
        self.normalize_positions()

    def remove_subject_at(self, index: int) -> SubjectT:
        removed = self._subjects.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        self._subjects.clear()

    def move_subject(self, old_index: int, new_index: int) -> None:
        subject = self._subjects.pop(old_index)
        self._subjects.insert(new_index, subject)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        for i, subject in enumerate(self._subjects):
            subject.is_primary = (i == index)

    def normalize_positions(self) -> None:
        for index, subject in enumerate(self._subjects):
            subject.position = index

    def primary_subject(self) -> SubjectT | None:
        for subject in self._subjects:
            if subject.is_primary:
                return subject
        return self._subjects[0] if self._subjects else None

    def validate(self) -> None:
        primary_count = 0

        for expected_index, subject in enumerate(self._subjects):
            self._validate_subject_shape(subject)
            subject.validate()

            if subject.position != expected_index:
                raise ValueError(
                    f"Subject position mismatch for {self.target_kind} "
                    f"{self.target_id}: expected {expected_index}, got {subject.position}"
                )

            if subject.is_primary:
                primary_count += 1

        if primary_count > 1:
            raise ValueError(
                f"Only one primary subject is allowed for "
                f"{self.target_kind} {self.target_id} kind {self.subject_kind}"
            )

    def as_write_payload(self) -> list[dict[str, object]]:
        return [subject.as_write_payload() for subject in self._subjects]

    def _validate_subject_shape(self, subject: SubjectT) -> None:
        if subject.target_kind != self.target_kind:
            raise ValueError(
                f"Cannot add {subject.target_kind} subject to {self.target_kind} container"
            )

        if subject.target_id != self.target_id:
            raise ValueError(
                f"Subject target_id {subject.target_id} does not match "
                f"container target_id {self.target_id}"
            )

        if subject.kind_key != self.subject_kind:
            raise ValueError(
                f"Subject kind {subject.kind_key} does not match "
                f"container kind {self.subject_kind}"
            )


@dataclass(slots=True, kw_only=True)
class WorkKindSubjectsContainer(KindSubjectsContainer[WorkSubject]):
    target_kind: ClassVar[str] = "work"

    @property
    def work_id(self) -> WorkID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ExpressionKindSubjectsContainer(KindSubjectsContainer[ExpressionSubject]):
    target_kind: ClassVar[str] = "expression"

    @property
    def expression_id(self) -> ExpressionID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ManifestationKindSubjectsContainer(KindSubjectsContainer[ManifestationSubject]):
    target_kind: ClassVar[str] = "manifestation"

    @property
    def manifestation_id(self) -> ManifestationID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ItemKindSubjectsContainer(KindSubjectsContainer[ItemSubject]):
    target_kind: ClassVar[str] = "item"

    @property
    def item_id(self) -> ItemID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class BaseTargetSubjectsContainer(
    MetadataSequenceStringMixin,
    Generic[SubjectT, KindContainerT],
    abc.ABC,
):
    """
    Top-level editable subjects container for one target entity.
    Holds one KindSubjectsContainer per subject kind.
    """

    _by_kind: dict[SubjectKind, KindContainerT] = field(default_factory=dict)
    STRING_COUNT_LABEL = "subjects"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """ID of the target object."""

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """work / expression / manifestation / item."""

    @abc.abstractmethod
    def _make_kind_container(self, subject_kind: SubjectKind) -> KindContainerT:
        """Build the correct per-kind container for this target type."""

    def kinds(self) -> tuple[SubjectKind, ...]:
        return tuple(self._by_kind.keys())

    def has_kind(self, subject_kind: SubjectKind) -> bool:
        return subject_kind in self._by_kind

    def get_kind(self, subject_kind: SubjectKind) -> KindContainerT | None:
        return self._by_kind.get(subject_kind)

    def ensure_kind(self, subject_kind: SubjectKind) -> KindContainerT:
        container = self._by_kind.get(subject_kind)
        if container is None:
            container = self._make_kind_container(subject_kind)
            self._by_kind[subject_kind] = container
        return container

    def add_subject(self, subject: SubjectT) -> None:
        if subject.target_id != self.target_id:
            raise ValueError(
                f"Subject target_id {subject.target_id} does not match "
                f"{self.target_kind} target_id {self.target_id}"
            )

        self.ensure_kind(subject.kind_key).add_subject(subject)

    def iter_all_subjects(self) -> Iterator[SubjectT]:
        for container in self._by_kind.values():
            yield from container

    def all_texts(self) -> tuple[str, ...]:
        return tuple(subject.text for subject in self.iter_all_subjects())

    def kind_text(self, subject_kind: SubjectKind, sep: str = ", ") -> str:
        container = self.get_kind(subject_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def primary_subjects(self) -> dict[SubjectKind, SubjectT]:
        result: dict[SubjectKind, SubjectT] = {}
        for subject_kind, container in self._by_kind.items():
            primary = container.primary_subject()
            if primary is not None:
                result[subject_kind] = primary
        return result

    def primary_subject_for_kind(self, subject_kind: SubjectKind) -> SubjectT | None:
        container = self.get_kind(subject_kind)
        if container is None:
            return None
        return container.primary_subject()

    def validate(self) -> None:
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkSubjectsContainer(
    BaseTargetSubjectsContainer[
        WorkSubject,
        WorkKindSubjectsContainer,
    ]
):
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        return "work"

    def _make_kind_container(self, subject_kind: SubjectKind) -> WorkKindSubjectsContainer:
        return WorkKindSubjectsContainer(subject_kind=subject_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionSubjectsContainer(
    BaseTargetSubjectsContainer[
        ExpressionSubject,
        ExpressionKindSubjectsContainer,
    ]
):
    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        return "expression"

    def _make_kind_container(
        self,
        subject_kind: SubjectKind,
    ) -> ExpressionKindSubjectsContainer:
        return ExpressionKindSubjectsContainer(
            subject_kind=subject_kind,
            target_id=self.expression_id,
        )


@dataclass(slots=True, kw_only=True)
class ManifestationSubjectsContainer(
    BaseTargetSubjectsContainer[
        ManifestationSubject,
        ManifestationKindSubjectsContainer,
    ]
):
    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        return "manifestation"

    def _make_kind_container(
        self,
        subject_kind: SubjectKind,
    ) -> ManifestationKindSubjectsContainer:
        return ManifestationKindSubjectsContainer(
            subject_kind=subject_kind,
            target_id=self.manifestation_id,
        )


@dataclass(slots=True, kw_only=True)
class ItemSubjectsContainer(
    BaseTargetSubjectsContainer[
        ItemSubject,
        ItemKindSubjectsContainer,
    ]
):
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        return "item"

    def _make_kind_container(self, subject_kind: SubjectKind) -> ItemKindSubjectsContainer:
        return ItemKindSubjectsContainer(subject_kind=subject_kind, target_id=self.item_id)


# ---------------------------------------------------------------------------
# Subject-kind convenience layer
# ---------------------------------------------------------------------------

def _kind_property_stem(subject_kind: SubjectKind) -> str:
    stems: dict[SubjectKind, str] = {
        SubjectKind.TOPIC: "topics",
        SubjectKind.CHARACTER: "characters",
        SubjectKind.PLACE: "places",
        SubjectKind.PERIOD: "periods",
    }
    return stems[subject_kind]


def _install_kind_convenience_properties(
    cls: type[BaseTargetSubjectsContainer],
) -> None:
    """
    Install per-kind convenience properties and methods on a subjects container class.

    This is deliberate runtime sugar, not the load-bearing core API. The
    explicit generic methods on the container remain the canonical surface. See
    `metadata_container_dynamic_convenience_policy.md`.

    For a kind stem of ``topics``, this creates:
    - .topics
    - .topics_text
    - .topics_to_text(sep=", ")
    """
    for subject_kind in SubjectKind:
        stem = _kind_property_stem(subject_kind)

        def kind_container_getter(self, _kind=subject_kind):
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=subject_kind):
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = ", ", _kind=subject_kind) -> str:
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkSubjectsContainer)
_install_kind_convenience_properties(ExpressionSubjectsContainer)
_install_kind_convenience_properties(ManifestationSubjectsContainer)
_install_kind_convenience_properties(ItemSubjectsContainer)


__all__ = [
    "SubjectKind",
    "SubjectBase",
    "WorkSubject",
    "ExpressionSubject",
    "ManifestationSubject",
    "ItemSubject",
    "KindSubjectsContainer",
    "WorkKindSubjectsContainer",
    "ExpressionKindSubjectsContainer",
    "ManifestationKindSubjectsContainer",
    "ItemKindSubjectsContainer",
    "BaseTargetSubjectsContainer",
    "WorkSubjectsContainer",
    "ExpressionSubjectsContainer",
    "ManifestationSubjectsContainer",
    "ItemSubjectsContainer",
]
