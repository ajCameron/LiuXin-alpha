"""
Containers for notes attached to W/E/M/I entities.

The classes in this module are editable metadata value objects. They are intended
for read / modify / write workflows around metadata hydration, not as live row or
database proxies.

The broad shape is:
- ``NoteBase`` and its W/E/M/I specialisations for individual note records.
- ``KindNotesContainer`` for an ordered list of notes of a single kind.
- ``BaseTargetNotesContainer`` and its W/E/M/I specialisations for all note
  data linked to one target entity.
"""

from __future__ import annotations

import abc

from dataclasses import dataclass, field
from enum import StrEnum
from typing import ClassVar, Generic, Iterator, Literal, TypeVar

from LiuXin_alpha.metadata.metadata_types import (
    WorkID,
    ExpressionID,
    ManifestationID,
    ItemID,
    LanguageID,
)


NoteT = TypeVar("NoteT", bound="NoteBase")
KindContainerT = TypeVar("KindContainerT", bound="KindNotesContainer")


class NoteKind(StrEnum):
    """
    Controlled kinds for long-form notes.

    The aim here is to capture the broad purpose of the note so callers can
    group, render, and filter note content without parsing free text.
    """

    DESCRIPTION = "description"
    REVIEW = "review"
    ANNOTATION = "annotation"
    SUMMARY = "summary"
    TRANSCRIPTION = "transcription"
    PROVENANCE = "provenance"
    CONDITION = "condition"
    ACQUISITION = "acquisition"
    CONTENTS = "contents"
    CITATION = "citation"
    INTERNAL = "internal"


class NoteFormat(StrEnum):
    """
    Storage or rendering format for the note body text.
    """

    PLAIN_TEXT = "plain_text"
    MARKDOWN = "markdown"
    HTML = "html"


class NoteVisibility(StrEnum):
    """
    Audience or exposure level for notes.

    This is deliberately lightweight. It gives downstream code enough signal to
    hide internal notes or prefer public-facing ones without turning the note
    container into a permissions system.
    """

    PRIVATE = "private"
    STAFF = "staff"
    PUBLIC = "public"


@dataclass(slots=True, kw_only=True)
class NoteBase(abc.ABC):
    """
    Shared relation data for one note attached to a bibliographic entity.

    A ``NoteBase`` instance is the editable value object for a single note plus
    the metadata needed to interpret it: kind, body format, ordering,
    visibility, provenance, and target attachment. It models the note-link, not
    a database row proxy.
    """

    note_kind: NoteKind
    body: str
    body_format: NoteFormat = NoteFormat.PLAIN_TEXT
    title: str | None = None
    language_id: LanguageID | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    visibility: NoteVisibility = NoteVisibility.PRIVATE
    notes: str | None = None

    association_start_ep_k: int | None = None
    association_end_ep_k: int | None = None

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        ID of the W/E/M/I entity this note attaches to.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        work / expression / manifestation / item.
        """

    @property
    def kind_key(self) -> NoteKind:
        """
        The kind this note is grouped by.

        :return:
        """
        return self.note_kind

    def validate(self) -> None:
        """
        Validate that the note is internally consistent.

        :return:
        """
        if not str(self.note_kind).strip():
            raise ValueError("note_kind cannot be blank")

        if not self.body.strip():
            raise ValueError("body cannot be blank")

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
            "note_kind": self.note_kind,
            "body": self.body,
            "body_format": self.body_format,
            "title": self.title,
            "language_id": self.language_id,
            "position": self.position,
            "is_primary": self.is_primary,
            "source": self.source,
            "visibility": self.visibility,
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
class WorkNote(NoteBase):
    """
    Note attached directly to a work.

    Work notes are appropriate for conceptual or work-wide commentary that does
    not belong to a specific expression, manifestation, or individual item.
    """

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
class ExpressionNote(NoteBase):
    """
    Note attached directly to an expression.

    Expression notes are useful for language-specific or realisation-specific
    commentary such as translation notes, abridgement notes, or performance
    notes.
    """

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
class ManifestationNote(NoteBase):
    """
    Note attached directly to a manifestation.

    Manifestation notes capture edition-, issue-, or publication-specific
    commentary such as jacket text, print-run notes, or edition-specific
    descriptions.
    """

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
class ItemNote(NoteBase):
    """
    Note attached directly to an individual item / copy.

    Item notes are the natural place for copy-specific observations such as
    condition, provenance, physical annotations, or local handling notes.
    """

    item_id: ItemID
    copy_specific: bool = True
    physical_observation: bool = False

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
                "physical_observation": self.physical_observation,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class KindNotesContainer(Generic[NoteT], abc.ABC):
    """
    Ordered editable container for all notes of one kind on one target entity.

    Example uses include:
    - all description notes for a work
    - all provenance notes for an item
    - all transcription notes for a manifestation

    The container owns ordering, primary-note selection, and shape validation
    for its children.
    """

    note_kind: NoteKind
    target_id: int
    _notes: list[NoteT] = field(default_factory=list)

    target_kind: ClassVar[str]

    def __iter__(self) -> Iterator[NoteT]:
        return iter(self._notes)

    def __len__(self) -> int:
        return len(self._notes)

    def __getitem__(self, index: int) -> NoteT:
        return self._notes[index]

    def notes(self) -> tuple[NoteT, ...]:
        return tuple(self._notes)

    def titles(self) -> tuple[str | None, ...]:
        return tuple(note.title for note in self._notes)

    def bodies(self) -> tuple[str, ...]:
        return tuple(note.body for note in self._notes)

    def to_display_string(self, sep: str = "\n\n") -> str:
        return sep.join(self.bodies())

    def add_note(self, note: NoteT) -> None:
        self._validate_note_shape(note)
        self._notes.append(note)
        self.normalize_positions()

    def replace_note(self, index: int, note: NoteT) -> None:
        self._validate_note_shape(note)
        self._notes[index] = note
        self.normalize_positions()

    def remove_note_at(self, index: int) -> NoteT:
        removed = self._notes.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        self._notes.clear()

    def move_note(self, old_index: int, new_index: int) -> None:
        note = self._notes.pop(old_index)
        self._notes.insert(new_index, note)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        for i, note in enumerate(self._notes):
            note.is_primary = (i == index)

    def normalize_positions(self) -> None:
        for index, note in enumerate(self._notes):
            note.position = index

    def primary_note(self) -> NoteT | None:
        for note in self._notes:
            if note.is_primary:
                return note
        return self._notes[0] if self._notes else None

    def validate(self) -> None:
        primary_count = 0

        for expected_index, note in enumerate(self._notes):
            self._validate_note_shape(note)
            note.validate()

            if note.position != expected_index:
                raise ValueError(
                    f"Note position mismatch for {self.target_kind} "
                    f"{self.target_id}: expected {expected_index}, got {note.position}"
                )

            if note.is_primary:
                primary_count += 1

        if primary_count > 1:
            raise ValueError(
                f"Only one primary note is allowed for "
                f"{self.target_kind} {self.target_id} kind {self.note_kind}"
            )

    def as_write_payload(self) -> list[dict[str, object]]:
        return [note.as_write_payload() for note in self._notes]

    def _validate_note_shape(self, note: NoteT) -> None:
        if note.target_kind != self.target_kind:
            raise ValueError(
                f"Cannot add {note.target_kind} note to {self.target_kind} container"
            )

        if note.target_id != self.target_id:
            raise ValueError(
                f"Note target_id {note.target_id} does not match "
                f"container target_id {self.target_id}"
            )

        if note.kind_key != self.note_kind:
            raise ValueError(
                f"Note kind {note.kind_key} does not match "
                f"container kind {self.note_kind}"
            )


@dataclass(slots=True, kw_only=True)
class WorkKindNotesContainer(KindNotesContainer[WorkNote]):
    target_kind: ClassVar[str] = "work"

    @property
    def work_id(self) -> WorkID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ExpressionKindNotesContainer(KindNotesContainer[ExpressionNote]):
    target_kind: ClassVar[str] = "expression"

    @property
    def expression_id(self) -> ExpressionID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ManifestationKindNotesContainer(KindNotesContainer[ManifestationNote]):
    target_kind: ClassVar[str] = "manifestation"

    @property
    def manifestation_id(self) -> ManifestationID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ItemKindNotesContainer(KindNotesContainer[ItemNote]):
    target_kind: ClassVar[str] = "item"

    @property
    def item_id(self) -> ItemID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class BaseTargetNotesContainer(Generic[NoteT, KindContainerT], abc.ABC):
    """
    Top-level editable note container for one target entity.

    This is the main write-side surface for note metadata on a work,
    expression, manifestation, or item. It groups notes by ``NoteKind`` while
    still allowing callers to iterate over every attached note record.
    """

    _by_kind: dict[NoteKind, KindContainerT] = field(default_factory=dict)

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        ID of the target object.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        work / expression / manifestation / item.
        """

    @abc.abstractmethod
    def _make_kind_container(self, note_kind: NoteKind) -> KindContainerT:
        """
        Build the correct per-kind container for this target type.
        """

    def kinds(self) -> tuple[NoteKind, ...]:
        return tuple(self._by_kind.keys())

    def has_kind(self, note_kind: NoteKind) -> bool:
        return note_kind in self._by_kind

    def get_kind(self, note_kind: NoteKind) -> KindContainerT | None:
        return self._by_kind.get(note_kind)

    def ensure_kind(self, note_kind: NoteKind) -> KindContainerT:
        container = self._by_kind.get(note_kind)
        if container is None:
            container = self._make_kind_container(note_kind)
            self._by_kind[note_kind] = container
        return container

    def add_note(self, note: NoteT) -> None:
        if note.target_id != self.target_id:
            raise ValueError(
                f"Note target_id {note.target_id} does not match "
                f"{self.target_kind} target_id {self.target_id}"
            )

        self.ensure_kind(note.kind_key).add_note(note)

    def iter_all_notes(self) -> Iterator[NoteT]:
        for container in self._by_kind.values():
            yield from container

    def all_titles(self) -> tuple[str, ...]:
        return tuple(note.title for note in self.iter_all_notes() if note.title)

    def kind_text(self, note_kind: NoteKind, sep: str = "\n\n") -> str:
        container = self.get_kind(note_kind)
        if container is None:
            return ""
        return container.to_display_string(sep=sep)

    def primary_notes(self) -> dict[NoteKind, NoteT]:
        result: dict[NoteKind, NoteT] = {}
        for note_kind, container in self._by_kind.items():
            primary = container.primary_note()
            if primary is not None:
                result[note_kind] = primary
        return result

    def validate(self) -> None:
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkNotesContainer(
    BaseTargetNotesContainer[
        WorkNote,
        WorkKindNotesContainer,
    ]
):
    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        return "work"

    def _make_kind_container(self, note_kind: NoteKind) -> WorkKindNotesContainer:
        return WorkKindNotesContainer(note_kind=note_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionNotesContainer(
    BaseTargetNotesContainer[
        ExpressionNote,
        ExpressionKindNotesContainer,
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
        note_kind: NoteKind,
    ) -> ExpressionKindNotesContainer:
        return ExpressionKindNotesContainer(
            note_kind=note_kind,
            target_id=self.expression_id,
        )


@dataclass(slots=True, kw_only=True)
class ManifestationNotesContainer(
    BaseTargetNotesContainer[
        ManifestationNote,
        ManifestationKindNotesContainer,
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
        note_kind: NoteKind,
    ) -> ManifestationKindNotesContainer:
        return ManifestationKindNotesContainer(
            note_kind=note_kind,
            target_id=self.manifestation_id,
        )


@dataclass(slots=True, kw_only=True)
class ItemNotesContainer(
    BaseTargetNotesContainer[
        ItemNote,
        ItemKindNotesContainer,
    ]
):
    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        return "item"

    def _make_kind_container(self, note_kind: NoteKind) -> ItemKindNotesContainer:
        return ItemKindNotesContainer(note_kind=note_kind, target_id=self.item_id)


# ---------------------------------------------------------------------------
# Note-kind convenience layer
# ---------------------------------------------------------------------------

def _kind_property_stem(note_kind: NoteKind) -> str:
    stems: dict[NoteKind, str] = {
        NoteKind.DESCRIPTION: "descriptions",
        NoteKind.REVIEW: "reviews",
        NoteKind.ANNOTATION: "annotations",
        NoteKind.SUMMARY: "summaries",
        NoteKind.TRANSCRIPTION: "transcriptions",
        NoteKind.PROVENANCE: "provenance_notes",
        NoteKind.CONDITION: "condition_notes",
        NoteKind.ACQUISITION: "acquisition_notes",
        NoteKind.CONTENTS: "contents_notes",
        NoteKind.CITATION: "citation_notes",
        NoteKind.INTERNAL: "internal_notes",
    }
    return stems[note_kind]


def _install_kind_convenience_properties(
    cls: type[BaseTargetNotesContainer],
) -> None:
    """
    Install per-kind convenience properties and methods on a notes container class.

    For a kind stem of 'descriptions', this creates:
    - .descriptions
    - .descriptions_text
    - .descriptions_to_string(sep="\n\n")
    """

    for note_kind in NoteKind:
        stem = _kind_property_stem(note_kind)

        def kind_container_getter(self, _kind=note_kind):
            return self.ensure_kind(_kind)

        def kind_text_getter(self, _kind=note_kind):
            return self.kind_text(_kind)

        def kind_text_method(self, sep: str = "\n\n", _kind=note_kind) -> str:
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_text_getter))
        setattr(cls, f"{stem}_to_string", kind_text_method)


_install_kind_convenience_properties(WorkNotesContainer)
_install_kind_convenience_properties(ExpressionNotesContainer)
_install_kind_convenience_properties(ManifestationNotesContainer)
_install_kind_convenience_properties(ItemNotesContainer)
