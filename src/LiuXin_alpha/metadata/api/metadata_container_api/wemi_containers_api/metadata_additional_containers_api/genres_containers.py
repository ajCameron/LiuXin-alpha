"""Genre metadata containers attached to W/E/M/I entities.

Category: additional metadata family.
These classes are editable metadata value objects and helper containers, not
independent identity objects and not joined read-side views.
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import Iterator, Literal

from LiuXin_alpha.metadata.constants.container_vocabularies import GenreKind
from LiuXin_alpha.metadata.metadata_types import (
    WorkID,
    ExpressionID,
    ManifestationID,
    ItemID,
    LanguageID,
)



@dataclass(slots=True, kw_only=True)
class GenreBase(abc.ABC):
    """
    Shared relation data for one genre record attached to a bibliographic entity.

    A ``GenreBase`` instance models one genre/form statement plus the metadata
    needed to interpret it: optional kinding, optional authority linkage,
    ordering, provenance, and target attachment. It is a metadata value object,
    not a live database row proxy.
    """

    text: str
    genre_kind: GenreKind = GenreKind.GENRE
    normalized_text: str | None = None
    sort_text: str | None = None
    language_id: LanguageID | None = None

    authority_scheme: str | None = None
    authority_identifier: str | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    notes: str | None = None

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        ID of the W/E/M/I entity this genre attaches to.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        work / expression / manifestation / item.
        """

    def validate(self) -> None:
        """
        Validate that the genre record is internally consistent.
        """
        if not self.text.strip():
            raise ValueError("text cannot be blank")

        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

        if bool(self.authority_scheme) ^ bool(self.authority_identifier):
            raise ValueError(
                "authority_scheme and authority_identifier must either both be set or both be empty"
            )

    def _common_write_payload(self) -> dict[str, object]:
        return {
            "text": self.text,
            "genre_kind": self.genre_kind,
            "normalized_text": self.normalized_text,
            "sort_text": self.sort_text,
            "language_id": self.language_id,
            "authority_scheme": self.authority_scheme,
            "authority_identifier": self.authority_identifier,
            "position": self.position,
            "is_primary": self.is_primary,
            "source": self.source,
            "notes": self.notes,
        }

    @abc.abstractmethod
    def as_write_payload(self) -> dict[str, object]:
        """
        Serialise to a write-layer payload.
        """


@dataclass(slots=True, kw_only=True)
class WorkGenre(GenreBase):
    """
    Genre record attached directly to a work.

    This is the most natural place for the conceptual genre of a work, such as
    "novel", "tragedy", or "science fiction".
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
class ExpressionGenre(GenreBase):
    """
    Genre record attached directly to an expression.

    Expression-level genres are useful when a particular realisation shifts the
    effective genre or form, for example an abridged audio drama of a novel.
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
class ManifestationGenre(GenreBase):
    """
    Genre record attached directly to a manifestation.

    Manifestation-level genres are for edition - or packaging-specific treatment,
    such as a publisher marketing an edition to a particular genre shelf.
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
class ItemGenre(GenreBase):
    """
    Genre record attached directly to an individual item / copy.

    Item-level genres should be rare, but can be useful for local shelving,
    box-level classification, or copy-specific treatment in an archive.
    """

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
class GenresContainerBase(abc.ABC):
    """
    Ordered editable container for all genres attached to one target entity.

    Unlike titles or notes, genres are usually most useful as a single ordered
    list with optional primary selection rather than as a family of per-kind
    sub-containers.
    """

    _genres: list[GenreBase] = field(default_factory=list)

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

    def __iter__(self) -> Iterator[GenreBase]:
        return iter(self._genres)

    def __len__(self) -> int:
        return len(self._genres)

    def __getitem__(self, index: int) -> GenreBase:
        return self._genres[index]

    def genres(self) -> tuple[GenreBase, ...]:
        return tuple(self._genres)

    def texts(self) -> tuple[str, ...]:
        return tuple(genre.text for genre in self._genres)

    def to_text(self, sep: str = " / ") -> str:
        return sep.join(self.texts())

    def add_genre(self, genre: GenreBase) -> None:
        self._validate_genre_shape(genre)
        self._genres.append(genre)
        self.normalize_positions()

    def replace_genre(self, index: int, genre: GenreBase) -> None:
        self._validate_genre_shape(genre)
        self._genres[index] = genre
        self.normalize_positions()

    def remove_genre_at(self, index: int) -> GenreBase:
        removed = self._genres.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        self._genres.clear()

    def move_genre(self, old_index: int, new_index: int) -> None:
        genre = self._genres.pop(old_index)
        self._genres.insert(new_index, genre)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        for i, genre in enumerate(self._genres):
            genre.is_primary = (i == index)

    def normalize_positions(self) -> None:
        for index, genre in enumerate(self._genres):
            genre.position = index

    def primary_genre(self) -> GenreBase | None:
        for genre in self._genres:
            if genre.is_primary:
                return genre
        return self._genres[0] if self._genres else None

    @property
    def display_genre(self) -> str | None:
        primary = self.primary_genre()
        return primary.text if primary is not None else None

    def kinds(self) -> tuple[GenreKind, ...]:
        return tuple(genre.genre_kind for genre in self._genres)

    def of_kind(self, genre_kind: GenreKind) -> tuple[GenreBase, ...]:
        return tuple(genre for genre in self._genres if genre.genre_kind == genre_kind)

    def kind_text(self, genre_kind: GenreKind, sep: str = " / ") -> str:
        return sep.join(genre.text for genre in self._genres if genre.genre_kind == genre_kind)

    def validate(self) -> None:
        primary_count = 0

        for expected_index, genre in enumerate(self._genres):
            self._validate_genre_shape(genre)
            genre.validate()

            if genre.position != expected_index:
                raise ValueError(
                    f"Genre position mismatch for {self.target_kind} "
                    f"{self.target_id}: expected {expected_index}, got {genre.position}"
                )

            if genre.is_primary:
                primary_count += 1

        if primary_count > 1:
            raise ValueError(
                f"Only one primary genre is allowed for "
                f"{self.target_kind} {self.target_id}"
            )

    def as_write_payload(self) -> list[dict[str, object]]:
        return [genre.as_write_payload() for genre in self._genres]

    def _validate_genre_shape(self, genre: GenreBase) -> None:
        if genre.target_kind != self.target_kind:
            raise ValueError(
                f"Cannot add {genre.target_kind} genre to {self.target_kind} container"
            )

        if genre.target_id != self.target_id:
            raise ValueError(
                f"Genre target_id {genre.target_id} does not match "
                f"container target_id {self.target_id}"
            )


@dataclass(slots=True, kw_only=True)
class WorkGenresContainer(GenresContainerBase):
    """
    Genre container for a single work.
    """

    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        return "work"


@dataclass(slots=True, kw_only=True)
class ExpressionGenresContainer(GenresContainerBase):
    """
    Genre container for a single expression.
    """

    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        return "expression"


@dataclass(slots=True, kw_only=True)
class ManifestationGenresContainer(GenresContainerBase):
    """
    Genre container for a single manifestation.
    """

    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        return "manifestation"


@dataclass(slots=True, kw_only=True)
class ItemGenresContainer(GenresContainerBase):
    """
    Genre container for a single item / copy.
    """

    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        return "item"


__all__ = [
    "GenreKind",
    "GenreBase",
    "WorkGenre",
    "ExpressionGenre",
    "ManifestationGenre",
    "ItemGenre",
    "GenresContainerBase",
    "WorkGenresContainer",
    "ExpressionGenresContainer",
    "ManifestationGenresContainer",
    "ItemGenresContainer",
]
