"""
Represent editable genre and form assertions attached to WEMI entities.

Genre records retain display text, optional normalization and authority data,
ordering and provenance. Target containers use one ordered list across kinds, with
optional primary selection.

Example:
    >>> genre = WorkGenre(work_id=1, text='Science fiction')
    >>> genre.validate()
    >>> genre.text
    'Science fiction'
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import Iterator, Literal

from LiuXin_alpha.metadata.constants.container_vocabularies import GenreKind
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



@dataclass(slots=True, kw_only=True)
class GenreBase(MetadataValueStringMixin, abc.ABC):
    """
    Hold a genre/form statement, optional authority reference and shared attachment metadata.

    Concrete keyword-only dataclasses add the WEMI target. These values are not live row
    proxies: construction stores fields, and validate must be called explicitly.

    Example:
        >>> genre = WorkGenre(work_id=1, text='Science fiction')
        >>> genre.genre_kind == GenreKind.GENRE
        True
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
    STRING_DISPLAY_KEYS = ("text", "genre_kind", "source")

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the row id of the WEMI target receiving this genre.

        Example:
            >>> genre = WorkGenre(work_id=1, text='Science fiction')
            >>> genre.target_id
            1


        :return: Concrete target row id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level receiving this genre.

        Example:
            >>> genre = WorkGenre(work_id=1, text='Science fiction')
            >>> genre.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    def validate(self) -> None:
        """
        Reject blank genre text, negative positions and incomplete authority pairs.

        Authority scheme and identifier must have matching truthiness; they are not stripped
        or looked up. Raise ValueError on the first failed constraint without repairing
        fields.

        Example:
            >>> genre = WorkGenre(work_id=1, text='Science fiction')
            >>> genre.authority_scheme = 'local'
            >>> genre.validate()
            Traceback (most recent call last):
            ...
            ValueError: authority_scheme and authority_identifier must either both be set or both be empty


        :return: None.
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
        """
        Collect shared genre fields without validation or target-specific additions.

        Example:
            >>> genre = WorkGenre(work_id=1, text='Science fiction')
            >>> genre._common_write_payload()['text']
            'Science fiction'


        :return: New dictionary retaining stored values, including the GenreKind enum.
        """
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
        Require a write payload combining genre fields and target-specific context.

        Example:
            >>> genre = WorkGenre(work_id=1, text='Science fiction')
            >>> genre.as_write_payload()['work_id']
            1


        :return: New dictionary; implementations do not persist or automatically validate
            it.
        """


@dataclass(slots=True, kw_only=True)
class WorkGenre(GenreBase):
    """
    Represent a genre assertion on a work, including the canonical-for-work flag.

    Use work-level assertions for conceptual genre, such as a novel or tragedy.
    Construction stores supplied values; validation is explicit.

    Example:
        >>> genre = WorkGenre(work_id=3, text='Drama')
        >>> genre.target_id, genre.text
        (3, 'Drama')
    """

    work_id: WorkID
    canonical_for_work: bool = False

    @property
    def target_id(self) -> WorkID:
        """
        Return the work row id attached to this genre.

        Example:
            >>> genre = WorkGenre(work_id=3, text='Drama')
            >>> genre.target_id
            3


        :return: Stored work id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify this genre as attached to a work.

        Example:
            >>> genre = WorkGenre(work_id=3, text='Drama')
            >>> genre.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared genre fields with the work id and the canonical-for-work flag.

        No validation, authority resolution or persistence occurs.

        Example:
            >>> genre = WorkGenre(work_id=3, text='Drama')
            >>> genre.as_write_payload()['work_id']
            3


        :return: New dictionary of shared and work-specific fields.
        """
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
    Represent a genre assertion on a expression, including an optional applies-to language id.

    Expression assertions can describe a realization whose form differs, such as an
    audio drama adaptation. Construction stores supplied values; validation is explicit.

    Example:
        >>> genre = ExpressionGenre(expression_id=3, text='Drama')
        >>> genre.target_id, genre.text
        (3, 'Drama')
    """

    expression_id: ExpressionID
    applies_to_language_id: LanguageID | None = None

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression row id attached to this genre.

        Example:
            >>> genre = ExpressionGenre(expression_id=3, text='Drama')
            >>> genre.target_id
            3


        :return: Stored expression id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify this genre as attached to a expression.

        Example:
            >>> genre = ExpressionGenre(expression_id=3, text='Drama')
            >>> genre.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared genre fields with the expression id and an optional applies-to language id.

        No validation, authority resolution or persistence occurs.

        Example:
            >>> genre = ExpressionGenre(expression_id=3, text='Drama')
            >>> genre.as_write_payload()['expression_id']
            3


        :return: New dictionary of shared and expression-specific fields.
        """
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
    Represent a genre assertion on a manifestation, including the edition-specific flag.

    Manifestation assertions can describe edition or packaging choices, such as a
    publisher genre shelf. Construction stores supplied values; validation is explicit.

    Example:
        >>> genre = ManifestationGenre(manifestation_id=3, text='Drama')
        >>> genre.target_id, genre.text
        (3, 'Drama')
    """

    manifestation_id: ManifestationID
    edition_specific: bool = True

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation row id attached to this genre.

        Example:
            >>> genre = ManifestationGenre(manifestation_id=3, text='Drama')
            >>> genre.target_id
            3


        :return: Stored manifestation id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify this genre as attached to a manifestation.

        Example:
            >>> genre = ManifestationGenre(manifestation_id=3, text='Drama')
            >>> genre.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared genre fields with the manifestation id and the edition-specific flag.

        No validation, authority resolution or persistence occurs.

        Example:
            >>> genre = ManifestationGenre(manifestation_id=3, text='Drama')
            >>> genre.as_write_payload()['manifestation_id']
            3


        :return: New dictionary of shared and manifestation-specific fields.
        """
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
    Represent a genre assertion on a item, including the copy-specific flag.

    Item assertions can describe local shelving or copy-specific archival
    classification. Construction stores supplied values; validation is explicit.

    Example:
        >>> genre = ItemGenre(item_id=3, text='Drama')
        >>> genre.target_id, genre.text
        (3, 'Drama')
    """

    item_id: ItemID
    copy_specific: bool = True

    @property
    def target_id(self) -> ItemID:
        """
        Return the item row id attached to this genre.

        Example:
            >>> genre = ItemGenre(item_id=3, text='Drama')
            >>> genre.target_id
            3


        :return: Stored item id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify this genre as attached to a item.

        Example:
            >>> genre = ItemGenre(item_id=3, text='Drama')
            >>> genre.target_kind
            'item'


        :return: The literal 'item'.
        """
        return "item"

    def as_write_payload(self) -> dict[str, object]:
        """
        Serialize shared genre fields with the item id and the copy-specific flag.

        No validation, authority resolution or persistence occurs.

        Example:
            >>> genre = ItemGenre(item_id=3, text='Drama')
            >>> genre.as_write_payload()['item_id']
            3


        :return: New dictionary of shared and item-specific fields.
        """
        payload = self._common_write_payload()
        payload.update(
            {
                "item_id": self.item_id,
                "copy_specific": self.copy_specific,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class GenresContainerBase(MetadataSequenceStringMixin, abc.ABC):
    """
    Maintain one ordered editable genre list across all kinds for a WEMI target.

    The generated constructor retains an explicitly supplied _genres list; the default
    creates a fresh list. Genre objects remain shared. Shape checks occur on insertion,
    while full validation and primary selection are separate operations.

    Example:
        >>> genres = WorkGenresContainer(work_id=1)
        >>> genre = WorkGenre(work_id=1, text='Drama')
        >>> genres.add_genre(genre)
        >>> genres.display_genre
        'Drama'
    """

    _genres: list[GenreBase] = field(default_factory=list)
    STRING_COUNT_LABEL = "genres"

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        Require the row id shared by all stored genre assertions.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.target_id
            1


        :return: Concrete WEMI target id.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        Require the WEMI level represented by this genre container.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.target_kind
            'work'


        :return: One of 'work', 'expression', 'manifestation' or 'item'.
        """

    def __iter__(self) -> Iterator[GenreBase]:
        """
        Iterate over shared genre objects in list order.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> next(iter(genres)) is genre
            True


        :return: Iterator over stored genre references.
        """
        return iter(self._genres)

    def __len__(self) -> int:
        """
        Count the genre assertions currently stored.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> len(genres)
            1


        :return: Number of stored genres.
        """
        return len(self._genres)

    def __getitem__(self, index: int) -> GenreBase:
        """
        Read a genre using list indexing, including negative indices.

        Invalid indices raise IndexError.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres[-1] is genre
            True


        :param index: List index of the genre to read.
        :return: Stored genre object.
        """
        return self._genres[index]

    def genres(self) -> tuple[GenreBase, ...]:
        """
        Take a tuple snapshot of genre order while retaining shared mutable objects.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.genres()[0] is genre
            True


        :return: Tuple of stored genre references.
        """
        return tuple(self._genres)

    def texts(self) -> tuple[str, ...]:
        """
        Collect stored display text in list order without normalization.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.texts()
            ('Drama',)


        :return: Tuple of genre text values, including duplicates.
        """
        return tuple(genre.text for genre in self._genres)

    def to_text(self, sep: str = " / ") -> str:
        """
        Join every stored genre text using the requested separator.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.to_text(sep='; ')
            'Drama'


        :param sep: Separator between consecutive genre texts.
        :return: Joined display text, or an empty string for an empty list.
        """
        return sep.join(self.texts())

    def add_genre(self, genre: GenreBase) -> None:
        """
        Check the target kind and id, append the shared genre and renumber positions.

        Shape mismatches raise ValueError before insertion. Other record constraints remain
        the responsibility of validate.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genre.position
            0


        :param genre: Genre record matching this container target.
        :return: None.
        """
        self._validate_genre_shape(genre)
        self._genres.append(genre)
        self.normalize_positions()

    def replace_genre(self, index: int, genre: GenreBase) -> None:
        """
        Check target shape, replace the indexed genre and renumber all positions.

        Shape errors raise ValueError; invalid list indices raise IndexError.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.replace_genre(0, WorkGenre(work_id=1, text='Novel'))
            >>> genres.texts()
            ('Novel',)


        :param index: List index of the genre to replace.
        :param genre: Replacement genre matching this target.
        :return: None.
        """
        self._validate_genre_shape(genre)
        self._genres[index] = genre
        self.normalize_positions()

    def remove_genre_at(self, index: int) -> GenreBase:
        """
        Pop the indexed genre and renumber survivors.

        The removed record retains its field values. Invalid indices raise IndexError.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.remove_genre_at(0) is genre
            True


        :param index: List index to remove, including negative indices.
        :return: Removed genre object.
        """
        removed = self._genres.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        """
        Clear the stored list without resetting previously returned genre objects.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.clear()
            >>> genres.genres()
            ()


        :return: None.
        """
        self._genres.clear()

    def move_genre(self, old_index: int, new_index: int) -> None:
        """
        Pop a genre, insert it at the destination and renumber the list.

        Source indices follow list.pop rules and destinations follow list.insert rules.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.move_genre(0, 99)
            >>> genres[0] is genre and genre.position == 0
            True


        :param old_index: Source index; invalid indices raise IndexError.
        :param new_index: Insertion index after source removal; out-of-range destinations
            are clipped.
        :return: None.
        """
        genre = self._genres.pop(old_index)
        self._genres.insert(new_index, genre)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        """
        Set primary only on the matching enumerated index.

        An unmatched index, including a negative one, clears every primary flag.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.set_primary(0)
            >>> genre.is_primary
            True
            >>> genres.set_primary(-1)
            >>> genre.is_primary
            False


        :param index: Nonnegative index to designate, or an unmatched index to clear all
            flags.
        :return: None.
        """
        for i, genre in enumerate(self._genres):
            genre.is_primary = (i == index)

    def normalize_positions(self) -> None:
        """
        Assign each shared genre its zero-based list index.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genre.position = 9
            >>> genres.normalize_positions()
            >>> genre.position
            0


        :return: None.
        """
        for index, genre in enumerate(self._genres):
            genre.position = index

    def primary_genre(self) -> GenreBase | None:
        """
        Select the first flagged genre, falling back to the first stored record.

        This does not validate competing flags or mark the fallback record primary.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.primary_genre() is genre, genre.is_primary
            (True, False)


        :return: Shared selected genre, or None for an empty container.
        """
        for genre in self._genres:
            if genre.is_primary:
                return genre
        return self._genres[0] if self._genres else None

    @property
    def display_genre(self) -> str | None:
        """
        Read display text from primary_genre, including its first-record fallback.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.display_genre
            'Drama'


        :return: Selected genre text, or None when empty.
        """
        primary = self.primary_genre()
        return primary.text if primary is not None else None

    def kinds(self) -> tuple[GenreKind, ...]:
        """
        Collect one genre kind per stored record in list order.

        Repeated kinds are retained.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.add_genre(WorkGenre(work_id=1, text='Novel'))
            >>> genres.kinds() == (GenreKind.GENRE, GenreKind.GENRE)
            True


        :return: Tuple of kinds, including duplicates.
        """
        return tuple(genre.genre_kind for genre in self._genres)

    def of_kind(self, genre_kind: GenreKind) -> tuple[GenreBase, ...]:
        """
        Select shared genre records matching a kind, preserving their order.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.of_kind(GenreKind.GENRE)[0] is genre
            True


        :param genre_kind: GenreKind value to match.
        :return: Tuple of matching record references.
        """
        return tuple(genre for genre in self._genres if genre.genre_kind == genre_kind)

    def kind_text(self, genre_kind: GenreKind, sep: str = " / ") -> str:
        """
        Join display texts whose genre kind matches, preserving list order.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.kind_text(GenreKind.GENRE, sep='; ')
            'Drama'


        :param genre_kind: GenreKind value to filter by.
        :param sep: Separator between matching texts.
        :return: Joined text, or an empty string when no records match.
        """
        return sep.join(genre.text for genre in self._genres if genre.genre_kind == genre_kind)

    def validate(self) -> None:
        """
        Check target shape, each record, contiguous positions and at most one primary genre.

        The primary limit applies across all kinds. Raise ValueError on the first failed
        constraint; no values are repaired.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.validate()
            >>> genre.position = 2
            >>> genres.validate()
            Traceback (most recent call last):
            ...
            ValueError: Genre position mismatch for work 1: expected 0, got 2


        :return: None.
        """
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
        """
        Serialize all genres in list order without validation or persistence.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres.as_write_payload()[0]['text']
            'Drama'


        :return: New list of genre payload dictionaries.
        """
        return [genre.as_write_payload() for genre in self._genres]

    def _validate_genre_shape(self, genre: GenreBase) -> None:
        """
        Require the record target kind and id to match this container.

        Raise ValueError on mismatch; genre kind and other fields are not inspected.

        Example:
            >>> genres = WorkGenresContainer(work_id=1)
            >>> genre = WorkGenre(work_id=1, text='Drama')
            >>> genres.add_genre(genre)
            >>> genres._validate_genre_shape(genre)


        :param genre: Candidate genre record whose target should match.
        :return: None.
        """
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
    Collect all genre kinds in one ordered list for a work.

    Construction retains a supplied list without validation. Mutations preserve shared
    record objects, and primary selection is global across kinds.

    Example:
        >>> genres = WorkGenresContainer(work_id=3)
        >>> genres.target_id, genres.display_genre
        (3, None)
    """

    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        """
        Return the work id for this genre collection.

        Example:
            >>> genres = WorkGenresContainer(work_id=3)
            >>> genres.target_id
            3


        :return: Stored work row id.
        """
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        """
        Identify the genre-collection target as a work.

        Example:
            >>> genres = WorkGenresContainer(work_id=3)
            >>> genres.target_kind
            'work'


        :return: The literal 'work'.
        """
        return "work"


@dataclass(slots=True, kw_only=True)
class ExpressionGenresContainer(GenresContainerBase):
    """
    Collect all genre kinds in one ordered list for a expression.

    Construction retains a supplied list without validation. Mutations preserve shared
    record objects, and primary selection is global across kinds.

    Example:
        >>> genres = ExpressionGenresContainer(expression_id=3)
        >>> genres.target_id, genres.display_genre
        (3, None)
    """

    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        """
        Return the expression id for this genre collection.

        Example:
            >>> genres = ExpressionGenresContainer(expression_id=3)
            >>> genres.target_id
            3


        :return: Stored expression row id.
        """
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        """
        Identify the genre-collection target as a expression.

        Example:
            >>> genres = ExpressionGenresContainer(expression_id=3)
            >>> genres.target_kind
            'expression'


        :return: The literal 'expression'.
        """
        return "expression"


@dataclass(slots=True, kw_only=True)
class ManifestationGenresContainer(GenresContainerBase):
    """
    Collect all genre kinds in one ordered list for a manifestation.

    Construction retains a supplied list without validation. Mutations preserve shared
    record objects, and primary selection is global across kinds.

    Example:
        >>> genres = ManifestationGenresContainer(manifestation_id=3)
        >>> genres.target_id, genres.display_genre
        (3, None)
    """

    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        """
        Return the manifestation id for this genre collection.

        Example:
            >>> genres = ManifestationGenresContainer(manifestation_id=3)
            >>> genres.target_id
            3


        :return: Stored manifestation row id.
        """
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        """
        Identify the genre-collection target as a manifestation.

        Example:
            >>> genres = ManifestationGenresContainer(manifestation_id=3)
            >>> genres.target_kind
            'manifestation'


        :return: The literal 'manifestation'.
        """
        return "manifestation"


@dataclass(slots=True, kw_only=True)
class ItemGenresContainer(GenresContainerBase):
    """
    Collect all genre kinds in one ordered list for a item.

    Construction retains a supplied list without validation. Mutations preserve shared
    record objects, and primary selection is global across kinds.

    Example:
        >>> genres = ItemGenresContainer(item_id=3)
        >>> genres.target_id, genres.display_genre
        (3, None)
    """

    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        """
        Return the item id for this genre collection.

        Example:
            >>> genres = ItemGenresContainer(item_id=3)
            >>> genres.target_id
            3


        :return: Stored item row id.
        """
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        """
        Identify the genre-collection target as a item.

        Example:
            >>> genres = ItemGenresContainer(item_id=3)
            >>> genres.target_kind
            'item'


        :return: The literal 'item'.
        """
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
