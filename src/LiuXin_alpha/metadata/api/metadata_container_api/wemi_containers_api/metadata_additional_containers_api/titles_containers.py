"""Title metadata containers attached to W/E/M/I entities.

Category: additional metadata family.
These classes are editable metadata value objects and helper containers. They
are not independent identity objects and only `ItemWemiTitleSlice` is read-side.
"""
from __future__ import annotations

import abc

from dataclasses import dataclass, field
from typing import ClassVar, Generic, Iterator, Literal, TypeVar

from LiuXin_alpha.metadata.constants.container_vocabularies import TitleKind
from LiuXin_alpha.metadata.metadata_types import (
    WorkID,
    ExpressionID,
    ManifestationID,
    ItemID,
    LanguageID,
)


TitleT = TypeVar("TitleT", bound="TitleBase")
KindContainerT = TypeVar("KindContainerT", bound="KindTitlesContainer")



@dataclass(slots=True, kw_only=True)
class TitleBase(abc.ABC):
    """
    Shared relation data for one title record attached to a bibliographic entity.

    A ``TitleBase`` instance is the editable value object for a single title
    string plus the metadata needed to interpret it: kind, language, script,
    ordering, provenance, and target attachment. It models the title-link, not
    a database row proxy.
    """

    title_kind: TitleKind
    text: str
    normalized_text: str | None = None
    sort_text: str | None = None
    language_id: LanguageID | None = None
    script_code: str | None = None

    position: int | None = None
    is_primary: bool = False

    source: str = "user_set"
    notes: str | None = None

    @property
    @abc.abstractmethod
    def target_id(self) -> int:
        """
        ID of the W/E/M/I entity this title attaches to.
        """

    @property
    @abc.abstractmethod
    def target_kind(self) -> Literal["work", "expression", "manifestation", "item"]:
        """
        work / expression / manifestation / item.
        """

    @property
    def kind_key(self) -> TitleKind:
        """
        The kind this title is grouped by.

        :return:
        """
        return self.title_kind

    def validate(self) -> None:
        """
        Validate that the title is internally consistent.

        :return:
        """
        if not str(self.title_kind).strip():
            raise ValueError("title_kind cannot be blank")

        if not self.text.strip():
            raise ValueError("text cannot be blank")

        if self.position is not None and self.position < 0:
            raise ValueError("position cannot be negative")

    def _common_write_payload(self) -> dict[str, object]:
        return {
            "title_kind": self.title_kind,
            "text": self.text,
            "normalized_text": self.normalized_text,
            "sort_text": self.sort_text,
            "language_id": self.language_id,
            "script_code": self.script_code,
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
class WorkTitle(TitleBase):
    """
    Title record attached directly to a work.

    Work titles are the most abstract title layer. They are useful for the
    canonical or conceptual title of a work, regardless of language-specific
    expression or edition-specific manifestation wording.
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
class ExpressionTitle(TitleBase):
    """
    Title record attached directly to an expression.

    Expression titles are the natural place for language-specific or
    transliterated variants that belong to a particular realisation of a work.
    """

    expression_id: ExpressionID
    applies_to_language_id: LanguageID | None = None
    transliterated_from_language_id: LanguageID | None = None

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
                "transliterated_from_language_id": self.transliterated_from_language_id,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ManifestationTitle(TitleBase):
    """
    Title record attached directly to a manifestation.

    Manifestation titles capture edition- or publication-specific wording such
    as a title-page transcription, jacket title, or other issue-level variant.
    """

    manifestation_id: ManifestationID
    edition_specific: bool = True
    transcribed_from_title_page: bool = False

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
                "transcribed_from_title_page": self.transcribed_from_title_page,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class ItemTitle(TitleBase):
    """
    Title record attached directly to an individual item / copy.

    Item titles are for copy-specific supplied or observed titles, such as a
    binder-supplied caption or a local title written on a container.
    """

    item_id: ItemID
    copy_specific: bool = True
    supplied_by_cataloguer: bool = False

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
                "supplied_by_cataloguer": self.supplied_by_cataloguer,
            }
        )
        return payload


@dataclass(slots=True, kw_only=True)
class KindTitlesContainer(Generic[TitleT], abc.ABC):
    """
    Ordered editable container for all titles of one kind on one target entity.

    Example uses include:
    - all main titles for a work
    - all translated titles for an expression
    - all spine titles for a manifestation

    The container owns ordering, primary-title selection, and shape validation
    for its children.
    """

    title_kind: TitleKind
    target_id: int
    _titles: list[TitleT] = field(default_factory=list)

    target_kind: ClassVar[str]

    def __iter__(self) -> Iterator[TitleT]:
        return iter(self._titles)

    def __len__(self) -> int:
        return len(self._titles)

    def __getitem__(self, index: int) -> TitleT:
        return self._titles[index]

    def titles(self) -> tuple[TitleT, ...]:
        return tuple(self._titles)

    def texts(self) -> tuple[str, ...]:
        return tuple(title.text for title in self._titles)

    def sort_texts(self) -> tuple[str, ...]:
        return tuple(title.sort_text or title.text for title in self._titles)

    def to_text(self, sep: str = " ; ") -> str:
        return sep.join(self.texts())

    def add_title(self, title: TitleT) -> None:
        self._validate_title_shape(title)
        self._titles.append(title)
        self.normalize_positions()

    def replace_title(self, index: int, title: TitleT) -> None:
        self._validate_title_shape(title)
        self._titles[index] = title
        self.normalize_positions()

    def remove_title_at(self, index: int) -> TitleT:
        removed = self._titles.pop(index)
        self.normalize_positions()
        return removed

    def clear(self) -> None:
        self._titles.clear()

    def move_title(self, old_index: int, new_index: int) -> None:
        title = self._titles.pop(old_index)
        self._titles.insert(new_index, title)
        self.normalize_positions()

    def set_primary(self, index: int) -> None:
        for i, title in enumerate(self._titles):
            title.is_primary = (i == index)

    def normalize_positions(self) -> None:
        for index, title in enumerate(self._titles):
            title.position = index

    def primary_title(self) -> TitleT | None:
        for title in self._titles:
            if title.is_primary:
                return title
        return self._titles[0] if self._titles else None

    def validate(self) -> None:
        primary_count = 0

        for expected_index, title in enumerate(self._titles):
            self._validate_title_shape(title)
            title.validate()

            if title.position != expected_index:
                raise ValueError(
                    f"Title position mismatch for {self.target_kind} "
                    f"{self.target_id}: expected {expected_index}, got {title.position}"
                )

            if title.is_primary:
                primary_count += 1

        if primary_count > 1:
            raise ValueError(
                f"Only one primary title is allowed for "
                f"{self.target_kind} {self.target_id} kind {self.title_kind}"
            )

    def as_write_payload(self) -> list[dict[str, object]]:
        return [title.as_write_payload() for title in self._titles]

    def _validate_title_shape(self, title: TitleT) -> None:
        if title.target_kind != self.target_kind:
            raise ValueError(
                f"Cannot add {title.target_kind} title to {self.target_kind} container"
            )

        if title.target_id != self.target_id:
            raise ValueError(
                f"Title target_id {title.target_id} does not match "
                f"container target_id {self.target_id}"
            )

        if title.kind_key != self.title_kind:
            raise ValueError(
                f"Title kind {title.kind_key} does not match "
                f"container kind {self.title_kind}"
            )


@dataclass(slots=True, kw_only=True)
class WorkKindTitlesContainer(KindTitlesContainer[WorkTitle]):
    target_kind: ClassVar[str] = "work"

    @property
    def work_id(self) -> WorkID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ExpressionKindTitlesContainer(KindTitlesContainer[ExpressionTitle]):
    target_kind: ClassVar[str] = "expression"

    @property
    def expression_id(self) -> ExpressionID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ManifestationKindTitlesContainer(KindTitlesContainer[ManifestationTitle]):
    target_kind: ClassVar[str] = "manifestation"

    @property
    def manifestation_id(self) -> ManifestationID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class ItemKindTitlesContainer(KindTitlesContainer[ItemTitle]):
    target_kind: ClassVar[str] = "item"

    @property
    def item_id(self) -> ItemID:
        return self.target_id


@dataclass(slots=True, kw_only=True)
class BaseTargetTitlesContainer(Generic[TitleT, KindContainerT], abc.ABC):
    """
    Top-level editable title container for one target entity.

    This is the main write-side surface for title metadata on a work,
    expression, manifestation, or item. It groups titles by ``TitleKind`` while
    still allowing callers to iterate over every attached title record.
    """

    _by_kind: dict[TitleKind, KindContainerT] = field(default_factory=dict)

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
    def _make_kind_container(self, title_kind: TitleKind) -> KindContainerT:
        """
        Build the correct per-kind container for this target type.
        """

    def kinds(self) -> tuple[TitleKind, ...]:
        return tuple(self._by_kind.keys())

    def has_kind(self, title_kind: TitleKind) -> bool:
        return title_kind in self._by_kind

    def get_kind(self, title_kind: TitleKind) -> KindContainerT | None:
        return self._by_kind.get(title_kind)

    def ensure_kind(self, title_kind: TitleKind) -> KindContainerT:
        container = self._by_kind.get(title_kind)
        if container is None:
            container = self._make_kind_container(title_kind)
            self._by_kind[title_kind] = container
        return container

    def add_title(self, title: TitleT) -> None:
        if title.target_id != self.target_id:
            raise ValueError(
                f"Title target_id {title.target_id} does not match "
                f"{self.target_kind} target_id {self.target_id}"
            )

        self.ensure_kind(title.kind_key).add_title(title)

    def iter_all_titles(self) -> Iterator[TitleT]:
        for container in self._by_kind.values():
            yield from container

    def all_texts(self) -> tuple[str, ...]:
        return tuple(title.text for title in self.iter_all_titles())

    def kind_text(self, title_kind: TitleKind, sep: str = " ; ") -> str:
        container = self.get_kind(title_kind)
        if container is None:
            return ""
        return container.to_text(sep=sep)

    def primary_titles(self) -> dict[TitleKind, TitleT]:
        result: dict[TitleKind, TitleT] = {}
        for title_kind, container in self._by_kind.items():
            primary = container.primary_title()
            if primary is not None:
                result[title_kind] = primary
        return result

    def primary_title_for_kind(self, title_kind: TitleKind) -> TitleT | None:
        container = self.get_kind(title_kind)
        if container is None:
            return None
        return container.primary_title()

    @property
    def main_title(self) -> TitleT | None:
        return self.primary_title_for_kind(TitleKind.MAIN)

    @property
    def display_title(self) -> str | None:
        for kind in (
            TitleKind.MAIN,
            TitleKind.TRANSLATED,
            TitleKind.ALTERNATIVE,
            TitleKind.SUPPLIED,
        ):
            title = self.primary_title_for_kind(kind)
            if title is not None:
                return title.text
        first = next(self.iter_all_titles(), None)
        return first.text if first is not None else None

    @property
    def sort_title(self) -> str | None:
        sort_title = self.primary_title_for_kind(TitleKind.SORT)
        if sort_title is not None:
            return sort_title.text

        main_title = self.main_title
        if main_title is not None:
            return main_title.sort_text or main_title.normalized_text or main_title.text

        return self.display_title

    def validate(self) -> None:
        for container in self._by_kind.values():
            container.validate()

    def as_write_payload(self) -> list[dict[str, object]]:
        payload: list[dict[str, object]] = []
        for container in self._by_kind.values():
            payload.extend(container.as_write_payload())
        return payload


@dataclass(slots=True, kw_only=True)
class WorkTitlesContainer(
    BaseTargetTitlesContainer[
        WorkTitle,
        WorkKindTitlesContainer,
    ]
):
    """
    Title container for a single work.
    """

    work_id: WorkID

    @property
    def target_id(self) -> WorkID:
        return self.work_id

    @property
    def target_kind(self) -> Literal["work"]:
        return "work"

    def _make_kind_container(self, title_kind: TitleKind) -> WorkKindTitlesContainer:
        return WorkKindTitlesContainer(title_kind=title_kind, target_id=self.work_id)


@dataclass(slots=True, kw_only=True)
class ExpressionTitlesContainer(
    BaseTargetTitlesContainer[
        ExpressionTitle,
        ExpressionKindTitlesContainer,
    ]
):
    """
    Title container for a single expression.
    """

    expression_id: ExpressionID

    @property
    def target_id(self) -> ExpressionID:
        return self.expression_id

    @property
    def target_kind(self) -> Literal["expression"]:
        return "expression"

    def _make_kind_container(
        self,
        title_kind: TitleKind,
    ) -> ExpressionKindTitlesContainer:
        return ExpressionKindTitlesContainer(
            title_kind=title_kind,
            target_id=self.expression_id,
        )


@dataclass(slots=True, kw_only=True)
class ManifestationTitlesContainer(
    BaseTargetTitlesContainer[
        ManifestationTitle,
        ManifestationKindTitlesContainer,
    ]
):
    """
    Title container for a single manifestation.
    """

    manifestation_id: ManifestationID

    @property
    def target_id(self) -> ManifestationID:
        return self.manifestation_id

    @property
    def target_kind(self) -> Literal["manifestation"]:
        return "manifestation"

    def _make_kind_container(
        self,
        title_kind: TitleKind,
    ) -> ManifestationKindTitlesContainer:
        return ManifestationKindTitlesContainer(
            title_kind=title_kind,
            target_id=self.manifestation_id,
        )


@dataclass(slots=True, kw_only=True)
class ItemTitlesContainer(
    BaseTargetTitlesContainer[
        ItemTitle,
        ItemKindTitlesContainer,
    ]
):
    """
    Title container for a single item / copy.
    """

    item_id: ItemID

    @property
    def target_id(self) -> ItemID:
        return self.item_id

    @property
    def target_kind(self) -> Literal["item"]:
        return "item"

    def _make_kind_container(self, title_kind: TitleKind) -> ItemKindTitlesContainer:
        return ItemKindTitlesContainer(title_kind=title_kind, target_id=self.item_id)


@dataclass(slots=True, kw_only=True)
class ItemWemiTitleSlice:
    """
    Read-side title slice for a single item across the whole W/E/M/I chain.

    This is intentionally a query-result helper rather than an editable metadata
    container. It gives callers a compact way to keep the work-, expression-,
    manifestation-, and item-level title containers together and to derive a
    composite display title for the item.

    The composition logic is deliberately simple:
    - ask each layer for its ``display_title``
    - drop empty values
    - optionally de-duplicate repeated strings while preserving order
    - join the remaining parts with a caller-supplied separator
    """

    work_titles: WorkTitlesContainer | None = None
    expression_titles: ExpressionTitlesContainer | None = None
    manifestation_titles: ManifestationTitlesContainer | None = None
    item_titles: ItemTitlesContainer | None = None

    def title_parts(self, *, dedupe: bool = True) -> tuple[str, ...]:
        """
        Return the best display-title contribution from each populated W/E/M/I layer.

        :param dedupe: If true, repeated text is collapsed while preserving order.
        :return: Tuple of non-empty title strings in W/E/M/I order.
        """
        raw_parts = [
            self.work_titles.display_title if self.work_titles is not None else None,
            self.expression_titles.display_title if self.expression_titles is not None else None,
            self.manifestation_titles.display_title if self.manifestation_titles is not None else None,
            self.item_titles.display_title if self.item_titles is not None else None,
        ]

        parts: list[str] = []
        seen: set[str] = set()
        for part in raw_parts:
            if not part:
                continue
            if dedupe and part in seen:
                continue
            parts.append(part)
            seen.add(part)
        return tuple(parts)

    def full_title(self, sep: str = " — ", *, dedupe: bool = True) -> str:
        """
        Render a composite item title from the W/E/M/I layers.

        A typical result might look like::

            Work Title — Translated Expression Title — Edition Title — Copy Title

        :param sep: Separator inserted between the title parts.
        :param dedupe: If true, repeated text is collapsed while preserving order.
        :return: Composite title string. Empty string if no layer contributes a title.
        """
        return sep.join(self.title_parts(dedupe=dedupe))

    def __str__(self) -> str:
        """
        Return the default composite item title.
        """
        return self.full_title()


# ---------------------------------------------------------------------------
# Title-kind convenience layer
# ---------------------------------------------------------------------------

def _kind_property_stem(title_kind: TitleKind) -> str:
    stems: dict[TitleKind, str] = {
        TitleKind.MAIN: "main_titles",
        TitleKind.SUBTITLE: "subtitles",
        TitleKind.ALTERNATIVE: "alternative_titles",
        TitleKind.SHORT: "short_titles",
        TitleKind.SORT: "sort_titles",
        TitleKind.UNIFORM: "uniform_titles",
        TitleKind.TRANSLATED: "translated_titles",
        TitleKind.TRANSLITERATED: "transliterated_titles",
        TitleKind.COVER: "cover_titles",
        TitleKind.SPINE: "spine_titles",
        TitleKind.RUNNING: "running_titles",
        TitleKind.SUPPLIED: "supplied_titles",
    }
    return stems[title_kind]


def _install_kind_convenience_properties(
    cls: type[BaseTargetTitlesContainer],
) -> None:
    """
    Install per-kind convenience properties and methods on a titles container class.

    This is deliberate runtime sugar, not the load-bearing core API. The
    explicit generic methods on the container remain the canonical surface. See
    `metadata_container_dynamic_convenience_policy.md`.

    For a kind stem of 'main_titles', this creates:
    - .main_titles
    - .main_titles_text
    - .main_titles_to_text(sep=" ; ")
    """

    for title_kind in TitleKind:
        stem = _kind_property_stem(title_kind)

        def kind_container_getter(self, _kind=title_kind):
            return self.ensure_kind(_kind)

        def kind_rendered_text_getter(self, _kind=title_kind):
            return self.kind_text(_kind)

        def kind_rendered_text_method(self, sep: str = " ; ", _kind=title_kind) -> str:
            return self.kind_text(_kind, sep=sep)

        setattr(cls, stem, property(kind_container_getter))
        setattr(cls, f"{stem}_text", property(kind_rendered_text_getter))
        setattr(cls, f"{stem}_to_text", kind_rendered_text_method)


_install_kind_convenience_properties(WorkTitlesContainer)
_install_kind_convenience_properties(ExpressionTitlesContainer)
_install_kind_convenience_properties(ManifestationTitlesContainer)
_install_kind_convenience_properties(ItemTitlesContainer)


__all__ = [
    "TitleKind",
    "TitleBase",
    "WorkTitle",
    "ExpressionTitle",
    "ManifestationTitle",
    "ItemTitle",
    "KindTitlesContainer",
    "WorkKindTitlesContainer",
    "ExpressionKindTitlesContainer",
    "ManifestationKindTitlesContainer",
    "ItemKindTitlesContainer",
    "BaseTargetTitlesContainer",
    "WorkTitlesContainer",
    "ExpressionTitlesContainer",
    "ManifestationTitlesContainer",
    "ItemTitlesContainer",
    "ItemWemiTitleSlice",
]
