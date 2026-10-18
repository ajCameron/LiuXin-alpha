"""Compose Catalog entity repositories without depending on the Catalog facade."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from LiuXin_alpha.catalog.api.common import DatabaseHandle
from LiuXin_alpha.catalog.matching.policy import DEFAULT_MATCHING_POLICY, MatchingPolicy
from LiuXin_alpha.catalog.repositories import (
    AgentRepository,
    AnnotationRepository,
    CommentRepository,
    ExpressionRepository,
    GenreRepository,
    IdentifierRepository,
    ItemIdentifierRepository,
    ItemRepository,
    LabelRepository,
    LanguageRepository,
    ManifestationRepository,
    NoteRepository,
    RatingRepository,
    SeriesRepository,
    SubjectRepository,
    SynopsisRepository,
    TagRepository,
    TitleRepository,
    WorkRepository,
)


@dataclass(slots=True)
class CatalogRepositories:
    """
    Hold the nineteen repository instances composed by a Catalog.

    The group stores supplied instances without validation or rebinding their dependencies. Its
    slotted fields remain assignable. Catalog convenience properties return these same members,
    while for_name resolves only public names from the explicit repository registry.

    Example:
        >>> repositories = catalog.repositories  # doctest: +SKIP
        >>> repositories.works is catalog.works  # doctest: +SKIP
        True


    :ivar works: Repository for works representing intellectual creations.
    :ivar expressions: Repository for expressions representing realizations of Works.
    :ivar manifestations: Repository for manifestations representing publication embodiments.
    :ivar items: Repository for items representing individual copies.
    :ivar agents: Repository for agents and their contributions to WEMI entities.
    :ivar identifiers: Repository for scheme-aware entity identifiers and their WEMI links.
    :ivar item_identifiers: Repository for identifiers observed on individual Items.
    :ivar titles: Repository for logical titles and their WEMI relationships.
    :ivar notes: Repository for notes attached to WEMI entities.
    :ivar tags: Repository for reusable Tag values.
    :ivar labels: Repository for reusable Label values.
    :ivar genres: Repository for genre values and their entity relationships.
    :ivar subjects: Repository for subject values and their entity relationships.
    :ivar series: Repository for series values and their entity relationships.
    :ivar languages: Repository for language values and their entity relationships.
    :ivar ratings: Repository for rating values and their entity relationships.
    :ivar comments: Repository for comment values and their entity relationships.
    :ivar synopses: Repository for synopsis text and its entity relationships.
    :ivar annotations: Repository for annotations scoped to individual Items.
    """

    works: WorkRepository
    expressions: ExpressionRepository
    manifestations: ManifestationRepository
    items: ItemRepository
    agents: AgentRepository
    identifiers: IdentifierRepository
    item_identifiers: ItemIdentifierRepository
    titles: TitleRepository
    notes: NoteRepository
    tags: TagRepository
    labels: LabelRepository
    genres: GenreRepository
    subjects: SubjectRepository
    series: SeriesRepository
    languages: LanguageRepository
    ratings: RatingRepository
    comments: CommentRepository
    synopses: SynopsisRepository
    annotations: AnnotationRepository

    def for_name(self, repository_name: str) -> Any:
        """
        Resolve a public singular or plural repository name within this group.

        Names are stripped, case-folded, and have hyphens replaced with underscores before explicit
        singular aliases are applied. Only names in the public repository registry are accepted;
        arbitrary attributes cannot be selected. Resolution returns the currently stored member
        without constructing a repository or reading schema/database state.

        Example:
            >>> catalog.repositories.for_name(" Item-Identifier ") is catalog.item_identifiers  # doctest: +SKIP
            True


        :param repository_name: Public singular or plural entity name, allowing surrounding whitespace and hyphens.
        :return: The existing repository instance stored under the normalized public name.
        :raises TypeError: If repository_name is not a string.
        :raises KeyError: If the normalized name is absent from the public registry.
        """

        if not isinstance(repository_name, str):
            raise TypeError("repository_name must be a string")
        normalized = repository_name.strip().casefold().replace("-", "_")
        aliases = {
            "work": "works",
            "expression": "expressions",
            "manifestation": "manifestations",
            "item": "items",
            "agent": "agents",
            "identifier": "identifiers",
            "item_identifier": "item_identifiers",
            "title": "titles",
            "note": "notes",
            "tag": "tags",
            "label": "labels",
            "genre": "genres",
            "subject": "subjects",
            "language": "languages",
            "rating": "ratings",
            "comment": "comments",
            "synopsis": "synopses",
            "annotation": "annotations",
        }
        normalized = aliases.get(normalized, normalized)
        from LiuXin_alpha.catalog.api.repositories import (
            CATALOG_REPOSITORY_NAMES,
        )

        if normalized not in CATALOG_REPOSITORY_NAMES:
            raise KeyError(f"unknown Catalog repository: {repository_name!r}")
        return getattr(self, normalized)

    def for_table(self, table: str) -> Any | None:
        """Return the semantic owner of a physical table, if Catalog owns it.

        Physical names are exact. The virtual title repository is deliberately
        excluded: Work title writes belong to the Work repository. None means
        this table belongs to another domain or a legacy schema.
        """
        if not isinstance(table, str):
            raise TypeError("table must be a string")
        from LiuXin_alpha.catalog.api.repositories import CATALOG_REPOSITORY_NAMES

        for name in CATALOG_REPOSITORY_NAMES:
            if name == "titles":
                continue
            repository = getattr(self, name)
            if repository.table_name == table:
                if (
                    repository.db.driver_wrapper.get_id_column(table)
                    == repository.id_column
                ):
                    return repository
                return None
        return None

    @classmethod
    def from_database(
        cls,
        db: DatabaseHandle,
        matching_policy: MatchingPolicy = DEFAULT_MATCHING_POLICY,
    ) -> CatalogRepositories:
        """Compose and bind the same entity owners for every write adapter."""
        repositories = cls(
            works=WorkRepository(db),
            expressions=ExpressionRepository(db),
            manifestations=ManifestationRepository(db),
            items=ItemRepository(db),
            agents=AgentRepository(db),
            identifiers=IdentifierRepository(db),
            item_identifiers=ItemIdentifierRepository(db),
            titles=TitleRepository(db),
            notes=NoteRepository(db),
            tags=TagRepository(db),
            labels=LabelRepository(db),
            genres=GenreRepository(db),
            subjects=SubjectRepository(db),
            series=SeriesRepository(db),
            languages=LanguageRepository(db),
            ratings=RatingRepository(db),
            comments=CommentRepository(db),
            synopses=SynopsisRepository(db),
            annotations=AnnotationRepository(db),
        )
        from LiuXin_alpha.catalog.api.repositories import CATALOG_REPOSITORY_NAMES

        for name in CATALOG_REPOSITORY_NAMES:
            repository = getattr(repositories, name)
            repository.bind_repositories(repositories)
            repository.bind_matching_policy(matching_policy)
        return repositories
