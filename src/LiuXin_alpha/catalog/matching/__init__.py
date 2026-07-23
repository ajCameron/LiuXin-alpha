"""Matching implementations for the catalog layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..api.common import DatabaseHandle
from .agent_matcher import AgentMatcher
from .exact_matcher import ExactEntityMatcher, ExactEntitySpec
from .identifier_matcher import IdentifierMatcher
from .item_identifier_matcher import ItemIdentifierMatcher
from .policy import DEFAULT_MATCHING_POLICY, MatchingPolicy
from .work_matcher import WorkMatcher


@dataclass(slots=True)
class CatalogMatching:
    """Grouped matchers exposed by `Catalog.matching`."""

    db: DatabaseHandle
    repositories: Any
    policy: MatchingPolicy = DEFAULT_MATCHING_POLICY
    works: WorkMatcher = field(init=False)
    agents: AgentMatcher = field(init=False)
    identifiers: IdentifierMatcher = field(init=False)
    item_identifiers: ItemIdentifierMatcher = field(init=False)
    tags: ExactEntityMatcher = field(init=False)
    labels: ExactEntityMatcher = field(init=False)
    genres: ExactEntityMatcher = field(init=False)
    subjects: ExactEntityMatcher = field(init=False)
    series: ExactEntityMatcher = field(init=False)
    languages: ExactEntityMatcher = field(init=False)
    ratings: ExactEntityMatcher = field(init=False)
    comments: ExactEntityMatcher = field(init=False)
    synopses: ExactEntityMatcher = field(init=False)
    notes: ExactEntityMatcher = field(init=False)
    annotations: ExactEntityMatcher = field(init=False)

    def __post_init__(self) -> None:
        self.works = WorkMatcher(self.db, self.repositories, self.policy)
        self.agents = AgentMatcher(self.db, self.repositories, self.policy)
        self.identifiers = IdentifierMatcher(self.db, self.repositories)
        self.item_identifiers = self.repositories.item_identifiers.matcher()
        self.tags = self.repositories.tags.matcher()
        self.labels = self.repositories.labels.matcher()
        self.genres = self.repositories.genres.matcher()
        self.subjects = self.repositories.subjects.matcher()
        self.series = self.repositories.series.matcher()
        self.languages = self.repositories.languages.matcher()
        self.ratings = self.repositories.ratings.matcher()
        self.comments = self.repositories.comments.matcher()
        self.synopses = self.repositories.synopses.matcher()
        self.notes = self.repositories.notes.matcher()
        self.annotations = self.repositories.annotations.matcher()

    def for_entity(self, entity_name: str) -> ExactEntityMatcher:
        """Return an exact-default matcher by singular or plural entity name.

        :param entity_name: Entity or table name such as ``tag`` or ``tags``.
        :return: Configured exact-default entity matcher.
        :raises KeyError: If no exact-default matcher exists for the name.
        """

        if not isinstance(entity_name, str):
            raise TypeError("entity_name must be a string")
        normalized = entity_name.strip().casefold().replace("-", "_")
        aliases = {
            "tag": self.tags,
            "tags": self.tags,
            "label": self.labels,
            "labels": self.labels,
            "genre": self.genres,
            "genres": self.genres,
            "subject": self.subjects,
            "subjects": self.subjects,
            "series": self.series,
            "language": self.languages,
            "languages": self.languages,
            "rating": self.ratings,
            "ratings": self.ratings,
            "comment": self.comments,
            "comments": self.comments,
            "synopsis": self.synopses,
            "synopses": self.synopses,
            "note": self.notes,
            "notes": self.notes,
            "annotation": self.annotations,
            "annotations": self.annotations,
        }
        try:
            return aliases[normalized]
        except KeyError as error:
            raise KeyError(f"no exact-default matcher for {entity_name!r}") from error


__all__ = [
    "DEFAULT_MATCHING_POLICY",
    "AgentMatcher",
    "CatalogMatching",
    "ExactEntityMatcher",
    "ExactEntitySpec",
    "IdentifierMatcher",
    "ItemIdentifierMatcher",
    "MatchingPolicy",
    "WorkMatcher",
]
