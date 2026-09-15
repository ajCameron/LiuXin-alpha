"""
Compose specialized and exact-default matchers for Catalog identity queries.

CatalogMatching directly constructs Work, Agent, and curated Identifier matchers,
then obtains observed-identifier and value-entity matchers from repositories.
Repository factories retain their already-bound policy; constructing this group
alone does not rebind them. The normal Catalog facade binds a common policy first.
Matching returns decisions without creating entities; repository mutation owners
decide how a match, miss, ambiguity, or conflict affects a requested write.

Example:
    >>> result = catalog.matching.for_entity("tag").exact("Gothic")  # doctest: +SKIP
"""

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
    """
    Group identity matchers over a borrowed database and repository collection.

    Dataclass construction fills the matcher attributes through __post_init__. Work and Agent
    matchers receive this group's policy; curated identifiers use their specialized rules, and
    repository factories supply the other matchers with each repository's configured policy. This
    group performs no policy/type validation or rebinding. Its slotted fields remain mutable, so
    later assignment of policy does not update existing matchers automatically.

    Example:
        >>> matching = catalog.matching  # doctest: +SKIP
        >>> matching.for_entity("synopsis") is matching.synopses  # doctest: +SKIP
        True


    :ivar db: Borrowed database passed to the directly constructed specialized matchers.
    :ivar repositories: Bound repository group used directly and through its matcher factories.
    :ivar policy: Policy forwarded to newly built Work/Agent matchers; does not overwrite repository policies.
    :ivar works: Specialized Work matcher using descriptive, Agent, and identifier evidence.
    :ivar agents: Specialized Agent matcher using names, aliases, types, and identifier evidence.
    :ivar identifiers: Matcher for normalized curated identifier storage rows.
    :ivar item_identifiers: Matcher for observed identifiers with optional Item scope.
    :ivar tags: Exact-default Tag matcher.
    :ivar labels: Exact-default Label matcher.
    :ivar genres: Exact-default Genre matcher with optional parent scope.
    :ivar subjects: Exact-default Subject matcher with optional parent scope.
    :ivar series: Exact-default Series matcher with optional parent scope.
    :ivar languages: Matcher for seeded Language names and code variants.
    :ivar ratings: Exact Rating matcher, with scale/source constraints when supplied.
    :ivar comments: Exact Comment matcher; matching does not permit global creation/reuse.
    :ivar synopses: Exact Synopsis matcher.
    :ivar notes: Exact Note matcher.
    :ivar annotations: Exact Annotation matcher requiring Item scope; candidate matching also requires identity fields.
    """

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
        """
        Construct and assign matcher members in the declared composition order.

        Build Work, Agent, and Identifier matchers directly, then request the remaining twelve
        matchers from their repositories. Assignments are immediate: a later factory failure leaves
        earlier attributes set. The dataclass constructor calls this once; manual calls replace
        current matcher attributes without rollback or database-lifetime management.

        Example:
            >>> matching = CatalogMatching(db, catalog.repositories, catalog.matching.policy)  # doctest: +SKIP
            >>> matching.works.policy is matching.policy  # doctest: +SKIP
            True


        :return: None after all fifteen matcher members have been assigned.
        :raises Exception: Missing repository attributes or matcher construction failures propagate with earlier assignments retained.
        """
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
        """
        Resolve a supported exact-entity matcher by singular or plural public name.

        The concrete group strips whitespace, case-folds, and changes hyphens to underscores before
        consulting its explicit aliases. Supported families are Tag, Label, Genre, Subject, Series,
        Language, Rating, Comment, Synopsis, Note, and Annotation. Work, Agent, and identifier
        matchers have dedicated attributes and are not returned by this lookup. The existing group
        member is returned without constructing a matcher or querying rows.

        Example:
            >>> catalog.matching.for_entity(" TAG ") is catalog.matching.tags  # doctest: +SKIP
            True


        :param entity_name: Supported singular or plural entity name, accepting the concrete group's case/whitespace normalization.
        :return: The configured exact-default matcher currently stored in the group.
        :raises TypeError: If entity_name is not a string in the concrete implementation.
        :raises KeyError: If the normalized name has no exact-default matcher.
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
