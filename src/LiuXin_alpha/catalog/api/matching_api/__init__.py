"""
Export read-only identity-decision protocols for Catalog matcher services.

Work, Agent, curated Identifier, and observed Item-Identifier matchers have
specialized contracts. Value entities share ExactEntityMatcherAPI, while
CatalogMatchingAPI groups them and resolves supported exact-entity names.
Final MatchResult decisions distinguish no_match, ambiguous, and conflict even
when no entity ID is selected. Repository mutation owners interpret those
decisions; querying a matcher does not create a catalogue entity.

Example:
    >>> result = catalog.matching.works.best(candidate)  # doctest: +SKIP
    >>> requires_review = result.requires_resolution  # doctest: +SKIP
"""

# Todo: Worth thinking about where file and storage related db ops should live...

from __future__ import annotations

from typing import Protocol, runtime_checkable

from LiuXin_alpha.catalog.api.matching_api.agent_matcher import AgentMatcherAPI
from LiuXin_alpha.catalog.api.matching_api.exact_entity_matcher import (
    ExactEntityMatcherAPI,
)
from LiuXin_alpha.catalog.api.matching_api.identifier_matcher import IdentifierMatcherAPI
from LiuXin_alpha.catalog.api.matching_api.item_identifier_matcher import (
    ItemIdentifierMatcherAPI,
)
from LiuXin_alpha.catalog.api.matching_api.work_matcher import WorkMatcherAPI


@runtime_checkable
class CatalogMatchingAPI(Protocol):
    """
    Describe the grouped specialized and exact-default matching surface.

    Dedicated attributes expose Work, Agent, curated Identifier, and observed Item-Identifier
    matchers. Eleven value-entity attributes share the exact matcher contract, including entities
    whose repositories disallow global reuse. for_entity selects only that exact-default subset.
    This structural protocol supplies no implementation; runtime checks do not verify signatures,
    result semantics, or database readiness.

    Example:
        >>> matching: CatalogMatchingAPI = catalog.matching  # doctest: +SKIP
        >>> result = matching.for_entity("tags").exact("Gothic")  # doctest: +SKIP


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

    works: WorkMatcherAPI
    agents: AgentMatcherAPI
    identifiers: IdentifierMatcherAPI
    item_identifiers: ItemIdentifierMatcherAPI
    tags: ExactEntityMatcherAPI
    labels: ExactEntityMatcherAPI
    genres: ExactEntityMatcherAPI
    subjects: ExactEntityMatcherAPI
    series: ExactEntityMatcherAPI
    languages: ExactEntityMatcherAPI
    ratings: ExactEntityMatcherAPI
    comments: ExactEntityMatcherAPI
    synopses: ExactEntityMatcherAPI
    notes: ExactEntityMatcherAPI
    annotations: ExactEntityMatcherAPI

    def for_entity(self, entity_name: str) -> ExactEntityMatcherAPI:
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

        ...


__all__ = [
    "AgentMatcherAPI",
    "CatalogMatchingAPI",
    "ExactEntityMatcherAPI",
    "IdentifierMatcherAPI",
    "ItemIdentifierMatcherAPI",
    "WorkMatcherAPI",
]
