"""Work repository API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, MetadataCandidate, MatchResult, RowMapping
from .base import BaseRepositoryAPI


@runtime_checkable
class WorkRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Storage and identity API for abstract Works.

    A Work represents the intellectual creation, before language, edition, or
    physical/digital copy distinctions. Public create aliases include
    ``title``, ``canonical_title``, ``original_year``, and ``medium``.

    Example::

        candidate = MetadataCandidate({
            "title": "Frankenstein",
            "original_year": 1818,
        })
        work_id = catalog.works.match_or_create(candidate)
    """

    def find_by_title(self, title: str, *, limit: int = 20) -> Sequence[RowMapping]:
        """
        Find works whose title data matches the supplied string.

        Matching is Unicode-normalized, whitespace-insensitive, and
        case-insensitive, but not approximate.

        :param title: Preferred or canonical Work title to find.
        :param limit: Maximum ID-ordered matches to return.
        :return: Work row mappings whose preferred or canonical title matches.
        """

    # Todo: Regex titles match?
    # Todo: Find by creators, with roll

    def match(self, candidate: MetadataCandidate) -> MatchResult:
        """
        Match a candidate to an existing work if possible.

        This method does not mutate. Inspect ``decision``, ``evidence``, and
        ``alternatives`` before choosing a follow-up action.

        :param candidate: Work fields plus optional provenance and hints.
        :return: Explained match, no-match, ambiguity, or conflict decision.
        """

    def match_or_create(self, candidate: MetadataCandidate) -> EntityId:
        """
        Return a matched work id, or create a new work.

        Creation occurs only for a genuine ``no_match`` result. Ambiguity and
        conflict raise catalog match errors instead of silently duplicating a
        Work.

        :param candidate: Work fields accepted by :meth:`create`.
        :return: Existing safely matched ID or newly created Work ID.
        :raises CatalogAmbiguousMatchError: If multiple Works remain plausible.
        :raises CatalogMatchConflictError: If decisive evidence conflicts.
        """
