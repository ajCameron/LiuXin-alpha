"""
Declare common identity conveniences for configured Catalog entity repositories.

The shared shape includes exact/scoped reads and match-or-create, but concrete
mutability and reuse flags restrict which entities permit writes. Approximate reuse
is explicitly requested and only exists for specifications defining a policy field.
Protocol checks establish structural presence rather than semantic correctness.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, MatchResult, MetadataCandidate, RowMapping
from .base import BaseRepositoryAPI


@runtime_checkable
class ExactEntityRepositoryAPI(BaseRepositoryAPI, Protocol):
    """
    Declare exact-first lookup and reuse operations on configured Catalog entities.

    Tags, Labels, Genres, Subjects, Series, Languages, Ratings, Comments, Synopses, Notes, and
    Annotations share this structural shape. The concrete implementation uses per-entity scope/case
    rules and opt-in approximate fallback where configured. Languages reject mutations; Comments and
    Annotations reject match_or_create while still allowing direct reads and contextual creation
    through other calls.

    The protocol inherits base CRUD but does not declare the concrete matcher() factory. Runtime
    presence checks do not validate signatures, policies, or database capabilities, and this
    contract adds no transaction layer.

    Example:
        >>> repository: ExactEntityRepositoryAPI = catalog.tags  # doctest: +SKIP
        >>> row = repository.resolve("Gothic")  # doctest: +SKIP
    """

    def match(
        self,
        candidate: MetadataCandidate,
        *,
        use_policy: bool = False,
    ) -> MatchResult:
        """
        Return an identity decision using a fresh matcher and the current repository policy.

        The concrete implementation normalizes candidate aliases, ignores unknown fields, removes a
        supplied entity ID from matching constraints, and applies configured scope and
        required-identity checks. All supplied non-None identity constraints must match one row.
        String equality preserves punctuation, with NFKC/whitespace normalization and case-folding
        only on configured fields.

        Missing required input or incompatible exact fields produce terminal decisions. Only an
        ordinary exact miss can use approximate fallback when use_policy is truthy and the spec
        defines a policy field. Exact matches still undergo common acceptance/ambiguity selection.
        This read operation is allowed for immutable and non-reusable entity types and does not
        derive storage-only values or write data.

        Example:
            >>> result = catalog.tags.match(MetadataCandidate({"name": "Gothic"}))  # doctest: +SKIP


        :param candidate: MetadataCandidate whose data supplies entity identity and any configured parent/Item scope.
        :param use_policy: Enable configured approximate fallback after a nonterminal exact miss; defaults to False.
        :return: Explained match, no_match, ambiguous, or conflict decision from the configured matcher.
        :raises Exception: Candidate access, schema normalization, scope/identity comparison, and database failures propagate.
        """

        ...

    def exact(self, value: object, **scope: object) -> MatchResult:
        """
        Look up one scalar across configured scalar fields within optional explicit scope.

        The concrete matcher strictly normalizes scope aliases and rejects unknown keys and
        caller-supplied IDs. Only configured scope fields constrain rows; other accepted columns do
        not become filters. Required scope must be present, but candidate-only required identity
        fields are not checked on this scalar path.

        Match any configured scalar field using punctuation-preserving exact comparison and
        per-field case rules, then resolve duplicates through common selection. There is no
        approximate fallback or mutation, even for non-reusable entities.

        Example:
            >>> result = catalog.genres.exact("Gothic", parent_id=parent_id)  # doctest: +SKIP


        :param value: Scalar compared to every configured scalar identity field without blanket type coercion.
        :param scope: Public aliases or storage columns supplying configured scope, such as parent_id or item_id.
        :return: Match, no_match, or ambiguous result; this scalar path does not construct conflict decisions.
        :raises Exception: Strict scope normalization, database reads, and comparison failures propagate.
        """

        ...

    def resolve(self, value: object, **scope: object) -> RowMapping | None:
        """
        Return the row selected by exact, or None for any non-match decision.

        The concrete implementation forwards value and scope to exact, returns the selected result's
        candidate mapping directly, and discards other decision details. Ambiguity and missing scope
        therefore look like absence to this convenience; use exact when the explanation matters. No
        extra row copy, mutation, or exception translation is performed.

        Example:
            >>> row = catalog.languages.resolve("en")  # doctest: +SKIP


        :param value: Scalar identity value passed to exact.
        :param scope: Keyword scope passed unchanged to exact.
        :return: The selected candidate mapping, or None if no unique match is selected.
        :raises Exception: Delegated normalization, lookup, and comparison failures propagate.
        """

        ...

    def match_or_create(
        self,
        candidate: MetadataCandidate,
        *,
        use_policy: bool = False,
    ) -> EntityId:
        """
        Reuse a permitted match or create a new row from candidate data after a miss.

        The concrete implementation checks mutable first and reusable second, before matching or
        validating the candidate. Languages therefore reject this operation even when a row already
        exists; Comments and Annotations reject it because they require contextual creation. These
        flags do not prohibit read-only matching.

        A selected match returns its ID without updating existing fields. Ambiguity and conflict
        raise Catalog match errors. Normal no_match results pass candidate.data to create, including
        fields or IDs that matching may have ignored; creation can still reject them. Candidate
        source/hints are not independently stored.

        Matching and insertion have no enclosing transaction or uniqueness lock here. Backend
        constraints and caller transactions determine concurrency guarantees.

        Example:
            >>> tag_id = catalog.tags.match_or_create(MetadataCandidate({"name": "Gothic"}))  # doctest: +SKIP


        :param candidate: Entity candidate whose original data supplies insertion fields only after a miss.
        :param use_policy: Permit configured approximate reuse after an ordinary exact miss; defaults to False.
        :return: Existing selected ID or newly created ID for a mutable, reusable entity.
        :raises CatalogMutationError: If the spec is immutable or non-reusable, or creation rejects the payload.
        :raises CatalogAmbiguousMatchError: If matching leaves several plausible identities.
        :raises CatalogMatchConflictError: If exact identity evidence conflicts.
        :raises Exception: Candidate access, normalization, matching, and insertion failures propagate.
        """

        ...


__all__ = ["ExactEntityRepositoryAPI"]
