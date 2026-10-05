"""
Implement configured entity persistence and exact-first repository matching.

ExactEntityRepository combines base CRUD with a subclass-supplied identity spec,
mutation/reuse restrictions, and persistence-only derived fields. Matching delegates
to a fresh matcher without writes; create/update normalize storage payloads separately.
Read-only entities and entities requiring contextual creation have distinct rules.
"""

from __future__ import annotations

from typing import ClassVar

from ..api.common import (
    CatalogMutationError,
    EntityId,
    MatchResult,
    MetadataCandidate,
    RowInput,
    RowMapping,
)
from ..matching.exact_matcher import ExactEntityMatcher, ExactEntitySpec
from ..matching.policy import normalise_match_text, raise_for_unresolved
from .base import BaseRepository


class ExactEntityRepository(BaseRepository):
    """
    Extend base CRUD with configured exact identity and entity mutation rules.

    Subclasses supply match_spec together with consistent table_name, id_column, and input_aliases;
    this class has no usable default spec. Reads use the base repository and fresh
    ExactEntityMatcher instances. The bound/default policy governs selection, with approximate
    fallback explicitly requested per call.

    Persistence derives only configured normalized fields missing from the supplied payload. Mutable
    controls create/update/delete and match_or_create; reusable additionally controls
    match_or_create, not direct reads or matching. The class adds no encompassing transaction around
    matching and creation and does not acquire ownership of the database.

    Example:
        >>> repository = catalog.labels  # doctest: +SKIP
        >>> label_id = repository.match_or_create(MetadataCandidate({"text": "To read"}))  # doctest: +SKIP


    :ivar match_spec: Entity identity, scope, normalization, mutability, and reuse configuration retained by reference.
    """

    match_spec: ClassVar[ExactEntitySpec]

    def _normalise_storage_input(self, data: RowInput) -> dict[str, object]:
        """
        Normalize persistence keys and fill absent configured derived-text fields.

        Use BaseRepository.normalise_input with its strict defaults, then process destination/source
        pairs in spec order. Derive a destination only when its source value is non-None and the
        destination key is absent. Explicit destination values, including None or inconsistent text,
        remain unchanged. Derivation uses normalise_match_text: NFKC/case-folding, punctuation
        removal, and ampersand expansion. It does not read the stored row or merge old values during
        updates.

        The first pass validates supplied fields. Derived destination names are added afterward and
        are checked by the later base create/update normalization. Values not replaced by derivation
        retain their original references; input is not mutated.

        Example:
            >>> from types import SimpleNamespace
            >>> repository = ExactEntityRepository(SimpleNamespace(
            ...     driver_wrapper=SimpleNamespace(get_column_headings=lambda table: ("text", "norm")),
            ... ))
            >>> repository.match_spec = ExactEntitySpec(
            ...     "Value", "values", "id", "text", ("text",), ("text",),
            ...     normalized_storage_fields=(("norm", "text"),),
            ... )
            >>> repository._normalise_storage_input({"text": "A & B!"})
            {'text': 'A & B!', 'norm': 'a and b'}
            >>> repository._normalise_storage_input({"text": "New", "norm": None})
            {'text': 'New', 'norm': None}


        :param data: Mapping of writable aliases or storage columns for this persistence operation.
        :return: New storage-column dictionary with eligible missing derived fields populated.
        :raises Exception: Base input/schema validation, missing configuration, and text-conversion failures propagate.
        """

        result = super().normalise_input(data)
        for destination, source in self.match_spec.normalized_storage_fields:
            source_value = result.get(source)
            if source_value is not None and destination not in result:
                result[destination] = normalise_match_text(source_value)
        return result

    def _require_mutable(self) -> None:
        """
        Reject mutation when the configured spec marks this entity immutable.

        Inspect the mutable flag only, without querying rows, validating input, or checking
        reusable. A truthy mutable value permits the operation; this helper does not enforce the
        flag's annotated bool type.

        Example:
            >>> repository = ExactEntityRepository(None)
            >>> repository.match_spec = ExactEntitySpec("Code", "codes", "id", "code", ("code",), ("code",), mutable=False)
            >>> repository._require_mutable()
            Traceback (most recent call last):
            ...
            LiuXin_alpha.catalog.api.common.CatalogMutationError: Code rows are read-only catalog constants


        :return: None when mutation is allowed.
        :raises CatalogMutationError: If match_spec.mutable is false-valued.
        """
        if not self.match_spec.mutable:
            raise CatalogMutationError(
                f"{self.match_spec.entity_name} rows are read-only catalog constants"
            )

    def create(self, data: RowInput) -> EntityId:
        """
        Check mutability, derive persistence fields, and delegate insertion to base CRUD.

        Reject immutable entities before input or schema inspection. Strictly normalize supplied
        keys and derive missing configured values, then let BaseRepository.create normalize the
        result again, reject an empty payload, and validate the returned ID after insertion.
        Non-reusable entities still permit direct creation; contextual relationship requirements are
        the caller's responsibility or backend constraints. This method does not perform matching or
        add an outer transaction.

        Example:
            >>> label_id = catalog.labels.create({"text": "To read"})  # doctest: +SKIP


        :param data: Writable public aliases or storage-column values, excluding caller-assigned IDs.
        :return: The database ID returned and type-checked by BaseRepository.create.
        :raises CatalogMutationError: If mutation is forbidden, fields/payload are rejected, or insertion returns an invalid ID type.
        :raises Exception: Input/schema/derived-value validation and insertion failures propagate.
        """

        self._require_mutable()
        return super().create(self._normalise_storage_input(data))

    def update(self, entity_id: EntityId, data: RowInput) -> None:
        """
        Check mutability and normalize derived values before updating an existing row.

        Unlike BaseRepository.update, this wrapper normalizes the incoming payload before the entity
        existence check. The base update then requires the row and normalizes the resulting storage
        mapping again before writing. Missing derived values are recomputed only from sources
        supplied in this update; existing row values are not consulted. Explicit derived fields,
        including None, are preserved. Even an empty update checks mutability, schema, and row
        existence.

        Example:
            >>> catalog.labels.update(label_id, {"text": "Finished"})  # doctest: +SKIP


        :param entity_id: ID required by base update after the first persistence normalization.
        :param data: Fields to replace, with eligible missing derived fields added from supplied source values.
        :return: None after the base update returns.
        :raises CatalogMutationError: If mutation is forbidden or input normalization rejects fields.
        :raises CatalogNotFoundError: If the row is absent after initial payload normalization.
        :raises Exception: ID/schema validation, derived-value conversion, and update failures propagate.
        """

        self._require_mutable()
        super().update(entity_id, self._normalise_storage_input(data))

    def delete(self, entity_id: EntityId) -> None:
        """
        Reject immutable entities before delegating existence validation and deletion.

        Reusability does not restrict direct deletion. Base CRUD requires the row and asks macros to
        delete it; backend constraints and subclass behavior determine relationship consequences. No
        extra transaction or ownership scan is added here.

        Example:
            >>> catalog.tags.delete(tag_id)  # doctest: +SKIP


        :param entity_id: ID required and deleted by the base repository after the mutability check.
        :return: None after the base deletion returns.
        :raises CatalogMutationError: If the entity spec forbids mutation.
        :raises CatalogNotFoundError: If the entity is missing.
        :raises Exception: ID validation and database/constraint failures propagate.
        """

        self._require_mutable()
        super().delete(entity_id)

    def matcher(self) -> ExactEntityMatcher:
        """
        Construct a fresh matcher retaining this repository, its spec, and current policy.

        No group binding or database query is needed to construct it. The matcher captures the
        current policy reference, so a later repository rebind does not update that existing
        matcher. Configuration is neither copied nor validated.

        Example:
            >>> repository = ExactEntityRepository(None)
            >>> repository.match_spec = ExactEntitySpec("Value", "values", "id", "value", ("value",), ("value",))
            >>> first, second = repository.matcher(), repository.matcher()
            >>> first.repository is repository, first.spec is repository.match_spec, first is second
            (True, True, False)


        :return: A newly constructed ExactEntityMatcher with the current dependencies.
        :raises AttributeError: If this repository has no match_spec configuration.
        """

        return ExactEntityMatcher(self, self.match_spec, self.matching_policy)

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

        return self.matcher().best(candidate, use_policy=use_policy)

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

        return self.matcher().exact(value, **scope)

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

        result = self.exact(value, **scope)
        return result.candidate if result.is_match else None

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

        self._require_mutable()
        if not self.match_spec.reusable:
            raise CatalogMutationError(
                f"{self.match_spec.entity_name} rows require contextual creation"
            )
        result = self.match(candidate, use_policy=use_policy)
        if result.is_match:
            assert result.entity_id is not None
            return result.entity_id
        raise_for_unresolved(result)
        return self.create(candidate.data)


__all__ = ["ExactEntityRepository"]
