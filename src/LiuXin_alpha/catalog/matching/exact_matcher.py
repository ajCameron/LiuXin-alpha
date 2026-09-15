"""
Match configured Catalog value entities using exact identity and explicit scope.

ExactEntitySpec describes stored fields, aliases, scope, and repository policy.
ExactEntityMatcher normalizes candidate input through its repository and scans
row snapshots, preserving exact punctuation and applying configured case folding.
Complete identity, contradictory field resolutions, duplicate matches, and opt-in
approximate fallback are separate outcomes. candidates is a limited display list;
best retains the final decision needed before any repository creation or reuse.

Example:
    >>> _normalise_exact_text("  Ａ & B!  ", casefold=True)
    'a & b!'
    >>> result = catalog.matching.tags.best(MetadataCandidate({"name": "Gothic"}))  # doctest: +SKIP
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast
import unicodedata

from ..api.common import MatchEvidence, MatchResult, MetadataCandidate, RowMapping
from .policy import (
    DEFAULT_MATCHING_POLICY,
    MatchingPolicy,
    decide_best,
    text_similarity,
)


def _empty_aliases() -> dict[str, str]:
    """
    Allocate an independent alias mapping for one identity specification.

    The dataclass default factory supplies a mutable dictionary, so separate specifications do not
    share aliases unless the caller supplies shared data.

    Example:
        >>> _empty_aliases() == {}, _empty_aliases() is _empty_aliases()
        (True, False)


    :return: A new empty dictionary mapping public input names to storage columns.
    """
    return {}


def _empty_scope_defaults() -> dict[str, object | None]:
    """
    Allocate an independent scope-default mapping for one specification.

    This factory prevents omitted defaults from sharing a dictionary across instances. Freezing the
    containing record does not freeze this mapping.

    Example:
        >>> _empty_scope_defaults() == {}, _empty_scope_defaults() is _empty_scope_defaults()
        (True, False)


    :return: A new empty dictionary of scope-column defaults.
    """
    return {}


@dataclass(frozen=True, slots=True)
class ExactEntitySpec:
    """
    Describe identity comparisons and repository policy for one entity table.

    Matching uses supplied non-None identity fields together, subject to explicit required fields
    and scope. A primary-field value may match any scalar field; scalar exact lookup also searches
    all scalar fields. Repository creation separately interprets aliases, derived storage fields,
    mutability, and reuse.

    Construction performs no schema, field, flag, or consistency validation and retains supplied
    containers. Frozen fields prevent rebinding but leave alias and default mappings mutable.
    Mutability/reuse flags do not disable read-only matcher operations.

    Example:
        >>> spec = ExactEntitySpec("Tag", "tags", "tag_id", "tag", ("tag",), ("tag",))
        >>> spec.input_aliases["name"] = "tag"
        >>> spec.input_aliases["name"], spec.reusable
        ('tag', True)


    :ivar entity_name: Human-readable singular name used in matching explanations.
    :ivar table_name: Storage table associated with the repository.
    :ivar id_column: Column holding IDs attached to possible matches.
    :ivar primary_field: Candidate identity field whose value may match any declared scalar field.
    :ivar identity_fields: Ordered fields contributing exact constraints when their supplied values are non-None.
    :ivar scalar_fields: Ordered stored fields searched for a scalar or primary-field value.
    :ivar casefold_fields: Stored text fields compared with case folding after NFKC/whitespace normalization.
    :ivar input_aliases: Public-to-storage field mapping used by the repository; retained by reference.
    :ivar scope_fields: Optional row filters applied when those keys are present in normalized input.
    :ivar scope_defaults: Values inserted for absent keys without replacing explicit None.
    :ivar required_scope_fields: Keys whose input values must be non-None before public matching proceeds.
    :ivar required_identity_fields: Non-None identity keys required for candidate matching, but not scalar exact lookup.
    :ivar policy_field: Single field used for opt-in approximate fallback, or None to disable it.
    :ivar reusable: Repository policy allowing match_or_create reuse; the matcher itself does not enforce this flag.
    :ivar mutable: Repository CRUD permission flag, independent of matcher reads.
    :ivar normalized_storage_fields: Destination/source column pairs derived during repository writes, not required matcher inputs.
    """

    entity_name: str
    table_name: str
    id_column: str
    primary_field: str
    identity_fields: tuple[str, ...]
    scalar_fields: tuple[str, ...]
    casefold_fields: frozenset[str] = frozenset()
    input_aliases: Mapping[str, str] = field(default_factory=_empty_aliases)
    scope_fields: tuple[str, ...] = ()
    scope_defaults: Mapping[str, object | None] = field(
        default_factory=_empty_scope_defaults
    )
    required_scope_fields: tuple[str, ...] = ()
    required_identity_fields: tuple[str, ...] = ()
    policy_field: str | None = None
    reusable: bool = True
    mutable: bool = True
    normalized_storage_fields: tuple[tuple[str, str], ...] = ()


def _normalise_exact_text(value: object, *, casefold: bool) -> str:
    """
    Normalize Unicode and whitespace while retaining punctuation for exact matching.

    Convert through str, apply NFKC, and collapse whitespace runs to single spaces. Case folding is
    optional. Unlike approximate match-text normalization, this preserves punctuation and ampersands
    and can return an empty string.

    Example:
        >>> _normalise_exact_text("  Ａ &  B!  ", casefold=True)
        'a & b!'
        >>> _normalise_exact_text("  Mixed Case  ", casefold=False)
        'Mixed Case'


    :param value: Object whose string representation should be normalized.
    :param casefold: Whether to case-fold after Unicode and whitespace normalization.
    :return: NFKC text with collapsed whitespace and optional case folding.
    """
    text = unicodedata.normalize("NFKC", str(value))
    text = " ".join(text.split())
    return text.casefold() if casefold else text


def _exact_equal(spec: ExactEntitySpec, field_name: str, left: object, right: object) -> bool:
    """
    Compare two strings under field policy, or use ordinary equality otherwise.

    Text operands receive NFKC/whitespace normalization and case folding only when field_name
    belongs to spec.casefold_fields. Non-string or mixed-type operands use their equality operator
    directly, including Python equality between booleans and integers. No missing-value or type
    validation occurs.

    Example:
        >>> spec = ExactEntitySpec("Tag", "tags", "id", "tag", ("tag",), ("tag",), casefold_fields=frozenset({"tag"}))
        >>> _exact_equal(spec, "tag", " Ａ ", "a")
        True
        >>> _exact_equal(spec, "tag", "A&B", "A and B")
        False


    :param spec: Identity configuration supplying the case-folded stored-field set.
    :param field_name: Stored field whose text comparison policy applies.
    :param left: First value to compare without database lookup.
    :param right: Second value to compare without coercion unless both operands are strings.
    :return: Normalized text equality for two strings, otherwise the result of their equality operator.
    """
    if isinstance(left, str) and isinstance(right, str):
        return _normalise_exact_text(
            left,
            casefold=field_name in spec.casefold_fields,
        ) == _normalise_exact_text(
            right,
            casefold=field_name in spec.casefold_fields,
        )
    return left == right


class ExactEntityMatcher:
    """
    Read entity rows using exact identity rules with optional approximate fallback.

    The bound repository supplies alias normalization and row snapshots; spec supplies comparison
    fields, scope, and text policy. Exact comparisons preserve punctuation. Candidate matching can
    report conflicts when supplied identity fields resolve to incompatible rows, while scalar exact
    lookup can only match, miss, or report ambiguity. No method creates or changes entities.

    MatchingPolicy governs final acceptance/ambiguity even without approximate fallback. Approximate
    text comparison is enabled only through use_policy. Construction stores dependencies without
    validating them or rebinding the repository's own configuration.

    Example:
        >>> matcher = catalog.tags.matcher()  # doctest: +SKIP
        >>> result = matcher.exact("Gothic")  # doctest: +SKIP


    :ivar repository: Bound repository exposing normalise_input and _all_rows.
    :ivar spec: Retained identity and scope specification; its nested mappings may remain mutable.
    :ivar matching_policy: Policy for acceptance, ambiguity, and opt-in approximate text cutoff.
    """

    def __init__(
        self,
        repository: Any,
        spec: ExactEntitySpec,
        matching_policy: MatchingPolicy = DEFAULT_MATCHING_POLICY,
    ) -> None:
        """
        Retain the repository, identity specification, and policy by reference.

        No type/schema validation, query, or configuration copying takes place. The supplied policy
        is used by this matcher without changing the repository; the default is the shared frozen
        default-policy instance.

        Example:
            >>> matcher = ExactEntityMatcher(repository, spec, policy)  # doctest: +SKIP
            >>> matcher.repository is repository  # doctest: +SKIP
            True


        :param repository: Owner supplying normalized candidate dictionaries and entity rows.
        :param spec: Identity specification used directly for comparison and explanation.
        :param matching_policy: Selection/approximation boundaries retained for subsequent calls.
        :return: None after assigning the three dependencies.
        """

        self.repository = repository
        self.spec = spec
        self.matching_policy = matching_policy

    def _candidate_data(self, candidate: MetadataCandidate) -> dict[str, object]:
        """
        Normalize candidate aliases, discard its entity ID, and fill absent scope defaults.

        Ask the repository to allow IDs and ignore unknown columns, then remove the configured ID so
        it cannot force identity. Apply defaults with setdefault, preserving explicit None. This
        mutates the repository-returned dictionary; the concrete base repository produces a fresh
        one. Candidate type and field value validity are not independently checked here.

        Example:
            >>> data = matcher._candidate_data(MetadataCandidate({"name": "Gothic"}))  # doctest: +SKIP


        :param candidate: Object supplying candidate.data for the repository's alias/column normalization.
        :return: The normalized mutable dictionary with entity ID removed and absent defaults filled.
        :raises Exception: Candidate access and repository normalization failures propagate.
        """
        data = self.repository.normalise_input(
            candidate.data,
            allow_id=True,
            ignore_unknown=True,
        )
        data.pop(self.spec.id_column, None)
        for field_name, default in self.spec.scope_defaults.items():
            data.setdefault(field_name, default)
        return cast(dict[str, object], data)

    def _scope_is_complete(self, data: Mapping[str, object]) -> bool:
        """
        Check that every required scope field has a non-None input value.

        Missing keys and explicit None fail. Zero, False, and empty strings satisfy this presence
        check; semantic validation belongs elsewhere. An empty required field list returns True
        without inspecting optional scope.

        Example:
            >>> complete = matcher._scope_is_complete({"annotation_item_id": item_id})  # doctest: +SKIP


        :param data: Normalized candidate or scalar-scope mapping to inspect.
        :return: True when all configured required scope fields have non-None values.
        """
        return all(
            data.get(field_name) is not None
            for field_name in self.spec.required_scope_fields
        )

    def _identity_is_complete(self, data: Mapping[str, object]) -> bool:
        """
        Check presence of the non-None fields required for candidate identity.

        This is a required-key/value-presence check, independent of available rows, optional
        identity fields, and value truthiness. Scalar exact lookup does not call it; its single
        value is matched under the scope rules instead.

        Example:
            >>> complete = matcher._identity_is_complete(candidate_data)  # doctest: +SKIP


        :param data: Normalized metadata fields whose required identity entries should be checked.
        :return: True if every required identity field has a non-None value, including when none are required.
        """
        return all(
            data.get(field_name) is not None
            for field_name in self.spec.required_identity_fields
        )

    def _scope_matches(self, row: RowMapping, data: Mapping[str, object]) -> bool:
        """
        Compare configured optional and required scope constraints against a row.

        Optional scope fields constrain only when their keys occur in data; explicit None is
        compared as a value. Required scope fields are always compared, using per-field exact
        equality. This method does not call the completeness check, so two missing required values
        can compare equal when invoked directly.

        Example:
            >>> same_parent = matcher._scope_matches(row, {"genre_parent_id": parent_id})  # doctest: +SKIP


        :param row: Existing entity row to test against the requested scope.
        :param data: Normalized input holding supplied/defaulted scope constraints.
        :return: False on the first unequal scope constraint, otherwise True.
        """
        for field_name in self.spec.scope_fields:
            if field_name not in data:
                continue
            if not _exact_equal(
                self.spec,
                field_name,
                data.get(field_name),
                row.get(field_name),
            ):
                return False
        for field_name in self.spec.required_scope_fields:
            if not _exact_equal(
                self.spec,
                field_name,
                data.get(field_name),
                row.get(field_name),
            ):
                return False
        return True

    def _identity_values(self, data: Mapping[str, object]) -> tuple[tuple[str, object], ...]:
        """
        Collect supplied non-None identity constraints in specification order.

        Ignore absent and None-valued fields while retaining false, zero, or empty values. Neither
        values nor repeated specification fields are deduplicated; this helper does not enforce
        required-identity completeness.

        Example:
            >>> constraints = matcher._identity_values(candidate_data)  # doctest: +SKIP


        :param data: Normalized candidate mapping from which identity fields are selected.
        :return: An ordered tuple of (storage field name, supplied value) pairs.
        """
        return tuple(
            (field_name, data[field_name])
            for field_name in self.spec.identity_fields
            if data.get(field_name) is not None
        )

    def _row_identity_match(
        self,
        row: RowMapping,
        field_name: str,
        expected: object,
    ) -> str | None:
        """
        Find the first stored field satisfying one candidate identity constraint.

        A primary-field constraint may match any scalar field in declared order; every other
        constraint compares only its own stored field. Skip missing/None stored values and apply
        case folding according to the field actually tested. Returned names identify stored evidence
        fields rather than necessarily the original candidate field.

        Example:
            >>> matched_field = matcher._row_identity_match(row, matcher.spec.primary_field, "Gothic")  # doctest: +SKIP


        :param row: Existing row supplying possible stored identity values.
        :param field_name: Candidate storage field whose identity constraint is being tested.
        :param expected: Candidate value to compare against eligible stored fields.
        :return: First matching stored-field name, or None when the constraint is unsatisfied.
        """
        row_fields = (
            self.spec.scalar_fields
            if field_name == self.spec.primary_field
            else (field_name,)
        )
        return next(
            (
                row_field
                for row_field in row_fields
                if row.get(row_field) is not None
                and _exact_equal(self.spec, row_field, expected, row[row_field])
            ),
            None,
        )

    def _exact_results(
        self,
        candidate: MetadataCandidate,
    ) -> tuple[tuple[MatchResult, ...], MatchResult | None]:
        """
        Build complete exact candidates or report a terminal missing-input/conflict decision.

        Normalize input and check required scope, required identity, and at least one supplied
        identity value before retrieving rows. Within scope, every supplied identity constraint must
        match. Integer-ID rows receive confidence one, decisive exact evidence, and the original row
        reference. The integer check admits booleans and does not require positive IDs.

        If no complete row matches, collect the nonempty row-ID sets matched by individual identity
        fields. Conflict requires at least two such sets, an empty common intersection, and more
        than one ID in their union. Fields matching no rows are omitted from that conflict
        calculation. A nonterminal miss returns no candidates and no decision; final
        ranking/ambiguity happens later.

        Example:
            >>> candidates, terminal = matcher._exact_results(candidate)  # doctest: +SKIP


        :param candidate: Proposed entity metadata to normalize and compare within configured scope.
        :return: An (exact-candidate tuple, optional terminal MatchResult) pair; terminal missing-input outcomes are no_match.
        :raises Exception: Input normalization, row retrieval, comparison, and evidence-construction failures propagate.
        """
        data = self._candidate_data(candidate)
        if not self._scope_is_complete(data):
            return (), MatchResult(
                None,
                0.0,
                f"{self.spec.entity_name} matching requires explicit scope",
                decision="no_match",
            )
        if not self._identity_is_complete(data):
            return (), MatchResult(
                None,
                0.0,
                f"{self.spec.entity_name} matching requires complete identity fields",
                decision="no_match",
            )
        identity_values = self._identity_values(data)
        if not identity_values:
            return (), MatchResult(
                None,
                0.0,
                f"no {self.spec.entity_name} identity fields supplied",
                decision="no_match",
            )

        scoped_rows = tuple(
            row
            for row in self.repository._all_rows()
            if self._scope_matches(row, data)
        )
        results: list[MatchResult] = []
        for row in scoped_rows:
            matched_fields = tuple(
                self._row_identity_match(row, field_name, expected)
                for field_name, expected in identity_values
            )
            if any(field_name is None for field_name in matched_fields):
                continue
            row_id = row.get(self.spec.id_column)
            if not isinstance(row_id, int):
                continue
            evidence = tuple(
                MatchEvidence(
                    matched_field,
                    "exact",
                    1.0,
                    1.0,
                    f"exact normalized {self.spec.entity_name} field",
                    expected,
                    row[matched_field],
                    decisive=True,
                )
                for (_candidate_field, expected), matched_field in zip(
                    identity_values,
                    matched_fields,
                    strict=True,
                )
                if matched_field is not None
            )
            results.append(
                MatchResult(
                    row_id,
                    1.0,
                    f"exact normalized {self.spec.entity_name} identity",
                    matched_on=tuple(
                        field_name
                        for field_name in matched_fields
                        if field_name is not None
                    ),
                    candidate=row,
                    evidence=evidence,
                )
            )

        if results:
            return tuple(results), None

        individual_matches: list[tuple[str, set[int]]] = []
        for field_name, expected in identity_values:
            row_ids = {
                row_id
                for row in scoped_rows
                if self._row_identity_match(row, field_name, expected) is not None
                and isinstance((row_id := row.get(self.spec.id_column)), int)
            }
            if row_ids:
                individual_matches.append((field_name, row_ids))
        if len(individual_matches) > 1:
            intersection: set[int] = set(individual_matches[0][1])
            alternatives: set[int] = set()
            for _, row_ids in individual_matches:
                intersection.intersection_update(row_ids)
                alternatives.update(row_ids)
            if not intersection and len(alternatives) > 1:
                return (), MatchResult(
                    None,
                    1.0,
                    f"exact {self.spec.entity_name} identity fields resolve differently",
                    decision="conflict",
                    evidence=tuple(
                        MatchEvidence(
                            field_name,
                            "conflict",
                            0.0,
                            1.0,
                            "exact identity field resolves to another entity",
                            data[field_name],
                            tuple(sorted(row_ids)),
                            decisive=True,
                        )
                        for field_name, row_ids in individual_matches
                    ),
                    alternatives=tuple(sorted(alternatives)),
                )
        return (), None

    def _policy_results(self, candidate: MetadataCandidate) -> tuple[MatchResult, ...]:
        """
        Collect approximate matches for the configured policy field and complete scope.

        Return immediately when policy_field is None. Otherwise normalize input, require a non-None
        policy value and complete required scope, then scan rows within scope. Similarity must meet
        the inclusive approximate-text threshold; accepted integer-ID rows retain one nondecisive
        approximate evidence item. Confidence equals text similarity, even when normalization yields
        exact text.

        This helper does not enforce required identity, acceptance, or ambiguity. Public
        candidate/best paths perform the exact-stage required-input checks before reaching it.
        Stored rows remain referenced, and no data is written.

        Example:
            >>> possible = matcher._policy_results(candidate)  # doctest: +SKIP


        :param candidate: Proposed metadata carrying a comparable policy-field value and required scope.
        :return: Possible matches in repository row order, before final confidence ranking and selection.
        """
        policy_field = self.spec.policy_field
        if policy_field is None:
            return ()
        data = self._candidate_data(candidate)
        expected = data.get(policy_field)
        if expected is None or not self._scope_is_complete(data):
            return ()
        results: list[MatchResult] = []
        for row in self.repository._all_rows():
            actual = row.get(policy_field)
            row_id = row.get(self.spec.id_column)
            if (
                actual is None
                or not isinstance(row_id, int)
                or not self._scope_matches(row, data)
            ):
                continue
            score = text_similarity(expected, actual)
            if score < self.matching_policy.approximate_text_threshold:
                continue
            evidence = (
                MatchEvidence(
                    policy_field,
                    "approximate",
                    score,
                    1.0,
                    f"opt-in approximate {self.spec.entity_name} policy",
                    expected,
                    actual,
                ),
            )
            results.append(
                MatchResult(
                    row_id,
                    score,
                    f"opt-in approximate {self.spec.entity_name} identity",
                    candidate=row,
                    evidence=evidence,
                )
            )
        return tuple(results)

    def candidates(
        self,
        candidate: MetadataCandidate,
        *,
        limit: int = 20,
        use_policy: bool = False,
    ) -> Sequence[MatchResult]:
        """
        List exact candidates, or opt-in approximate candidates after a nonterminal miss.

        Exact matches take precedence. Missing required scope/identity, absent identity values, and
        a detected conflict are terminal outcomes: they return an empty sequence and block approximate
        fallback. Use best to distinguish these outcomes. Approximate candidates pass the text
        cutoff but are not filtered by the final acceptance threshold or resolved for ambiguity
        here.

        The concrete matcher sorts by descending confidence and then ID, using -1 for false-valued
        IDs, before applying the limit. Zero still performs matching and returns an empty tuple; it
        is not a query short circuit. No rows are written.

        Example:
            >>> candidates = catalog.matching.tags.candidates(  # doctest: +SKIP
            ...     MetadataCandidate({"name": "Gothic"}), limit=5,
            ... )


        :param candidate: Proposed metadata; concrete repository alias normalization ignores unknown columns and drops the entity ID.
        :param limit: Maximum results after matching and sorting; use a nonnegative integer.
        :param use_policy: Enable approximate fallback only after an exact miss without a terminal decision.
        :return: Ranked possible matches; the concrete implementation returns a tuple and suppresses terminal decisions.
        :raises ValueError: If limit compares below zero.
        :raises TypeError: If limit cannot be compared or used as a slice bound; booleans currently act as zero/one.
        :raises Exception: Repository input, row retrieval, and comparison failures propagate.
        """

        if limit < 0:
            raise ValueError("limit cannot be negative")
        exact_results, terminal = self._exact_results(candidate)
        if terminal is not None or exact_results:
            results = exact_results
        elif use_policy:
            results = self._policy_results(candidate)
        else:
            results = ()
        return tuple(
            sorted(
                results,
                key=lambda result: (-result.confidence, result.entity_id or -1),
            )[:limit]
        )

    def best(
        self,
        candidate: MetadataCandidate,
        *,
        use_policy: bool = False,
    ) -> MatchResult:
        """
        Return the complete exact-default decision, optionally trying approximate fallback.

        Missing required input and conflicting exact fields return their terminal decision
        immediately. Exact candidates pass through the common acceptance and ambiguity selection.
        Only an exact miss without a terminal decision can invoke approximate matching when
        use_policy is enabled. All eligible rows participate; there is no candidate display limit
        and no persistence step.

        Example:
            >>> result = catalog.matching.tags.best(  # doctest: +SKIP
            ...     MetadataCandidate({"name": "Gothic"}), use_policy=False,
            ... )
            >>> needs_choice = result.requires_resolution  # doctest: +SKIP


        :param candidate: Candidate fields interpreted by the configured entity repository and identity specification.
        :param use_policy: Permit approximate matching after a nonterminal exact miss; defaults to False.
        :return: A match, no_match, ambiguous, or conflict decision with the available explanation/evidence.
        :raises Exception: Repository input, row retrieval, and evidence/decision failures propagate.
        """

        exact_results, terminal = self._exact_results(candidate)
        if terminal is not None:
            return terminal
        if exact_results:
            return decide_best(
                exact_results,
                subject=self.spec.entity_name,
                policy=self.matching_policy,
            )
        if not use_policy:
            return MatchResult(
                None,
                0.0,
                f"no exact {self.spec.entity_name} match",
                decision="no_match",
            )
        return decide_best(
            self._policy_results(candidate),
            subject=self.spec.entity_name,
            policy=self.matching_policy,
        )

    def exact(
        self,
        value: object,
        **scope: object,
    ) -> MatchResult:
        """
        Match one scalar against any configured scalar field within the requested scope.

        Normalize scope aliases strictly, apply omitted scope defaults, and return no_match when
        required scope is incomplete. The concrete matcher compares every eligible row using exact
        Unicode/whitespace normalization and per-field case folding. Any matching scalar field
        qualifies a row; full candidate required-identity checks are not applied on this path. Known
        input fields outside the configured scope do not become additional filters.

        Matches have confidence one and decisive evidence. Common selection resolves duplicates as
        ambiguity. This operation neither tries approximate matching nor writes data, including for
        non-reusable entity types.

        Example:
            >>> result = catalog.matching.genres.exact("Gothic", parent_id=parent_id)  # doctest: +SKIP


        :param value: Scalar identity value compared against every declared scalar field.
        :param scope: Repository field aliases or columns constraining configured scope, such as parent_id or item_id.
        :return: An exact match, no_match, or ambiguous result; this scalar path does not construct conflict decisions.
        :raises Exception: Strict repository normalization, row retrieval, and comparison failures propagate.
        """

        scope_data = self.repository.normalise_input(scope, ignore_unknown=False)
        for field_name, default in self.spec.scope_defaults.items():
            scope_data.setdefault(field_name, default)
        if not self._scope_is_complete(scope_data):
            return MatchResult(
                None,
                0.0,
                f"{self.spec.entity_name} matching requires explicit scope",
                decision="no_match",
            )
        results: list[MatchResult] = []
        for row in self.repository._all_rows():
            if not self._scope_matches(row, scope_data):
                continue
            matched_fields = tuple(
                field_name
                for field_name in self.spec.scalar_fields
                if row.get(field_name) is not None
                and _exact_equal(self.spec, field_name, value, row[field_name])
            )
            row_id = row.get(self.spec.id_column)
            if not matched_fields or not isinstance(row_id, int):
                continue
            results.append(
                MatchResult(
                    row_id,
                    1.0,
                    f"exact normalized {self.spec.entity_name} value",
                    matched_on=matched_fields,
                    candidate=row,
                    evidence=tuple(
                        MatchEvidence(
                            field_name,
                            "exact",
                            1.0,
                            1.0,
                            f"exact normalized {self.spec.entity_name} value",
                            value,
                            row[field_name],
                            decisive=True,
                        )
                        for field_name in matched_fields
                    ),
                )
            )
        return decide_best(
            results,
            subject=self.spec.entity_name,
            policy=self.matching_policy,
        )


__all__ = ["ExactEntityMatcher", "ExactEntitySpec"]
