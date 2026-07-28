# Catalog matching policy implementation

Date: 2026-07-22
Status: complete

## Objective

Implement the policy in `dev-docs/catalog-matching-policy.md` without
breaking the existing `candidates()`, `best()`, `exact()`, repository
`match()`, and `match_or_create()` entry points.

## Delivered

- add explicit decision and evidence types to the public catalog API;
- add catalog-specific ambiguity/conflict errors;
- centralize normalization and ranking in the matching package;
- make Work and Agent matching conservative and ambiguity-aware;
- make identifier matching scheme-aware;
- add one spec-driven exact-default matcher and repository contract for Tags,
  Labels, Genres, Subjects, Series, Languages, Ratings, Comments, Synopses,
  Notes, and Annotations;
- keep approximate value matching off unless `use_policy=True` is explicit;
- add scheme-aware matching for raw Item identifier observations;
- preserve schema ownership: publishers remain organization Agents, and Agent
  subtype rows do not acquire separate identity;
- route repository matching through the same policy;
- block automatic creation for ambiguous or conflicting matches;
- cover pure policy rules and real relationship-backed evidence.

The generic average-field scorer has been removed from catalog repositories.
One immutable `MatchingPolicy` is shared by `Catalog.matching` and repository
entry points. `MatchResult` remains positional-call compatible while exposing
explicit decisions, evidence, alternatives, and resolution state.

## Verification

- focused strict mypy check: the 14 new or directly changed exact-entity source
  files passed with imported legacy modules skipped;
- focused basedpyright check: no source errors;
- original matching policy behavior: 14 tests passed;
- extended matching, semantic, and public-import lane: 84 tests passed in
  127.42 seconds;
- updated matching example: live temporary-database run passed;
- complete catalog regression suite: 391 tests passed in 825.07 seconds on the
  final tree.

## Later slices

- source-specific trust profiles, if demonstrated by ingest data;
- richer transliteration and language-aware title normalization;
- indexed candidate generation when catalog scale makes full scans material;
- optional human-review queues built above the catalog decision contract.
