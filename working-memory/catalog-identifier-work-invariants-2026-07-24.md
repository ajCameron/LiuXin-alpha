# Catalog identifier and Work invariants

Date: 2026-07-24

## Conclusion

The caller-visible Identifier replacement, ownership, rollback, cache
consistency, Work Unicode determinism, and selected generic repository
invariants are now covered over the live schema.

Two defects were found and fixed:

1. `IdentifierRepository.replace_for_wemi()` accepted distinct raw mapping
   keys which normalized to the same logical scheme. It could retain multiple
   primary rows while returning only the last ID under that normalized key.
   The repository now rejects duplicate normalized schemes before opening the
   mutation transaction.
2. `IdentifiersWrite.identifiers()` updated the in-memory cache before the
   authoritative SQL replacement had validated and committed. A rejected
   identifier could therefore leave cache and storage inconsistent. Storage
   replacement now succeeds before cache maps advance.

The runtime-checkable `IdentifierRepositoryAPI` protocol was also brought into
line with the public implementation by declaring Agent assignment, WEMI
replacement, and Agent-owned listing.

## Added invariant lane

`tests/catalog/test_catalog_repository_invariants.py` contains eleven
real-database tests covering:

- duplicate normalized identifier schemes and invalid values leave storage
  unchanged;
- a fault injected after the first replacement assignment rolls back creation,
  assignment, and stale deletion;
- normalized DOI `match_or_create()` is idempotent and retains first-observation
  provenance;
- assigning an owned identifier copies it for the second owner and retains
  provenance while applying owner-specific primary state;
- authoritative replacement and empty replacement delete only the target
  Work's stale identifiers;
- the cache writer changes memory only after storage succeeds, and its success
  path keeps both maps synchronized;
- Work creation is idempotent across NFKC, case, and whitespace variants;
- canonical-title lookup has stable Work-ID ordering and limiting;
- Unicode-equivalent duplicate Works remain explicitly ambiguous and create
  nothing;
- conflicting public/storage aliases are rejected without a write;
- repeated ordered links retain priority, and dangling links raise
  `CatalogNotFoundError`.

## Verification

- invariant lane: `11 passed in 27.04s`;
- focused repository/matching/semantic/aggregate/Unicode regression:
  `43 passed in 298.12s`;
- complete Catalog regression after the production changes and first ten new
  tests: `500 passed in 640.17s`;
- current-source branch-instrumented focused regression:
  `43 passed in 635.86s`, followed by the added DOI idempotency test:
  `1 passed in 50.28s`;
- strict isolated Catalog mypy scope: no issues in `74` source files;
- `py_compile`: passed for all changed production modules and the new test;
- scoped `git diff --check`: passed.

The focused current-source coverage database is:

`/tmp/liuxin-catalog-invariants-cov.CkX7qh/.coverage-focused`

Relevant module coverage:

- Identifier repository: `86%` (previous focused baseline `71%`);
- Work repository: `80%` (previous focused baseline `71%`);
- Base repository: `77%` (previous focused baseline `75%`);
- identifier cache writer: `85%`;
- Identifier matcher: `87%`;
- Work matcher: `88%`.

The only uncovered Work repository statements are invalid title/limit and
blank-or-zero-limit guards. Remaining Identifier misses are likewise malformed
legacy-row and input-validation branches. They should not displace a future
caller-visible operation or corruption invariant merely to increase the raw
percentage.

## Working tree

The intended uncommitted files are:

- `src/LiuXin_alpha/caches/write/identifiers_writer.py`;
- `src/LiuXin_alpha/catalog/api/repositories/identifiers.py`;
- `src/LiuXin_alpha/catalog/repositories/identifiers.py`;
- `tests/catalog/test_catalog_repository_invariants.py`;
- this handoff.
