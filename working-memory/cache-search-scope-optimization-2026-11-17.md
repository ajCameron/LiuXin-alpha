# Candidate and definition optimization — 2026-11-17

The user returned to cache work. The earlier three local cache commits remain
`b0a27bd7`, `f412808a`, `c1d2cb74` on `caches-calibre-compat-boundary`; this follow-up
is uncommitted. Preserve the unrelated architectural-review README row, untracked
file-formats review and `Claude outputs/`, dirty data submodule and six stashes.

## Implemented

- Complete `CalibreBookRef` reverse index for candidate requests, candidate sorting
  and template-database result conversion.
- Reuse the native engine's immutable full scope and public frozen result sets.
  Validation, definition invalidation, dynamic bypass and template permission
  checks remain before cache hits.
- Keep the fixed engine projection across definition changes; cache inferred
  descriptors and stop at the first non-null value during initial inference.
  Initial adapter/native defensive row copies remain separate.
- Fix rejected alias revisions corrupting the last valid metadata: validate the
  new aliases before publication, including exact-state restoration after failure.

Only the LiuXin-owned compatibility adapter changed at runtime. No native cache,
database, Catalog, upstream Calibre or licence files changed.

## Evidence

- Red focused selection: seven failures, two passing controls.
- Corrected adapter and adapter-stress selection: **71 passed**, 16.59 s.
- Nine new required cases in `calibre-candidate-and-definition-work-bounds`;
  manifest increases 2,637 → **2,646** with earlier memberships preserved.
- Matched benchmark at 1k/10k/100k records, five repeats, isolated scale processes.
  At 100k: warm ten-candidate query 22.636 → 0.0146 ms; definition edit/search
  951.982 → 3.849 ms. Refresh peak traced allocation 63,438,350 → 87,244 bytes.
  Reverse index adds about 5 MiB retained at construction. Cold whole-projection
  predicates, native sorting/pagination and small invalidation remain separate.
- [Review and reproducible benchmark](../dev-docs/caches/metadata-search-scope-optimization-2026-11-17.md)
  links raw before/after samples and hashes. The benchmark source is identical
  across the two recorded runs; after hashes match the final runtime source.

Local evidence:

- `/tmp/liuxin-search-scopes-before.json`, `-after.json` and corresponding `.log`.
- `/tmp/liuxin-search-scopes-red.log`, `-focused.log`.
- `/tmp/liuxin-search-scopes-acceptance/`: selected adapter/native-predicate/Catalog
  tests plus the full 10k-record, 1k-step Calibre soak, source hashes and phase gate.
  Harness `/tmp/liuxin_run_adapter_acceptance.py`; log sibling `-acceptance.log`.
- `/tmp/liuxin-search-scopes-quality.log`: standard complete quality runner.
- `/tmp/liuxin-search-scopes-tooling.log`: docs, ratchet and formatting tooling.

## Final validation

- Acceptance: **122 passed, four expected capability skips**, 472.77 seconds.
  All **102/102 required cases** passed setup/call/teardown; gate has no failures.
  The four skips are native live-versus-snapshot controls, not missing drivers.
- Full Calibre soak: **passed**, 10,000 records and 1,000 steps; 1,000 candidate
  and 1,000 warm operations recorded. Case time 163.4 seconds. This overlapped
  quality checks and is not a matched performance comparison with the earlier run.
- SQLite/APSW Catalog integration passed. No PostgreSQL cases were selected and
  no PostgreSQL server was started. Other long lanes and the complete database
  regression gate were not rerun for this adapter-only runtime change.
- Complete standard quality runner: **passed**. Format409, lint, annotation
  coverage, architecture/import checks, both type checkers and negative examples.
- Docs/ratchet/format tooling: **64 passed, one deselected**. The selection excludes
  the tracked-content documentation test because the new documents and the user's
  pre-existing review draft are untracked. A separate working-tree link audit
  verifies existing destinations without treating this as commit validation.
- Baseline manifest memberships retained; benchmark after hashes match working
  runtime sources; whitespace check passed. Test counts above overlap.

No staging, new commit, push or PR. Whole-cache lifecycle/registry sign-off and
the remaining search-parity/Windows/ICU limitations are unchanged.
