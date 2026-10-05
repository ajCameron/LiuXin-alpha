# Ten-unit database-test documentation batch: D169–D178 complete

The explicit request for **ten more units** authorized **D169–D178**, overriding the
usual one-unit limit for this bounded batch. All **256 selected declarations across
25 files** are verified: **24 complete files promoted** (221 declarations) and
**35 declarations** in the first locking-test slice. Work stopped after D178.
**D179 is next**: the remaining 19 declarations in `tests/databases/test_locking.py`.
Earlier broader authorizations remain paused; a new request is required to continue.

| Unit | Declarations |
|---|---:|
| D169 | 39 |
| D170 | 11 |
| D171 | 33 |
| D172 | 26 |
| D173 | 24 |
| D174 | 39 |
| D175 | 26 |
| D176 | 17 |
| D177 | 6 |
| D178 | 35 |

This batch covers database vocabulary tests, preferences and fingerprints, WEMI
aggregate projections, generated link columns/cardinality/type registries, language
lookup and seeding, storage-schema checks, strict TOML validation, schema metadata,
blank-row insertion, low-level SQL generation and shared/exclusive locking tests.
Docs explain fixture ownership, pending writes, helper return shapes, expected
exceptions, parser aliases and the exact scope of each assertion.

Coverage limits are explicit: required-member checks are not exhaustive enum checks;
annotated runtime assignments do not establish static typing; one publisher does
not exercise tie-breaking; type-guard tests execute inserts but only inspect update
trigger names; skipped role-type checks also bypass later priority assertions;
thread wait/join results are not all asserted. SQL/TOML fixture strings, production
behavior and existing test assertions are unchanged.

Verification:

- All 256 selected declarations pass documentation audit and normalizer checks.
  All 24 promoted files pass complete-file checks. The locking file remains partial;
  all 19 D179 docstrings are unchanged. Executable AST, signatures, comments, SQL,
  TOML and other non-docstring literals are unchanged across all 25 files.
  No added Ruff findings or whitespace errors; original/maintenance hashes retained.
- D169 passed selected static checks, 38 runnable example statements and 30
  prerequisite regressions before D170 proceeded. Saved hashes prove D170 began
  from the verified slice. The 30 prerequisite reruns are excluded from final totals.
- **141 regressions passed**, **1 skipped**, **0 expected failures**: schema and
  vocabulary cases 96 passed; driver/legacy/language integration cases 25 passed;
  selected locking cases 20 passed. SQLite and APSW were explicitly selected where
  parameterized. The skip is the conformance case for requested_cols='all', since
  current TOML entries do not use that setting.
- **180 runnable example statements passed**, with **no doctest skips**. Fixture
  and concurrency tests provide pytest invocation examples where direct execution
  would require fixture or thread setup; those shell examples are not counted as
  executed doctests. Runnable examples cover pure vocabulary assertions, schema
  inspection, TOML normalization, registry expansion and lock wrapper behavior.
- Full configured quality runner passed: 156 formatted files, 454 annotation-checked
  files, 215 protected modules, zero basedpyright errors, mypy 182 files and both
  type-contract checkers rejecting all 37 invalid examples. Two examples initially
  omitted SafeReadLock's existing context-exit diagnostic tuple. Their expected
  output was corrected after regression/quality jobs started. Saved before/after
  hashes prove only those two expected-output lines changed: executable AST,
  comments, example statements and other doc fields are identical. Final static
  checks and all examples passed against the corrected source.
- Discovery still finds **2,671 source files**, with no added or missing paths.
  **22 out-of-scope storage files** differ from historical reviewed hashes. Their
  edits and historical review records are preserved without counting a new review.

Coverage: **952/2,671 complete-file review records**, **13,495 declarations** in
those files, plus **72 partial-file declarations** (37 existing search declarations
and 35 locking declarations). Remaining queued scope: **1,719 files and 28,594
declarations**. D progress: **178/232 units**, **4,302/5,685 declarations**,
**266/374 complete files**. Across all tracks: **210 documentation units verified**,
**1,162 remaining**. No commit or publication requested; no dependencies installed.
Token usage unavailable.

- [Exact observations and commands](test-results/docstrings-d169-d178-2026-09-24/observations.json)
- [Static proof and partial-file boundary](test-results/docstrings-d169-d178-2026-09-24/static.json)
- [Examples](test-results/docstrings-d169-d178-2026-09-24/examples-all.json)
- [Expected-output correction proof](test-results/docstrings-d169-d178-2026-09-24/example-output-review.json)
- [Schema/vocabulary regressions](test-results/docstrings-d169-d178-2026-09-24/schema.log)
- [Driver/legacy/language regressions](test-results/docstrings-d169-d178-2026-09-24/drivers.log)
- [Selected locking regressions](test-results/docstrings-d169-d178-2026-09-24/locking.log)
- [Quality log](test-results/docstrings-d169-d178-2026-09-24/quality.log)
- [Scope and reviewed-hash discovery](test-results/docstrings-d169-d178-2026-09-24/discovery.json)
- [Inventory reconciliation](test-results/docstrings-d169-d178-2026-09-24/reconciliation.json)
- [Documentation diff](test-results/docstrings-d169-d178-2026-09-24/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
