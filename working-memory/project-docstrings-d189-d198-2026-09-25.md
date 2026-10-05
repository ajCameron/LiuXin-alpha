# Ten-unit cache and Database contract documentation batch: D189–D198 complete

The explicit request for **ten more units** authorized **D189–D198**. All
**290 declarations across 24 files** are verified and all 24 complete files are
promoted. Work stopped after D198. **D199 is next**, with 40 declarations in
`test_db_dirty_queue_and_maintenance.py` and `test_db_fuzz_unicode_and_injection_slow.py`.
A new request is required to continue; earlier broader authorizations remain paused.

| Unit | Declarations |
|---|---:|
| D189 | 35 |
| D190 | 22 |
| D191 | 40 |
| D192 | 31 |
| D193 | 20 |
| D194 | 32 |
| D195 | 18 |
| D196 | 15 |
| D197 | 39 |
| D198 | 38 |

The batch covers legacy Calibre cache harnesses and bootstrap/category tests,
active field semantics, the modern cache facade, schema-backed invalidation and
relation updates, writer imports/dispatch, Database import and temporary-file
initialization, shared contract fixtures, FRBR adders, bootstrap repairs, CRUD,
Unicode/search behavior, and custom-column schema/lifecycle contracts.

Docs describe parameter meanings, exact return shapes, ownership, mutations,
conditional branches, and observed assertions. Notable corrections include:
file-backed temporary databases were previously described as in-memory; teardown
that suppresses close exceptions does not guarantee cleanup; table classification
does not prove insertability; highest-ID helpers assume isolated inserts; and
several tests check subsets or result shapes rather than every value implied by
their names. The many-to-one field test’s conditional-expression precedence only
compares the creators value; the series branch is a truthy literal. The multilingual
search test checks membership in the inserted ID set, not a per-term ID pairing.
These are documented limits of existing assertions, not behavior changes.

Verification:

- All **290 declarations in 24 complete files** pass documentation audit and
  normalizer checks. Executable AST, signatures, comments, SQL, embedded subprocess
  source, and other non-docstring literals are unchanged. No added Ruff findings
  or whitespace errors. Original and maintenance baseline hashes are retained.
- **331 regressions passed**, **12 skipped**,
  **0 expected failures**. Cache group: 99 passed,
  12 skipped. Database group: 232 passed,
  0 skipped. SQLite and APSW were explicitly selected
  for parameterized database fixtures. Exact skip reasons are retained in the logs.
- Six legacy integration modules, numbered 00 through 05, retain their existing
  default module-level skip under the FRBR-first schema. The legacy opt-in
  environment variable was unset. Their full integration behavior was not run.
  The 06 and 07 field-semantic modules remain active. Capability-specific facade
  checks also retain their existing skips for incompatible storage plugins.
- **119 runnable example statements passed**, with no doctest skips. Of these,
  **92** exercised explicitly selected, reviewed standalone helper definitions
  extracted from the six gated files’ ASTs; those files were not imported to
  bypass their gates. **27** ran against normally imported selected modules.
  The extraction harness registers isolated module metadata for dataclass support.
  Pytest command examples for fixture-dependent checks are not counted as executed
  doctest statements. Import-time optional Magick/circular-import fallback messages
  were captured; the active examples and regression commands completed successfully.
- Full configured quality runner passed: 156 formatted files, 454 files checked
  for annotation coverage, 215 protected modules checked for dependency rules,
  zero basedpyright errors, mypy across 182 files, and both type-contract checkers
  rejecting all 37 invalid examples. Final source hashes match the verification
  runs for every scoped file apart from five fixture CLI example paths corrected
  after regression jobs started. Saved before/after source proves those paths are
  the only changes: executable AST and comments are identical. Complete-file static
  checks and all 119 examples passed again. The corrected pytest command also
  passed four fixture-consuming checks; these reruns are excluded from the totals.
- Source discovery remains **2,671 files**, with no new or missing paths.
  **24 out-of-scope storage files** differ from historical reviewed hashes.
  Since the preceding checkpoint, `storage_manager_api/derivations_api.py` adds
  a drift entry and `storage_manager_api/convenience_api.py` has a different
  working hash. These unrelated files and their historical review records were
  left untouched and were not counted as new reviews.

Coverage is **992/2,671 complete-file review records**, containing **14,091
declarations**, plus **37 existing partial-file declarations**. Remaining scope:
**1,679 files and 28,033 declarations**. D progress: **198/232 units**,
**4,863/5,685 declarations**, **306/374 complete files**. Across all tracks:
**230 documentation units verified**, **1,142 remaining**. No commit or publication
was requested; no dependencies were installed. Token usage is unavailable.

- [Observations and exact commands](test-results/docstrings-d189-d198-2026-09-25/observations.json)
- [Static proof](test-results/docstrings-d189-d198-2026-09-25/static.json)
- [Examples and execution modes](test-results/docstrings-d189-d198-2026-09-25/examples-all.json)
- [Fixture command correction proof](test-results/docstrings-d189-d198-2026-09-25/fixture-example-correction.json)
- [Fixture command recheck](test-results/docstrings-d189-d198-2026-09-25/fixture-command.json)
- [Runtime doc references and legacy gates](test-results/docstrings-d189-d198-2026-09-25/runtime-doc-review.json)
- [Cache regressions](test-results/docstrings-d189-d198-2026-09-25/cache.log)
- [Database regressions](test-results/docstrings-d189-d198-2026-09-25/database.log)
- [Quality checks](test-results/docstrings-d189-d198-2026-09-25/quality.log)
- [Discovery and hash drift](test-results/docstrings-d189-d198-2026-09-25/discovery.json)
- [Inventory reconciliation](test-results/docstrings-d189-d198-2026-09-25/reconciliation.json)
- [Documentation diff](test-results/docstrings-d189-d198-2026-09-25/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
