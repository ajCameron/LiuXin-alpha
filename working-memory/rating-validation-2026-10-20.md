# Rating validation checkpoint, 2026-10-20

A08 is implemented and locally verified on `catalog-rating-validation`, based on PostgreSQL
prerequisite PR #136 (`ce43fccd`), itself based on A07 PR #135 (`f733f76b`).
The full A01–A16 programme remains open. No PR is merged.

The six unrelated staged storage files and dirty data submodule remain untouched.
`/tmp/liuxin-docs-pr-preserved-storage.json` verifies their contents/index.
Commit only the explicit A08 manifest, never all staged paths.

## Implementation

Pure `metadata/ratings.py::normalise_rating_pair` validates nullable zero-based
scores and optional scales. Values must be finite and nonnegative; a supplied
scale is a positive signed-64-bit integer and bounds a supplied score. Numeric
text and Decimal inputs are accepted; bool and empty strings are rejected.
Missing score means unrated; missing scale means unknown. No rescaling or
compatibility display-field derivation occurs.

RatingRepository preparation validates the effective pair on create or changes
to either half, emitting only supplied columns. Reads/unrelated edits preserve
historical data. Match-or-create validates before matching, then validates the
full stored pair before reuse (including matches by source alone). Matching,
creation and validated readback now share a transaction.

BaseRepository's private reference preparation checks a reused table-value-link
destination. ExactEntityRepository allows references to immutable seeded constants
without treating them as edits. RatingRepository checks its whole stored pair.

Ensure.rating delegates to RatingRepository instead of guessing `int(value)+1`
as an ID; Apply.rating preserves fractions and validates existing Rating Rows.
The old ID-based halving of integer inputs is deliberately removed and documented:
8 now means a score of 8, not seed row 9's score of 4. Scale conversion is explicit.
Public compatibility protocol docstrings and SQL schema comments match the rules.

The retained Library importer now wraps metadata-only writes in one macro unit.
Title/book projections and all six subsequent Row lookups use that active
connection, so uncommitted WEMI/Agent/Series rows remain visible. Optional cover/file
publication runs afterward. External publication failure can leave successfully
written metadata; no filesystem rollback is claimed. Borrowed outer transactions
retain their own commit responsibility.

Database rating seed repair now uses macro reads/inserts/updates in one transaction,
so explicit seed inserts advance PostgreSQL identity allocation and a late repair
failure rolls back earlier changes.

## Reproduced failures and prerequisite

The first red test reproduced invalid negative creation. The first broad run was
37 passed, eight skipped, four failed: real importer partial metadata writes and
an incorrect test insertion into immutable languages. A subsequent live-backend
run found seed/identity collisions, visibility failures inside the new importer
transaction, and PostgreSQL's pre-existing DB06 full-schema discovery defect.

DB06 also blocked schema-based Rating writers. It is now fixed separately in
[PR #136](https://github.com/ajCameron/LiuXin-alpha/pull/136), with native name
resolution and removal of the snapshot caches' SQLite-only probes. See the
[prerequisite handoff](postgres-discovery-2026-10-20.md): 81 checks, 21 final
controls/docs checks, and full quality passed (overlapping selections).

PostgreSQL still lacks SQLite's legacy `titles`/`books` compatibility views, so
that one retained importer case is explicitly skipped. Other Rating routes run
against live PostgreSQL. The Language reference control seeds its own English
row there because PostgreSQL does not ship SQLite's language dataset.
Modern `write_container`/`write_values` still reject ratings as an unsupported
field; this change does not add write-back support to those APIs.

## Final validation and next steps

All **29 pure rule cases** and **26 supported integration cases** pass across
SQLite/APSW/live PostgreSQL, plus both existing aggregate-import cases. The one
PostgreSQL retained Library import is explicitly skipped for missing compatibility
views. These were verified in successive focused runs:

- `/tmp/liuxin-a08-verified.log`: 43 passed, one failed. The failure was the rating
  fixture reaching a pre-existing primary-language uniqueness conflict before its
  injected failure. It reproduced on unchanged parent `ce43fccd`; CAT05 tracks it.
- `/tmp/liuxin-a08-remaining.log`: **10 passed, one skipped in 223.44s** after the
  rating fixture omitted that unrelated default language. All remaining backend
  cases and both existing aggregate callers passed.
- `/tmp/liuxin-a08-regressions.log`: **175 passed in 206.56s**, including the final
  29 pure cases (64-bit scale limits and decimal rounding bounds), existing link
  writers, matching, legacy import boundaries, bootstrap repair and Library callers.
- `/tmp/liuxin-a08-final.log`: **47 passed in 53.00s**, including strengthened new
  and replacement importer rollback, Catalog API docs, developer links, public
  documentation, type-runner contracts, hygiene and Core boundary checks.
- `/tmp/liuxin-a08-quality.log`: full quality passed: 195 formatted files, 244
  protected import modules, 1,802 layer modules, 25 cold-import budgets,
  lint/complexity, both type checkers and both 38-example negative contracts.
  Mypy checked 200 source files.

Selections overlap; do not add these counts together. CAT05's parent reproduction
is `/tmp/liuxin-a08-parent-language-conflict.log`. No broad baseline waiver was
introduced. The Rating score/scale validation and bootstrap/import transaction
changes are the A08 slice; further semantic field coverage remains open.

Published as [PR #137](https://github.com/ajCameron/LiuXin-alpha/pull/137), based
on `postgres-link-discovery`. Continue A09 durable job-state/dispatch after its
CI follow-up. Full suites belong in CI.

## CI follow-up

The confidence lane exposed a bootstrap ordering regression when wrapping a
standalone driver: its macros still had `db=None` when transactional rating repair
ran. The existing-driver constructor now binds the driver and macros before
schema discovery/repair; maintenance still starts afterward. All **56 existing
database lifecycle contracts pass** in `/tmp/liuxin-a08-ci-fix.log`.
The isolated follow-up checkout is `/tmp/liuxin-a09-review`, based on `5b07d182`.
The confidence rerun passed **304 tests, three skipped in 311.60s**:
`/tmp/liuxin-a08-confidence-fixed.log`.
New job-dispatch tests in that checkout belong to A09, not this follow-up commit.

The disposable PostgreSQL cluster `/tmp/liuxin-a05-postgres/data` is **stopped**;
all client connections were confirmed closed first. Tests used serial execution,
telemetry and immediate scratch cleanup. C: free space held near 12 GiB.

Parent full CI is green: #133 has 7,438 passed, #134 has 7,476 passed, #135 has
7,487 passed; each has 269 skipped and 17 xfailed. PR #136 full CI is also green.
No PR was merged.
