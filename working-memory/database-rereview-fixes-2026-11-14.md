# Independent database re-review fixes — ready for user review

Implementation and local verification completed on 2026-11-15. The goal was to
implement the suggested changes and let the user review. No new commit was
requested or made. HEAD remains `5d5c0c88fb8f9e9e4a3ff14ef15c79b4b27806ba`.
No test process or private PostgreSQL server remains running.

## Implemented

- RR06: native base schema revision 1, written by generators and validated before
  normal driver access/bootstrap on SQLite, SQLite_apsw and PostgreSQL. All
  unversioned pre-release catalogues require regeneration, including 5d5c0c88
  files. Private creation/backup/restore targets have explicit preparation and
  validation boundaries. Dynamic custom-field DDL does not change the base marker.
- RR03: keep the policy/Unicode guard; show recorded/current stamps and an actual
  interpreter/path recovery command. Add the explicit rebuild_search_indexes CLI,
  which validates the base revision before rebuilding indexes. Linux recovery was
  executed; PowerShell quoting was source-checked only.
- RR05: refuse single/bulk pruning across six known undeclared references. Bulk
  refusal rolls back previous deletions. Adding real FKs and choosing their
  deletion policies remains a separate decision; no guessed cascade was added.
- RR04: infrastructure ownership and API annotation repaired; owner/custom-field
  fixtures repaired; gate now runs all tests/databases while retaining the strict
  mandatory manifest (2,314 retained + 87 added = 2,401). Driver fixtures now use
  explicit format metadata. Rebinding tests use a generated but damaged schema.
- Keep current-format identity-profile migration, the lossy-profile guard and
  explicit preference-key repair. None admits unsupported catalogue formats.

## Verification and limits

The complete database run finished with 5,000 passed, 90 skipped, two failed,
eight setup errors and 26 marker/deprecation warnings in 5,559 seconds. All 2,401
mandatory cases passed setup/call/teardown without skips or xfails. The only ten
unsuccessful cases were test_sqlite_progress.py's empty :memory: fixtures. They
were changed to a minimal explicitly versioned SQL fixture; all ten cases passed
as a separate module run. No production code changed after the complete run
began. The full suite was not repeated after this isolated fixture-only repair;
do not call the raw full gate green (its exit code is 1).

The full quality runner passed formatting, annotations, lint, architecture/import
checks and both type checkers. The supplementary fixture also passes lint and
formatting; its formatting AST matches the tested version. Tooling/documentation:
120 passed, one unrelated existing tracked-link failure for the user's untracked
file-formats review. The unnamed order-dependent error from the external review
was not reproduced. Actual July catalogue copies were refused by both SQLite
names without changing their bytes. Private PostgreSQL shutdown was verified
with pg_ctl status exit 3 (no server running).

All 33 frozen inputs match the workspace and the Linux snapshot used for the
complete run. The supplementary progress fixture is the sole 34th changed
implementation/test/config input, with separate execution and AST proof.

## Evidence and review handoff

- Authoritative contracts/results: dev-docs/databases/review-repairs-2026-11-14.md,
  independent re-review follow-up and November 15 verification.
- Review disposition: final follow-up in
  dev-docs/architectural_reviews/databases-review-2026-11-14.md and its index.
- Local machine-readable audit: .tmp/database-rereview-fixes-2026-11-14/
  final-validation-summary.json, final-source-audit.json, completion-audit.json,
  final-gate-source-fingerprints.json and post-gate-progress-fixture.json.
- Full run: /tmp/liuxin-rr-final-gate.log and
  /tmp/liuxin-database-rereview-final-2026-11-14/ratchet.json. Only current telemetry
  events-controller-615841-35685dbf.jsonl belongs to that completed run. Older
  interrupted telemetry is archived separately and is not passing evidence.
- Supplement: /tmp/liuxin-rr-progress-fixture.log and
  /tmp/liuxin-rereview-progress-fixture-2026-11-15/.
- Quality: /tmp/liuxin-rr-quality3.log. Tooling: /tmp/liuxin-rr-tooling.log.
- Chronological investigation notes were preserved in the ignored local artifact
  working-memory-history.md. Do not resume their obsolete test sessions.

Pre-existing user work was preserved: dirty LiuXin_alpha_data, review/index edits,
and untracked dev-docs/architectural_reviews/file-formats-top-level-review-2026-11-14.md.
Changes remain uncommitted for the user's review. Do not stage unrelated work or
start another full test run merely to hide the explicitly recorded result split.
