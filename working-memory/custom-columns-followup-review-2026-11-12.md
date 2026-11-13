# Custom-column follow-up review and repairs — 2026-11-12

## Authorized repairs complete

The user subsequently asked to fix all six findings. CC07–CC12 are now repaired
and locally validated. The work remains uncommitted on `main` at `a88a5230`.
No commit, push, PR or production catalogue upgrade was requested.

- Retirement requires explicit version-two storage before setting the flag.
- Wrapper/Core native metadata edits use migration validation and revisions.
- Facade metadata edits and native retirement reject borrowed publication;
  retained facades consult the live storage version after upgrades.
- All incoming compatibility text-set additions/renames reject delimiters.
- Bulk removal cleans inputs and compares NFC/case-folded identities; additions win.
- SQLite audit and repair share complete canonical cleanup/reference trigger DDL.

Final validation:

- **2,101 passed** in one complete database gate, with **1,990/1,990 mandatory
  cases** passing setup/call/teardown, no skips/failures and ten existing
  deprecation warnings. Duration: 1,422.04 seconds.
- **128 new mandatory cases** (120 SQLite/APSW and eight PostgreSQL) retain all
  prior 1,862 cases; manifest comparison also passed against HEAD.
- Full quality passed: both type checkers and internal contracts, lint,
  formatting, annotations, import cycles/direction/footprint and complexity.
- Core/tooling integration: **93 passed**. Documentation: **16 passed** using a
  disposable index and object store. `git diff --check` passed.

Evidence: `.tmp/custom-columns-followup-fixes-2026-11-12/`. Read `ratchet.json`
and `ratchet.log`, `quality.log`, `integration.json`, `documentation.json`, and
`completion.json`. `ratchet-telemetry.tar.gz` preserves the complete UTF-8 run
and interrupted ASCII run. `before/` and baseline JSON preserve the 65
pre-existing changed files plus index/data-submodule state. All 55 captured
source/test/gate fingerprints matched after the final gate.

The first full run was interrupted because the private PostgreSQL database was
initialized with SQL_ASCII. The 80 resulting driver Unicode failures all passed
on a separate UTF-8 database (`unicode.json`), then the entire gate was rerun
successfully. This was test setup, not a repository code change. Earlier focused
harness errors (cacheless property access and scalar getter) remain in the
`first`/`second` logs; corrected cases passed in the final gate.

All checks have finished. The temporary PostgreSQL16 server under
`/tmp/liuxin-cc-followup-pg/` was stopped successfully after validation. Its corrected database was `liuxin_followup_utf8` on
`127.0.0.1:55439`; do not assume this temporary server survives a restart.
Preserve all earlier implementation work, the real Git index and the dirty
`LiuXin_alpha_data` submodule. No required repair work remains.

## Original review evidence


The user requested another review after crash recovery. Reviewed the current
uncommitted native implementation and earlier custom-column repairs on `main`
at `a88a5230`. No implementation fixes, commit, push or PR were requested.

Read [the full review](../dev-docs/databases/custom-columns-followup-review-2026-11-12.md).
Six new findings remain open, all reproduced on SQLite and SQLite_apsw:

1. **CC07 / P1:** native retirement accepts legacy storage; the next legacy
   loader permanently purges the supposedly retired definition and values.
2. **CC08 / P2:** wrapper and existing Core metadata updates bypass native
   migration validation/revisions, accepting enum changes that make live
   values unreadable. The public Core command reports success.
3. **CC09 / P2:** the upgraded facade metadata-edit branch bypasses the
   ownership refusal and publishes maps/notifications before outer rollback.
4. **CC10 / P2:** legacy rename and bulk-add accept delimiter-bearing text-set
   values, commit, then fail during compatibility projection; later reads fail.
5. **CC11 / P2:** upgraded bulk removal compares raw spelling instead of
   identity; removing `READY` leaves `Ready`, unlike the version-one control.
6. **CC12 / P2:** audit misses absent vocabulary cleanup/reference guards and
   ineffective owner triggers; it reports a clean schema before deletes orphan
   memberships.

Existing focused suite: **296 passed**, with eight known datetime adapter
deprecation warnings. Review probes: **18 passed**, asserting the defects and
controls through setup/call/teardown. Documentation/navigation: **16 passed**,
using a disposable index/object store. `git diff --check` passes; the real
index hash and data-submodule status are unchanged. All checks have finished.
No fresh PostgreSQL, hosted CI or complete
ratchet run. Earlier implementation signoff remains historical test evidence,
not a claim that these newly reproduced findings are fixed.

Evidence: `.tmp/custom-columns-followup-review-2026-11-12/`. Read `existing.log`,
`probes-verified.log`, their JSON reports and completion markers. The earlier
`probes-final.log` includes an empty-preference-store harness error; the final
verified Core probe uses normal isolated preferences and returns success.
`test_review_probes.py` contains disposable generated-catalogue reproductions.
`baseline-fingerprints.json` and `baseline-state.json` preserve the reviewed
snapshot. Do not turn defect-asserting probes into permanent regression tests
without reversing the assertions to the desired repaired behavior.

This turn adds review/handoff documents and their navigation entries only.
Preserve all implementation changes, the real Git index and the dirty
`LiuXin_alpha_data` submodule. Repair work awaits a user request.
