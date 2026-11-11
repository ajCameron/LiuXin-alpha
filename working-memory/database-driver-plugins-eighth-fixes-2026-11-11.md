# Eighth driver review fixes — 2026-11-11

Completed locally. User authorized fixing DDP28/DDP29. Continue in persistent
isolated clone `/mnt/c/dev/LiuXin-alpha/.tmp/database-driver-review-2026-11-10`,
branch `database-driver-plugins-review-2026-11-05`, HEAD `bab2b682` plus the
sixth/seventh/eighth fixes. Changes are staged, uncommitted. No push or merge.
The clone's origin points to the original local checkout, not GitHub.

Read the [implementation and original findings](../dev-docs/databases/database-driver-plugins-eighth-review-2026-11-11.md).

- DDP28: reserved function prefixes, DISTINCT ON group context and window frame
  contexts recognize bindings while bare ROWS/RANGE/GROUPS remain JSON columns.
  Registered-driver single/batch commits match native expression results.
- DDP29: `postgres_standard_strings` reads native ParameterStatus without SQL or
  transaction effects. Execution, standalone cursors, script splitting, runtime
  guards and borrowed handlers honor the connection's current string mode.
  Reused cursors follow SET LOCAL, savepoints, rollback and commit. Lazy hooked
  scripts follow setting changes between statements; native unhooked scripts
  still execute whole. Real chaining remains rejected and prompt effects roll back.
- All prior 1,180 required cases retain their invariant membership. Added 147:
  expression contexts 66, string-mode preservation/ownership/maintenance 81.
  Required total: 1,327. All 147 new cases pass setup/call/teardown.

Final validation:
- Mandatory gate: **1,327/1,327** required, failures=[], exit 0; **1,358 selected
  tests passed**, zero skips, in 347.08 seconds.
- Initial focused green: 145 passed in 15.58 s. Initial red: 69 failures and 76
  passing controls. Two further real-chaining cases joined the final gate.
- Native keyword sweep: 742 comparisons, zero mismatches; 100 identifiers
  rejected by the native engine are excluded.
- Full quality passed in 251.15 s: both type checkers (458 mypy sources), 95/93
  invalid-call probes, format/lint, annotations, imports/layers and complexity.
- Maintenance/callback/transaction integration: **285 passed, 29 backend skips**,
  150.05 s. Both SQLite and live PostgreSQL variants ran.
- **121** native-driver/adapter doctest examples and **85** documentation/runner
  checks passed (the latter in 94.16 s).
- Final source, protected original preference/source, old worktree index,
  recovery-patch and 19 recovered-file hashes all match. Earlier SQLite fixes
  remain unchanged. Final validation recorded zero OS OOM kills.

The first full gate had 1,356 passes and two failures in the extended lifecycle
fixture: it issued SET LOCAL outside a transaction after raw SQL ROLLBACK.
Native PostgreSQL also left the mode unchanged and rejected the next literal.
An explicit BEGIN corrected that fixture while retaining the commit-restoration
assertion. Both corrected cases passed, followed by the complete final gate.
No implementation change or weakened assertion was needed. The first quality
attempt's formatting-only failure was corrected before its passing rerun.

Private PG16 `/tmp/liuxin-driver-eighth-fixes-pg` is STOPPED AND REMOVED.
All validation processes are finished; there are no handles to resume. Current
results supersede the failed first gate and obsolete running notes. No full
repository suite or fresh remote CI is claimed; require fresh CI after publication.

Persistent evidence: `/mnt/c/dev/LiuXin-alpha/.tmp/driver-eighth-fixes-2026-11-11/`.
Key files: baseline-manifest.json, new-cases.json, red.log, focused.log,
quality-final.log, lexer-sweep-results.json, ratchet.log, lifecycle-debug.log,
lifecycle-fixed.log, ratchet-final/ratchet.json, new-case-coverage.json,
integration.log, tooling.log, doctests-results.json, tested-source.json,
checks-complete.json, preservation-checks.json, resources-summary.json,
validation-summary.json, final-changes.patch, pg-server.log and pg-cleanup.json.
The failed gate's source snapshot is tested-source-before-lifecycle-correction.json.

Preserve the original checkout's source/preferences/data, old worktree index and
persistent WSL recovery backup. Only its working-memory index points here. The
prior staged fixes and review docs remain included. No agents or skills used.
