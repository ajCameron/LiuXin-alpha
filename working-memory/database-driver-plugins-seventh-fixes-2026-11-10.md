# Seventh driver review fixes — 2026-11-10

Completed locally. User authorized fixing all three findings and adding
regressions across the applicable drivers. Continue in persistent isolated clone
`/mnt/c/dev/LiuXin-alpha/.tmp/database-driver-review-2026-11-10`, branch
`database-driver-plugins-review-2026-11-05`, based on `bab2b682` plus the recovered
sixth-review patch. Changes are staged, uncommitted. No push or merge.
`origin` points to the original local repo, not GitHub.

Implementation and original evidence:
[seventh review](../dev-docs/databases/database-driver-plugins-seventh-review-2026-11-10.md).

- DDP25: shared SQLite serialization preserves accessible hidden row IDs,
  including external-content FTS identities. True primary-key aliases,
  WITHOUT ROWID tables and generated/shadowing columns retain their behavior.
  Unaddressable IDs refuse before replacement; export never ends borrowed work.
- DDP26: continued PostgreSQL strings retain their original escape mode across
  CR/LF and line comments; adaptation, script splitting and maintenance see the
  same opaque token. A trailing-comment backtracking issue found during the
  fix is repaired and guarded by four bounded subprocess/native comparisons.
- DDP27: the four bare keyword JSON operands remain operands while actual
  BETWEEN/ESCAPE/SET/VALUES grammar works, including NOT, SYMMETRIC/ASYMMETRIC
  and nested VALUES tuple bindings.

Mandatory manifest: 959 -> 1,180, retaining every prior invariant membership.
Added 221: 56 SQLite value/FTS cases, 124 PG adaptation cases, five completion
cases and 36 portable text/binary/commit/rollback cases across SQLite,
SQLite_apsw and PostgreSQL. These are driver-name contracts: ordinary
SQLite_apsw connections still use stdlib sqlite3; native APSW shell rebuilds
are exercised by its real export/rebuild tests. Format/lint and database typing
coverage include the changed/new modules.

Final validation:
- Mandatory gate: 1,180/1,180 required cases passed every phase, failures=[],
  exit 0. All 1,211 selected tests passed, zero skips, in 609.79 s. The runner
  compares against bab2b682; a separate baseline check retains the recovered 959.
- New focused selection: 217 passed; final scanner selection: 70 passed,
  including the four additional trailing-comment cases.
- Native keyword sweep: 742 comparisons, zero mismatches; 100 names rejected
  natively are excluded. All 121 driver/adapter doctest examples pass.
- Full quality passes: formatting/lint, annotations/imports/layers/complexity,
  basedpyright and mypy (458 sources), plus 95/93 invalid-call probes.
- Documentation/runner checks: 58 passed in 182.16 s.
- Affected wrapper/schema/transaction integration: all 177 test calls passed,
  with one shared-lock-pool error during schema cleanup in 1,062.23 s. Its exact
  case reran alone: one passed in 11.36 s, including cleanup. No skips.
- Staged/unstaged whitespace and source-preservation checks pass.

Validation issues are retained, not erased: the initial red run reproduced the
reviewed bugs and contained two repaired test-fixture errors. A bounded test
then reproduced the first scanner pattern's trailing-comment stall. The initial
1,176-case gate passed 1,207 test calls but failed one teardown while overlapping
integration dropped another large schema. Both teardown errors were PostgreSQL
`out of shared memory`, with max_connections=16 and max_locks_per_transaction=64.
The complete final gate passed; the integration cleanup retry passed without
other database tests running. No assertions or gate rules were weakened.
Use serial large-schema lanes or a larger lock pool for future local runs.

Private PG16 `/tmp/liuxin-driver-seventh-fixes-pg` is STOPPED AND REMOVED.
All validation processes are finished. Final-gate telemetry: 90.5 MiB peak
test-process-tree RSS, at least 5.60 GiB available memory, zero OS OOM kills.
The three protected original preference/source files, old worktree index,
recovery patch and all 19 recovered files remain unchanged. Preserve them.

Persistent evidence: `/mnt/c/dev/LiuXin-alpha/.tmp/driver-seventh-fixes-2026-11-10/`.
Key files: baseline-manifest.json, new-cases.json, red.log, focused.log,
comment-red.log, comment-green.log, ratchet-final.log, ratchet-final/ratchet.json,
quality-final.log, integration.log, integration-retry.log, integration-collected.txt,
lexer-sweep-results.json, doctests.log, tooling.log, resources-summary.json,
tested-source.json, preservation-checks.json, final-changes.patch,
validation-summary.json, pg-server.log and pg-cleanup.json.

This complete local gate supersedes the WSL-interrupted 959-case run. No full
repository/confidence suite or current remote CI is claimed; require fresh CI
after eventual publication. Connection consolidation and measured schema-query
performance remain later work. No subagents or skills used.
