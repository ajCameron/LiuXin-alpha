# Seventh driver-plugin review — 2026-11-10

Follow-up: all three findings have repairs and a passing 1,180-case mandatory
gate. Read the [fixes handoff](database-driver-plugins-seventh-fixes-2026-11-10.md)
for final validation and cleanup status. The review-time notes below are historical.

User requested another database-driver-plugin review after the WSL restart.
Recovered `bab2b682` plus the surviving 19-file staged sixth-fix patch into:
`/mnt/c/dev/LiuXin-alpha/.tmp/database-driver-review-2026-11-10`.
Branch: `database-driver-plugins-review-2026-11-05`.

This is a local clone sharing the original object store, not the old `/tmp`
worktree. Its `origin` points to `/mnt/c/dev/LiuXin-alpha`; do not treat that
remote as the GitHub publication target. The missing worktree's staging index
and the persistent `.tmp/wsl-recovery-2026-11-10` backup remain unchanged.

Read [the review](../dev-docs/databases/database-driver-plugins-seventh-review-2026-11-10.md).
Three open findings:

- DDP25 high: shared SQLite dump omits hidden row IDs, desynchronizing external
  FTS shadow indexes. Both exports and rebuilds reproduce; both rebuilt files
  report integrity `ok` while MATCH fails. Direct backups preserve the control.
- DDP26 medium: PG newline-continuations of E-strings lose escape context.
  Backticks in literal data are rewritten to quotes; separately read committed
  INSERT values prove persisted changes. Bound values/standalone E-strings pass.
- DDP27 medium: bare `between`, `escape`, `set`, `values` columns confuse JSON
  operator binding. Eight native/qmark failures; 16 quoted/qualified controls pass.

Existing focused suite: 724 passed, zero skips (127.17 seconds), including live
PG16. Native keyword sweep: 742 comparisons, 734 matching and eight reproducing
DDP27; 100 natively invalid names excluded. No new permanent test/manifest edits.
Full 959-case ratchet/final quality remain unverified after the crash; no full
local suite or fresh remote CI check was performed.

Documentation/link/boundary checks passed 22 cases; whitespace passes. Focused
telemetry: 70.6 MiB peak test-process-tree RSS and zero OOM kills. Preference,
source/test recovery and original staging-index checksums are unchanged.

Evidence: `/mnt/c/dev/LiuXin-alpha/.tmp/driver-seventh-review-2026-11-10/`.
The review enumerates logs, probes, source hashes and telemetry. The private
PG cluster at `/tmp/liuxin-driver-seventh-review-pg` is stopped and removed;
the receipt and retained server log are in the evidence directory.

Review/navigation/handoff docs only this turn. Recovered implementation remains
staged; review changes are unstaged, with new docs marked intent-to-add so the
repository's tracked-link checks include them. No commit,
push or merge. No agents or skills used. Preserve original source/preferences,
data submodule and architecture proposal; its memory index points here.
