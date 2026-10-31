# Database review and validation — 2026-10-31

The requested delivery is a commit/PR checkpoint, live PostgreSQL regression
coverage, a full test run with telemetry, and representative catalogue benchmarks.
Keep all four parts open until their results are recorded.

## Commit boundaries

The existing staged changes form the Catalog/database ownership checkpoint:
WEMI search and repositories, Expression-based Calibre conversion, retired
compatibility SQL, physical-table SQL mixins, adapter/locking moves and caller/docs
updates. The next commit contains SQL pagination, graph batches, aggregate reads,
staged writes, normalized search and explicit query-index migrations.

The previously recorded 397 passing selected cases and one expected skip apply
to the combined worktree, not the intermediate ownership commit in isolation.
The dirty `LiuXin_alpha_data` submodule is outside this delivery.

## Current verification

- No PR existed for `database-review` when this work began.
- Ownership checkpoint committed as `50782bc6`. The following commit packages
  the SQL efficiency increment; full-suite and benchmark evidence remain pending.
- The first live PostgreSQL selection passed 18 tests. New efficiency tests
  exposed reversed Catalog priority ownership and omitted backup-workflow tables
  in PostgreSQL schema creation. Both are corrected; live reruns are in progress.
- The local quality runner found two leading blank lines in its format targets;
  those formatting-only corrections are included with validation.
- Full-suite CI already captures resource/event telemetry and removes per-test
  scratch. Use that runner for the full suite rather than the space-limited host.
- Main's last inspected push passed confidence, packaging and the fast format
  lane; its architecture/typing lane failed. Inspect that baseline separately.
- An isolated PostgreSQL 16 cluster is available for disposable-schema tests;
  no configured application database or system cluster is used.
- Benchmark queries must cover pagination, shared WEMI graphs, exact lookups and
  bulk writes, reporting time, memory and query counts. Existing benchmark tools
  supply conventions, but their older read scenarios do not cover every new path.

Implementation contracts and earlier focused evidence live in
`dev-docs/databases/sql-efficiency-2026-10-31.md`.
