# Database regression ratchet — 2026-11-05

Work continues in `/tmp/liuxin-databases-typing` on `databases-full-typing`,
following published PR #155 (`01d5cf7c`). The original checkout's earlier
implementation, preferences and data remain untouched.

Requested: patch/mitigate the typing findings, add regression tests for every
category and document an enforced test ratchet in dev-docs.

Implemented:

- Maintenance staged startup, repeated-init worker reuse, stop/join on disable,
  graph/preferences rollback, and retained ownership after failed startup cleanup.
- Legacy cached custom writes own transactions, journal touched cache rows,
  restore enumeration lists, and defer notifications until commit. Active
  transactions and `commit=False` are refused. SQL macros preserve borrowed units.
- TEMP DDL no longer uses implicitly committing SQLite scripts. Per-write scratch
  names are unique. Shared-value updates reload owners' remaining choices.
- Explicit Row state and atomic full-payload draft insertion, including null IDs.
- Required named regression manifest, skip/xfail-resistant runner, monotonic
  baseline comparison, live PostgreSQL/APSW CI and required merge-summary job.
- Guide: `dev-docs/databases/regression-ratchet-2026-11-05.md`.

Validation complete:

- 240 required cases passed with no skips/xfails, including live PostgreSQL.
- Integration selection: 292 passed; the one unstaged-doc-link failure was
  resolved by staging and rerun in the final 87-pass documentation/tooling lane.
- Full quality runner passed. Both checkers passed again against declared
  base/typing dependencies in a fresh /tmp virtualenv (453 mypy sources,
  zero basedpyright errors/warnings). No diagnostic weakening.
- Existing PR #155's full CI suite passed, but its quality job failed because
  APSW was undeclared in the typing extra. Added APSW and psycopg2-binary to that
  extra and an enforced dependency regression. No fresh full-suite result claimed.
- Private PostgreSQL at `/tmp/liuxin-ratchet-pg.json` is stopped and removed.

Evidence: `/tmp/liuxin-ratchet-signoff.log`,
`/tmp/liuxin-ratchet-signoff/ratchet.json`,
`/tmp/liuxin-ratchet-integration.log`,
`/tmp/liuxin-ratchet-tooling-final.log`,
`/tmp/liuxin-ratchet-quality-final.log`,
`/tmp/liuxin-ratchet-clean-mypy.log`, and
`/tmp/liuxin-ratchet-clean-pyright-isolated.log`.

Delivery: follow-up for PR #155 on the existing `databases-full-typing` branch.
The final commit can be located by the regression-ratchet commit message.
No merge is authorized or performed. Keep the original checkout untouched.
