# Database follow-up review — 2026-11-13

Reviewed committed checkpoint `7780b7eb` after the user requested another review.
The previous commit is on `main`; do not treat the older handoffs' historical
uncommitted statements as current repository status.

The canonical findings are in
`dev-docs/databases/module-boundary-and-lifecycle-review-2026-11-13.md`, under
“Post-commit follow-up review”. Three additional P2 findings remain:

- **DR01:** native writes inside ordinary managed PostgreSQL transactions skip
  the advisory writer lock. Concurrent edits both accept expected revision zero
  and commit. On the same owner, the resumed writer overwrites its competitor's
  committed value. Standalone native writes correctly reject the stale edit.
- **DR02:** rebuilding a field resets allocation bounds. A display-name migration
  allows deleted entry/vocabulary IDs to be reused; stale handles can then edit
  unrelated replacement data. Reproduced with both SQLite backend names.
- **DR03:** SQL driver close errors are swallowed. Facade close reports closed
  with a usable failed handle, and replacement loses the retired cleanup owner.
  Reproduced for primary/auxiliary handles on both SQLite backend names.

The architecture decision remains sound. LC01–LC04 remain repaired. These are
additional correctness issues to resolve before sign-off, not a request to
restore any compatibility aliases or cache responsibilities in native services.

Evidence lives in `.tmp/database-followup-review-2026-11-13/`. `baseline.log`
records **150 passed** for lifecycle repairs, rebinding, custom-column ownership
and native field behavior. `probes.py` and `postgres_probes.py` assert the
observed defects and include controls; they are review evidence, not permanent
regressions asserting desired behavior. No new full database-gate run.
The final SQLite probe log records 12 cases (10 reproductions, two controls).
`postgres-probes.log` and `postgres-same-owner.log` record four cases combined
(two reproductions, two controls), covering different owners and the same owner.
Documentation/navigation: **22 passed**, recorded in `docs.log`. The private
PostgreSQL cluster is stopped; all workers and probe handles were released.
The real Git index remains unchanged and empty of staged changes.

Only review documentation and this handoff/index are changed. No production
fixes, staging, commits or pushes were requested or performed in this review.
The pre-existing dirty `LiuXin_alpha_data` submodule is preserved. Next work is
to fix DR01–DR03 and add permanent regressions when requested.
