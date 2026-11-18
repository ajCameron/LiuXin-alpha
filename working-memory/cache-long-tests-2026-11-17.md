# Gated longer cache tests — 2026-11-17

User requested: “Let's get those longer tests in - gated as needed”.

## Final status — complete

All 13 full cases passed every required setup/call/teardown phase, no skips,
xfails or gate violations. Full synthetic **4 passed, 1,937.53 s**; SQLite/APSW
**6 passed, 787.76 s**; PostgreSQL **3 passed, 983.07 s**. Each directory below
contains run.json, gate.json, JUnit, durable telemetry and per-case measurements:

- /tmp/liuxin-cache-long-full-synthetic (log at the same path plus .log).
- /tmp/liuxin-cache-long-full-sqlite (same log convention).
- /tmp/liuxin-cache-long-full-postgres (same log convention).

Samples verify 3,000 native mutations, 1,000 Calibre query/definition steps,
108 full-schema load/close cycles and 144 live captures. Workloads overlapped
on Linux/WSL, Python3.12.3; timings are not controlled latency comparisons.

Other evidence: reduced gates4/6/3 all passed; default gating13 intentional skips;
supporting tests188 passed; final runner8 passed (including two added nested
profile assertions); docs22 passed using isolated Git index; format409 and
targeted lint passed. Relevant logs are listed in the historical notes below;
the final runner log is /tmp/liuxin-cache-long-runner-final.log. Four smoke
cProfile artifacts load with pstats. Counts overlap, and the whole database
suite was not rerun. Its 2,637 mandatory-case manifest is unchanged.

Private PostgreSQL is **stopped**, confirmed by pg_ctl status. All validation
sessions finished. No production change, staging, commit or remote workflow
dispatch. The database test's final change after its full runs was a comment
clarifying that fresh-connection bulk insertion specifically describes SQLite.
The synthetic run's unused database-test hash predates that fixture's repair;
its exercised test, support and runner hashes match. Full database test hashes
match after reversing only that final comment clarification.

## Historical implementation and validation checkpoints

The active-session notes below are superseded by the final status above.

Implementation is test/tooling/documentation only. No production code change,
commit, staging or remote dispatch. Preserve all earlier dirty migration/search
work, user review drafts, data submodule and stash.

New dedicated runner `scripts/run_cache_long_tests.py`: synthetic/sqlite/postgres/all
lanes, smoke/full sizes, optional cProfile, fresh external artifact directory,
source hashes, JUnit, telemetry, per-operation samples and RegressionGate audit.
13 cases are optional in ordinary pytest but mandatory when selected by this
runner. Existing 2,637-case database manifest remains unchanged.

New tests/support/cache_long_runs.py, test_search_long.py and
test_search_long_database.py. Full native/Calibre cases use 10,000 records and
1,000 steps. Real database cases use 1,000 work rows, 12 fresh load/query/close
cycles, committed writes and rollback. Live queries always perform actual
capture; no immutable-view shortcut. Three cache backends, both SQLite drivers
and PostgreSQL. Manual workflow .github/workflows/cache-long-tests.yml;
ordinary quality job runs small runner tests. Formatting scope now 409 files.

See [run guide](../dev-docs/caches/long-running-tests.md) for commands and limits.

## Active validation checkpoint

- Synthetic smoke with cProfile: 4 passed, 12.26 s. Saved-definition churn was
  subsequently improved; full synthetic run tests that final logic.
  `/tmp/liuxin-cache-long-smoke-synthetic` and `.log`.
- Initial SQLite smoke: 6 fixture failures from nesting a fresh-connection bulk
  insert inside a macro transaction. Corrected the fixture to let bulk insertion
  own its transaction. No production change.
- Corrected SQLite smoke: 6 passed, 179.91 s;
  `/tmp/liuxin-cache-long-smoke-sqlite-fixed` and `.log`.
- Ordinary pytest selection: 13 intentional opt-in skips, 14.96 s.
  `/tmp/liuxin-cache-long-default-gate.log`.
- Initial runner/CI/ratchet tooling: 63 passed, 13.95 s.
- New-file Ruff clean, full formatting scope 409 passed, bash syntax and diff
  whitespace clean.

Running handles (revalidate before use):
- 6054 full synthetic, `/tmp/liuxin-cache-long-full-synthetic(.log)`.

Further completed validation: PostgreSQL smoke **3 passed, 183.10 s**, exact gate
3/3 with no violations. Combined existing stress + runner/CI/ratchet/format/type
script tests **188 passed, 78.69 s**, log /tmp/liuxin-cache-long-regressions.log.
Documentation **22 passed, 93.67 s**, real index unchanged;
/tmp/liuxin-cache-long-docs.log. Four smoke cProfile artifacts load with pstats.
Full Calibre and all three standard SQLite backend cases have passed; remaining
full cases continue. Full synthetic source hashes match its exercised code;
its unused database-test hash predates that fixture's repair. Both full database
lanes started after final fixture formatting and match all four harness files.

Full SQLite gate **6/6 passed, 787.76 s**, and full PostgreSQL gate **3/3 passed,
983.07 s**. Every required phase passed, no failures/skips/xfails. Together these
cover 108 fresh cache load/close cycles and 144 live captures, plus commit/rollback
assertions on all nine database/backend combinations. All database sessions are
complete. Private PostgreSQL was stopped successfully with pg_ctl; port5432 was
untouched. Only full synthetic session6054 remains running.

Remaining: finish the full synthetic workload, record exact final evidence and
limits in docs/memory, final whitespace/scope/status review. Documentation and
quality checks already passed; no production changes.
