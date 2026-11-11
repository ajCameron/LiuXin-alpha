# Driver-plugin follow-up fixes — 2026-11-05

Completed locally after the user requested fixes and mandatory regressions for
DDP07–DDP10, then explicitly confirmed retiring the obsolete scratch API and
recording the replacement workflow. Worktree `/tmp/liuxin-databases-typing`,
branch `database-driver-plugins-review-2026-11-05`, base
`07fc33264aab57e1975fd13688b3c58e59f5e0dc` (merged PR #155 into database-review).
Prior six fixes and both reviews remain part of the same uncommitted work.
No commit/publication was requested for this fix goal. Preserve original checkout,
preferences and data; only its working-memory pointer was updated.

All four additional findings are addressed:

- DDP07: direct_executemany preserves scalar-tuple shorthand before execution,
  streams binding rows once, never retries, closes the cursor and chains failures.
  Input/adaptation/SQL errors roll back through the existing primary context.
  Existing direct-execute semantics still complete earlier primary work.
- DDP08: lexical classification retains qualification-dot context across comments
  and whitespace. Keyword-shaped qualified attributes remain JSON operands.
  Updated stale PostgreSQL script docstrings. This remains a lexical adapter,
  not a full SQL grammar translator or an exhaustive SQL correctness claim.
- DDP09: PostgreSQL physical specs and uniqueness groups share inspect_indexes.
  INCLUDE payload is separate from keys; partial/expression/invalid/nonunique
  indexes cannot prove global keys. Short owned connections close; primary work
  is preserved. Unique groups now order by index name instead of OID.
- DDP10: removed Database.lock_writing and driver make_scratch, abstract APIs and
  PostgreSQL stub included. No database callers remain. Replacement: verified
  committed backup plus a separately opened driver/graph; source stays writable
  and keeps its connection/transaction. See creation-and-replacement.md.

Permanent coverage adds 50 cases to the mandatory manifest (350 -> 400), retaining
all earlier IDs and group membership: 14 batch, 24 SQL-adaptation, 7 index-key,
5 retirement/independent-working-copy cases. Red runs: 14 failures for the first
three defects and 3 retirement-guard failures. Strengthened adapter cases use
auto-assigned IDs to allow duplicate prefixes on retry; both fail with the exact
pre-fix staged method injected into a separate process. Production was not reverted.

Final verification:

- Mandatory ratchet: 400 passed, zero skips/xfails, 145.42 s. JSON reports all
  400 setup/call/teardown phases complete and failures=[], exit 0.
- Affected integration: 319 passing cases, 3 skips and 2 existing xfails for
  DriverWrapper.get(all=False). Initial run had 2 teardown errors from concurrent
  mypy creating .mypy_cache; guard removed it. Both complete cases reran cleanly
  after quality finished (2 passed). No runtime expectations were weakened.
- All 121 driver/adapter doctest examples passed.
- Full quality passed: format 308, annotations 463, cycles 255, layers 1901,
  25 cold-import budgets, Ruff, basedpyright zero diagnostics, mypy 455 sources,
  95/93 deliberately invalid type examples rejected. Final test-only strengthening
  separately passed Ruff/format and the mandatory gate.
- Final mandatory/integration sampled test-tree peaks: 82/139 MiB, OOM delta 0.
  Test process figures exclude PostgreSQL. No full-repository or real import run.

Evidence prefix: /tmp/liuxin-driver-followup-fixes-. Authoritative ratchet JSON
and telemetry are in ratchet-final/; other logs: integration.log,
integration-selection.json, teardown-rerun.log, quality.log, doctests.log,
adapter-red.log. Do not run quality alongside pytest unless MYPY_CACHE_DIR is
outside the checkout: pytest correctly rejects newly created root entries.

The private PG16 cluster is STOPPED AND REMOVED. Receipt: pg.json under that
prefix; retained server log: pg-server.log. No prior review cluster remains.
Review and navigation docs record repair/retirement and validation. All 22 final
documentation checks pass. Whitespace and the 350-to-400 manifest-membership
audits pass. Changes are ready on the existing branch, with no commit/publication.

Next driver work remains the documented structural cleanup (duplicate stdlib
connection setup and oversized implementation modules) and profiling full-schema
PostgreSQL inspection. Production import validation is still separate.

## Original review handoff (historical)


User requested another driver-plugin review after the six local fixes. Work stays
on `database-driver-plugins-review-2026-11-05` in `/tmp/liuxin-databases-typing`,
based on `07fc3326`. Existing implementation/tests remain unchanged and staged;
this turn adds only review/navigation/handoff documentation. No commit or PR
publication was requested.

Review: `dev-docs/databases/database-driver-plugins-follow-up-review-2026-11-05.md`.
Four open findings:

1. DDP07 high: shared SQLite direct_executemany catches ValueError after partial
   SQL execution, retries an exhausted iterator and commits its prefix while
   reporting success. Reproduced on both SQLite driver names with an independent
   reader. Wrapper executemany delegates here. Normalize before SQL, execute once,
   and test generator/conversion failures; preserve the original exception.
2. DDP08 medium: new PostgreSQL classifier treats qualified `t.first` as an
   expression-introducing keyword. Native `t.first ? %s` JSONB query works in raw
   psycopg2 but adapter falsely rejects mixed parameter styles. Ordinary `payload`
   and quoted `"first"` controls work. This qualifies DDP04’s original completion.
3. DDP09 medium: PG _get_unique_column_groups includes non-key INCLUDE columns.
   UNIQUE(left_id) INCLUDE(right_id) becomes a false two-column key, and the
   wrapper reports many_to_many. The physical index reader already separates
   key/included columns correctly; PostgreSQL rejects repeated left_id values.
4. DDP10 medium: make_scratch / Database.lock_writing raises AttributeError on the
   removed CompatLogger.dump_to_file call in ptempfiles. Replacing ONLY the temp
   provider shows latent raw-copy WAL loss and primary writes still targeting the
   original file. These latter outcomes are conditional; do not claim the current
   unmodified failing call writes through. Prefer retiring the obsolete surface.

Other notes: PG direct-script docstrings still describe naive splitting even
though the adapter was repaired. Existing structural/performance recommendations
remain. Do not turn documented explicit-refresh/registry-cache limits into
unsupported additional findings.

Validation: existing mandatory ratchet **350 passed**, zero skips/xfails; baseline
`07fc3326`, 129.18 s. No broad integration/full-repository rerun. Local standalone
repro scripts/logs: `/tmp/liuxin-driver-second-review-probes.py` / `.log`, and
`/tmp/liuxin-driver-second-review-postgres.py` / `.log`. Ratchet result/telemetry:
`/tmp/liuxin-driver-second-review-ratchet`. These external probes are evidence;
new permanent regression tests have not been added in this review-only turn.

PG16 cluster `/tmp/liuxin-driver-second-review-pg-tgyhapxs` is stopped and removed;
receipt `/tmp/liuxin-driver-second-review-pg.json`, server log sibling. The initial
server-start approval review timed out before execution; one retry succeeded.
No user approval is outstanding. Preserve the original checkout’s dirty work,
preferences and data. Its working-memory pointer identifies this review checkout.

Final documentation navigation/boundary checks: **22 passed**. Staged diff passes
whitespace checks. Review documents are staged with the existing uncommitted
repair; the four findings remain open for the next implementation request.
