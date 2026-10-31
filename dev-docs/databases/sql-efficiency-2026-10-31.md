# SQL efficiency implementation — 2026-10-31

This follows the physical-table mixin split. The scope is all six efficiency
gaps identified in the review, including production callers and regression
coverage. A helper alone does not complete a task whose caller still scans.

## Completion checklist

- [x] Catalog pagination: SQL pages and cursor traversal, with bounded reads,
  stable ID ordering and unchanged validation.
- [x] Graph batching: bounded many-owner link queries, distinct destination
  reads, production traversal/hydration callers and preserved link metadata.
- [x] Search and matching: SQLite/APSW predicates executed in SQL; canonical
  text/identifier rules and monotonic WEMI traversal retained, with a bounded
  semantic fallback for other drivers.
- [x] Aggregates: grouped counts, totals and existence checks used by browse
  and storage callers without changing cache or effective-record precedence.
- [x] Bulk writes: batch inserts, staged relationship changes and predicate
  deletes used by real callers, with rollback, normalization and constraints.
- [x] Query indexes: new-schema definitions and existing-database migration,
  verified with query plans and representative data.

## Boundaries and verification

Catalog selects WEMI meaning and prepares semantic values. Database helpers
operate on physical tables and declared columns. Storage retains byte effects
and its typed-record precedence. A database optimization must not bypass the
selected cache read source. Generic machinery stays in portable macros;
table-specific operations stay with their physical table's mixin.

Tests must cover result equivalence, bounded query/binding counts, transaction
ownership, later-chunk failures and shared WEMI nodes. Query plans demonstrate
index use, not wall-clock performance. SQLite/APSW and portable adapter tests
are required; live PostgreSQL coverage must be reported separately.

## Evidence

Initial inspection confirmed full-table Catalog pagination/search, per-owner
relationship queries, Work materialization for category counts and scalar
loops inside bulk relationship writes. EXPLAIN on the generated test database
reported SCAN for annotation owner/user/kind reads and workflow status reads.
Implementation and validation results will be recorded here as each area lands.

### Implemented paths

- Catalog `list()` now requests a SQL page. `list_after()` and `iter_rows()`
  advance by physical ID, with unchanged strict ID/paging validation.
- `get_linked_rows_bulk()` batches links and distinct destinations across owners.
  Both link-only and destination reads use bounded bindings. Browse enriches
  its visible page through the selected read source, and legacy Row traversal
  batches its destinations without changing its prior link sorting.
- Generic aggregates expose total/sum/conditional counts, grouped owner counts,
  existence checks and link/distinct/dangling counts. Browse category totals
  retain relationship multiplicity. Core replica assessments consume grouped
  raw status/mode labels and preserve Python interpretation; storage's typed
  records and cache precedence are unchanged.
- Generic inserts use multi-value statements; composite members and backup
  source replacement use them. Predicate deletes remove storage read/delete
  loops. Relationship replacement stages indexed temporary tables, preserves
  surviving link IDs/extras and moves priorities before final assignment.
  Bulk upsert initial/final reads are batched; 1,001 inserts and repeated updates
  each require two relationship SELECTs and preserve input result order.
- New-schema SQL includes annotation owner/order and owner/user/kind/order
  indexes plus workflow status/order. `db.audit_query_indexes()` checks stored
  definitions; `db.migrate_query_indexes()` installs missing indexes atomically
  and rejects name collisions with incompatible definitions. The Core
  `database.migrations.status`, `.plan` and `.apply` routes include this work.
  Opening an existing database does not silently perform the migration.
- SQLite/APSW Catalog search now builds physical predicates for Boolean text
  queries and monotonic WEMI paths. SQL filters and pages results; one batched
  integrity preflight checks dangling traversed endpoints. Exact Work title,
  Agent name and curated/observed identifier lookups use shared Python
  normalization through deterministic SQL functions. Identifier matchers retain
  duplicate observations, evidence, normalized overrides and Python owner equality.
- Five versioned SQLite expression indexes cover preferred/canonical Work titles,
  Agent names and curated/observed identifier pairs. The generator installs them;
  existing databases use the same explicit index migration. New-schema coverage
  also caught and fixed a missing final BREAK delimiter that previously omitted
  the annotation owner/user/kind index during real generation.

### Validation so far

The first focused run passed 15 tests. A broader run passed 179 cases with one
expected adapter skip and exposed two missing Database-facade migration hooks;
the hooks were added and the real Core migration tests subsequently passed.
The write-focused run passed 70 cases and exposed two fault-injection tests
that still intercepted scalar composite-member inserts. Those tests now fail
after the first batch member has been written, verifying the enclosing rollback.
The following focused run passed **80 tests, one expected adapter skip**.

Regressions in `tests/databases/test_sql_efficiency.py` exercise gaps/deleted
cursors, borrowed transaction reads, 1,001-owner graph reads, shared endpoint
copies, raw replica-label equivalence, index plans/migration idempotence and
rollback, normalized bulk insertion and later-chunk conflicts, surviving link
IDs and extras, 1,002-owner staged replacement and temporary-table cleanup.
The existing portable macro tests also pass after staged replacements.
The subsequent link/write selection passed **24 tests**, including the new
large upsert and conflict rollback checks (34 unrelated cases deselected).
The final query-plan/grouped-binding checks passed **4 tests**. Across these
selected runs, the latest outcome for each distinct case is **234 passed,
one expected skip, no remaining failures**. This is selected coverage, not the
full test suite. Logs and telemetry are under `/tmp/liuxin-sql-efficiency-*`.

For an existing database, inspect and apply indexes explicitly:

```python
report = db.audit_query_indexes()
assert not report.conflicts  # Investigate incompatible same-name indexes first.
installed = db.migrate_query_indexes()
assert installed.clean
```

Core clients can use `database.migrations.status`, `database.migrations.plan`
and `database.migrations.apply`. The apply command also runs the existing
storage/normalized-identity migrations; it does not form a single transaction
across every migration subsystem.

### Search and index boundaries

The normalized SQL capability is explicit. SQLite and the APSW-backed driver
provide it; PostgreSQL and minimal adapters keep the semantic Python fallback.
Candidate pages are bounded when the adapter supports cursor reads. PostgreSQL
receives the portable query indexes, not SQLite function indexes. Live PostgreSQL
has not been exercised, and equivalent indexed normalization there remains future
work. General substring and fuzzy matching are not full-text indexes and can
still scan; exact title/identifier lookup is the indexed path.

Graph searches validate every requested physical path before selecting a page.
Consequently corrupt links may now raise where Python Boolean short circuits or
an early result limit previously hid them. Result semantics for valid graphs
remain unchanged. The preflight and result SELECT borrow the active connection;
they establish no snapshot of their own.

Expression-index functions delegate to the metadata policies rather than writing
alternate rules to `*_norm` columns. Their `v1` names are part of the persisted
schema contract. Changes to normalization semantics, including Unicode data
upgrades, require versioned functions and an explicit index rebuild/migration.
Driver, generator, restore, backup-verification and recovery connections register
the functions. External raw SQLite writers or full integrity checks must call
`register_search_functions(connection)` too; ordinary raw row reads are unaffected.

### Final selected validation

All six areas have been checked against their production callers and selected
regressions. Across the recorded runs, the latest result for each distinct case
is **397 passed, one expected adapter skip, no outstanding failures**. This is
selected coverage, not a single full-suite run.

The search/index selection passed 37 cases. Separate checks passed for recovery
and replacement (two), Agent-name index use (two), all six local RPC cases, and
the migration protocol documentation contract. The broader integration run's
replacement failures were fixed by registering the comparison functions on the
backup integrity-check connection; both existing replacement tests then passed.
RPC initially failed because the sandbox disallowed loopback sockets; rerunning
those six tests with that restriction lifted passed. Migration handler docs were
synchronized with their service owners and the documentation contract passed.

Coverage includes new-schema generation, SQLite/APSW dump/restore, explicit index
migration and rollback, query-plan index use, Unicode/identifier equivalence,
normalized overrides, all four WEMI result levels and pagination, missing links,
backend failures, read-only execution and borrowed transactions. Final layer
validation passes (1,855 modules, 15 reviewed backward edges, 28 unresolved
dynamic imports). Targeted Ruff lint and formatting checks and `git diff --check`
pass. The full suite and live PostgreSQL tests have not been run.

Logs and resource telemetry are under `/tmp/liuxin-search-*` and
`/tmp/liuxin-sql-efficiency-*`; the merged latest-case results are saved in
`/tmp/liuxin-sql-efficiency-completed-results.json`.
