# Database facade correctness review

Reviewed **2026-11-11** at `47156490aa4c71fecc414f9bb237d977d5d6febc`, after
PR #160 merged into `main`. Scope is `LiuXin_alpha.databases.database`, its
mixins, the facade API contracts and relevant driver/wrapper/Row callers.

**Original review verdict: not ready for correctness signoff.** Seven defects reproduce on both
`SQLite` and `SQLite_apsw` using disposable databases generated from the current
FRBR schema. Some are already acknowledged in docstrings or expected-failure
tests; documenting those behaviors has not repaired them. No implementation
changes were made during this review.

## Repair follow-up

The user subsequently requested repairs for all seven findings. The implementation
now addresses each one, with all seven findings verified as repaired. The findings below are
retained as the historical review of the original commit, including its line numbers.

| Finding | Implemented behavior | Regression coverage |
| --- | --- | --- |
| DBF01 | One transaction exchanges both priorities through a temporary NULL or unused integer slot. | Stored values, endpoint ordering, non-null priorities, late failure, caller rollback. |
| DBF02 | One complete insert replaces blank allocation followed by synchronization. | Invalid vocabulary, duplicates, preserved native causes, nested rollback. |
| DBF03 | Candidate schema is prepared before publication; owners, queues, metadata, categories and aliases are bound together. | Same/new driver, live maintenance, storage transactions, failed preparation, ownership and active-transaction rejection. |
| DBF04 | Delete matching relationship records directly in one transaction. | Multiple roles, exact filters, preserved endpoints, trigger failure, caller rollback. |
| DBF05 | Both directions return opposite endpoint Rows; missing endpoints raise an integrity error. | Outgoing/incoming endpoints, missing references, removal of the old xfail. |
| DBF06 | Walk with cycle detection and delete descendants before their parents in one transaction. | SET NULL, CASCADE and RESTRICT hierarchies, unrelated branches, cycles, late failure and caller rollback. |
| DBF07 | Consume the requested ID iterable once into a set. | Lists, iterators, generators and empty inputs. |

Driver replacement is a quiescent lifecycle operation: callers must stop direct
SQL use before switching. It rejects an active macro transaction or a driver
owned by another facade. Switching to a different physical catalogue while
services or queued work are attached requires a separate Database instance.
Same-catalogue maintenance workers, callbacks, telemetry and queues are retained.
Storage repositories resolve the current macro provider so their transactions
continue to join the facade's active unit. Failed candidate preparation leaves
both the old facade and caller-owned candidate available.

Deterministic tests live in
[test_database_facade_repairs.py](../../tests/databases/test_database_facade_repairs.py),
[test_database_driver_rebinding.py](../../tests/databases/test_database_driver_rebinding.py)
and [the live PostgreSQL suite](../../tests/databases/test_database_facade_repairs_postgres_live.py).
Evidence for this follow-up is in `.tmp/database-facade-repairs-2026-11-11/`.

Final validation:

- Focused repairs plus existing lifecycle contracts: **110 passed**. The final
  priority optimization was rechecked separately: **10 passed** on SQLite.
- Live PostgreSQL: **7 cases passed** across the completed run and corrected
  role-filter rerun; the final priority change additionally passed all **3**
  PostgreSQL swap cases. PostgreSQL's generated schema restricts an agent-work
  pair to one relationship; its role-filter test uses distinct Works. SQLite
  tests cover multiple roles on the same pair.
- Broader facade, transaction-ownership and storage contract suites:
  **832 passed, 40 skipped, 6 xfailed**, 1,309.99 seconds. The remaining skips
  and expected failures are existing fixture/legacy limitations described below;
  the two intralink endpoint xfails are now ordinary passes.
- Formatting (326 files), lint, annotation coverage (463 files), architecture,
  import-footprint, complexity, basedpyright and mypy source checks passed.
  Basedpyright's static contracts passed. The shared-cache contract run reported unexpected Any types after a
  concurrent narrow mypy invocation; the complete source-and-contract check passed
  with an isolated cache, preserving all 93 expected rejection locations. Its
  cold run needed more than the helper's 180-second timeout, so the final run
  used the same commands and diagnostic assertions with a 900-second allowance.
- Documentation/navigation: **16 passed**, using a temporary Git index and
  temporary object directory containing the proposed new files. The checkout's
  Git index and object database were left unchanged.
- All **85 new mandatory cases** have passing setup, call and teardown records
  (`new-case-proof.json`). The regression manifest now requires **1,529** cases
  and retains every prior requirement. The entire 1,529-case ratchet was not
  rerun; validation covered the affected suites and every newly required case.

The disposable PostgreSQL server was shut down and test databases removed.
No commit, push or PR has been made for this repair; the pre-existing dirty
`LiuXin_alpha_data` submodule remains untouched.

## Consistency follow-up

The subsequent module assessment identified three remaining areas: transaction
boundaries in compound helpers, NULL queries and legacy cycle handling. The user
requested those repairs too.

- `unlinked_intralink` now deletes a directed pair or all links in one direction
  in one macro transaction. Missing IDs and mismatched endpoint tables are rejected.
- `dupe_interlinks` groups creation and optional priority swaps in one transaction.
  This required fixing `interlink_rows`: its independent allocation connection
  conflicted with the outer SQLite write unit. Creation now uses macro inserts,
  updates and readback. The `not_set` default-allocation path is contained in that
  same unit; any constraint or readback failure rolls it back. Extrema are read
  on the active connection so successive links see earlier pending priorities.
- `nest_rows` validates persisted parent/children and rejects self/ancestor
  nesting before writing. Only parent fields are persisted, preserving unrelated
  staged edits. Failed writes leave input Row mappings unchanged. After a later
  outer caller rollback, previously returned Rows still need reloading, as with
  other detached Row snapshots.
- SQLite, APSW and PostgreSQL equality searches accept `None` as SQL NULL;
  literal `"None"` stays text. NULL values form a real `chunk_iterator` group.
  Searches and wrapper ID reads see active macro transaction changes without
  completing that transaction.
- `get_all_tree_rows` delegates to the existing cycle-detecting walker. Both
  root discovery and subtree collection reject cycles in custom/legacy data.
- The obsolete scalar-get xfail now asserts the documented complete-row result
  of `get(all=False)` and uses `fetch_scalar` for a scalar. All three old xfail
  expectations, across both SQLite drivers, are ordinary regression requirements.

Coverage lives in
[test_database_facade_followup.py](../../tests/databases/test_database_facade_followup.py)
and [its PostgreSQL counterpart](../../tests/databases/test_database_facade_followup_postgres_live.py).
They share the same behavioral cases, with independent readers checking durable
state and explicit late-failure/caller-rollback checks. The regression manifest
adds 93 required cases (87 new backend cases and six promoted contracts), for
1,622 total, preserving all previous requirements.

Validation completed:

- All 87 new backend cases passed: 58 on SQLite/APSW and 29 on live PostgreSQL.
  All six promoted contracts also passed. Setup, call and teardown are verified
  for all 93 new required cases in `new-case-proof.json`.
- The broad facade/driver/transaction run recorded 1,013 passes and 40 existing
  fixture skips, with no xfails. Its two failures were an additional obsolete
  contract rejecting explicit None. That contract now checks omission of the
  required argument instead; the complete 72-case error-contract file passed.
- Live PostgreSQL: 36 passed, including the seven earlier repair regressions.
  PostgreSQL adapter and facade API checks: 46 passed. Across the completed
  runs and corrected-contract rerun, 1,103 distinct database checks passed.
- Documentation/navigation and regression-gate checks: 37 passed. An alternate
  temporary index/object store included the proposed new files; the real Git
  index was unchanged.
- Formatting, lint, annotations, architecture/import/complexity and both source
  type checkers passed. Basedpyright accepted the valid contracts and rejected
  all 95 negative examples. The default shared-cache mypy contract run reproduced
  the earlier unexpected Any return types. The complete source-plus-contract
  check passed with an isolated cache and a 900-second cold-run allowance;
  the canonical contract helper then also passed with that cache, retaining
  all 93 expected rejections.
- The complete 1,622-case ratchet was not rerun. Validation covered the affected
  suites, all new required cases, and the earlier live PostgreSQL repair cases.

Evidence is under `.tmp/database-facade-followup-2026-11-11/`. The disposable
PostgreSQL server was stopped. The user requested a local commit on 2026-11-12.

## DBF01 P2 Priority swaps overwrite a value with NULL

Owner: [interlink mixin](../../src/LiuXin_alpha/databases/database/interlink_mixin.py),
`swap_priorities`, lines 819–829.

The method loads the two priorities, assigns the intended replacement to the
first Row, then overwrites it with `None`. It synchronizes that same value twice
without restoring the replacement. Starting with priorities `(10, 20)` on two
`tag_work_links`, the call succeeds and commits `(NULL, 10)`, instead of
`(20, 10)`. A subsequent `get_interlinked_rows` raises `TypeError` while sorting
the mixture of `None` and integers. Independent connections confirm the
incorrect values remain after closing the facade.

Implement the exchange as one constraint-aware transaction, restoring both
values and rolling back every intermediate write on failure. A regression
should verify the committed result, the subsequent ordered read and a failure
between updates. Merely restoring the missing assignment leaves the separate
commit boundaries exposed.

## DBF02 P2 Failed intralink creation leaves committed placeholder rows

Owner: [intralink mixin](../../src/LiuXin_alpha/databases/database/intralink_mixin.py),
`intralink_rows`, lines 131–134.

`ensure_row_has_id()` commits a blank relationship before `sync()` validates
and writes its endpoints and type. There is no cleanup or encompassing
transaction when the update fails. On a fresh `work_work_intralinks` table,
requesting an unregistered type raises `DatabaseIntegrityError` but changes
the row count from zero to one, leaving both endpoints and the type NULL.

The same failure occurs with valid inputs: create an `inspired_by` relationship,
then repeat it. The uniqueness error leaves the original relationship plus a
second committed blank row. Independent readers confirm both failure outcomes.

Insert the complete relationship through one owned write operation, or make
allocation and population share an actual transaction. Cover both vocabulary
rejection and duplicate relationships; an exception must leave the table as it
was before the request.

## DBF03 P2 Driver replacement leaves the facade partially disconnected

Owner: [Database lifecycle](../../src/LiuXin_alpha/databases/database/__init__.py),
`set_driver`, lines 847–878.

Replacing a live facade's driver with a standalone driver for the **same file**
closes the old connections and creates a wrapper, but does not bind the new
driver, wrapper or macros back to the facade. The wrapper's dirty queue is also
unset. A raw `SELECT 1` succeeds after replacement, while an ordinary
`db.macros.insert_row("tags", ...)` fails with
`DatabaseIntegrityError: Database macros are not attached to a database.`

The existing replacement tests check handle ownership and cleanup, but do not
perform a facade write afterward. The method's docstring acknowledges missing
back-references; the public operation still leaves a nominally live database
unable to perform its normal writes.

Give replacement a complete lifecycle contract: prepare and validate the new
collaborators, wire owners, queues and metadata, and reconcile the runtime
services before exposing the replacement. Refuse unsupported replacement modes
explicitly. Test useful reads/writes and callback ownership after both replacing
and reattaching a driver, as well as failure during preparation.

## DBF04 P2 Unfiltered unlink rejects valid typed relationships

Owner: [interlink mixin](../../src/LiuXin_alpha/databases/database/interlink_mixin.py),
`unlink_all`, lines 1033–1046.

An Agent can legitimately have both `aut` and `edt` links to the same Work.
Calling `unlink_all(agent, "works")` then raises
`DatabaseIntegrityError: Only one link is permitted between each row pair`
and leaves both relationships in place. The unfiltered branch resolves each
endpoint through the single-link default of `get_interlink_row`; the filtered
branch already accounts for multiple records, but the unfiltered branch does
not.

Delete the matching relationship records directly, including all types when no
filter is supplied. Group the operation so a later failure cannot leave a
partially unlinked owner. Test a valid multi-type pair on the generated schema.

## DBF05 P2 Intralink endpoint queries return relationship rows

Owner: [intralink mixin](../../src/LiuXin_alpha/databases/database/intralink_mixin.py),
`get_intralinked_rows`, lines 352–358 and 386–392.

Both directions load the opposite endpoints into `intralinked_rows`, then
return `intralink_rows` instead. For one valid Work-to-Work `inspired_by` link,
both queries return Rows belonging to `work_work_intralinks`, not `works`.
Callers cannot use the result as the linked Work records. The current contract
suite explicitly marks the outgoing endpoint expectation as an expected failure.

Return the intended endpoint collection, define how missing endpoints are
reported, and cover both directions. If compatibility requires retaining the
old return shape, expose that behavior under a distinct relationship-query
name and migrate callers deliberately; the existing `get_intralink_rows`
already provides relationship records.

## DBF06 P2 Deleting a series tree preserves its descendants

Owner: [tree mixin](../../src/LiuXin_alpha/databases/database/tree_mixin.py),
`delete_tree`, lines 275–277; [series foreign key](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/metadata_additional/series.sql).

The method deletes only the supplied row and assumes foreign-key cascades will
remove the descendants. The current `series_parent_id` foreign key instead
uses `ON DELETE SET NULL`. For a three-row chain, `delete_tree(root)` succeeds
but leaves the child as a new root and the grandchild beneath it. This persists
after closing the database. The docstring describes the root-only limitation,
but the generic tree-deletion operation does not accomplish subtree deletion
on this supported hierarchy.

Choose an explicit contract: delete the subtree atomically, or refuse/rename
the operation where only root deletion is provided. Do not infer cascade
behavior from the existence of a parent column. Cover the current series
schema and a cascading hierarchy separately.

## DBF07 P2 Tree search consumes one-shot ID iterables

Owner: [tree mixin](../../src/LiuXin_alpha/databases/database/tree_mixin.py),
`search_tree`, lines 202–205.

The public type accepts `Iterable[int]`, but the implementation repeatedly
performs membership tests against that same object. Membership consumes an
iterator. In a three-row series chain, asking for the child and grandchild
using a list returns both IDs; supplying `iter(the_same_list)` returns an
empty set because the first unmatched ancestor exhausts it.

Materialize the requested IDs once into a set, then traverse. Test a generator,
a reusable container and an empty iterable with identical membership semantics.

## Architecture and coverage

The facade has useful separation from the drivers, a declared API, shared
transaction machinery and careful connection cleanup. The largest remaining
weakness is the legacy relationship helper layer: several public methods still
compose committed row allocations/updates or assume a single link where the
current schema permits typed relationships. Repair those operations around the
existing transaction owner before undertaking a broad class split. Database
should continue to own physical integrity; Catalog owns bibliographic meaning.

Existing tests completed with **776 passed, 40 skipped and 8 xfailed** in
386.65 seconds:

```bash
python -m pytest -q -n 2 tests/databases/database tests/databases/test_transaction_ownership.py
```

The dynamic contract fixture skipped multiple priority/type cases because its
selected relationship lacked those columns, despite the generated FRBR schema
having suitable tables. Add deterministic generated-schema regressions for
these operations instead of relying solely on schema-dependent selection.
DBF05 is already represented among the expected failures. The other existing
xfails cover NULL search/grouping and the legacy scalar-get path.

The additional probes ran on both supported SQLite driver names. Their write
outcomes were checked again through independent connections after facade
cleanup. Live PostgreSQL was not exercised in this pass.

A suspected series-cycle path was rejected by the current schema's cycle
trigger; it is **not counted as a finding**. Both normal traversals returned
the three valid nodes afterward. `get_all_tree_rows` still lacks a visited-set
guard for an already cyclic legacy/custom hierarchy, which remains a defensive
hardening opportunity rather than evidence of an ordinary generated-schema
failure.

Reproduction scripts, logs, JSON results and independent-reader checks are in
the local evidence directory `.tmp/database-facade-review-2026-11-11/`.

Return to the [database guide](README.md).
