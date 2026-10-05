# Ten-unit Database contract documentation batch: D199–D208 complete

The explicit request for **another ten units** authorized **D199–D208**. All
**270 declarations across 12 files** are verified and all 12 complete files are
promoted. Work stopped after D208. **D209 is next**, with 20 declarations in
`tests/databases/database/database_contract/test_db_unicode_nightmares_fast.py`.
The ten-unit authorization is complete; earlier broader requests remain paused.

| Unit | Declarations | Complete files |
|---|---:|---:|
| D199 | 40 | 2 |
| D200 | 22 | 1 |
| D201 | 37 | 1 |
| D202 | 27 | 1 |
| D203 | 18 | 2 |
| D204 | 25 | 1 |
| D205 | 25 | 1 |
| D206 | 32 | 1 |
| D207 | 17 | 1 |
| D208 | 27 | 1 |

The batch covers dirty queues and maintenance callbacks, slow Unicode and
SQL-looking fuzz payloads, interlink retrieval/writes/unlinking, intralinks and
trees, Row errors/factories/round trips, searches and sets, lifecycle and metadata
delegation, transactions and locking, and trigger/direct-SQL helpers.

Docs state parameter meanings, return shapes, connection ownership, mutations,
fallbacks, and actual assertions. Specific corrections include best-effort commit,
close and rollback behavior; heuristic column/shape selection; empty type-registry
fallbacks; possible collisions in languages-row selection; case-preserving versus
lowercased registration; stop requests versus completed thread termination; and
rename checks as platform-dependent evidence of handle release. Stale docstrings
claiming that Database SQL helpers are missing were replaced with descriptions of
the calls the tests make. Original comments and test bodies remain unchanged.

Verification:

- All **270 declarations in 12 complete files** pass documentation audit and
  normalizer checks. Executable AST, signatures, comments, SQL, payloads, and other
  non-docstring literals are unchanged. No added Ruff findings or whitespace
  errors. Original and maintenance baseline hashes are retained.
- **402 regressions passed**, **40 skipped**,
  **12 expected failures**, and **0 non-strict unexpected
  passes**. No unexpected failures or errors. SQLite and APSW were explicitly
  selected for parameterized fixtures. All twelve selected files were run.

| Regression group | Passed | Skipped | Xfailed | Xpassed |
|---|---:|---:|---:|---:|
| queue-fuzz | 28 | 0 | 2 | 0 |
| relations | 92 | 40 | 4 | 0 |
| rows | 162 | 0 | 4 | 0 |
| lifecycle | 120 | 0 | 2 | 0 |
| total | 402 | 40 | 12 | 0 |

- The 40 relation skips reflect the discovered write-test shape lacking optional
  type, priority, or extra columns, or the absence of an unlinkable table pair.
  Existing xfail markers cover the interlink callback arity, linked-row retrieval,
  secondary-only unlink, NULL search/grouping, and scalar-get compatibility.
  Exact outcomes and reasons are saved in each regression log. Passing the suite
  does not imply that skipped capabilities were exercised. Existing unknown
  catalog-marker and deprecated logging warnings are retained in the logs.
- **50 runnable example statements passed**, with no doctest skips. Examples use
  normally imported reviewed modules and pure helpers or temporary in-memory
  SQLite. The **245 documented pytest commands** have valid module paths and
  top-level test selectors; command text is not counted as a runnable doctest
  statement. No source-level runtime doc consumers were found in the selected
  original files, and no runtime-doc exceptions were needed.
- Full configured quality runner passed: 156 formatted files, 454 files checked
  for annotation coverage, 215 protected modules checked for dependency rules,
  zero basedpyright errors, mypy across 182 files, and both type-contract checkers
  rejecting all 37 invalid examples. Final scoped source hashes match the static,
  example, regression, and quality records.
- Source discovery remains **2,671 files**, with no new or missing paths.
  **25 out-of-scope storage files** differ from historical reviewed hashes.
  Since the preceding checkpoint, `storage_manager_api/ingest_api.py` adds a drift
  entry and `storage_manager_api/derivations_api.py` has a different working hash.
  These files and their historical review records were left untouched and were
  not counted as new reviews.

Coverage is **1,004/2,671 complete-file review records**, containing **14,361
declarations**, plus **37 existing partial-file declarations**. Remaining scope:
**1,667 files and 27,763 declarations**. D progress: **208/232 units**,
**5,133/5,685 declarations**, **318/374 complete files**. Across all tracks:
**240 documentation units verified**, **1,132 remaining**. No commit or publication
was requested; no dependencies were installed. Token usage is unavailable.

- [Observations and exact commands](test-results/docstrings-d199-d208-2026-09-25/observations.json)
- [Static proof](test-results/docstrings-d199-d208-2026-09-25/static.json)
- [Runnable examples](test-results/docstrings-d199-d208-2026-09-25/examples-all.json)
- [Runtime-doc and pytest command review](test-results/docstrings-d199-d208-2026-09-25/runtime-doc-review.json)
- [Queue and fuzz regressions](test-results/docstrings-d199-d208-2026-09-25/queue-fuzz.log)
- [Relation regressions](test-results/docstrings-d199-d208-2026-09-25/relations.log)
- [Row and search regressions](test-results/docstrings-d199-d208-2026-09-25/rows.log)
- [Lifecycle, transaction, and trigger regressions](test-results/docstrings-d199-d208-2026-09-25/lifecycle.log)
- [Quality checks](test-results/docstrings-d199-d208-2026-09-25/quality.log)
- [Discovery and hash drift](test-results/docstrings-d199-d208-2026-09-25/discovery.json)
- [Inventory reconciliation](test-results/docstrings-d199-d208-2026-09-25/reconciliation.json)
- [Documentation diff](test-results/docstrings-d199-d208-2026-09-25/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
