# Core source docstring milestone — 2026-09-09

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following the [first program-service batch](project-docstrings-program-services-2026-09-09.md).
This is a Core milestone, **not completion of the whole-project goal**. Work stays
local and uncommitted on `codex/project-docstrings`, based on `edf6bf05`. No push,
PR mutation, submodule publication, or delegation occurred. The intervening PR
status check did not advance documentation; the resumed work revalidated the
worktree and completed the pending service verification before adding more docs.

## Reviewed scope

All **57 Core Python modules, 58 named classes, and 635 named functions** now pass
the strict docstring audit. This includes private/nested definitions, protocol
methods, compatibility wrappers, and source-defined module introductions.
One explicitly documented runtime-created vacuum adapter class is additional to
these AST counts. Source review and behavioral checks supplement the structural
audit; the latter alone cannot prove descriptive accuracy.

This batch adds fifteen production modules to the reviewed ledger:

- Seven remaining `core/program_services` modules: `database.py`, `schema.py`,
  `metadata.py`, `discovery.py`, `storage_integrity.py`, `storage_repair.py`, and
  `storage_status.py` (38 functions and two source-defined dataclasses).
- Six remaining endpoint providers: `backup_maintenance.py`, `catalog_search.py`,
  `content_workflows.py`, `database_schema.py`, `storage.py`, and `system_jobs.py`
  (twelve installer functions). Their runnable examples use recording mocks and
  verify registration without invoking handlers or doing real content/storage work.
- `core/program_endpoints/handlers.py`: all seven family/aggregate protocols and
  eighty endpoint methods. Each method has a descriptive summary, meaningful
  argument/result fields, an integration example, and a resolvable reST function
  link to its maintained service owner. Existing protocol ellipses are retained.
- `core/program_api.py`: class, installer helpers, and all eight instance wrappers
  (ten named functions). Static aliases retain their already-reviewed service
  function docs. The facade still contains no new workflow implementation.

`tests/core/test_program_service_contracts.py` now documents all **19 functions**,
including a nested test writer. Nine additions cover the dynamic vacuum class and
routing, unavailable identity audits inside collision-free plans, UTF-8 log byte
offsets/read-time EOF, callback failures after partial metadata writes, failed
reload action budgets versus later health, repair attempt/estimated-byte accounting,
observed Store status versus configuration hints, and all eighty protocol/service
documentation links and argument lists. Only test-local temporary files are written;
no real plugin writer, vacuum, copy, or production database is exercised by these
new characterization tests. Their module remains normally pytest-discoverable,
without expansion of the default formatter/lint/type scopes.

Cumulative reviewed ledger: **143 modules, 194 classes, 1,328 functions = 1,665
declarations**, plus the separately reviewed native C and dynamic class documentation.

## Contract details made explicit

Database docs distinguish unknown observations from failed operations, collision-free
plans from healthy/available audits, backend backup from SQLite quick-check
verification, and post-migration refresh/reconciliation from a single transaction.
Metadata docs distinguish capability checks, hydration, decoding, actual writes,
callback error collection, and later size observation; in-place failures need not
undo partial bytes. Schema docs describe canonical/legacy fallback boundaries,
field coercions, and post-write refresh failures.

Job discovery is declared-name coverage, not dependency readiness. Log offsets
are bytes; UTF-8 replacement at page boundaries and EOF on a growing file are
intentional existing semantics. Integrity/reconciliation and repair docs distinguish
selected scope, attempted versus successful actions, estimated versus physical
transfer bytes, and final health versus action success. Store status documents
durable overlays, observation precedence, independent snapshots, and declared
Asset accounting. Review corrected `free_percent` prose to say **None**, not an
omitted field, when capacity is unknown or total capacity is zero; the new test
also covers negative used bytes and percentages over 100 without clamping.

Endpoint docs identify family ownership, sequential/partial registration, descriptive
payload fields rather than executable validation, and submission versus completion.
The system/jobs command installer registers nothing but still retrieves the
registrar attribute. Protocol fields reuse their reviewed owners' contract text,
with an executable consistency test to catch stale links, summaries, fields, or
argument names. This is deliberate source reuse, not name-derived placeholder prose.

## Executable-structure and runtime-text verification

All **126 edited existing production Python modules** and **ten edited existing
test modules** retain baseline executable ASTs after removing literal docstrings,
subject to two specifically verified documentation-only exceptions:

1. The previously recorded local-proxy `_caller.__doc__` template literal.
2. `database_vacuum` adds a string-valued `__doc__` entry to the exact existing
   `type("_VacuumTarget", (), {"vacuum": vacuum})` construction. Verification checks
   the unique type call, name, empty bases, dictionary keys, unchanged vacuum value,
   and absence of extra arguments before removing only that doc entry for comparison.
   Focused runtime tests verify class name/bases, selected callable identity,
   direct/backend precedence, returned receipt, and no retry after execution failure.

The generic normalizer's AST guard was **not weakened** for either exception.
The data generator also matches its own submodule HEAD under docstring stripping.
Each facade wrapper still has exactly one executable Return(Call), after its
new leading docstring is removed; protocol method ellipses remain unchanged.

API introspection after the facade/protocol pass remains **83 commands, 93 queries,
three targets**. With core_uuid omitted, sorted-JSON SHA-256 remains
`2b0dadbe883060aeefde4918889f4c4197a2067c470c6678fb1df84ec95293e3` for the documented
application fixture. The older fixture fingerprint is historical, as explained
in the [application API note](project-docstrings-application-api-2026-09-09.md).
The existing no-write-access Calibre config warning selected temporary config;
the user's configuration was not edited.

## Known ownership-test conflicts — no guard changes

The unchanged physical-line/statement guard is incompatible with these added
docstrings. Complete inspection finds these current size violations, beyond the
first path at which pytest stops:

| Owner | Current physical lines | Guard |
| --- | ---: | ---: |
| `program_services/database.py` | 451 | 450 |
| `program_services/payloads.py` | 475 | 450 |
| `program_services/storage_status.py` | 479 | 450 |
| `program_services/stores.py` | 611 | 450 |
| `program_api.py` | 365 | 250 |

`storage_status.storage_status` is 178 physical lines against the 160-function
limit. All ten facade functions exceed the guard's `end_lineno - lineno < 10`
requirement: spans are 14 for install, 18 for install_program_api, 15 for
preferences_get, and 17 for the other seven wrappers. Each of the eight wrappers
now has two AST statements (docstring and its unchanged Return(Call)), conflicting
with the literal one-statement check. No other service function exceeds 160 lines.

Permission to count genuine docstrings separately remains unanswered. The test and
numeric limits are unchanged, as confirmed by `git diff --exit-code` on the guard.
Do not compress useful prose, leave these larger modules undocumented, waive
modules, or change implementation to disguise this conflict. Other project work
remains available, so this is not an overall blocked-goal condition.

## Verification

Selections overlap and must not be added into distinct-test totals.

- Extended service characterization module and its doctests: **40 passed,
  two explicit integration-example skips**. An initial test used nonexistent
  ReplicaMode text `replica`; the test input was corrected to `active`. Production
  behavior was not changed to satisfy that failed first attempt.
- Six endpoint-provider doctest modules: **12 passed**.
- Final Core program API/application API/facade/evacuation regression rerun:
  **44 passed**, with permission for local HTTP servers.
- Full quality runner after facade/protocol documentation: **passed**, with
  159 formatter-clean files, 456 annotated modules, 221 protected dependency modules,
  188 strict-mypy files, zero production type errors, selected lint/complexity clean,
  and all 37 negative examples rejected by each checker.
- All 143 reviewed files pass strict audit and normalizer `--check`. The first
  thirteen production files had zero Ruff findings before and after; standalone
  Ruff checks on the two final Core modules and the new test module are also clean.
- Before the facade/protocol addition, the integrated seven-service/six-provider/
  contract/migration/public-doc/link/owner selection had **101 passed, 41 skipped,
  one failed** (service physical-line guard). The subsequent facade/protocol/facade-
  regression/owner selection had **20 passed, 90 skipped, two failed** (service and
  facade physical-line guards). Neither selection is all-green.
- Final **all-Core** doctest selection plus service contracts, migration tests,
  ownership guards, public documentation, and developer links: **339 passed,
  424 explicitly skipped integration examples, two failed** in 87.97 seconds.
  The only failures are the service and facade physical-line guards above. This
  is not a whole-project pytest run or execution of the skipped integrations.
- Working-memory link scan verifies all **123 local destinations across six
  notes**. Root and submodule diffs are whitespace-clean; final handoff link/public
  contracts: **23 passed**. All observed behavioral, doctest, quality, audit,
  introspection, and handoff-test handles have completed; the two known guard
  failures are terminal results, not pending work or unobserved passes.

## Whole-project remainder

The refreshed `/tmp/liuxin-docstring-current-2026-09-09.json` inventories **2,724
modules, 4,399 classes, 35,028 functions**, without parse failures. Missing/blank
docstrings remain on **1,118 modules, 2,043 classes, 23,590 functions = 26,751
declarations**. Other overlapping findings: 4,386 delimiter layouts, 10,627 missing
examples, 3,093 parameter fields, 4,045 return fields, 4,143 empty parameter
descriptions, 5,581 empty return descriptions, and 29 missing summaries.
The reviewed-file report is `/tmp/liuxin-docstring-reviewed-2026-09-09.json`.

Next source work can finish the remaining terminal composition/read helpers:
`browser.py`, `windowed_ui.py`, `__main__.py`, browser components `catalog.py`,
`rows.py`, `contracts.py`, and windowed components `contracts.py`. Then continue
all remaining source, tests, scripts, examples, inherited code, and configuration
module consumers. These are navigation suggestions, not a reduced objective.
The whole-project goal remains active, unfinished, and uncommitted.
