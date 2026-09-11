# Core program-service docstrings — 2026-09-09

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following the [application API batch](project-docstrings-application-api-2026-09-09.md).
Work remains local on `codex/project-docstrings`, based on `edf6bf05`, without
commit, push, PR mutation, submodule publication, or agents. The preceding goal
turn was verified progress; this turn adds further source/test documentation.

## Reviewed files

Eight complete production modules, all under `src/LiuXin_alpha/core/program_services`:

- `payloads.py` (15 functions): distinct integer/text/list coercions, ordered
  metadata adapters, projection before generic iteration/attributes, opaque/byte
  passthrough, key collisions, missing cycle/size guards, database-path metadata,
  role aliases, and submission settings versus completed-job outcomes.
- `stores.py` (12 functions): live/durable lookup fallback boundaries, advisory
  status versus loaded state, allowlisted edits, live-claim topology checks,
  readback failure after update, default selection, strict location stat, registry
  declarations, unregister-versus-byte-delete distinction, full read/add/locate copy,
  source registration, and post-registration reconciliation.
- `conversion.py` (three functions): registry-advertised formats, input/output/
  pipeline option precedence, Plumber initialization, and job submission rather
  than conversion completion.
- `ingest.py` (three functions): extension normalization, path-backed database
  identity, truth-tested switches, disk/remote worker request construction, and
  provider validation versus later scanning/network/persistence.
- `maintenance.py` (five functions): optional capability inspection, errors from
  present methods, independent queue observations, duplicate discovery, event
  budget, cleanup integer-array differences, merge IDs, and reconciliation timing.
- `storage_recovery.py` (three functions): filtered end-of-page completeness,
  10,000-item cap, requested/record whitespace asymmetry, optional/required UUID
  parsing differences, synchronous recovery/retry, and post-recovery listing.
- `backup.py` (eight functions): planning versus persistence/execution, inventory
  and digest-reading during planning, paged checkpoint reads, DRAFT declarations,
  durable validation before submission, and SquashFS build/publication options.
- `catalog.py` (nine functions): field families, repository-owned WEMI validation,
  exact projected Agent link filtering, identifier replacement, full text scans,
  skipped initial reads versus visible iteration failures, and mutation receipts.

New `tests/core/test_program_service_contracts.py` has its module and all ten
functions documented. Recording mocks and local data characterize projection
precedence/errors, opaque values, iterable coercion, job settings, recovery paging,
topology rejection and post-write status failure, global-search error boundaries,
Agent role filtering, and Store lookup/status fallback. No real job, Store, network,
or canonical row is mutated by these new tests. Normal full pytest discovers the
module; no default formatter/lint/type scope was expanded for it.

Cumulative reviewed scope: **128 modules, 184 classes, 1,179 functions**
(1,491 declarations). This ledger does not narrow the full project objective.

## Known ownership-test conflict — approval still pending

The existing `tests/scripts/test_workflow_ownership.py` counts all physical lines,
including docstrings, against 450 per service module and 160 per function. The
new documentation therefore produces these module-size failures:

| Module | Physical lines | Non-docstring lines, unchanged from baseline |
| --- | ---: | ---: |
| `payloads.py` | 475 | 234 |
| `stores.py` | 611 | 412 |

A complete scan found no other current service-owner size violation and no
function over 160 lines. The ordinary pytest test stops at the first oversized
module; both are listed here from that separate scan. Byte-for-byte comparison
after removing only genuine leading docstring statement lines confirms every
other line in all eight edited service modules is identical to baseline.

The previously unanswered question was renewed nonblockingly: may tests ignore
docstrings while retaining executable-code limits? **No answer has arrived and
no test or numeric limit has been changed.** Documentation was retained as real
progress toward every declaration, with the resulting test failure reported
explicitly rather than dropping the larger modules or compressing away useful prose.

If approval arrives, count only genuine AST-leading literal docstrings as
documentation and keep all existing implementation limits and single-return
facade checks. Do not raise ceilings, exclude modules, or classify arbitrary string
expressions as docstrings. The main `program_api.py` facade is not yet fully
documented; its stricter body/line constraints are also part of the pending question.
Other whole-project documentation remains available, so this is not an overall
blocked-goal condition and the goal remains active.

## Verification

Selections overlap and must not be summed as distinct tests.

- New characterization tests plus their runnable doctests: **24 passed**.
- Full Core program API, application API, program-facade, and evacuation regression
  modules: **44 passed**, with permission for local HTTP servers. This includes
  real fixture-database storage/admin/recovery, backup, schema, metadata, discovery,
  and direct/RPC workflows, not a live PostgreSQL or whole-project run.
- Final eight-service/new-test doctests, migration, public-documentation, developer
  links, and ownership tests: **88 passed, 46 explicitly skipped integration
  examples, one failed**. The sole observed failure is the physical-line owner
  guard described above. The earlier first-six-service run similarly had **41
  passed, 29 skipped, one owner-guard failure**. Do not report either as all-green.
- All **128 reviewed files** pass strict structural docstring audit and normalizer
  `--check`. All **111 edited production Python modules** and **ten edited existing
  test modules** preserve baseline executable ASTs, except for the previously
  verified single local-proxy runtime docstring-template literal. The data generator
  separately matches its own submodule HEAD after docstring stripping.
- All nine current-batch files are formatter-clean and lint-clean. Each production
  module had zero Ruff findings before and after; no findings were waived.
- Full `bash scripts/run_type_checks.sh` passes: 159 formatter-clean files,
  456 annotated modules, 221 dependency-protected modules, clean selected lint/
  complexity, 188 strict-mypy files, zero production type errors, and all 37
  negative examples rejected by each checker. That runner does not execute the
  separate pytest physical-line owner guard; its success does not erase that failure.
- API introspection with the documented application-test fixture remains unchanged:
  83 commands, 93 queries, three targets; sorted JSON SHA-256 with core_uuid omitted
  is `2b0dadbe883060aeefde4918889f4c4197a2067c470c6678fb1df84ec95293e3`.
- Root and data-submodule diffs are whitespace-clean. Final handoff developer-link/
  public-boundary checks: **23 passed**; a separate Markdown scan verifies all
  **120 local link destinations** across five working-memory notes. All observed
  source/test/audit/quality/handoff handles have completed, including the failed
  ownership selection; none is being treated as a still-running or missing result.

The final one-line backup-planning prose clarification followed the integrated
test run; executable code and examples did not change. Formatting, strict audit,
and normalizer checks were rerun on that module and the new contract-test module.
Referenced planner/customization source was read for accuracy, not added to the
reviewed ledger merely because those dependency excerpts were inspected.

## Remaining scope

The refreshed `/tmp/liuxin-docstring-current-2026-09-09.json` covers **2,724 modules,
4,399 classes, 35,019 functions**, including the new ten-function test module,
without parse failures. Missing/blank docstrings remain on **1,118 modules,
2,045 classes, 23,712 functions = 26,875 declarations**, plus overlapping prose/
structural gaps in the main note. The reviewed report is
`/tmp/liuxin-docstring-reviewed-2026-09-09.json`.

Next Core services: `database.py`, `discovery.py`, `metadata.py`, `schema.py`,
`storage_integrity.py`, `storage_repair.py`, and `storage_status.py`. Then remaining
endpoint modules/protocols and all other project packages, tests, scripts, examples,
inherited code, configuration-module docstring consumers, and the data submodule.
The whole-project goal is unfinished; neither these passing batches nor structural
audit alone prove descriptive completeness across the repository.
