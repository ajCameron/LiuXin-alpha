# Whole-codebase review — 2026-10-27

Reviewed revision: **`0eaadbadf8ffe472a83bc12e4e82d3fdcd8b48e5`**, current local
and remote `main` when checked. Review branch: `codebase-review-2026-10-27`.
The operator's uncommitted `databases/row.py` edits and dirty data
submodule were excluded and left untouched. No production code, real catalogue,
source collection, branch protection or mount configuration was changed.

**The high-level architecture is substantially better than September's baseline.
Keep the present package structure. The next work should close incomplete
integration and runtime contracts, rather than start another package rename or
rewrite.** The strongest improvements are real ownership boundaries with refusal,
rollback and failure tests. Remaining risks cluster around alternative mutation
routes, concurrency, failure acknowledgement and large-input behaviour.

This is a whole-repository structural scan plus a targeted runtime review, not a
line-by-line certification of every format/provider. Findings below distinguish
executed reproductions, source-traced gaps and known limitations. A passing test
selection is not a full-suite or deployment sign-off.

## Findings and priorities

P1 means resolve before deploying the affected feature or execution mode. P2
means a reproducible correctness gap or an important operational contract to close
in a focused change. P3 means contained compatibility/maintenance debt.

| ID | Priority | Finding | Evidence |
| --- | --- | --- | --- |
| CR01 | P1 | Merged stacked PRs have not delivered the status panel or setup defaults to `main`. | Git ancestry, current source and fresh GitHub merge records. |
| CR02 | P1 for concurrent in-process conversion | Conversion changes process-wide working directory and can write into another conversion's directory. | Deterministic two-thread execution through `InputFormatPlugin.__call__`. |
| CR03 | P2 | Generic administration can bypass the new storage record owner and leave contradictory records. | Real SQLite/Core mutation; scalar size changed while the manager's size and revision did not. |
| CR04 | P2 | SQLite Composite membership rejects a valid repeated Asset with the same role. | Real declaration fails its generated uniqueness constraint. |
| CR05 | P2 | Concurrent schedulers can enqueue one due occurrence twice. | Two scheduler instances, one SQLite ledger and a barrier after due selection. |
| CR06 | P2 | Directory discovery allocates an entire directory before enforcing the source-file limit. | A limit of one still consumes all 40 instrumented directory entries. |
| CR07 | P2 | Dirtied-event persistence can duplicate events after partial failure and lose a drained batch on malformed input. | Real database with an injected first-insert failure, then a malformed row ID. |
| CR08 | P2 | Conversion jobs omit structured loss reports from their returned result. | Source trace from PML report generation to the Core job return. |
| CR09 | P2 | Current `main` fails its quality gate, and required-check enforcement is absent. | Local reproduction and current-main CI/branch API observations. |
| CR10 | P3 | Retained compatibility paths still contain unresolved runtime names and unavailable providers. | Broad static scan plus inspection of representative callers and imports. |

### CR01. Finish integration of the status-panel and setup-default branches

The GitHub **merged** state is accurate for the named PR targets, but does not
establish inclusion in `main`:

| PR | Merge target | Merge commit | Present in reviewed `main`? |
| --- | --- | --- | --- |
| [#148](https://github.com/ajCameron/LiuXin-alpha/pull/148), resource guards | `main` | `752421ba` | Yes, through feature head `77276ec4`. |
| [#149](https://github.com/ajCameron/LiuXin-alpha/pull/149), status panels | `local-ingest-resource-guards` | `94a645ee` | No. |
| [#150](https://github.com/ajCameron/LiuXin-alpha/pull/150), setup defaults | `ingest-status-panels` | `e743aed0` | No. |

The later two merges occurred on October 27 after the relevant parent revisions
had already been integrated into `main`. Neither `11e22752` nor `559910f2` is an
ancestor of the reviewed revision. Current `main` lacks `ingest_status.py`,
`ingest_display.py`, `ingest_watch.py`, their tests, and `init --make-default`.
Its existing `liuxin connect` persistence is still present. This is an integration
gap, not evidence that those implementations themselves failed their PR tests.

**Next:** integrate the remaining feature commits through one PR against current
`main`; compare its final tree and installed command help. Require tests of the
combined tree, including selector-free import/watch after setup. Do not infer
delivery from the previous PR badges. This review does not merge or cherry-pick them.

### CR02. Enforce process isolation for conversion that changes cwd

[`InputFormatPlugin.__call__`](../../src/LiuXin_alpha/file_formats/conversion/plugin.py)
at line 452 enters `CurrentDir(output_dir)`, removes the directory's existing
contents, then calls the converter. [`CurrentDir`](../../src/LiuXin_alpha/utils/storage/local/__init__.py)
at line 60 uses `os.chdir`, which affects every thread in the process.

The reproduction used two tiny input-plugin subclasses and separate temporary
output directories. A entered its directory; B entered its own directory while A
waited; A then wrote a relative output. The file appeared in **B's directory**,
not A's. No real ebook or user file was involved. Relative cleanup has the same
shared-state problem, although this probe demonstrated misplaced output, not
deletion of another job's data.

Scope matters: [`get_backend`](../../src/LiuXin_alpha/utils/jobs/api.py) defaults
to **process** execution, which avoids this particular cross-job interleaving.
The selectable **serial** backend executes inside the utility manager's thread
pool; its name does not impose process-wide serialization. Direct concurrent
converter callers are also exposed. Distinct temporary directory names alone do
not solve this problem.

**Next:** require process execution for this conversion path, or serialize its
entire cwd-dependent lifetime before permitting in-process use. The longer-term
change is explicit absolute work paths. Test two overlapping real conversions
and failure cleanup, not just independent directory allocation. This is the
concrete remaining concurrency part of **A13**.

### CR03. Extend storage ownership to the generic administrative route

The dedicated `storage.resource.*` endpoints now refuse raw Asset/Replica and
Composite-member changes. That is useful, but
[`CoreApplicationAPI.admin_row_update`](../../src/LiuXin_alpha/core/application_api.py)
at line 2783 still falls back to
[`Library.update_row_fields`](../../src/LiuXin_alpha/library/library.py) at line
312 for non-Catalog tables. This reaches raw Row persistence without storage's
record mutation/revision owner. Generic create/delete and relation routes need
the same inventory.

In a fresh real SQLite catalogue, declare an Asset of size 1 through storage,
then issue:

```python
core.command("admin.row.update", {
    "table": "digital_assets",
    "row_id": asset.digital_asset_id,
    "updates": {"digital_asset_size_bytes": 2},
})
```

The database scalar becomes **2**, while `manager.get_digital_asset_record(...)`
still reports **1** and the same revision. Cache reconciliation on the Core
receipt does not synchronize storage's record representations.

This is **not an unauthenticated or read-only transport bypass**: the endpoint
requires `admin` authority and describes itself as raw administration. The gap is
the incomplete public mutation contract and the ability to produce inconsistent
storage records through ordinary administrative tooling. The existing scope note
that raw database access is privileged must not be read as universal storage
consistency for these endpoints.

**Next:** refuse these tables on generic routes or delegate to storage. If a raw
repair escape remains necessary, give it an explicit repair contract and a
rebuild/revalidation receipt. Test named storage, generic admin and relation paths
against the same owned tables. This extends **A10**, rather than introducing a
second storage manager.

### CR04. Repair ordered Composite membership in SQLite and existing catalogues

An ordered Composite can legitimately contain the same Asset twice under the same
role at different sequence positions. Storage's declaration accepts that model.
The SQLite generator's role-style many-to-many rule at
[`database_generator.py`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/database_generator.py)
line 1644 makes `(Asset, Composite, type)` unique instead.

Declaring two `CompositeDigitalAssetMembership` entries for one Asset, with
`role="chapter"` and sequence numbers **0** and **1**, fails with
`IntegrityError: UNIQUE constraint failed` on those three link columns. The
PostgreSQL declaration at
[`schema.py`](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/schema.py)
line 1368 does not impose that same triple constraint. PostgreSQL was source
compared here, not live-tested for this probe.

The owned mutation rolls back; the demonstrated problem is rejection of valid
input and backend disagreement, not a partial Composite. This was already listed
in **A10** and remains reproducible.

**Next:** define uniqueness around ordered membership identity, update the
generator and provide an explicit migration preserving existing rows. Cover
repeated same-role members, sequence conflicts and rollback on SQLite/APSW and
PostgreSQL. A fixture with distinct roles does not exercise this case.

### CR05. Claim scheduled occurrences atomically

[`JobScheduler.tick`](../../src/LiuXin_alpha/jobs/scheduler.py) selects due
definitions before calling `enqueue_run` separately for each. Enqueue serializes
its own mutation, but does not recheck the schedule occurrence and the scheduler
does not supply a stable request key. The durable ledger's existing submission
deduplication therefore cannot identify duplicate schedule delivery.

Two scheduler instances read the same due definition with `ALLOW_PARALLEL`,
synchronized immediately after due selection. Both `tick()` calls returned **1**;
the ledger contained **two** runs for that occurrence. `ALLOW_PARALLEL` permits
overlapping legitimate runs; it need not mean two deliveries of one timer event.
The default skip policy suppresses some overlaps, but is not a general schedule
identity contract.

**Next:** atomically claim a due occurrence and advance its cursor, or derive a
stable occurrence key and use the ledger's deduplication owner. Test two scheduler
instances, restart and a very fast completing handler. Keep this distinct from
the already implemented worker lease fencing and explicit retry ancestry (**A09**).

### CR06. Bound discovery work before materializing directory contents

[`MixedFormatIngestCoordinator._discover`](../../src/LiuXin_alpha/storage/ingest/mixed_format.py)
at line 1079 calls `sorted(scandir_iterator, ...)` before checking the file-count
budget. Cancellation, time and resource checks occur outside that enumeration.
A directory containing many entries is fully allocated even when only one file
is permitted; many subdirectories also expand the pending-directory list.

The instrumented iterator contained 40 regular entries. With
`MixedIngestBudget(max_source_files=1)`, discovery retained one file but consumed
all **40** entries first. This demonstrates the ordering problem without a large
memory allocation. It does not measure the memory footprint of a real collection.

The resource monitor records memory observations but currently enforces disk
headroom. SquashFS's own inventory has separate entry/metadata limits, so this
finding is specifically about filesystem discovery and its interaction with the
advertised limits, not a claim that archive inventory is wholly unbounded.

**Next:** bound and poll enumeration before sorting, or stream bounded batches and
state the resulting ordering contract. Add a directory-entry/directory budget
where file counts cannot bound work. Verify that a stop request is observed during
enumeration. Prioritize this before treating a successful small import as evidence
for arbitrarily large collections (**ING resource work**, alongside A12).

### CR07. Make dirtied-event persistence transactional and acknowledgement explicit

[`persist_dirtied_records`](../../src/LiuXin_alpha/databases/database/dirtied_mixin.py)
at line 468 drains the queue before insertion, converts row IDs during draining,
and best-effort requeues after an insertion error. It owns no atomic persistence
and acknowledgement contract.

Two real-database fault probes confirmed the consequences:

- After inserting the first of two events and injecting an insertion error, the
  method returned **0**, one row was already persisted and both events were
  requeued. Retrying persisted two more rows: **three rows for two events**.
- Queueing a valid event followed by a non-integer row ID raised `ValueError`
  before insertion. Both events were removed from the queue; the preceding valid
  event was not persisted or restored.

The ordinary success probe did survive close/reopen. The defect is failure
handling, not a claim that every call loses its rows because the method lacks an
explicit `commit`. Current backend behaviour must be distinguished from the
missing cross-queue transaction contract.

**Next:** validate a bounded batch before consuming it irreversibly, use the
database transaction owner, and retain stable event identities through retry.
Specify acknowledgement after durable commit. Do this before relying on the
queue for unattended sidecar publication (**MD01/A09**). Metadata record commits
themselves were not lost in these probes.

### CR08. Carry conversion losses through the application result

[`PMLOutput`](../../src/LiuXin_alpha/file_formats/conversion/plugins/pml_output.py)
attaches a `ConversionReport`, and the PML writer records unsupported-character
replacement. But [`run_conversion_job`](../../src/LiuXin_alpha/core/workflow_jobs.py)
at line 455 returns only paths, format names, output existence and size. Its
result does not contain the report or distinguish a lossy conversion from one
without reported losses. A normal converter return can also produce an
`exists=False` result without the job wrapper rejecting it.

This is source-traced; the review did not run an end-to-end lossy PML conversion.
The existing report unit tests demonstrate the report data model, not transport
delivery to the user.

**Next:** define one conversion outcome containing artifact publication status,
warnings/losses and unsupported capabilities, then carry it through jobs, Core
and CLI. Test a real lossy fixture and missing/invalid output. This is **A13**,
independent of CR02's execution isolation.

### CR09. Restore the quality baseline and enforce checks before integration

`bash scripts/run_type_checks.sh` stops at formatting:
`storage/durable_manager.py:1:1: unformatted`; **1 file would be reformatted,
245 were already formatted**. This also fails the current-main
[Tests run 40577364945](https://github.com/ajCameron/LiuXin-alpha/actions/runs/40577364945).
That run's confidence tests, format fast lane and installed wheel passed. Its
full suite and heavy lane were skipped because this was a push run; the later
quality-job steps, including Linux mount checks, did not execute after the failure.

On October 27, the branch API reports `protected: false`, and the effective
branch-rules endpoint returns `[]`. This records configuration as observed; the
review did not alter repository settings. PR #150's earlier green full suite is
evidence for its own revision, not a current-main full-suite result.

**Next:** fix the formatting change without unrelated source churn, obtain a green
combined-tree PR run, and make the intended checks required. Verify branch ancestry
and final-tree behaviour when finishing a stack. This refreshes **A15** rather
than reopening the old dependency failures that were already repaired.

### CR10. Triage retained compatibility defects without calling them all dead code

A broad `ruff check --select F821` reports **103 diagnostics**: caches 4,
constants 1, databases 9, file formats 22, metadata 4, startup scripts 8,
surfaces 12 and utilities 43. Ruff could not decode two legacy files and warned
about a misplaced suppression in another. This is a diagnostic inventory,
not 103 proven runtime failures or an exhaustive missing-import scan.

Examples worth handling through their real caller boundaries:

- `OneToOneWriter.series_index_one_one_db_updater` calls the undefined
  `library_set_series_index`. The generic legacy writer selects this hook for
  `series_index`; modern Catalog writer coverage should not be used to certify it.
- `surfaces.thumbnail_cache` refers repeatedly to undefined `as_unicode` in
  error paths. This review did not establish that current primary surfaces invoke
  those paths; an unused compatibility module should be retired through caller
  evidence, not silently counted as a production defect.
- `file_formats/docx/images.py:299` imports `utils.wmf.emf` for an EMF image,
  but that package is absent. The following handler also uses `e.message`.
  Availability needs an explicit unsupported outcome or a working implementation.
- Several reported names occur in deferred annotations. Those are not equivalent
  to immediately executing an undefined name in a handler.

A separate AST pass found **zero statements directly after an unconditional
return/raise/break/continue in the same statement list** across the scanned
package. That narrow result supports the earlier straight-line cleanup; it does
not prove every module is reachable or detect all conditional dead branches.
`library.legacy`, `library.backend`, `customize` and `plugin_system` remain retired.

**Next:** select one live compatibility boundary, reproduce its supported failure,
then fix or explicitly retire it and tighten the relevant gate (**A13/A16**).
Avoid another blanket deletion or generated-docstring campaign. Many older
docstrings still describe unrelated “file-format” operations or generic return
types; coverage counts alone do not make them accurate contracts.

## What has improved, and what remains by subsystem

| Area | Current assessment and remaining work |
| --- | --- |
| Core and Library | `LibraryServices` and Core own constructed resources; Library borrows collaborators. Failure cleanup and transaction lifetime now have explicit tests. Keep retiring remaining upward path-opening adapters and implicit maintenance ownership through A11. |
| Catalog and metadata | Shared repository/writer preparation, schema-declared normalized identities and rating validation materially reduce the old divergent-writer problem. This is not complete semantic validation of every identifier/date/text field. CR07 concerns delivery of later metadata work. |
| Database backends and schema | Create-only publication, explicit replacement and transaction/savepoint ownership address the earlier destructive-default problems. CR04 is a concrete remaining backend-parity defect; low-level repair still needs an explicit contract. |
| Read models and caches | Category semantics and preferred WEMI selection have shared owners. Cache refresh failures have committed-write receipt support. Snapshot invalidation, broader search/acquisition projections and query costs still need workload-specific work. CR03 concerns storage representations, beyond ordinary Catalog cache refresh. |
| Storage and backup | Read-only builders, deletion policy, staging ownership, Asset/Replica, policy and Composite mutation slices are substantial improvements. Generic administration, Item/workflow adapters and physical-cache leases remain incomplete; see CR03/04. |
| Local/SquashFS ingest | Source/cache separation, private read-only mount verification, protected image/device paths and disk-headroom telemetry are present on `main`. No new source-write bypass was found in the inspected paths. CR06 limits the large-directory resource guarantee. Actual privileged mounts and a representative large collection were not exercised in this review. |
| Web ingest | Scope checks, bounded discovery/downloads, owned staging, durable selection and publication receipts are implemented. It has its own run/lease machinery; do not present that as complete convergence with durable jobs. Long backend operations and real network/server variability remain separate deployment evidence. |
| Jobs | Atomic ledger transitions, captured inputs, lease fencing, request identity and durable Core audit are implemented. CR05 is schedule occurrence ownership. Conversion/ingest/backup still have distinct execution and recovery contracts; consolidation should follow actual workflows. |
| Plugins and formats | Unified `plugins`, passive composition and format containment are worth preserving. Availability and loss reporting remain incomplete; CR02/08/10 matter more than another namespace change. |
| Surfaces and transport | Server-owned capabilities, read-only composition and refusal tests are present. Raw admin remains privileged. Mainline CLI currently lacks the two later feature slices in CR01. Large surface modules merit extraction only at measured or tested seams. |
| Configuration, utilities and startup | Application preference ownership is consolidated. `ptempfiles` still imports the hook-bearing `startup_scripts` package; scratch and process-global legacy hooks remain A12 work. Separate domain registries are not automatically duplicate authorities. |
| Resources, packaging and documentation | Installed-wheel verification passed for current main in CI. The architecture index and subsystem contracts are useful, but many “implemented for PR review” dates describe old checkpoints. Record ancestry/integration separately from implementation and tests. |

## Fresh measurements and validation

The source inventory contains **1,826 Python modules across 16 top-level
packages, plus six root modules**; the 1,826 total includes the root modules.
The 1,220,300 physical lines include docstrings, vendored code and large static
Unicode/data tables. They are not a measure of executable complexity. An
encoding-aware AST parse found no syntax errors in these files.

The independently executed checks passed:

```bash
.venv/bin/python scripts/check_modern_import_cycles.py
.venv/bin/python scripts/check_layer_direction.py
.venv/bin/python scripts/check_import_footprint.py
```

They report **249 protected modules**, **1,826 modules in the direction scan**,
**15 reviewed backward edges**, **28 unresolved dynamic imports**, and
**25 passing cold-import budgets**. The checked baseline retains a largest
combined/runtime component of **214** and domain component of **9**, with no
combined Catalog/cache component. These gates enforce named boundaries and
ratchet debt; they do not certify universal acyclicity, dynamic imports or all
runtime behaviours. The full quality runner failed as described in CR09; its
later typing stages must not be reported as freshly passed.

The targeted regression selection covers jobs, lifecycle, transport permissions,
durable audit, storage record/Composite ownership, transactions, ratings, WEMI
selection, read-only builders, deletion policy, backup staging, mount primitives,
resource guards, web-ingest CLI and plugin boundaries. It runs serially with
per-test scratch cleanup and telemetry: **394 passed, 18 skipped in 887.31
seconds**. The skipped cases require an unconfigured live PostgreSQL URL; they
are not backend-parity evidence. This review does not run the entire application
suite. The earlier GitHub full-suite result belongs to PR #150's feature revision.

A final, separate selection passed **48 tests** covering documentation navigation,
public documentation boundaries and the SquashFS read-only backend with temporary
archives. Combined, the two selections give **442 passed and 18 skipped**; they
are disjoint selections, not a full-suite total. No additional runtime change was
made between those checks and this report.

All diagnostic reproductions used disposable directories and databases. The
conversion and scheduler probes use deterministic synchronization; the discovery
probe counts an iterator instead of allocating a huge directory; CR07 injects
specific failures. No remote website, real library or source device was modified.

## Suggested next sequence

1. **Finish integration and restore the gate:** CR01 and CR09. Verify the final
   tree, installed CLI and CI before deploying the features already requested.
2. **Close the storage consistency holes:** CR03 and CR04 as separate focused
   PRs under A10, including caller inventories and real-backend tests.
3. **Bound large-import discovery:** CR06, then run a representative staged
   SquashFS pilot with resource telemetry. Keep registration, metadata extraction
   and conversion as separately observed operations.
4. **Fence concurrency and delivery:** CR02 for any in-process conversion;
   CR05 for multiple scheduler instances; CR07 before durable sidecar automation.
5. **Expose actual outcomes and retire one live legacy boundary:** CR08/10,
   followed by the remaining A11/A12 caller migrations.

These priorities refine [next_tasks.md](next_tasks.md); they do not replace its
subsystem ownership or duplicate its implementation plans. The September review
remains historical evidence. New fixes should reference the relevant CR ID and
record implementation, verification and inclusion in `main` separately.
