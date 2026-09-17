# Complete project docstrings in bounded modules

Status: **D106–D115 complete**, 2026-09-17. The latest request authorized ten
additional units: **257 declarations in 25 complete files**, all verified.
**Stop here; D116 is next only on a new request.** See the
[latest ten-unit checkpoint](../working-memory/project-docstrings-d106-d115-2026-09-17.md).
Earlier full-D/hundred-unit authorizations remain paused; they do not enable
continuation beyond this completed batch.

## Baseline and navigation

The baseline contains **2,730 Python files**, of which **662 are source-reviewed**:
869 classes and 7,146 functions, or 8,677 declarations including modules. There
are **2,068 remaining files** and **33,572 remaining declarations to review**.
P00's whole-project audit reported 23,003 missing/blank docstrings; existing docs
also need review. These measures are different and must not be added together.

Since that baseline, G01 and the completed C batches reached 722 complete files.
After the [shim cleanup](shim-removal-plan.md) and D106–D115 continuation,
the live D scope contains
**115/232 verified units**,
**2,836/5,685 declarations**
and **153/374 complete files**.
Current project coverage is **839/2,671 files**,
**12,064 declarations in complete files**,
plus **37 verified partial-file declarations**.
Remaining: **1,832 files and 30,060 declarations**.

The historical D095 checkpoint remains 95/232 units; source removals changed the
live inventory rather than completing new documentation work. Removed scopes are
archived in the [reconciliation evidence](../working-memory/test-results/shim-removal-2026-09-15/documentation-reconciliation.json).
Affected files have new functional baselines in documentation-rebase.after.json;
their original baseline hashes and historical documentation-only proofs are
preserved. Do not replay old batch helpers with fixed file/declaration totals.
The latest ten-unit continuation is complete, with D116 next only on request.

- [Exact work inventory](project-docstrings-work-units.json): stable module IDs,
  source hashes, exact file/declaration selections, prerequisites, and checkpoints.
- [Reviewed manifest](../working-memory/project-docstrings-reviewed-files.txt):
  files whose entire documentation pass is complete.
- [Established rules and exceptions](../working-memory/project-docstrings-2026-09-08.md):
  historical evidence, native-C coverage, runtime-doc exceptions, and known guards.
- [Quality gates](maintainability-quality-gates.md) and
  [test streams](test-streams.md): existing verification owners.

The inventory includes all unreviewed files, including those with no structural
findings. Scope includes private/nested/async definitions, tests, scripts,
examples, inherited code, preferences, and tracked submodule Python. Retain the
completed native-C checkpoint separately. Ignored artifacts and anonymous lambdas
are not named Python declarations to document.

## Completed SQL continuation — D106–D115, 2026-09-17

Shared SQL support, Calibre/FRBR builders and common driver mixins are complete.
Each exact selection has [saved observations](../working-memory/test-results/docstrings-d106-d115-2026-09-17/campaign.json).
The FRBR generator was promoted after both assigned units passed.

Verification: 139 focused regressions passed, three skipped, 35 runnable examples
passed, full configured quality gates passed, and all 257 declarations are clean.
Executable ASTs and comments are unchanged; no Ruff findings were added.
D116 (SQL driver utility functions, first selection) remains queued. This request
covered exactly ten units and does not resume older broad batches.

## Previous ten-unit continuation — D096–D105, 2026-09-17

D096–D105 cover the driver registry/macros and PostgreSQL backend. Each exact
selection has its own saved observations in the
[batch evidence](../working-memory/test-results/docstrings-ten-2026-09-16/campaign.json).
The batch began on 2026-09-16 and completed on 2026-09-17. Connection and native
driver files were promoted only after all their assigned units passed.

Verification: 35 focused regressions, 78 runnable examples, four isolated macro
result modes, full configured quality gates, and complete-file docstring checks.
Executable ASTs and comments are unchanged; no Ruff findings were added.
D106 was the next queued unit at that checkpoint; the subsequent SQL continuation
is recorded above. The earlier batch authorized only D096–D105.

## Authorized batch — 2026-09-13

The user authorized thirty queued modules, C03 through C032, in inventory order.
**All thirty are now verified.** Both planned milestones and individual checkpoints
are saved. This completed batch was the sole override of the one-module stop rule;
stop for token-spend review. See the
[campaign record](../working-memory/test-results/docstrings-thirty-2026-09-13/campaign.json).

## Authorized D range — 2026-09-14

The user authorized the complete D range, D001 through D232. This supersedes the
one-module stop rule for this track. Keep individual declaration selections,
partial-file promotion rules and verification checkpoints. Work in coherent test
groups, reuse unchanged evidence, and keep the full D objective active until its
requirements are verified. Exact baseline snapshots and progress are in the
[D campaign](../working-memory/test-results/docstrings-d-2026-09-14/campaign.json).
No commit or publication is requested.

## Module contract and cost controls

A normal module targets **2–4 files and no more than 40 declarations**. A package
with one remaining file can have a one-file module. Do not combine unrelated
packages to fill capacity. P00 and G01/G02 are separately scoped enabling modules.

- One explicit module request authorizes one module, followed by a checkpoint and
  stop. There is no automatic continuation, commit, or publication.
- Use this guide, the requested inventory entry, and its latest checkpoint as the
  starting context. Do not load the complete inventory or historical conversation
  into the agent context; extract just the selected module and its file records.
- Freeze exact scope and relevant tests during preflight, before editing. Existing
  regression commands in queued entries are candidates, not proven coverage.
  Confirm them by source review; narrow broad candidates to relevant selectors.
  If none exist, select caller coverage or record why static/doctest evidence is
  appropriate. Do not run unrelated tests simply to obtain a green result.
- Read selected source and only necessary dependency excerpts. Preserve reviewed
  work and reuse evidence for unchanged source hashes. Record available token
  usage; a user-supplied budget takes precedence. Do not claim an unenforced hard cap.
- Large files are split at lexical declaration boundaries. Keep complete class or
  function subtrees together where they fit; split oversized containers into their
  own docstring and separately selected members. Line ranges are context hints,
  not permission to rewrite all nested declarations within those lines.
- Declaration IDs use `kind:qualified.name#occurrence`. Occurrence distinguishes
  repeated definitions in lexical traversal order. Docstring insertion changes
  line numbers without changing these IDs. Each selected class/function ID covers
  only its own docstring, not its members. Never duplicate declaration assignments.
- A verified partial-file module does not make the file reviewed. Promote the file
  only after all assigned modules and a complete-file audit/normalizer pass. Never
  mark a partially reviewed file complete to fit a size or cost boundary.

Every module records purpose, prerequisites, exact scope, intended changes,
validation, evidence, and a resume point. States are `queued`, `in_progress`,
`partial`, `verified`, and `blocked`. Unavailable checks remain unverified.

To retrieve a single module without dumping the backlog, run from the repository
root (replace `C01` with the requested ID):

```bash
python3 - <<'PY'
import json
from pathlib import Path
inventory = json.loads(Path('dev-docs/project-docstrings-work-units.json').read_text())
unit = next(unit for unit in inventory['units'] if unit['id'] == 'C01')
print(json.dumps(unit, indent=2))
for item in unit['scope']:
    if isinstance(item, dict):
        print(item['path'], inventory['files'][item['path']])
PY
```

At module start, compare files with `current_sha256` when present, otherwise with
`baseline_sha256`. Keep baseline hashes unchanged when recording later edits;
record their new current hashes and `last_changed_by`. Expected changes
from a preceding partial-file module must be supported by its checkpoint; unrelated
changes require revalidation and refreshed scope. Save pre-edit source/AST evidence
outside tracked source. At completion, record current hashes, commands, terminal
exit codes, skips/failures, selected declaration IDs, and the exact resume point in
`working-memory/test-results/project-docstrings-<ID>-<date>-observations.json`.
Record the checkpoint location in the unit, then refresh its file records.

## Initial modules and milestones

| ID | Fixed scope | Verifiable checkpoint |
|---|---|---|
| P00 | Reconcile the paused batch; save plan, queue, and navigation | Exact inventory partition; bounded modules; completed prior results recorded; links pass |
| G01 | Shared docstring-aware ownership measurement and focused tests | Docstrings excluded; executable content still counted |
| G02 | Integrate the helper into existing ownership tests | All seven recorded guard failures resolved without raised limits |
| C01 | Agent repository and API: 2 files, 31 declarations | Agent matching, aliases, types, and credit regressions |
| C02 | Curated identifier repository and API: 2 files, 26 declarations | Normalization, reuse, owner isolation, and rollback regressions |
| C03 | Observed Item identifier repository and API: 2 files, 13 declarations | Observation storage and optional Item-scope regressions |
| C04 | Title repository and API: 2 files, 20 declarations | Preferred-title and title-update regressions |
| C05 | Bundle retrieval and API: 2 files, 16 declarations | Path selection and attachment regressions |
| C06 | Hierarchy retrieval and API: 2 files, 9 declarations | Parent/child traversal regressions |
| C07 | Graph retrieval and API: 2 files, 10 declarations | Graph contents, limits, and truncation regressions |
| C08 | Projection retrieval and API: 2 files, 10 declarations | Display-title and summary regressions |
| C09 | Retrieval composition and API exports: 2 files, 5 declarations | Import, composition, and protocol checks |

After C04, require all 13 concrete repository modules and all 15 repository API
modules reviewed, plus full Catalog regression and quality checks. After C09,
apply the same milestone checks to the complete retrieval/API trees. Subsequent
inventory IDs use the track letter plus three digits. IDs remain stable when
statuses change; append new units rather than renumbering published IDs.

G01/G02 are user-approved changes to test measurement, not production behavior.
Use a shared helper in `tests/support/docstring_ownership.py` with focused tests in
`tests/scripts/test_docstring_ownership.py`. Exclude only recognized leading
literal docstrings from body statement counts and documentation-only physical
lines from size measurements. Preserve numeric thresholds, comparison operators,
non-doc comments/blank lines, delegation requirements, and dependency checks.
Mixed code/docstring lines still count. Ordinary strings, assigned strings, bytes,
and f-strings must not acquire an exemption. Test module/class/function/async/nested
cases, single/multiline forms, same-line code, and truly oversized code. G02 must
update the affected guard descriptions as well as their measurement calls.
Guard edits do not by themselves document unreviewed test files; their remaining
documentation assignments stay queued. G01's new Python files enter the inventory.

## Whole remaining programme

Every path has one track and one or more non-overlapping declaration assignments.
The queue groups immediate packages and corresponding API packages where possible,
then uses lexical owner/file order and packs within the size limits. The fixed
C01–C09 entries take precedence. Module prerequisites express hard dependencies;
track order is a recommended progression, not authorization to continue.

| Track | Remaining files | Modules | Module families |
|---|---:|---:|---|
| C — Catalog | 28 | 20 | Repositories, retrieval, policy/writers, search, metadata tools, compatibility, remaining tests |
| T — Test infrastructure | 116 | 66 | Shared fixtures, provisioning, database helpers, support utilities, typing fixtures |
| U — Utilities | 313 | 208 | Configuration, logging/resources, text/language, images, jobs/IPC, archives, plugins, bundled libraries |
| D — Databases and caches | 246 | 127 | Contracts/schema, individual backends, portable macros, rows/links, cache models/readers/writers, tests |
| M — Metadata | 344 | 274 | APIs/constants, container families, book helpers, file/local/web sources, tests |
| F — File formats | 574 | 347 | Conversion/OEB, individual format families, readers/writers, compatibility helpers, tests |
| A — Application/configuration remainder | 133 | 128 | Library, customization, remaining surfaces/jobs, root modules, constants/resources, preferences, residual tests |
| S — Tools and script tests | 51 | 28 | Operational tools, benchmarks, fixture generators, build/package tools, verification tools |
| L — Legacy source trees | 52 | 37 | Retained setup/test trees and compatibility shims |
| **Total** | **1,857** | **1,235** | **All currently unreviewed files** |

Implementation, API, and test documentation can occupy different modules when size
requires it. Existing tests can validate multiple modules without being edited or
counted repeatedly. A source path with several partial modules remains one file in
the track total. Reviewed baseline files have no queued documentation assignment.

At each track milestone, enumerate through `audit_project_docstrings.project_paths`
again, including submodules. Add new source paths and flag changed reviewed hashes;
never silently shrink scope because discovery, parsing, or a backend failed.

## Documentation rules and acceptance

Read source before writing substantive reST prose. Include useful summaries,
examples, ordered parameter descriptions, returns, and applicable errors. Explain
validation order, state changes, resource ownership, transactions, and limitations
where they matter; do not substitute symbol-name restatements for review.

Preserve executable ASTs, signatures, API aliases, constants, comments, fixture
literals, and assertions. Separate implementation bugs from documentation work.
Retain the three narrow established runtime-doc exceptions without generalizing
them. Inspect consumers of generated documentation and configuration executed into
dictionaries: adding a module docstring can introduce observable `__doc__` data.

Each documentation module must pass the following checklist:

- [ ] Entire selected scope source-reviewed, including private/nested declarations.
- [ ] Selected declarations pass `documentation_issues` from the project auditor.
- [ ] Selected docstrings are normalizer-clean; unsafe literals are handled explicitly.
- [ ] Executable ASTs match the module's pre-edit snapshot; comments/headers are preserved.
- [ ] No new Ruff findings and no whitespace errors.
- [ ] Runtime-doc consumers checked; appropriate runnable examples and focused regressions pass.
- [ ] Terminal exits, skips, failures, hashes, and partial-file status saved in the checkpoint.
- [ ] Module status updated and execution stopped.

For complete files, use `scripts/audit_project_docstrings.py --check <files>` and
`scripts/normalize_docstrings.py --check <files>`. For partial files, resolve the
selected stable IDs through the auditor's `definitions()` traversal and check only
those definitions. Check normalizer replacements for those nodes with their actual
implicit-receiver context; do not require unrelated unfinished docstrings to pass,
or rewrite them to make the selected unit green. Complete-file checks are mandatory
before file promotion. Static compliance is not evidence of descriptive accuracy.

At subsystem milestones, run its broader regression suite, documentation contracts,
the full quality runner, and inventory reconciliation. Reuse valid unchanged evidence
instead of repeating broad checks after every small module. Required focused tests
are not replaced by unrelated passing tests. Standalone probes importing LiuXin
must use isolated runtime directories when pytest is active; avoid the previously
observed shared-directory cleanup race.

The programme is complete only when every current project-owned file is fully
reviewed, no assignments are partial/blocked/unassigned, the whole-project audit
has no required structural findings or parse failures, and applicable regression,
documentation, typing/formatting, dependency, and ownership checks pass. Preserve
separate native-C/runtime-doc evidence. Environment-dependent checks remain explicitly
unverified until run. Final manifests, portable developer links, and counts must agree.
