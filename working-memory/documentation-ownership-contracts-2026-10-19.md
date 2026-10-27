# Documentation ownership contracts — 2026-10-19

Branch: `docs-ownership-contracts`, based on `15080c6e` after PR #126 merged into
`configuration-ownership`. The previous plugin rename is complete and closed.

## Scope and corrections

The first pass covers guide 04 and the Catalog contracts/work list, with a
merge-checkpoint update in guide 02 and a dated structural-review follow-up.
The subsequent schema pass below rewrites guide 03 and corrects a stale test
path left by the earlier documentation move. No runtime policy changes or
package moves are included.

- Guide 04 now separates implemented ownership, accepted policy and remaining
  migrations; repeated storage prose is consolidated with links to its owner.
- Catalog's roadmap claimed both legacy import allowances were empty. The actual
  boundary permits only Catalog to import `metadata_tools`; the indirect allowance
  is empty, and `catalog_macros` has no production imports.
- The migration record now distinguishes frozen macros, live Row adapters and the
  retired `library.legacy` / `library.backend` facades.
- The cache guide now permits Catalog's existing `ReadCache` composition while
  keeping writable-cache delegation and reconciliation in the cache owner.
- API/write guidance describes shared repository preparation and relationships,
  retaining the field-validator and non-Catalog/raw-write limits. Historical
  identifier codec descriptions no longer imply live retired Library writers.
- The fitness review is clearly historical. The roadmap is the current work list;
  it no longer describes repository-wide gates as an unimplemented future task.
- The structural review records completed navigation/plugin work and the focused
  contract corrections without changing its original audit measurements.

## Source evidence

Inspected `catalog/catalog.py`, `catalog/row_mutations.py`, repository preparation,
`tests/catalog/test_legacy_mutation_boundary.py`, `catalog/retrieval/metadata.py`,
`caches/read_cache.py`, `caches/cache.py`, `core/library_services.py`,
`core/browse_api.py`, `ingest/mixed_application.py` and identifier helper callers.
Existing configuration/plugin contracts and the dated write/read handoffs provide
context; historical test counts are labelled as such.

## Validation

All **25 tests passed in 60.05 seconds**:

```bash
PYTHONPATH="$PWD/src" PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q \
  tests/scripts/test_developer_documentation_links.py \
  tests/scripts/test_public_documentation_boundaries.py \
  tests/catalog/test_legacy_mutation_boundary.py \
  --telemetry-dir=/tmp/liuxin-docs-ownership-telemetry \
  -o tmp_path_retention_policy=none
```

The checks cover tracked local link targets, guide reachability, public ownership
signposts, the Catalog writer boundary and the actual legacy import allowances.
`git diff --check` passes. There is no runtime change requiring a full application
suite. The run emitted one PytestCacheWarning because its canonical uppercase
Windows checkout path is read-only in the sandbox; all assertions passed. Use an
explicit writable `/tmp` cache directory on subsequent runs.

Local-only evidence: `working-memory/test-results/documentation-ownership-20261019/`
contains the test log and telemetry. Historical test totals in the guides were not
re-run or relabelled as current results.

## Workspace and next slice

The main checkout carries this branch. The six pre-existing storage docstring/comment
edits are preserved byte-for-byte; their starting digests are in the local-only
`/tmp/liuxin-docs-ownership-local-edits.json`. The data submodule is untouched.

## Schema overview follow-up, ready for user review

The user requested the next schema slice before reviewing. Guide 03 now has a
source map, WEMI/storage diagram, many-to-many WEMI relationships, Agent credits,
curated versus observed identifiers, atomic/composite assets, replicas, policy,
generated links, normalized identities, compatibility views and migration/backend
limits. The database index presents it as the current overview.

A fresh in-memory SQLite build produced **207 tables and 22 views**, with integrity
`ok` and no foreign-key violations. The connection was closed after inspecting
objects and metadata. Confirmed 34 named tables, the `books`/`titles`/`identifiers`
view aliases, separate composite tables/links, and all five current full-text
identity profiles against that snapshot. The builder still reports its existing
duplicate-FK/interlink declaration warnings; this documentation work does not
change those declarations.

Linked asset-policy and field-guide documents now clearly identify their older
model and point to the current schema. The column-policy guide now lists
`unicode_nfkc_whitespace_casefold` for tags, labels, genres, subjects and series,
matching source declarations and generated metadata. Current copy minima allow
zero; composites do not use an asset-kind column or `digital_asset_compositions`.

### Schema-slice verification

Latest outcomes: **42 distinct passing cases** across:

- `tests/scripts/test_developer_documentation_links.py`
- `tests/scripts/test_public_documentation_boundaries.py`
- `tests/databases/test_frbr_wemi_smoke.py`
- `tests/databases/test_frbr_aggregate_views_wemi.py`
- `tests/databases/test_frbr_storage_policy_store_modes.py`
- `tests/databases/test_frbr_interlink_constraints.py`

The initial run had 41 passes and one failure: the WEMI smoke test read the old
`dev-docs/column-metadata.md` location after successfully checking generated
metadata. Its path now includes `databases/`; both smoke cases pass on rerun
(7.69 seconds). This one-line path correction is the only test-code change.
No assertion was weakened. The documentation checks passed in the initial run.

Runs used absolute source PYTHONPATH, serial execution, telemetry, `/tmp` scratch
and pytest cache, and immediate temporary-directory cleanup. Peak process-tree RSS
was about 59 MiB; minimum available RAM exceeded 6.5 GiB; OOM-kill delta was zero.
Task scratch and the pytest cache were removed. No full application suite was run.

Local-only evidence: `working-memory/test-results/documentation-schema-20261019/`
contains the generated schema snapshot, build log, both test logs, telemetry and
latest-outcome summary. `git diff --check` passes; all six storage-edit digests
still match. Changes remain uncommitted on `docs-ownership-contracts` for the
user's review.

After review, the detailed field inventory and older numbered storage/compatibility
guides still need reconciliation. Keep the broader proposed path migration separate
until their current content has clear owners.


## Worked link examples follow-up

The user requested worked examples, especially WEMI relationships. Added
`dev-docs/databases/wemi-link-examples.md`, linked from guide 03, the database
index and Catalog retrieval guidance. Five cumulative examples cover:

1. One Work, English/French Expressions, three editions and three Items.
2. A bilingual Manifestation and one Item appearing on two rays.
3. Sharing one Expression across two Works, including all reachable Manifestations
   and the inability of binary links to restrict the Work by Manifestation context.
4. Primary/priority selection and a real Catalog primary-change call.
5. Author/translator roles, an Item's payload and two physical replicas.

A final section distinguishes unlinking from deleting endpoints and reassignment
of an Item's one Manifestation foreign key from moving bytes.

### Selection discrepancy made explicit

The SQLite default-ray view ranks primary flags first and priorities ascending.
Catalog bundles follow repository ordering (descending priority then destination
ID), without a primary-marker preference. The worked fixture confirms the default
view chooses `1:12:103` after promotion while the Work bundle selects `[1,12,104]`.
The API guide's incorrect claim that an Item has an unambiguous full upward path
is corrected, and the Catalog roadmap records selection-policy convergence as
remaining work. No runtime selection rule changed.

### Verification

The example checker builds a disposable fresh SQLite database, populates the
shown rows, executes all four SQL blocks read directly from the guide, and checks
the listed paths/copy counts. It then opens the real Database/Catalog facade,
executes the guide's Python block, verifies the primary flags/default view and
bundle choice, and exercises the described unlink/reassignment behaviour.
All five scenarios pass; the database is closed and its temporary directory removed.
All **22 documentation/public-boundary checks pass in 40.84 seconds** after adding
the guide. `git diff --check` passes. The six existing storage edits still match
their preserved hashes. Local-only evidence:
`working-memory/test-results/documentation-wemi-examples-20261019/` contains the
example checker/results/log, documentation test log and telemetry. Test scratch
and the task-specific pytest cache were removed.

The new guide has an intent-to-add index entry so it appears in the uncommitted
review diff and the tracked-target documentation check. The user's review state
is preserved; there is no new commit or PR.

## Multiple Works in one Manifestation follow-up

The user requested more examples of Manifestations embodying several Works,
especially a novel and its introduction. Added examples 6 and 7 to the worked
link guide, with links from the schema overview and database index:

- *The Winter Orchard* and *Reading The Winter Orchard* have separate Works,
  Expressions and author credits, sharing Manifestation 105 and Item 1005.
- An omnibus reuses the novel's Expression alongside a second novel. Its links
  omit the introduction, so that Work reaches only the first edition. An explicit
  optional link adds the introduction to the omnibus without another Item.

The introduction's separate Work is an explicit cataloguing choice. The guide
distinguishes sharing a Manifestation from sharing an Expression and explains
that WEMI link priorities do not encode page order or the volume's main content.
A Mermaid diagram, stored link rows, SQL queries and results show the two paths
to one owned volume and why joined row counts differ from copy counts.

### Verification

The extended disposable-database checker passes all seven scenarios and reads all
six SQL blocks and the existing Python block directly from the guide. The new
WEMI edges are created through the real Catalog API. Assertions cover separate
author credits, shared Item identities, the introduction's absence from the
omnibus, its optional inclusion, foreign keys and SQLite integrity. The temporary
database is closed and removed. All **22 documentation/public-boundary checks pass
in 35.49 seconds**, using telemetry and `/tmp` scratch/cache. `git diff --check`
passes, and the six unrelated storage-file hashes still match.

Local-only evidence:
`working-memory/test-results/documentation-wemi-multiwork-20261019/` contains the
updated checker, example results/log, documentation test log and telemetry. Task
scratch and pytest cache were removed. Changes remain uncommitted for review.

## Calibre compatibility documentation follow-up

The user asked to continue the developer-documentation review. This slice covers
guide 05 and its compatibility policy, mapping guide and subsystem index. The
developer index and structural-review status table now reflect the current guide.

### Findings and corrections

- Guide 05 was only an import-diagnostics snippet. It now maps the actual readers,
  metadata adapters, generated SQL projections, web routes and diagnostic owner.
- The canonical-import draft described candidate tables, a compatibility-record
  class and a Catalog package as though implementation were imminent/current.
  These names are absent from source. Its Manifestation-anchor proposal is
  retained, with the unimplemented persistence/workflow work stated explicitly.
- Existing `CalibreImportPolicy` and `iter_import_jobs()` classify payloads as
  full, metadata-only or skip; they do not execute a canonical WEMI import.
- Source Calibre IDs, SQL ray IDs and surface Work tokens are separate identities.
  The mapping guide uses the novel/introduction example to show two SQL book rows
  reaching one owned volume. A source row remains separate from both projections.
- The draft now records unresolved anchor referential integrity, repeat imports,
  splits across editions, format/Item assignment and recovery across SQL/byte
  operations. Candidate DDL has been replaced by requirements, not a migration.
- Reading guidance distinguishes strict schema/version inspection from payload
  iteration, describes connection ownership and the absent-database OPF fallback,
  and preserves the import-diagnostics example as a scoped callback wrapper.

### Example verification

Both Python fences in guide 05 were extracted and executed. A temporary generated
Calibre library containing seven books yielded the first five source IDs/titles;
the source database hash was unchanged, and both inspection/streaming connections
were confirmed closed after early iteration. Raising the fixture's schema version
confirmed the explicit version policy refuses it. The diagnostic wrapper preserved
a successful callback result, logged/re-raised a missing import, and restored the
original import function. The temporary library and its template cache were removed.

### Focused checks

All **45 checks passed in 71.80 seconds**: documentation links/public boundaries,
import diagnostics, source database/schema reads, import-job classifications,
strict-version refusal, SQL books/titles projections and web Work-token/metadata
retrieval. The run used telemetry, serial execution and `/tmp` scratch/cache with
immediate test-directory retention disabled. Scratch and pytest cache were removed
after completion; no full application suite was run.

`git diff --check` passes, and the six pre-existing storage edit digests still
match. Local-only evidence is saved under
`working-memory/test-results/documentation-calibre-compat-20261019/`: the example
checker/results/log, focused test log and telemetry. Changes remain uncommitted
on `docs-ownership-contracts` for review.

No runtime code changed in this slice. The next numbered guide is `06 - Storage.md`,
which mixes a deprecated model with newer storage/workflow material. Reconcile it
with the current storage API and detailed subsystem guides; keep the outstanding
database field inventory and larger path-reorganisation proposal separate.

## Publication handoff

The user approved getting this documentation review in. Publish the accumulated
review on `docs-ownership-contracts`, based on `configuration-ownership`, including
the one-line WEMI smoke-test documentation-path correction. Keep the six unrelated
storage edits and the existing data-submodule state out of the documentation commit.

The latest verification is the 45-check compatibility/documentation run above;
the earlier schema/ownership checks and executable examples provide the remaining
evidence. Only publication notes changed after that run. There is no runtime policy
change. Continue with guide 06 after this review is integrated.
