# WEMI selection checkpoint, 2026-10-20

A07 is implemented on `catalog-wemi-selection`, based on A06 PR #134 at
`13d6e5aa`. Final validation passed; publication is next. The full A01–A16 programme
is open; continue to A08 semantic write coverage next. No PR has been merged.

Preserve the six unrelated staged storage files and dirty data submodule.
`/tmp/liuxin-docs-pr-preserved-storage.json` records their hashes and exact index
entries; synchronization verifies both. Commit only the A07 explicit manifest.

## Contract and changes

Catalog's `retrieval/selection.py` owns preferred WEM paths. Bundles enumerate
candidates through the requested root and rank complete paths before partial
ones, then Work–Expression primary/ascending priority, Expression–Manifestation
primary/ascending priority, Expression/Manifestation/Work IDs. Null link ranks
mean zero. Choose the lowest Item ID only after choosing the bibliographic path;
for_item keeps its requested Item. No complete ray means the fullest available
partial bundle. The existing SQL primary-ray rank is unchanged and needs no
database migration.

Repositories still return all related rows in their existing list order. Graph
membership retains translations, shared Expressions, introductions and omnibus
Works. Bundles do not guarantee a snapshot and may inspect multiple branches.
The API docstrings, shared result description and worked examples now agree.
The pure selection module enters both type checkers; changed selection/bundle/test
files enter the existing format/lint gates.

Hydrated metadata's primary-relation accessors are explicitly relation-local:
primary flag, ascending optional priority/index, input order; absent values last.
They do not answer the SQL complete-ray question. A real-backend contrasting-output
test retains both hydrated Works and distinguishes that accessor from the bundle.
This is documented, not presented as full hydration/SQL default parity.

## Validation

The initial failing regression reproduced the old mismatch: SQL chose
`1/11/101/1001`, Catalog chose `1/12/103/1003`.

Initial new parity suite: **20 passed in 76.97s**, SQLite and APSW.
Broader selection: **52 passed, two failed in 154.46s**. Both failures came from
the added worked-example test calling nonexistent `catalog.relationships`;
the test now uses public `catalog.mutations.writer.unlink_wemi`.
The final follow-up passed **22 tests in 90.17s**, including the additional
hydration distinction and exact worked bilingual/introduction/omnibus graph.
Documentation links, API contracts, CI workflow and public documentation boundaries
passed **55 tests in 9.41s**. The complete quality runner passed: 192 formatted
files, 244 protected import modules, 1,801 layer modules, 25 import budgets, lint,
complexity, both type checkers and both sets of 38 negative contracts. Mypy checks
199 files. Adding bundles to lint required updating its existing collections imports.
Selections overlap; do not sum them into a unique total.

Logs: `/tmp/liuxin-a07-{red,policy,consumers,verified,docs}.log` and
`/tmp/liuxin-a07-quality-verified.log`, copied to ignored
`working-memory/test-results/wemi-selection-20261020`.
Telemetry: `/tmp/liuxin-a07-telemetry`. All tests are serial with immediate tmp
cleanup; use remote CI for the full suite. C: retained roughly 12 GiB free.

## Publication and next work

A06 PR #134 is published, including the CI correction centralizing deployment-role
lookup in Core. That correction passed 41 boundary/HTTP checks and the complete
quality runner; inspect its separate remote CI.
A05 PR #133 is at `47c5c0d3`; its corrected full CI is still pending.
A04 #132 and A15 #131 have verified full CI.

The isolated A07 checkout `/tmp/liuxin-a07-review` is detached at `13d6e5aa` with
this branch's overlay and a borrowed .venv symlink.
`/tmp/liuxin-sync-a07.py` copies only A07 scopes and verifies protected files.
Publish a focused PR based on `core-transport-permissions`; do not merge PRs.

A08 mapping has only begun: the shared write hook is
`catalog/repositories/base.py::_prepare_write`; language entities are in
`repositories/entities.py`, not a separate languages module. Languages are already immutable reference data. Ratings are a clearer uncovered
family: RatingRepository explicitly has no numeric range/scale validation, and
ratings.sql stores nullable rating, integer rating_out_of and a separate legacy
display field. Inspect supported container/Row/Core/schema routes and preserve
partial-update and historical-read behavior before implementing rules. No A08
code has changed.
