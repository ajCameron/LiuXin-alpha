# Ten-unit metadata documentation batch: M107–M116 complete

The request for ten more units completed **M107–M116**: **263 new declarations
across seven files**. Six files are complete (**223 declarations**); the first
**40 declarations** in `item_metadata_api.py` form the remaining partial scope.
**M117 is next**. Its 27 declarations and every later declaration in the item
metadata API remain unchanged.

| Unit | Declarations |
|---|---:|
| M107 | 24 |
| M108 | 31 |
| M109 | 29 |
| M110 | 24 |
| M111 | 40 |
| M112 | 14 |
| M113 | 1 |
| M114 | 40 |
| M115 | 20 |
| M116 | 40 |

The reviewed contracts now document shared, human and organisation agent
profiles; expression and item identities; the complete expression metadata API;
and item metadata through its file relations. The docs distinguish abstract API
guarantees from concrete normalization, id guards, collection ownership and
persistence behavior. They also preserve the runtime-inspected `relation_key`,
`RELATION_KEYS` and physical-database-table wording in metadata class docs.

Verification completed against the final source hashes:

- All **263 selected declarations** pass audit and normalization checks. The six
  complete files pass whole-file checks. Executable AST, signatures, decorators,
  annotations, comments, non-doc literals and all unselected docs are unchanged.
- **103 focused regressions passed**, covering package boundaries, API symmetry,
  identity aliases, relation properties, profiles, concrete item containers and
  expression/item hydration. No live database or network service was used.
- **1,022 runnable example statements passed**, with no failures or skips. Two
  command examples point to existing tests included in the passing suite.
- The initial example pass found 17 setup mistakes. A symmetry regression then
  caught required runtime doc wording. These were corrected and all final checks
  rerun; the correction record is retained with the evidence.
- Full quality checks passed: 156 formatted files, annotation coverage over 454
  files, import boundaries over 215 protected modules, zero basedpyright errors,
  mypy over 182 files, and 37 invalid contract examples rejected by both checkers.
- Discovery remains **2,671 files**, with no new or missing paths. There are 39
  unrelated review-hash differences; `storage/api/store_api/file_api.py` is newly
  different since the previous checkpoint. This batch did not edit those sources
  or their review records.

M progress is **116/274 units**, **2,876/7,198 declarations**, and **128/344
complete files**. D remains archived complete at 232 units, 5,685 declarations
and 374 files. Project coverage is **1,188/2,671 complete-file records**, holding
**17,749 declarations**, plus **77 partial declarations**: 37 in the catalog slice
and 40 in item metadata. Remaining work is **1,483 files and 24,335 declarations**.
Across tracks, 380 documentation units are verified and 992 remain.

No dependencies were installed and no commit or publication was performed.
Historical evidence helpers must not be replayed against this completed batch.

- [Observations and exact commands](test-results/docstrings-m107-m116-2026-09-28/observations.json)
- [Static proof](test-results/docstrings-m107-m116-2026-09-28/static.json)
- [Runnable examples](test-results/docstrings-m107-m116-2026-09-28/examples-all.json)
- [Example and runtime wording corrections](test-results/docstrings-m107-m116-2026-09-28/example-corrections.json)
- [Runtime-doc review](test-results/docstrings-m107-m116-2026-09-28/runtime-doc-review.json)
- [M117 boundary](test-results/docstrings-m107-m116-2026-09-28/M117-boundary.json)
- [Metadata regressions](test-results/docstrings-m107-m116-2026-09-28/metadata.log)
- [Quality checks](test-results/docstrings-m107-m116-2026-09-28/quality.log)
- [Discovery](test-results/docstrings-m107-m116-2026-09-28/discovery.json)
- [Inventory reconciliation](test-results/docstrings-m107-m116-2026-09-28/reconciliation.json)
- [Documentation diff](test-results/docstrings-m107-m116-2026-09-28/docstrings.diff)
- [Previous checkpoint](project-docstrings-m097-m106-2026-09-27.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
