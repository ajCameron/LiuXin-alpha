# Ten-unit metadata documentation batch: M117–M126 complete

The request for ten more units completed **M117–M126**: **268 new declarations
across nine files**. All nine files are complete (**308 declarations**), including
the 40 item-metadata declarations reviewed in the preceding batch and promoted
unchanged here. **M127 is next**; its 22 declarations across `comic.py` and
`docx.py` remain unchanged.

| Unit | Declarations |
|---|---:|
| M117 | 27 |
| M118 | 26 |
| M119 | 40 |
| M120 | 13 |
| M121 | 1 |
| M122 | 40 |
| M123 | 30 |
| M124 | 40 |
| M125 | 21 |
| M126 | 30 |

The reviewed contracts now document the complete item metadata API;
manifestation and work identity and metadata APIs; registry-backed metadata
reader dispatch; and archive recognition, extraction and ComicBookInfo parsing.
The docs distinguish logical relation buckets from physical tables, compatibility
aliases from canonical fields, and abstract API guarantees from concrete
normalization, ownership and persistence policy.

Verification completed against the final source hashes:

- All **268 selected declarations** and all nine complete files pass audit and
  normalization checks. Executable AST, signatures, decorators, annotations,
  comments and non-doc literals are unchanged.
- **183 focused regressions passed**, covering package boundaries, API symmetry,
  identities, relation properties, concrete bundles, projections, hydrators,
  metadata-reader dispatch and archive behavior. No live database or network
  service was used.
- **915 runnable example statements passed**, with no failures or skips. Five
  command examples point to existing owning tests.
- Initial examples exposed a missing manifestation relation, concrete-import
  setup, the `work_name`/`work_title` alias, and ZIP header construction. Runtime
  symmetry checks also enforced established doc wording. These were corrected
  before all final checks were rerun.
- Full quality checks passed: 156 formatted files, annotation coverage over 454
  files, import boundaries over 215 protected modules, zero basedpyright errors,
  mypy over 182 files, and 37 invalid contract examples rejected by both checkers.
- Discovery remains **2,671 files**, with no new or missing paths. The same 39
  unrelated review-hash differences remain; `storage/api/store_api/file_api.py`
  changed again during this batch. This work did not edit that source or its
  review record.

M progress is **126/274 units**, **3,144/7,198 declarations**, and **137/344
complete files**. D remains archived complete at 232 units, 5,685 declarations
and 374 files. Project coverage is **1,197/2,671 complete-file records**, holding
**18,057 declarations**, plus **37 partial catalog declarations**. Remaining work
is **1,474 files and 24,067 declarations**. Across tracks, 390 documentation units
are verified and 982 remain.

No dependencies were installed and no commit or publication was performed.
Historical evidence helpers must not be replayed against this completed batch.

- [Observations and exact commands](test-results/docstrings-m117-m126-2026-09-28/observations.json)
- [Static proof](test-results/docstrings-m117-m126-2026-09-28/static.json)
- [Runnable examples](test-results/docstrings-m117-m126-2026-09-28/examples-all.json)
- [Runtime-doc review](test-results/docstrings-m117-m126-2026-09-28/runtime-doc-review.json)
- [M127 boundary](test-results/docstrings-m117-m126-2026-09-28/M127-boundary.json)
- [Metadata regressions](test-results/docstrings-m117-m126-2026-09-28/metadata.log)
- [Quality checks](test-results/docstrings-m117-m126-2026-09-28/quality.log)
- [Discovery](test-results/docstrings-m117-m126-2026-09-28/discovery.json)
- [Inventory reconciliation](test-results/docstrings-m117-m126-2026-09-28/reconciliation.json)
- [Documentation diff](test-results/docstrings-m117-m126-2026-09-28/docstrings.diff)
- [Previous checkpoint](project-docstrings-m107-m116-2026-09-28.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
