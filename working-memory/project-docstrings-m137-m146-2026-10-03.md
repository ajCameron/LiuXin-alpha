# Ten-unit metadata-source documentation batch: M137–M146 complete

The request for ten more docstring tasks completed **M137–M146**: **290
declarations across sixteen files**. All sixteen files are complete. **M147 is
next**; its 27 declarations across four PDB source files remain unchanged.

| Unit | Declarations |
|---|---:|
| M137 | 21 |
| M138 | 23 |
| M139 | 28 |
| M140 | 39 |
| M141 | 37 |
| M142 | 20 |
| M143 | 36 |
| M144 | 27 |
| M145 | 31 |
| M146 | 28 |

The reviewed modules cover primary and beta ODT readers, generic and Calibre OPF,
PDF Info/XMP parsing and updates, Plucker/PML/RAR/Rocket eBook, reader-registry
mutation, RTF/SNB/Topaz, TXT/TXTZ inference, metadata workers and ZIP dispatch.
The docs record malformed-input policy, optional dependencies and tools, binary
bounds and offset repair, cover selection, registry revisions, merge precedence,
job execution and path/stream ownership.

Verification completed against the final source hashes:

- All **290 declarations** and all sixteen complete files pass audit and
  normalization checks. Executable AST, signatures, decorators, annotations,
  comments and non-doc literals are unchanged.
- **201 focused regressions passed**. Three environment-dependent checks skipped:
  two optional `pypdf` round trips and one real RAR extraction without an
  `unrar` executable.
- All **290 command examples** reference existing focused pytest consumers.
  These parser, registry, worker and optional-tool modules are exercised through
  owning suites instead of being imported automatically through doctest.
- Full quality checks passed: 156 formatted files, annotation coverage over 454
  files, import boundaries over 215 protected modules, zero basedpyright errors,
  mypy over 182 files, and 37 invalid contract examples rejected by both checkers.
- Discovery remains **2,671 files**, with no new or missing paths. The same 39
  unrelated review-hash differences remain. The existing
  `storage/api/store_api/file_api.py` record changed again through concurrent
  storage-review comments outside this batch; this batch did not edit that file
  or its review record.

M progress is **146/274 units**, **3,714/7,198 declarations**, and **165/344
complete files**. D remains archived complete at 232 units, 5,685 declarations
and 374 files. Project coverage is **1,225/2,671 complete-file records**, holding
**18,627 declarations**, plus **37 partial catalog declarations**. Remaining work
is **1,446 files and 23,497 declarations**. Across tracks, 410 documentation
units are verified and 962 remain.

No dependencies were installed and no commit or publication was performed. The
dirty `LiuXin_alpha_data` nested checkout and concurrent storage-review comments
remain separate from this batch. Historical evidence helpers must not be replayed
against this completed batch.

- [Observations and exact commands](test-results/docstrings-m137-m146-2026-10-03/observations.json)
- [Static proof](test-results/docstrings-m137-m146-2026-10-03/static.json)
- [Example policy and records](test-results/docstrings-m137-m146-2026-10-03/examples-all.json)
- [Runtime-doc review](test-results/docstrings-m137-m146-2026-10-03/runtime-doc-review.json)
- [M147 boundary](test-results/docstrings-m137-m146-2026-10-03/M147-boundary.json)
- [Metadata regressions](test-results/docstrings-m137-m146-2026-10-03/metadata.log)
- [Quality checks](test-results/docstrings-m137-m146-2026-10-03/quality.log)
- [Discovery](test-results/docstrings-m137-m146-2026-10-03/discovery.json)
- [Inventory reconciliation](test-results/docstrings-m137-m146-2026-10-03/reconciliation.json)
- [Documentation diff](test-results/docstrings-m137-m146-2026-10-03/docstrings.diff)
- [Previous checkpoint](project-docstrings-m127-m136-2026-09-29.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
