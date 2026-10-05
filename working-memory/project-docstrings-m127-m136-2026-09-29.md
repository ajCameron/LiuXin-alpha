# Ten-unit metadata-source documentation batch: M127–M136 complete

The request for ten more units completed **M127–M136**: **280 declarations
across twelve files**. All twelve files are complete. **M137 is next**; its 21
declarations in `odt.py` remain unchanged.

| Unit | Declarations |
|---|---:|
| M127 | 22 |
| M128 | 40 |
| M129 | 3 |
| M130 | 15 |
| M131 | 40 |
| M132 | 23 |
| M133 | 30 |
| M134 | 36 |
| M135 | 38 |
| M136 | 33 |

The reviewed modules cover comic and DOCX metadata, EPUB/OCF reading and writing,
EXTZ archives, FB2 XML, filename inference, tolerant HTML extraction, IMP, LIT,
LRF, LRX and MOBI. The docs record path and stream ownership, strict versus
fallback parsing, optional dependencies, archive mutation, cover selection,
namespace handling and binary record bounds.

Verification completed against the final source hashes:

- All **280 declarations** and all twelve complete files pass audit and
  normalization checks. Executable AST, signatures, decorators, annotations,
  comments and non-doc literals are unchanged.
- **354 focused regressions passed**. One real-corpus LRX test skipped because
  the optional fixture corpus is absent. The suites cover file-source adapters,
  malformed inputs, registry integration and the MOBI binary framework.
- All **280 command examples** reference existing focused pytest consumers.
  These binary parsers and optional-dependency adapters are exercised through
  their owning suites instead of importing every module through doctest.
- Full quality checks passed: 156 formatted files, annotation coverage over 454
  files, import boundaries over 215 protected modules, zero basedpyright errors,
  mypy over 182 files, and 37 invalid contract examples rejected by both checkers.
- Discovery remains **2,671 files**, with no new or missing paths. The same 39
  unrelated review-hash differences remain unchanged. This batch did not edit
  those sources or their review records.

M progress is **136/274 units**, **3,424/7,198 declarations**, and **149/344
complete files**. D remains archived complete at 232 units, 5,685 declarations
and 374 files. Project coverage is **1,209/2,671 complete-file records**, holding
**18,337 declarations**, plus **37 partial catalog declarations**. Remaining work
is **1,462 files and 23,787 declarations**. Across tracks, 400 documentation units
are verified and 972 remain.

No dependencies were installed or publication performed. The repository
checkpoint includes the completed M067–M136 documentation work, the 2026-09-28
core-owned lifecycle decision in the architecture documents, and the adjacent
storage-API review comments. The dirty `LiuXin_alpha_data` nested checkout is
not part of the parent-repository checkpoint. Historical evidence helpers must
not be replayed against this completed batch.

- [Observations and exact commands](test-results/docstrings-m127-m136-2026-09-29/observations.json)
- [Static proof](test-results/docstrings-m127-m136-2026-09-29/static.json)
- [Example policy and records](test-results/docstrings-m127-m136-2026-09-29/examples-all.json)
- [Runtime-doc review](test-results/docstrings-m127-m136-2026-09-29/runtime-doc-review.json)
- [M137 boundary](test-results/docstrings-m127-m136-2026-09-29/M137-boundary.json)
- [Metadata regressions](test-results/docstrings-m127-m136-2026-09-29/metadata.log)
- [Quality checks](test-results/docstrings-m127-m136-2026-09-29/quality.log)
- [Discovery](test-results/docstrings-m127-m136-2026-09-29/discovery.json)
- [Inventory reconciliation](test-results/docstrings-m127-m136-2026-09-29/reconciliation.json)
- [Documentation diff](test-results/docstrings-m127-m136-2026-09-29/docstrings.diff)
- [Previous checkpoint](project-docstrings-m117-m126-2026-09-29.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
