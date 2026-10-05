# Utility documentation batch: U001–U034 complete

The first utility batch completed **U001–U034**: **807 declarations across 36
files**. Every selected file is complete. The U track is now at **34/208 units,
807/5,046 declarations and 36/313 files**; U035 is next under the continuing
remaining-programme authorization.

The reviewed contracts cover shared utility adaptors, date and identifier
normalization, MIME/path/tempfile helpers, sync/async bridges, terminal and ZIP
helpers, and the retained Calibre database, metadata and template-formatter
compatibility surfaces.

Verification completed against the final source hashes:

- All **807 declarations** and all 36 files pass audit and normalization checks.
  Executable AST, signatures, decorators, annotations, comments and non-doc
  literals are unchanged.
- **472 scoped regressions passed**, with two skips and eleven expected failures.
  The initial broader run also exercised two unrelated image-backend test files
  and recorded eight failures in unchanged production modules outside U001–U034;
  the adjudication binds those source files byte-for-byte to `HEAD`.
- All **807 command examples** reference existing consuming pytest modules.
  Runtime documentation consumers were reviewed without importing the complete
  compatibility surface through doctest.
- Full quality checks passed: formatting over 156 files, annotation coverage over
  454 files, import boundaries over 215 protected modules, zero basedpyright
  errors, mypy over 182 files, and 37 invalid contract examples rejected by both
  checkers.
- Discovery remains **2,671 files**, with no new or missing paths. The same 39
  unrelated review-hash differences remain, with no drift delta from T066.
- Preflight reconciled five committed functions added to `utils/adaptors.py`
  since the original inventory baseline. The original baseline remains preserved;
  the clean `HEAD` source is recorded as its maintenance baseline.
- U035's source boundary remains unchanged.

Project coverage is now **1,584/2,671 complete-file records**, holding **24,478
declarations**, with no partial reviewed files. Remaining work is **714 units,
1,087 files and 17,688 declarations** across U, F, A, S and L. The active
campaign is at **99/813 units, 1,791/19,479 declarations and 150/1,237 files**.

No dependencies were installed and no commit or publication was performed. The
dirty `LiuXin_alpha_data` nested checkout remains separate from this campaign.

- [Observations and exact commands](test-results/docstrings-u001-u034-2026-10-05/observations.json)
- [Static proof](test-results/docstrings-u001-u034-2026-10-05/static.json)
- [Example policy and records](test-results/docstrings-u001-u034-2026-10-05/examples-all.json)
- [Runtime-doc review](test-results/docstrings-u001-u034-2026-10-05/runtime-doc-review.json)
- [U035 boundary](test-results/docstrings-u001-u034-2026-10-05/U035-boundary.json)
- [Scoped regressions](test-results/docstrings-u001-u034-2026-10-05/regressions-scoped.log)
- [Broad regression adjudication](test-results/docstrings-u001-u034-2026-10-05/regression-adjudication.json)
- [Quality checks](test-results/docstrings-u001-u034-2026-10-05/quality.log)
- [Discovery](test-results/docstrings-u001-u034-2026-10-05/discovery.json)
- [Inventory reconciliation](test-results/docstrings-u001-u034-2026-10-05/reconciliation.json)
- [Documentation diff](test-results/docstrings-u001-u034-2026-10-05/docstrings.diff)
- [Previous checkpoint](project-docstrings-t002-t066-2026-10-05.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
