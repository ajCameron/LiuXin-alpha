# Utility documentation batch: U035–U067 complete

The second utility batch completed **U035–U067**: **772 declarations across 38
files**. Every selected file is complete. Together with U001–U034, the U track is
now at **67/208 units, 1,579/5,046 declarations and 74/313 files**.

The reviewed contracts cover configuration persistence and option parsing, the
retained APSW shell, ZIP/7-Zip/RAR extraction, image backends and format
recognition, isolated workers, managed jobs, and language/name/ICU helpers.

Verification completed against the final source hashes:

- All **772 declarations** and all 38 files pass audit and normalization checks.
  Executable AST, signatures, decorators, annotations, comments and non-doc
  literals are unchanged.
- The focused run produced **55 passes and the same five failures before and
  after** the documentation edits. All five are the existing PyQt-dependent
  `image_tools/img.py` failures; exact node IDs and source hashes are retained.
  Excluding that captured baseline-failing test file, all 55 selected regressions
  pass. Three existing multiprocessing deprecation warnings were emitted.
- All **772 command examples** reference existing consuming pytest modules.
  Runtime documentation consumers were reviewed without importing the legacy
  compatibility surface through doctest.
- Full quality checks passed: formatting over 156 files, annotation coverage over
  454 files, import boundaries over 215 protected modules, zero basedpyright
  errors, mypy over 182 files, and 37 invalid contract examples rejected by both
  checkers.
- Discovery remains **2,671 files**, with no new or missing paths. The same 39
  unrelated review-hash differences remain, with no drift delta from U034.
- U068's source boundary remains unchanged.

Project coverage is now **1,622/2,671 complete-file records**, holding **25,250
declarations**, with no partial reviewed files. Remaining work is **681 units,
1,049 files and 16,916 declarations** across U, F, A, S and L. The active
campaign is at **132/813 units, 2,563/19,479 declarations and 188/1,237 files**.
U068 is next without another authorization stop.

No dependencies were installed and no commit or publication was performed. The
dirty `LiuXin_alpha_data` nested checkout remains separate from this campaign.

- [Observations and exact commands](test-results/docstrings-u035-u067-2026-10-05/observations.json)
- [Static proof](test-results/docstrings-u035-u067-2026-10-05/static.json)
- [Example policy and records](test-results/docstrings-u035-u067-2026-10-05/examples-all.json)
- [Runtime-doc review](test-results/docstrings-u035-u067-2026-10-05/runtime-doc-review.json)
- [U068 boundary](test-results/docstrings-u035-u067-2026-10-05/U068-boundary.json)
- [Baseline/post-edit regression comparison](test-results/docstrings-u035-u067-2026-10-05/regression-comparison.json)
- [Scoped regressions](test-results/docstrings-u035-u067-2026-10-05/regressions-scoped.log)
- [Quality checks](test-results/docstrings-u035-u067-2026-10-05/quality.log)
- [Discovery](test-results/docstrings-u035-u067-2026-10-05/discovery.json)
- [Inventory reconciliation](test-results/docstrings-u035-u067-2026-10-05/reconciliation.json)
- [Documentation diff](test-results/docstrings-u035-u067-2026-10-05/docstrings.diff)
- [Previous checkpoint](project-docstrings-u001-u034-2026-10-05.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
