# File-format documentation batch: F101–F120 complete

The sixth file-format batch completed **F101–F120**: **549 declarations across
40 files**. Every selected file is complete. The F track is now at **120/347
units, 3,192/9,099 declarations and 200/574 files**.

The reviewed contracts cover Markdown tree processing and extensions plus MOBI
compression, markup, diagnostics and MOBI6/KF8 reader internals.

Verification completed against the final source hashes:

- All **549 declarations** and all 40 files pass audit and normalization checks;
  executable AST, signatures, annotations, comments and non-doc literals remain
  unchanged.
- The focused Markdown/MOBI result matches its pre-edit baseline at **110
  passed**, with the same 370 deprecation warnings.
- All **549 command examples** reference existing consuming pytest modules.
- Full repository quality checks passed across typing, lint, formatting,
  annotations, protected import boundaries and complexity checks.
- Discovery remains **2,671 files**, with no new or missing paths and no delta in
  the same 39 unrelated review-hash differences.
- F121's source boundary remains unchanged.

Project coverage is now **2,061/2,671 complete-file records**, holding **31,909
declarations**, with no partial reviewed files. Remaining work is **420 units,
610 files and 10,257 declarations** across F, A, S and L. The active campaign is
at **393/813 units, 9,222/19,479 declarations and 627/1,237 files**. F121 is next
without another authorization stop.

No dependencies were installed and no commit or publication was performed. The
dirty `LiuXin_alpha_data` nested checkout remains separate from this campaign.

- [Observations](test-results/docstrings-f101-f120-2026-10-05/observations.json)
- [Static proof](test-results/docstrings-f101-f120-2026-10-05/static.json)
- [Examples](test-results/docstrings-f101-f120-2026-10-05/examples-all.json)
- [Runtime-doc review](test-results/docstrings-f101-f120-2026-10-05/runtime-doc-review.json)
- [F121 boundary](test-results/docstrings-f101-f120-2026-10-05/F121-boundary.json)
- [Regression comparison](test-results/docstrings-f101-f120-2026-10-05/regression-comparison.json)
- [Quality checks](test-results/docstrings-f101-f120-2026-10-05/quality.log)
- [Discovery](test-results/docstrings-f101-f120-2026-10-05/discovery.json)
- [Reconciliation](test-results/docstrings-f101-f120-2026-10-05/reconciliation.json)
- [Documentation diff](test-results/docstrings-f101-f120-2026-10-05/docstrings.diff)
- [Previous checkpoint](project-docstrings-f081-f100-2026-10-05.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
