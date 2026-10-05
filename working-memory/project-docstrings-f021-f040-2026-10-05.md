# File-format documentation batch: F021–F040 complete

The second file-format batch completed **F021–F040**: **543 declarations across
55 files**. Every selected file is complete. The F track is now at **40/347
units, 1,066/9,099 declarations and 93/574 files**.

The reviewed contracts cover conversion input/output plugins from EPUB through
TXT, the DjVu reader and BZZ decoder, and DOCX package, style, field, font,
footnote, image, numbering and table internals.

Verification completed against the final source hashes:

- All **543 declarations** and all 55 files pass audit and normalization checks;
  executable AST, signatures, annotations, comments and non-doc literals remain
  unchanged.
- The focused conversion-plugin, DjVu and DOCX regression result matches its
  pre-edit baseline exactly at **46 passed**.
- All **543 command examples** reference existing consuming pytest modules.
- Full repository quality checks passed across typing, lint, formatting,
  annotations, protected import boundaries and complexity checks.
- Discovery remains **2,671 files**, with no new or missing paths and no delta in
  the same 39 unrelated review-hash differences.
- F041's source boundary remains unchanged.

Project coverage is now **1,954/2,671 complete-file records**, holding **29,783
declarations**, with no partial reviewed files. Remaining work is **500 units,
717 files and 12,383 declarations** across F, A, S and L. The active campaign is
at **313/813 units, 7,096/19,479 declarations and 520/1,237 files**. F041 is next
without another authorization stop.

No dependencies were installed and no commit or publication was performed. The
dirty `LiuXin_alpha_data` nested checkout remains separate from this campaign.

- [Observations and exact commands](test-results/docstrings-f021-f040-2026-10-05/observations.json)
- [Static proof](test-results/docstrings-f021-f040-2026-10-05/static.json)
- [Example policy and records](test-results/docstrings-f021-f040-2026-10-05/examples-all.json)
- [Runtime-doc review](test-results/docstrings-f021-f040-2026-10-05/runtime-doc-review.json)
- [F041 boundary](test-results/docstrings-f021-f040-2026-10-05/F041-boundary.json)
- [Baseline/post-edit regression comparison](test-results/docstrings-f021-f040-2026-10-05/regression-comparison.json)
- [Regressions](test-results/docstrings-f021-f040-2026-10-05/regressions.log)
- [Quality checks](test-results/docstrings-f021-f040-2026-10-05/quality.log)
- [Discovery](test-results/docstrings-f021-f040-2026-10-05/discovery.json)
- [Inventory reconciliation](test-results/docstrings-f021-f040-2026-10-05/reconciliation.json)
- [Documentation diff](test-results/docstrings-f021-f040-2026-10-05/docstrings.diff)
- [Previous checkpoint](project-docstrings-f001-f020-2026-10-05.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
