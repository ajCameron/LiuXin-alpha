# File-format documentation batch: F001–F020 complete

The first file-format batch completed **F001–F020**: **523 declarations across
38 files**. Every selected file is complete. The F track is now at **20/347
units, 523/9,099 declarations and 38/574 files**.

The reviewed contracts cover top-level file-format APIs and constants, archive
preflight, cover handling, retained markup parsing, tables of contents, container
tweaking, AZW4, CHM, comic and compression readers, conversion plumbing and its
initial AZW4, CHM, comic, DjVu and DOCX input plugins.

Verification completed against the final source hashes:

- All **523 declarations** and all 38 files pass audit and normalization checks;
  executable AST, signatures, annotations, comments and non-doc literals remain
  unchanged.
- The focused file-format regression result matches its pre-edit baseline
  exactly at **304 passed**.
- All **523 command examples** reference existing consuming pytest modules.
- Full repository quality checks passed across typing, lint, formatting,
  annotations, protected import boundaries and complexity checks.
- Discovery remains **2,671 files**, with no new or missing paths and no delta in
  the same 39 unrelated review-hash differences.
- F021's source boundary remains unchanged.

Project coverage is now **1,899/2,671 complete-file records**, holding **29,240
declarations**, with no partial reviewed files. Remaining work is **520 units,
772 files and 12,926 declarations** across F, A, S and L. The active campaign is
at **293/813 units, 6,553/19,479 declarations and 465/1,237 files**. F021 is next
without another authorization stop.

No dependencies were installed and no commit or publication was performed. The
dirty `LiuXin_alpha_data` nested checkout remains separate from this campaign.

- [Observations and exact commands](test-results/docstrings-f001-f020-2026-10-05/observations.json)
- [Static proof](test-results/docstrings-f001-f020-2026-10-05/static.json)
- [Example policy and records](test-results/docstrings-f001-f020-2026-10-05/examples-all.json)
- [Runtime-doc review](test-results/docstrings-f001-f020-2026-10-05/runtime-doc-review.json)
- [F021 boundary](test-results/docstrings-f001-f020-2026-10-05/F021-boundary.json)
- [Baseline/post-edit regression comparison](test-results/docstrings-f001-f020-2026-10-05/regression-comparison.json)
- [Regressions](test-results/docstrings-f001-f020-2026-10-05/regressions.log)
- [Quality checks](test-results/docstrings-f001-f020-2026-10-05/quality.log)
- [Discovery](test-results/docstrings-f001-f020-2026-10-05/discovery.json)
- [Inventory reconciliation](test-results/docstrings-f001-f020-2026-10-05/reconciliation.json)
- [Documentation diff](test-results/docstrings-f001-f020-2026-10-05/docstrings.diff)
- [Previous checkpoint](project-docstrings-u176-u208-2026-10-05.md)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
