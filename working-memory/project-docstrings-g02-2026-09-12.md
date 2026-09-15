# G02 — adopt docstring-aware ownership guards, 2026-09-12

Integrated the verified G01 helper into the three assigned guard files:
[workflow ownership](../tests/scripts/test_workflow_ownership.py),
[terminal ownership](../tests/surfaces/test_terminal_component_ownership.py), and
[storage composition](../tests/storage/api/test_storage_manager_composition.py).
Their descriptions now explain the documentation exemption. Production source
and the G01 helper are unchanged.

## Behavior and preserved contracts

Each measured file uses SourceMetrics and reuses its parsed tree. File/function
limits exclude documentation-only lines; other comments, blank lines, strings,
and mixed code/doc lines remain counted. The facade keeps its exclusive span
comparison as `line_count(node) - 1 < 10` and still requires a single Return whose
value is a Call after filtering only the leading docstring. Manager mixin files
are read and measured once each, retaining the oversized-file diagnostic map.

All numeric ceilings and operators are preserved: 450/160 for workflow/component
files and functions, 250 for facade/terminal roots, `< 10` for facade spans, and
120/900 for the manager/mixins. File discovery, function selection, installation
exceptions, dependency/cycle checks, composition ordering, method membership,
abstractness, quality-scope checks, and codec/delegation regressions retain their
existing contracts.

## Verification

The pre-edit four-file selection reproduced **7 failed, 37 passed** (25.85s):
two workflow-owner parameters, the program facade, two terminal component trees,
the curses composition root, and the manager/mixin size check. All seven now pass.
The final selection, including doctests, finished **48 passed, 7 skipped** (14.56s):
all 44 ordinary tests pass, four runnable doctests pass, and seven explicitly
skipped examples remain skipped. No live service or broad production suite ran.

Ruff lint and formatting pass. The strict audit passes for both previously
reviewed guard files and the changed workflow module docstring; all three files
are normalizer-clean. The workflow file's three function docstrings remain
unfinished, as before, and its full documentation assignment stays queued in
S028. No file was newly promoted to reviewed.

Static comparisons against pre-edit snapshots verify unchanged numeric comparison
limits/operators, comments, globals, signatures, decorators, and functions outside
the five measurement owners. Those five bodies intentionally change measurement;
this is not a claim of whole-file executable AST equality. Root/data-submodule
whitespace checks pass. G01's unchanged helper retains its 65-test/doctest evidence.

[Observations](test-results/project-docstrings-G02-2026-09-12-observations.json)
record source hashes, terminal exits, resolved failure IDs, static checks,
inventory reconciliation, and local links. Log/done files use the matching prefix
with `baseline`, `regression`, and `static` suffixes; regression also has JUnit XML.

## Inventory and resume

Coverage remains **664/2,732 files**, 870 reviewed classes and 7,162 reviewed
functions. The remaining 2,068 files and 33,572 declarations retain their 1,372
documentation assignments. Updated file records preserve their original
`baseline_sha256` and record G02's `current_sha256` and `last_changed_by`.
Current-source validation uses the current hash when present.

**G02 is complete. Stop here.** Next is **C01**, the Agent repository/API module,
on explicit request. All guard-enabling work is verified; no Catalog documentation
module was started. No commit or push was made.
