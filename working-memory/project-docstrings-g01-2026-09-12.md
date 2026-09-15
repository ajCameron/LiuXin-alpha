# G01 — docstring-aware ownership measurements, 2026-09-12

Implemented and reviewed the two files assigned to G01:
[shared helper](../tests/support/docstring_ownership.py) and
[focused regression tests](../tests/scripts/test_docstring_ownership.py).
The [inventory](../dev-docs/project-docstrings-work-units.json) records their full
declaration selections and verification. G02 guard integrations remain queued.

## Contract for G02

Import `SourceMetrics` and `body_without_docstring` from
`tests.support.docstring_ownership`. Construct one `SourceMetrics(source)` per file
and reuse its `tree` for inspection. `line_count()` counts retained file lines;
`line_count(node)` counts the inclusive AST span after excluding documentation-only
lines. Use `line_count(node) - 1 < 10` to retain the existing exclusive facade span
comparison. Keep all existing numeric limits and comparison operators.

`body_without_docstring(node)` returns a new list with only the recognized leading
literal removed. Retained statement objects and nested bodies are unchanged, so
G02 can still require exactly one Return whose value is a Call. Inspect this list
for delegation; do not rewrite the input AST. Nodes measured by SourceMetrics must
come from its unchanged tree.

The implementation parses without importing or executing source. AST byte offsets
are converted to token character offsets. Only literal/parenthesis tokens inside
recognized docstring expressions qualify. Comments, non-doc blank lines, ordinary
strings, and mixed code/doc lines remain counted. A trailing semicolon also keeps
its line counted. Blank lines inside multiline literals are documentation content.
File counts retain splitlines semantics; function counts retain AST span semantics,
including nested code and the original exclusion of decorators outside that span.

## Verification and inventory

Focused pytest plus all examples in the two files: **65 passed**, no skips. Tests
cover module/class/sync/async/nested owners, empty and concatenated literals,
multiline docs, comments, blank lines, Unicode columns, newline encodings, body/AST
preservation, non-doc strings, invalid input, and exact/oversized limit boundaries.
The final run follows a return-description correction; the earlier run also passed.
Lint, formatting, strict documentation audit, and normalizer checks are recorded in
[observations](test-results/project-docstrings-G01-2026-09-12-observations.json).

All three G02 guard files match their preflight hashes; no production code or guard
thresholds changed. The seven previously recorded guard failures are unresolved
until G02 integrates and verifies the helper. No broad production suite was rerun.

The two new files add 2 modules, 1 class, and 16 functions (19 declarations), all
source-reviewed. Current coverage is **664/2,732 files**, with 870 reviewed classes
and 7,162 reviewed functions (8,696 reviewed declarations). Remaining work is
unchanged: 2,068 files, 33,572 declarations, 1,372 documentation modules.
The P00 baseline remains historical; `current` records the updated inventory.
New file records identify G01 as their introduction and review module, with no
duplicate queued documentation assignments. A snapshot preserves the original
662-file manifest before adding these two files.

## Stop and resume

**G01 is complete. Stop here.** Next is G02 on explicit request: integrate the helper
into the three guard owners, update their measurement descriptions, preserve all
delegation/dependency checks, and run the G02 regression selection. Do not begin
Catalog or other documentation modules automatically. No commit or push was made.
