# Ten-unit driver documentation batch: D129–D138 complete

The explicit request for **ten more tasks** authorized **D129–D138**, overriding the
usual one-unit limit for this bounded batch. All **208 declarations across 19
complete files** are reviewed and promoted. Work stopped after D138. **D139 is next**:
eight declarations across the update and view API mixins. Earlier broader
authorizations remain paused; a new request is required to continue.

| Unit | Declarations |
|---|---:|
| D129 | 2 |
| D130 | 21 |
| D131 | 1 |
| D132 | 40 |
| D133 | 4 |
| D134 | 20 |
| D135 | 26 |
| D136 | 22 |
| D137 | 38 |
| D138 | 34 |

The APSW initializer and backend now describe their version quirks, ordinary
stdlib connections versus APSW rebuild connections, native connection settings,
legacy scalar-cursor limitation, dynamic filters, and the unimplemented epoch
hook. Dump/restore docs cover the bundled shell, trusted prefix commands, scratch
path parsing, file replacement and reopen behavior. APSW **3.53.4.0** was installed
in the existing repository `.venv` to run real backend checks; dependency manifests
were not changed.

Driver API docs distinguish abstract contracts from concrete behavior. Shared SQL
implementation notes were reused only with exact parameter-order and reviewed
source-hash checks, with backend-specific caveats labelled explicitly. These cover
row conversion, connection ownership, transaction effects, custom columns,
relationships, identity/policy metadata, naming, searches, trees and triggers.
The duplicated `direct_executescript` declaration remains intact and is documented.
The two concrete case-sensitivity aliases have runnable forwarding examples.

Verification:

- All 208 declarations pass complete-file audit and normalizer checks. Executable
  AST, signatures, comments and non-docstring literals are unchanged; no added Ruff
  findings or whitespace errors. Original and maintenance baselines are preserved.
- D132's first 40 declarations were checked separately, with the four D133
  docstrings unchanged. Six API parity checks and selected examples passed before
  D133 proceeded. The complete 44-declaration file passed before promotion.
- **185 regressions passed**, **4 skipped** across
  distinct final test selections: backend lifecycle/dump/surface
  (21 passed), API signature parity
  (6 passed), and broader row/query/schema/metadata/policy/
  relationship/tree contracts (158 passed).
  SQLite and APSW were explicitly selected. Exact skip reasons are in the logs.
- **109 runnable example statements passed**; **157 integration statements were
  explicitly skipped**, principally abstract-backend usage examples. Skipped calls
  are not counted as executed. Native APSW queries, stdlib driver paths, scalar/row
  fetching, timestamps, dump transactions, abstract-class introspection and concrete
  alias forwarding were exercised in isolated runtime directories.
- Full quality runner passed: 156 formatted files, 454 annotation-checked files,
  215 protected modules, zero basedpyright errors, mypy 182 files, and both contract
  checkers rejecting all 37 invalid examples. Evidence matches final source hashes.
- Full source discovery still finds **2,671 files**, with no added or missing paths.
  **21 out-of-scope storage files** differ from their saved reviewed hashes; these
  existing changes are listed in discovery.json and were not rewritten or counted
  as newly reviewed work. Manifest coverage below retains those historical records.

Coverage: **870/2,671 complete-file review records**, **12,556 declarations** in
those files, plus **37 partial-file declarations**. Remaining queued scope:
**1,801 files and 29,568 declarations**. D progress: **138/232 units**,
**3,328/5,685 declarations**, **184/374 complete files**. Across all tracks:
**170 documentation units verified**, **1,202 remaining**.
No commit or publication requested. Token usage unavailable.

- [Exact observations and commands](test-results/docstrings-d129-d138-2026-09-24/observations.json)
- [Static proof](test-results/docstrings-d129-d138-2026-09-24/static.json)
- [Examples](test-results/docstrings-d129-d138-2026-09-24/examples-all.json)
- [Backend regression log](test-results/docstrings-d129-d138-2026-09-24/backend.log)
- [API parity log](test-results/docstrings-d129-d138-2026-09-24/api.log)
- [Broader contract log](test-results/docstrings-d129-d138-2026-09-24/contracts.log)
- [Quality log](test-results/docstrings-d129-d138-2026-09-24/quality.log)
- [Scope and reviewed-hash discovery](test-results/docstrings-d129-d138-2026-09-24/discovery.json)
- [Inventory reconciliation](test-results/docstrings-d129-d138-2026-09-24/reconciliation.json)
- [Documentation diff](test-results/docstrings-d129-d138-2026-09-24/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
