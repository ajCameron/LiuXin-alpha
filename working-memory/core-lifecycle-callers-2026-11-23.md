# Core registration lifecycle callers — 2026-11-23

User requested a separate follow-up commit after the Core integration review
(`eb096f3c`). Work remains on `core-review`; the original checkout, its data
submodule edit and six stashes are untouched.

Moved all four local-disk/rclone/wget/native-HTML `*_with_database_path` helpers
to `src/LiuXin_alpha/core/store_registration.py`. Each retains its options,
defaults, report type and LibraryServices context. The standalone disk CLI moved
to `src/LiuXin_alpha/surfaces/cli/store_registration.py`. Repository callers and
the example use the new imports; no lower-layer compatibility reexports remain.
Handle-based domain functions and existing Core job routes are unchanged.

The layer ratchet now forbids ingest-to-Core/surfaces dependencies and removes
the storage-to-Core baseline exception (10 to 9 unique direction allowances).
Cycle measurements and unresolved dynamic-import allowances are unchanged.
New modules enter the formatting, lint and typing scopes.

Canonical migration instructions and CLI invocation:
`dev-docs/architectural_reviews/lifecycle-opening-path-review-2026-10-09.md`.
Core, surface, ingest and storage docs now reflect the ownership split.

Validation complete:
- 127 tests passed: helper success/startup/body/cleanup cases, callback lifetime,
  CLI flags/reports/errors, dependency rules and surface boundaries.
- 76 tests passed: real registration callers (SQLite and APSW), LibraryServices
  ownership/teardown and Core workflow facade. One existing iso639 deprecation warning.
- AST comparison confirms all four helpers preserve option names/defaults and
  the complete delegated LibraryServices operation.
- 26 documentation/link/workflow-ownership checks passed (229 total tests).
- Complete quality gate passed: formatting, annotations, import/cycle/layer
  checks, 31 cold-import budgets, Ruff, basedpyright and mypy (573 files).
  Both negative type suites passed: 127/123 rejected examples.
- The relocated `python -m` CLI help exposes all existing flags. `git diff --check`
  passed. No network crawler integration or live PostgreSQL was exercised.

Next independent work: CORE-04/A12 configuration and operational-path ownership,
starting with `utils.ptempfiles` and startup imports. Resolved-settings propagation,
remaining Database opt-ins/back-references and durable maintenance are not claimed
complete by this caller relocation. No push or PR requested.
