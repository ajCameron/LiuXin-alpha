# Core application API and regression docstrings — 2026-09-09

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following the [database/storage/browse batch](project-docstrings-core-apis-2026-09-09.md).
Documentation stays local on `codex/project-docstrings`, based on `edf6bf05`.
No commit, push, PR change, submodule publication, agent, or owner-guard change
occurred. The latest PR request was answered by verifying existing PR #115;
that status-only turn did not advance this documentation goal. This continuation
made source/test documentation changes and verified them.

## Reviewed scope

- `src/LiuXin_alpha/core/application_api.py`: module, class, and all 71 functions.
  Resumed the earlier partial edit, then read every remaining implementation and
  registration declaration. All query/command metadata and executable statements
  are unchanged; Ruff formatting accompanies the docstrings.
- `tests/core/test_core_application_api.py`: module, all eight classes, and all
  50 functions, including the two nested event/error helpers. Documents fake
  limitations separately from production guarantees: ignored Store preferences,
  length-derived file IDs, synthetic replica IDs/tombstone reports, shallow row
  copies, plain integer conversions, and fixed schema/status data. All existing
  assertions and helper implementations are unchanged.

Source-derived application contracts now describe:

- Shared scalar/array/predicate validation, paging sentinels, case-sensitive
  selections, recursive text/sort behavior, advisory schema writability, and ID
  projection/fallback. Count-only reads still scan/filter but skip sorting and
  projection; complete query coverage is not a last-page indicator.
- Only an absent structured-cache method selects database querying. Cache
  capability failures remain visible rather than triggering a database retry.
  Related-row type filtering does not also filter optional raw link rows.
- Repository-specific match call shapes, parent scoping, WEMI writer ownership,
  post-write readback/reconciliation failures, former-entity deletion receipts,
  and flags reporting returned calls rather than independent readback verification.
- Field-write facade selection and its routing flags, raw administrative
  operations, UUID/exact-name Store lookup, error-suppressing location stat,
  separate location/read observations, strict base64 input, no adapter byte cap,
  and possible persistence before later file-resolution/reporting failure.

The cumulative reviewed ledger is **119 modules, 184 classes, 1,111 functions**
(1,414 declarations), not a restriction of the entire-project objective.

## Verification

Selections overlap; do not add them into a distinct-test total.

- Application doctests plus migration contracts initially: **42 passed, 55
  explicitly skipped integration illustrations**. Standalone final application
  module doctests: **20 passed, 52 skipped**. Pure test-double doctests separately
  execute **69 examples with zero failures**; fixture/daemon examples are skipped.
  Nested-helper examples are source-reviewed, not automatically discovered by
  module doctest traversal.
- Full application regression module plus adjacent real database browse,
  identity/tree, and managed-graph direct/RPC selection: **19 passed, 6 deselected**
  with permission for local HTTP servers. The first sandboxed run had **14 passed
  and five failures**, all verified socket-creation PermissionErrors; it completed
  before the permission-approved rerun. No code change was needed for those failures.
- Final combined application/test/semantic-contract doctests, regressions,
  migration, public-documentation, and developer-link selection: **161 passed,
  71 explicitly skipped integration illustrations**. Session `41485` completed;
  all test/audit/quality handles from this batch have returned terminal results.
- All **119 reviewed files** pass the strict structural audit and normalizer
  `--check`. All **103 edited production Python modules** and **ten edited existing
  test modules** retain the baseline executable AST, with only the previously
  verified local-proxy runtime docstring-template exception. The data generator
  separately matches its own submodule HEAD after stripping docstrings.
- Both batch files are formatter-clean and introduce no new Ruff findings.
  Outside-scope baseline findings remain: application API 117, application tests 10.
- Full `bash scripts/run_type_checks.sh` passed: 159 formatter-clean files,
  456 annotated modules, 221 dependency-protected modules, clean selected lint
  and complexity, 188 strict-mypy files, no production typing errors, and all
  37 negative examples rejected by each checker. No gate limit or scope changed.
- Root and data-submodule diffs are whitespace-clean. Final handoff
  developer-link/public-boundary checks: **23 passed**. A separate Markdown scan
  resolves all **117 local link destinations** across the four current
  working-memory notes, including the new handoff. All handles have completed.

These checks do not claim whole-project pytest, live PostgreSQL, installed-wheel
verification, or remote CI success.

## Introspection evidence: intentional fixture-documentation changes

The production application edits alone retain the full previous fake-runtime
description SHA-256 (sorted JSON, core_uuid omitted):
`6c4dfd704980bd0e971291338880ad747a8ba4f7750db5fa4fcbcc07f168fa26`.

Documenting the test doubles intentionally changes their generic target descriptions.
The old literal-hash assertion therefore failed after the test docstrings were
added. A complete recursive comparison against the baseline fixture source finds
**exactly 27 changed fields**: class descriptions for `_Library`, `_Database`, and
`_Storage`, plus summary/description pairs for their 12 public methods. Every new
description equals the corresponding fixture's `inspect.getdoc`; every summary
equals its first line. All **83 command and 93 query definitions**, target names,
method call shapes, and other metadata remain equal.

The documented fixture's new full-description SHA-256 is:
`2b0dadbe883060aeefde4918889f4c4197a2067c470c6678fb1df84ec95293e3`.
Loading the old test source from `git show edf6bf05:tests/core/test_core_application_api.py`
against current production still yields the old hash. Future comparisons must use
the appropriate fixture version, not interpret newly descriptive test metadata
as a production routing change or suppress arbitrary introspection differences.

## Remaining whole-project work

The subsequent [program-services batch](project-docstrings-program-services-2026-09-09.md)
documents the payload/Store helpers and six additional service modules. Its note
supersedes the cumulative counts below and records the new physical-line guard
conflict; the fixture fingerprint established here remains current.

The refreshed `/tmp/liuxin-docstring-current-2026-09-09.json` covers 2,723 modules,
4,399 classes, and 35,009 functions without parse failures. There remain **26,930
missing/blank docstrings**, down from 27,057 in the preceding complete report,
plus overlapping structural/prose findings recorded in the main goal note.
The reviewed report is `/tmp/liuxin-docstring-reviewed-2026-09-09.json`.

Continue with remaining Core program services/endpoints, then all other packages,
tests, scripts, examples, inherited source, and the data submodule. In particular,
`program_services/payloads.py`, `stores.py`, and `program_api.py` are not documented
merely because they were read as dependencies. The pending question about owner
tests ignoring literal docstring lines/statements is still unanswered; no guard
changes were made, and other useful work remains available. The full goal remains
active and unfinished.
