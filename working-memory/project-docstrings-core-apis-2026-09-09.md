# Core database, storage graph, and browse docstrings — 2026-09-09

Continuation of the active [whole-project docstring goal](project-docstrings-2026-09-08.md),
not a new scope or completion claim. Work remains local on `codex/project-docstrings`
at baseline `edf6bf05`; no commit, push, PR update, or submodule publication occurred.
All earlier permissions and the pending docstring-aware owner-guard question remain
unchanged. No agents or guard changes were used.

## Reviewed files and behavior

- `src/LiuXin_alpha/core/database_semantics_api.py`: module, class, and all 28 functions. Documents
  database-owned identity normalization, shortest-ID/parent-column heuristics,
  breadth-first subtree traversal, root-first lineage, differing scalar/array ID
  coercion, duplicate child IDs, literal-True deletion confirmation, sequential
  mutation attempts, and reconciliation failures after writes. Advertised metadata
  and implementation statements are unchanged.
- `src/LiuXin_alpha/core/storage_graph_api.py`: module, both classes, and all 27 functions, including
  nested placement selection. Documents the closed row-resource registry, schema
  fallback when headings fail, managed ID/timestamp checks, permissive health labels,
  explicit-policy/default counts, and minimum-based violations despite the route's
  historical target wording. Planning can suggest a copy at zero shortfall and reuse
  one Store for both families. These are characterized behaviors, not repaired defects.
- `src/LiuXin_alpha/core/browse_api.py`: module, class, and all 33 functions, including nested Asset
  listing accumulation. Documents read-source retries/error suppression, relationship
  reconstruction heuristics, category/Work sort-direction differences, raw-row text
  search, full reads before paging, legacy/managed deduplication and ordering,
  redirect precedence, optimistic readability hints, unbounded byte reads, trusted
  unconfined paths/URLs, and fallback after managed-reader failure. Unavailable
  resources still use the existing acquisition_redirect error on read.
- New `tests/core/test_semantic_api_contracts.py`: module and all 20 functions
  documented. Fakes record tree mutation attempts without deleting fixture rows.
  Tests cover the contracts above using fixed dictionaries/mocks and pytest-owned
  temporary paths; example HTTP URLs are never fetched. This test module is discovered
  by the normal full pytest suite and is independently lint/formatter-clean; no
  default gate scope was enlarged for it.

The cumulative reviewed ledger is now **117 modules, 175 classes, 990 functions**
(1,282 declarations). The exact cumulative file set remains in the main goal note.

## Verification

Selections overlap; do not add them into a distinct-test total.

- Final three API modules plus new contract tests, migration tests, public-documentation
  and link contracts: **125 passed, 53 explicitly skipped integration examples**.
  Hidden nested examples are source-reviewed but not automatically exercised by
  module doctest discovery. Standalone final contract suite/doctests: **38 passed,
  2 skipped**; the two temporary-file tests ran as normal tests, with only their
  fixture-dependent doctest illustrations skipped.
- Permission-approved real-database browse/acquisition, managed graph direct/RPC,
  identity/tree, and full Core application API selection after the browse edits:
  **19 passed**. Earlier first-two-module/new-contract selection: **34 passed**.
  This is not a whole-project pytest run or a live PostgreSQL claim.
- All 117 reviewed files pass strict structural audit and normalizer `--check`.
  All **102 edited production Python modules** and **nine edited existing test
  modules** retain baseline executable ASTs, with the previously documented single
  local-proxy runtime docstring-template exception. The data generator separately
  matches its submodule HEAD after docstring stripping.
- API introspection remains unchanged on the shared fake runtime: 83 commands,
  93 queries, three targets. Sorted-JSON SHA-256 with core_uuid omitted remains
  `6c4dfd704980bd0e971291338880ad747a8ba4f7750db5fa4fcbcc07f168fa26`.
- Full quality runner passed: 159 formatter-clean files, 456 annotated modules,
  221 dependency-protected modules, clean selected lint/complexity, 188 strict-mypy
  files, no production typing errors, and all 37 negative examples rejected by
  each checker. No numeric guard or gate scope changed.
- The three production modules introduce no new Ruff findings against the baseline;
  each retains 47 pre-existing outside-scope findings. All four batch files are
  formatter-clean; the new test module has zero Ruff findings. Root and data-submodule
  diffs are whitespace-clean.
- Final documentation-link/public-boundary checks after the handoff update:
  **23 passed**.

All test/audit/quality handles for this batch completed, including the handles
recovered after the deliberate interruption. No missing result is treated as success.

## Remaining scope

The refreshed whole-project report, `/tmp/liuxin-docstring-current-2026-09-09.json`,
covers 2,723 modules, 4,399 classes, and 35,009 functions with no parse failures.
There are still **27,057 missing/blank docstrings**, plus overlapping structural
and incomplete-prose findings recorded in the main note. The full goal remains active.

The subsequent [application API batch](project-docstrings-application-api-2026-09-09.md)
completes `src/LiuXin_alpha/core/application_api.py` and its full regression module;
its note supersedes this batch's cumulative counts and fixture fingerprint.
Continue with the remaining program services/endpoints, then the wider project.
Do not forget tests, scripts, inherited code, configuration
module docstring consumers, or the data submodule. The program-facade/owner tests
still count docstrings as physical lines/statements; changing them awaits the
previous nonblocking question, but other documentation work is available.
