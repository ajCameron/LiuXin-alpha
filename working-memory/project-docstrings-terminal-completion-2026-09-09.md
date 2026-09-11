# Terminal source docstring milestone — 2026-09-09

Continuation of the active [whole-project descriptive reST goal](project-docstrings-2026-09-08.md),
following the [Core source milestone](project-docstrings-core-completion-2026-09-09.md).
The previous goal turn made verified progress; this turn revalidated the dirty
worktree and completed the remaining terminal source declarations. Work remains
local on `codex/project-docstrings`, based on `edf6bf05`, without commit, push,
PR mutation, submodule publication, or delegation.

**This is not completion of the whole-project objective.** All **69 terminal
Python modules, 114 named classes, and 536 named functions** now pass the strict
docstring audit. The earlier 57-module Core milestone remains reviewed too.

## Reviewed scope and contracts

Seven newly completed production files:

- `terminal/browser.py`: module, concrete browser class, constructor, and extension
  host property. Docs distinguish borrowed clients from compatibility Core
  bootstrap, constructor ordering and missing rollback, truthy stream/read-source
  fallbacks versus non-None job-manager selection, clamped page size, expanded
  history paths, and registration versus lifecycle startup. Pure examples use
  mock clients/managers and in-memory streams, not real databases.
- `terminal/windowed_ui.py`: driver/browser composition and all adapter methods,
  including the nested runner. Docs describe unbound/pre-window state, independent
  driver history-path semantics, minimum console capacity, forwarding rather than
  alternate workflows, one-shot boolean prompts, dynamic panel-selection return
  flags, and setup/history/browser lifecycle boundaries. Empty accepted command
  text remains EOF to the inherited shell loop; this surprising existing behavior
  is documented and tested, not silently changed.
- `terminal/__main__.py`: import-only binding versus python -m execution and
  SystemExit propagation; application/cleanup ownership remains elsewhere.
- `browser_components/catalog.py`: schema/display/exact/prefix precedence,
  ambiguous display aliases, ordered spelling guesses, unchanged selection state,
  language ID/search/scan behavior, unknown count sentinels, and sequential paging.
  Numeric language conversion and present read errors propagate. Stored language
  whitespace is not trimmed by the casefolded scan. Raw nonpositive page limits
  can return the first eligible row; the public wrapper clamps bounds first.
- `browser_components/rows.py`: Mapping versus non-Mapping lookup error boundaries,
  mutable rendered-preview deduplication, falsey-value handling, schema/group ordering,
  character-width fitting, rendering-before-emission, preferred/fallback/full-row
  precedence, and visibility of present falsey values in repr fallback.
- Both component `contracts.py` modules: 36 browser and 22 windowed abstract methods,
  both ABCs, and module introductions. Docs distinguish required annotations from
  initialized state, link to the actual method owner with reST :meth: references,
  reuse its reviewed summary and meaningful fields, and retain all abstract
  decorators, signatures, property/static descriptor kinds, and ellipsis bodies.

Existing tests are included rather than excluded from the project goal:

- `tests/surfaces/test_terminal_component_behavior.py`: its module, one test command
  class, and all fourteen functions, including fixture/nested recording helpers.
- `tests/surfaces/test_terminal_component_ownership.py`: its module and all five
  functions, documenting exactly what the current physical-line, method-set,
  abstract-composition, origin, and quality-scope checks do. **Guard logic, limits,
  and selected paths are unchanged**; only its docstrings changed.
- New `tests/surfaces/test_terminal_documentation_contracts.py`: module and all ten
  functions. Headless tests cover constructor/history distinctions, count/paging
  sentinels and failures, lookup precedence, language scan whitespace, row-access
  retry boundaries, preview collisions/full fallback, blank-command EOF, invalid
  boolean defaults, and all 58 abstract-method owner links/fields/signatures/kinds.
  Its normal pytest discovery does not expand the default quality-tool scopes.

Cumulative reviewed scope: **153 modules, 203 classes, 1,465 functions = 1,821
declarations**, plus previously documented native C and the runtime-created Core
vacuum adapter class. The exact full file set remains in the main ledger.

## Verification

Selections overlap and must not be summed as distinct tests.

- Catalog/row doctests: **five passed, 28 explicitly skipped integration examples**.
- Browser/windowed/entrypoint doctests: **five passed, sixteen skipped**.
- Existing component behavior/ownership doctests/tests plus both ABC modules:
  **78 passed, 68 skipped, three failed**. All failures are the terminal physical-
  line guards described below, not abstractness, ownership origins, or behavior.
- New documentation-contract test module and runnable examples: **18 passed**.
- Existing windowed tests plus eight selected fixture-based browser regressions:
  **25 passed** in 80.16 seconds. Covers basic browsing, bootstrap failure,
  table/row output, terminal width, table aliases, display-column mutation, and
  lifecycle startup/shutdown. This is not interactive curses, live PostgreSQL,
  or a new complete text-browser/full-project regression run.
- Final **all-terminal-source** doctests plus new/existing component tests,
  docstring migration/public-boundary tests, and both ownership suites:
  **410 passed, 370 explicitly skipped integration examples, five failed** in
  93.71 seconds. The failures are exactly three terminal and two previously known
  Core ownership guards. Do not call this selection all-green.
- Full `bash scripts/run_type_checks.sh`: **passed** with 159 formatter-clean files,
  456 annotated modules, 221 protected dependencies, 188 strict-mypy files, zero
  production type errors, clean selected lint/complexity, and all 37 negative
  examples rejected by each checker. This runner does not execute physical-line
  pytest guards, so its success does not erase their failures.
- Strict audit and normalizer `--check` pass on all 153 reviewed files. All ten
  current-batch files are formatter-clean. Ruff findings remain unchanged: zero
  in the maintained files/new test, and the entrypoint retains its one baseline
  I001 import-layout finding outside the default lint scope. No baseline finding was waived or repaired
  as an unrelated implementation change.
- All **133 edited existing production Python modules** and **twelve edited existing
  test modules** preserve executable ASTs against `edf6bf05`, with only the two
  specifically verified runtime documentation-value exceptions recorded in the
  Core note. The generic normalizer was not weakened. The data generator separately
  matches its own submodule HEAD after stripping genuine leading docstrings.

After the integrated run, one no-op test-command doctest was made type-compatible
by passing an uninitialized TextDatabaseBrowser instead of None; the command ignores
that argument either way. Strict audit/normalizer checks were rerun for that file;
its final behavior/doctest rerun has **51 passed, eight skipped**. All three current
test modules remain formatter-clean. Working-memory link verification resolves
all **125 local destinations across seven notes**; final public/link handoff tests:
**23 passed**. Root and data-submodule diffs are whitespace-clean. All observed
test, audit, AST, lint, quality, and handoff handles have completed; the five
known guard failures are terminal results, not pending checks or hidden passes.

## Ownership-test conflict — documentation retained, rules unchanged

New terminal violations, measured across every component file rather than just
pytest's first failing path:

| Terminal owner | Physical lines | Existing file ceiling |
| --- | ---: | ---: |
| `browser_components/catalog.py` | 522 | 450 |
| `browser_components/contracts.py` | 725 | 450 |
| `browser_components/rows.py` | 543 | 450 |
| `windowed_components/contracts.py` | 467 | 450 |
| `windowed_ui.py` | 410 | 250 |

No terminal component function exceeds 160 physical lines. The concrete browser
and driver still implement the required ABCs, and their direct method-name sets
and inherited implementation origins are unchanged. The test module has descriptive
doc edits but its stripped executable AST matches baseline, including all numbers.

The earlier Core file/function/body conflicts remain recorded in the Core note.
Permission to count genuine docstrings separately while retaining implementation
limits remains unanswered. No guard logic or numeric limit changed. Do not omit
larger files, remove useful prose, or reinterpret these failures as behavior
regressions. Other whole-project source work is available, so the goal is not blocked.

## Remaining project scope

`/tmp/liuxin-docstring-current-2026-09-09.json` now inventories **2,725 modules,
4,399 classes, 35,038 functions**, with no parse failures. Missing/blank docs remain
on **1,118 modules, 2,042 classes, 23,484 functions = 26,644 declarations**. Additional
overlapping findings: 4,348 delimiter layouts, 10,598 missing examples, 3,076 parameter
fields, 4,024 return fields, 4,143 empty parameter descriptions, 5,581 empty return
descriptions, and 29 missing summaries. The reviewed report remains
`/tmp/liuxin-docstring-reviewed-2026-09-09.json`.

Next useful source boundary is `src/LiuXin_alpha/surfaces/core.py` (1,098 lines),
whose current coercion/session/model excerpts were read for this batch but which
has **not** been fully documented or added to the reviewed ledger. Other shared
surface modules still include `__init__.py`, `api.py`, `acquisition_types.py`,
`categories.py`, `presentation.py`, `system_profile.py`, `tags_icons.py`, and
`thumbnail_cache.py`. Existing reviewed `metadata_facets.py` and `write_refresh.py`
need not be repeated. CLI/HTTP/web and all remaining packages, tests, scripts,
examples, inherited code, and configuration-module consumers remain in scope.
These are navigation suggestions, not a narrower substitute for the original goal.
