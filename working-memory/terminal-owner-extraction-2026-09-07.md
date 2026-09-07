# Terminal owner extraction — 2026-09-07

## Scope and current state

Stage 8 resumes from committed stage 7, `9eee6c84`, on
`codex/package-calibre-resources`. Stage 8 is complete for the announced owner
extraction and enforcement scope. This note accompanies the stage-8 checkpoint;
it and the previous stage-7 and stages-5–6 checkpoints have not been pushed.
The programme is paused after this checkpoint for reassessment.

The announced scope covers both large terminal owners, not only their facades:
split commands, session/history/completion, browsing/rows, curses input, and pane
responsibilities; reduce inherited complexity and include the complete extracted
implementations in maintained lint/typing/complexity coverage. Legacy failure
policy changes and inherited command-family rewrites are separate work.

## Implementation

- `terminal/browser.py` is a 128-line composition root for ten browser owners.
  `terminal/windowed_ui.py` is a 178-line composition/browser-adapter root for
  eight curses owners. Each component has one responsibility and stays below
  the 450-line owner/160-line function boundaries. Roots are capped at 250 lines.
- `BrowserState[HostT]` declares shared state and abstract cross-owner calls.
  The root's `_extension_host` supplies the real browser to typed command and
  lifecycle extensions. No catch-all helper access or host casts are used.
- `WindowedState` declares driver state and named cross-pane calls.
  `WindowedBrowser`, `CursesWindow`, `RowRecord`, and `ReadlineBackend` are narrow
  structural capabilities; cursor, completion, and configuration records have
  explicit owners. Historical facade/type/helper aliases remain available.
- Readline replacement tests now patch `browser_components.session`; Core
  coercion remains in `browser`. Existing public call shapes and entry points
  remain unchanged. Core `get_all_rows` overloads describe the existing
  list-versus-iterator return; its runtime implementation is unchanged.
- All 18 original complexity violations are reduced to at most 10 using named
  completion/legacy/search handlers, row/column rendering helpers, input-key
  handlers, and status/telemetry/allocation helpers. Broad recovery boundaries
  retain their original behavior; this is not a failure-policy tranche.
- Both complete component trees and roots enter strict mypy and standard
  basedpyright, lint, formatting, and complexity-10 checking. New model/structural
  capability leaves enter strict basedpyright. The global scope is now 154
  formatted files, 188 strict-mypy files, 221 protected dependency modules
  (all 69 terminal modules), and 37 negative examples per checker.
- Dependency rules protect concrete-root direction, browser/curses separation,
  and contract/implementation separation in every import context. CI includes
  new ownership/scope and headless behavior suites alongside existing terminal
  and scanner contracts.

Canonical guidance: [terminal composition](../dev-docs/terminal-composition.md)
and [quality gates](../dev-docs/maintainability-quality-gates.md).

## Verification

- Full `bash scripts/run_type_checks.sh`: passed with the expanded scopes above,
  zero production type errors, both sets of 37 rejected negative examples,
  456 annotated modules, and green lint/dependency/complexity checks.
- Focused new ownership/behavior plus existing terminal/windowed composition:
  **96 passed**, with seven cold-import cases reserved for the broad run.
- Final terminal, real-database/Core/direct-RPC, architecture, documentation,
  formatter/runner, and type-verifier regressions: **455 passed**. The 12 warnings
  are the existing multiprocessing/fork deprecation warnings also seen in stage 7.
  The broad run includes the cold-import and optional-curses cases omitted from
  the earlier focused selection.
- Read-only comparison against the captured stage-7 owners matched **66,429**
  legacy rewrites, **7,810** command completion cases, **20** header cases,
  **540** table-rendering cases, **944** pane allocations, and **2,744** input
  sequences. These are generated differential cases, not distinct pytest tests.
- Further baseline comparisons matched **1,344** row summaries (including
  non-mapping membership failures) and **448** status/telemetry cases, including
  unavailable browsers, query failures, pane flags, and event-tail limits.
- Final compatibility snapshots exactly match stage 7: parser help/actions,
  all 67 default command registrations, and all nine exported call shapes.
- Normalized structural audit accounts for all 98 browser and 60 driver methods:
  85 browser and 56 driver bodies retain their executable structure, while the
  remaining changes are the planned helper extractions/readline guard. All 13
  curses-adapter method bodies remain unchanged. No missing or unexpected
  changes were found; safe Ruff rewrites, annotations, docstrings, and the
  concrete extension-host binding are normalized in this comparison.
- All 117 relative links in the index, stage-7/stage-8 notes, and terminal
  composition/quality-gate guides resolve. The reviewed diff is whitespace-clean.
- Early extraction checks caught and repaired incomplete abstract composition,
  explicit private re-export typing, and exact-line negative-fixture placement.

The broad run selects `tests/surfaces/test_text_browser.py`,
`test_windowed_ui.py`, `test_terminal_dependency_contracts.py`,
`test_terminal_component_ownership.py`, `test_terminal_component_behavior.py`,
`test_core_surface_acceptance.py`, and `test_core_boundary_enforcement.py`;
`tests/core/test_core_application_api.py`; and the import scanner, public
documentation, quality runner, formatter runner, and internal-type-verifier
suites under `tests/scripts`. It uses the prepared environment and explicitly
permitted local Core/RPC sockets. Do not add these overlapping counts to the
focused run above.

## Follow-up boundaries

- The announced stage-8 scope is complete. Inherited command-family typing,
  lower-level recovery policies, and further formatter expansion remain separate
  future work; no whole-terminal strict-basedpyright claim is made.
- Code, tests, quality gates, developer guidance, and working memory are included
  together in this checkpoint. No unrelated edits were included. The follow-up
  areas above are candidates for reassessment, not authorized next work.
- Checkpoint preparation only updates handoff status; the verification above
  applies to the unchanged implementation and was not rerun for the commit.
- No live PostgreSQL, manual interactive curses, whole-project test suite,
  remote CI, or push sign-off is implied by these local checks.
