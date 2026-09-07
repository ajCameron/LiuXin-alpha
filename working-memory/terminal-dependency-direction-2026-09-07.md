# Terminal dependency direction — 2026-09-07

## Scope and ownership

Stage 7 follows the committed stages 5–6 checkpoint `12c1fd69` on
`codex/package-calibre-resources`. This note accompanies the stage-7 checkpoint;
the checkpoint has not been pushed. Its bounded scope is the terminal
browser/windowed-UI cycle and associated extension type dependencies, not a
complete rewrite of the large terminal implementations.

- `terminal/app.py` owns parser/startup composition, command-mode precedence,
  Core session opening, and lazy curses selection.
- `terminal/browser.py` owns the existing browser class, command dispatch,
  lifecycle, history, completion, and Core-backed browsing/mutation behavior.
- `terminal/database_creation.py` owns wizard configuration/prompts and Core
  creation. Shared rendering/stream prompts live in `terminal/presentation.py`,
  with documented public names and standard-library-only dependencies.
- `terminal/windowed_ui.py` imports the browser owner, never its entry-point
  facade. Its implementation is otherwise formatter-only.
- `terminal/text_browser.py` retains explicit public/private aliases, including
  the old helper names. Package browser/application exports resolve lazily and
  remain discoverable through `dir()`. No dynamic monkeypatch forwarding is
  added; tests patch consuming owners instead of imported facade globals.
- Command/lifecycle bases are generic over their accepted host rather than
  importing `TextDatabaseBrowser` under `TYPE_CHECKING`. Application registries
  name the real browser host. The command remains abstract; lifecycle hooks
  remain optional no-ops, and inherited ABC virtual registration is retained.
  A single local, documented B024 lint exception protects this existing API
  without making optional hooks abstract or introducing a file-wide suppression.

Canonical guide: [terminal composition](../dev-docs/terminal-composition.md).

## Enforcement

- The baseline combined terminal graph had a 39-module component (41 terminal
  modules total), because type-only extension imports joined the executable
  text-browser/windowed-UI cycle.
- The protected combined graph now includes all 45 terminal modules: 197
  modules overall. Import-time, deferred, and type-only dependencies all count.
- Implementations may not import terminal package/application/compatibility
  entry points. Explicit entry wrappers may delegate. Browser execution may
  not import its curses adapter, and presentation/extension leaves may not
  import another LiuXin module. Acyclic backward edges also fail.
- Seven terminal sources enter typing/lint, three leaves enter strict
  basedpyright, and six startup/facade/API sources enter complexity-10 checks.
  Current strict mypy scope is 162 files; current formatter scope is 128 files.
  The large browser/curses owners and rendering-helper complexity remain
  explicit debt, not silently covered or granted a raised ceiling.
- Five new negative examples protect extension hosts, argument/keyword types,
  and command override conformance. Different diagnostic locations retain
  exact-line per-checker markers. The fixture checks real APIs and a concrete
  command, plus real browser calls; mypy's skipped-import policy for the large
  browser remains unchanged.
- CI includes terminal dependency/compatibility and existing windowed driver
  contracts. Scanner tests protect recursive terminal scope and every new
  forbidden direction in all import contexts. The two existing terminal
  suites no longer skip wholesale if the terminal package cannot import.

## Verification

- Full quality gate passed: 128 formatter-controlled files, 456 annotated
  modules, 197 protected dependency modules, zero production typing errors,
  162 strict-mypy files, and all 32 negative examples rejected by each checker.
  Lint and complexity checks remain green.
- Focused terminal/import/documentation contracts: **107 passed**. Final ABC,
  typed-extension, lazy-export, and documentation checks: **7 passed**. These
  counts overlap.
- Broader terminal, windowed driver, real Core/direct-RPC acceptance, application
  boundaries, formatter/runner, and type-verifier contracts: **245 passed**.
  The 12 warnings are the existing multiprocessing/fork deprecation warnings.
- The original baseline also completed **192 passed**. An attempted interrupt
  landed in a SQLite progress callback and was reported as an unraisable warning;
  it did not stop the run. This is not clean-warning sign-off or evidence from
  the final implementation. The post-extraction run above had no such warning.
- Before/after snapshots preserve terminal help/action metadata, all 67 default
  command registrations/aliases, and all nine exported call shapes.
- Read-only source audit against `12c1fd69` finds all 23 original text-browser
  definitions in their new owners and all four windowed definitions unchanged
  in executable structure. The comparison normalizes safe Ruff rewrites,
  annotations/docstrings, helper-owner names, and the unused loop-index rename.
- Relative documentation links resolve, and `git diff --check` is clean.

The database-backed tests selected SQLite, with explicitly permitted local
Core/RPC sockets. No live PostgreSQL, manual interactive-curses, full-project,
or remote-CI sign-off is claimed. Test counts overlap and are not additive.

Stage 7 is complete for the bounded dependency repair and is grouped with its
tests, quality gates, and documentation in this checkpoint. Stages 5–6 are
committed separately in `12c1fd69`; neither checkpoint has been pushed.

## Checkpoint verification — 2026-09-07

- `bash scripts/run_type_checks.sh` passed again, retaining 128 formatted files,
  456 annotated modules, 197 dependency-protected modules, zero production type
  errors, 162 strict-mypy files, and all 32 negative examples per checker.
- Focused terminal dependency/compatibility, windowed driver, import scanner,
  and public-documentation boundary suites: **124 passed**.
- The source audit against `12c1fd69` again preserved the executable structure
  of all 23 original text-browser definitions and four windowed definitions
  under the normalization described above.
- All 122 relative links in the index, affected stage handoffs, and terminal
  composition/quality-gate guides resolve. `git diff --check` is clean.

The checkpoint tidy-up updates working-memory status only. The broader 245-test
run above remains stage-7 evidence, not a new checkpoint rerun.

## Remaining programme

Further terminal work should split the large browser and curses driver along
their actual command, history/completion, and panel responsibilities, then widen
strict typing/complexity enforcement. Legacy recovery policies and additional
formatter coverage remain separate work. No whole-project acyclicity or
full-terminal strict-typing claim is made by this stage.

Stage-8 preparation inspected the browser, curses driver, and rendering helpers,
captured source and compatibility baselines, and identified 18 functions above
complexity 10. No stage-8 implementation changes are included in this checkpoint.
Resume with the responsibility split above, preserving the current command,
completion, panel, and failure behavior while adding named internal contracts
and widening lint/complexity coverage. The existing stage-7 verification is not
stage-8 sign-off.
