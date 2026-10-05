# Terminal composition and dependency direction

Terminal startup selects a plain shell, non-interactive commands, or the curses
adapter. Browser execution does not select or import its presentation adapter.

## Ownership

| Module under `surfaces/terminal` | Responsibility |
| --- | --- |
| `app` | Argument grammar, Core-session startup, command-mode precedence, UI selection, exit/error handling. |
| `browser` | Public browser construction and binding the concrete extension host. |
| `browser_components` | Command registration/dispatch, legacy spellings, help, session/history, completion sources/routing, catalog lookup, row rendering, browsing commands, and Core/output capabilities. |
| `database_creation` | Interactive creation choices and applying them through a Core session. |
| `presentation` | Shared text/table rendering and stream prompts; standard library only. |
| `windowed_ui` | Curses driver construction and browser adapter; depends on the browser owner, not startup. |
| `windowed_components` | Console/job scrollback, telemetry and status content, completion, input/history, layout/drawing, and shared compact presentation. |
| `commands/base`, `plugins/base` | Generic extension APIs independent of concrete browser implementations. |
| `__init__`, `__main__` | Lightweight namespace and module entry point calling app.main. |

`app.run_windowed_text_browser()` imports the curses adapter only when selected.
Importing the terminal package, extension bases, shared helpers, or plain browser
does not load the application or curses adapter. Import browser and application objects from their owning modules. The commands and
plugins packages retain their existing registration/export behavior.

## Extension contracts

`TerminalCommandAPI[BrowserT]` and `TerminalLifecyclePluginAPI[BrowserT]` describe
the host accepted by each extension. An external extension may parameterize them
with `TextDatabaseBrowser` or a smaller protocol declaring only the operations it
needs. The application specializes its registries to `TextDatabaseBrowser`;
neither API imports that implementation back, even for type checking.

Commands still require an `execute(browser, args) -> bool` implementation.
Lifecycle hooks remain optional no-ops with their existing call shapes and
ordering, including the inherited ABC virtual-subclass registration API. One
local, documented B024 lint exception preserves that compatibility: these hooks
must not become abstract just to satisfy a lint heuristic. Existing unparameterized
extensions still run, but new typed extensions should name their accepted host.
This is not a claim that every inherited command implementation is fully typed.

The browser's mixins share `BrowserState[HostT]`: explicit initialized state and
abstract cross-owner methods. `TextDatabaseBrowser` supplies itself through
`_extension_host`, preserving the concrete host accepted by its extensions without
casting the mixins or importing the composition root back into them. Optional
readline and row access have narrow structural contracts in `browser_components/models`.

The curses owners share `WindowedState`. `WindowedBrowser` and `CursesWindow`
describe the capabilities they consume; neither imports a concrete composition
root. Missing required owner methods remain abstract and prevent construction.
Methods used only within an owner stay local, rather than enlarging the shared
contract. Dynamic Core payloads remain explicit wire-data boundaries, not a way
to erase host conformance.

Positive and negative static examples check the real APIs, concrete command
conformance, wrong hosts/arguments, and lifecycle keywords. Real browser hosts
are also passed through those calls; basedpyright resolves their implementation,
and mypy now checks the complete browser and curses implementation trees. The
configured skipped-import policy remains only for dependencies outside the ratchet.

## Compatibility and tests

The historical package exports and text_browser facade have been removed.
Import TextDatabaseBrowser from browser and main/build_parser from app.
Parser options/help, default command registrations and aliases, plain/history
behavior, non-interactive precedence, windowed configuration bounds, creation
prompts, and failure policies remain covered by the terminal regression suites.
Tests replacing dependencies must patch the consuming owner (`app` for startup,
`browser` for Core coercion, `browser_components.session` for readline), not an
old imported facade global.

`tests/surfaces/test_terminal_dependency_contracts.py` covers cold imports,
operation without curses for plain help, direct owner imports, real typed
extension dispatch/lifecycle, and curses-driver/browser composition. Existing
text-browser and windowed tests retain their behavior assertions. A missing
terminal package now fails those suites rather than skipping them wholesale.

Run the affected tests and the shared quality gate:

```bash
.venv/bin/python -m pytest -q tests/surfaces/test_text_browser.py tests/surfaces/test_windowed_ui.py tests/surfaces/test_terminal_dependency_contracts.py tests/surfaces/test_terminal_component_ownership.py tests/surfaces/test_terminal_component_behavior.py
bash scripts/run_type_checks.sh
```

The combined import graph protects all 69 terminal modules in every import
context. Implementations may not import package/application/compatibility entry
points; only explicit entry wrappers are allowed to delegate. The browser cannot
import its curses adapter, and the three shared leaves cannot import another
LiuXin module. Components may not import concrete composition roots; browser
components cannot depend on curses components, and contracts cannot depend on
component implementations. Backward directions fail even before they form a cycle.

Both complete component directories, both composition roots, and shared terminal
rendering now enter lint and complexity-10 checking. The 18 original complexity
violations are replaced by named command handlers, lookup/summary helpers, key
handlers, and pane-content/allocation helpers. No raised ceiling or blanket
suppression is used. All 33 reviewed terminal source files are formatter-controlled
and strict-mypy targets; basedpyright remains standard for dynamic orchestration
and strict for the two new model/structural-contract leaves plus the previous
presentation and extension leaves.

`test_terminal_component_ownership.py` keeps component files at most 450 lines,
functions at most 160, and roots at most 250; it also checks actual method owners,
complete abstract composition, and quality-scope inclusion. The headless behavior
suite exercises legacy validation precedence, command-specific completion, ordered
ID scanning, input editing/history/EOF, and pane allocation. These are maintained
regressions alongside the existing real-database and windowed suites, not a
replacement for them. Inherited command families and recovery-policy changes
remain separate work; no whole-terminal strict-basedpyright or whole-project
acyclicity claim is made.
