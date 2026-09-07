# Terminal composition and dependency direction

Terminal startup selects a plain shell, non-interactive commands, or the curses
adapter. Browser execution does not select or import its presentation adapter.

## Ownership

| Module under `surfaces/terminal` | Responsibility |
| --- | --- |
| `app` | Argument grammar, Core-session startup, command-mode precedence, UI selection, exit/error handling. |
| `browser` | Command dispatch, lifecycle, history, completion, browsing, and the Core-backed extension host. |
| `database_creation` | Interactive creation choices and applying them through a Core session. |
| `presentation` | Shared text/table rendering and stream prompts; standard library only. |
| `windowed_ui` | Curses driver and browser subclass; depends on the browser owner, not startup. |
| `commands/base`, `plugins/base` | Generic extension APIs independent of concrete browser implementations. |
| `text_browser` | Explicit historical public/private aliases; no duplicate implementation. |
| `__init__`, `__main__` | Package exports and module entry point. Browser/application exports resolve lazily. |

`app.run_windowed_text_browser()` imports the curses adapter only when selected.
Importing the terminal package, extension bases, shared helpers, or plain browser
does not load the application or curses adapter. Package-level browser exports
resolve to their actual owners, with the same public `__all__`. The commands and
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

Positive and negative static examples check the real APIs, concrete command
conformance, wrong hosts/arguments, and lifecycle keywords. Real browser hosts
are also passed through those calls; basedpyright resolves their implementation,
while mypy retains the existing skipped-import policy for unratcheted owners.

## Compatibility and tests

Historical package and `text_browser` imports retain the same public objects.
Parser options/help, default command registrations and aliases, plain/history
behavior, non-interactive precedence, windowed configuration bounds, creation
prompts, and failure policies remain covered by the terminal regression suites.
Tests replacing dependencies must patch the consuming owner (`app` for startup,
`browser` for Core coercion/readline), not an old imported facade global.

`tests/surfaces/test_terminal_dependency_contracts.py` covers cold imports,
operation without curses for plain help, compatibility identity, real typed
extension dispatch/lifecycle, and curses-driver/browser composition. Existing
text-browser and windowed tests retain their behavior assertions. A missing
terminal package now fails those suites rather than skipping them wholesale.

Run the affected tests and the shared quality gate:

```bash
.venv/bin/python -m pytest -q tests/surfaces/test_text_browser.py tests/surfaces/test_windowed_ui.py tests/surfaces/test_terminal_dependency_contracts.py
bash scripts/run_type_checks.sh
```

The combined import graph protects all 45 terminal modules in every import
context. Implementations may not import package/application/compatibility entry
points; only explicit entry wrappers are allowed to delegate. The browser cannot
import its curses adapter, and the three shared leaves cannot import another
LiuXin module. Backward directions fail even before they form a cycle.

Seven reviewed terminal sources enter typing and lint; the presentation and two
extension leaves enter strict basedpyright. Six startup/facade/API sources enter
complexity-10 checking. The larger browser, curses driver, and inherited rendering
helpers still need their own complexity reductions; no higher ceiling or blanket
suppression was added for them. All nine touched terminal sources and the new
contract suite are formatter-controlled. This is a bounded dependency repair,
not a whole-terminal strict-typing or whole-project acyclicity claim.
