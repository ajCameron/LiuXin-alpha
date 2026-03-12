# Interface Findings

Date: 2026-03-11
Scope: terminal interface review, with attention to the ongoing shift toward a central core service over RPC.
Status: documentation only; no code changes made in this note.

## Findings

### 1. High: windowed console double-spaces normal output

`_CursesUiDriver.append_output()` splits incoming text and then appends an empty string whenever `end` is a newline. Two ordinary writes therefore become:

```python
["hello", "", "world", ""]
```

That makes the curses console look double-spaced and reduces visible output capacity.

Files:
- `src/LiuXin_alpha/interfaces/terminal/windowed_ui.py:77`
- `src/LiuXin_alpha/interfaces/terminal/windowed_ui.py:80`

Validation:
- Reproduced by instantiating the UI driver and calling `append_output("hello")`, `append_output("world")`.

### 2. Medium: `RemoteJobsProxy.list(states={"running"})` is not JSON-safe

`RemoteJobsProxy.list()` accepts a generic `Collection[str]` for `states` and forwards it directly into `_http_json()`. If the caller passes a `set`, JSON encoding fails before the request reaches the daemon.

`json.dumps({"states": {"running"}})` raises `TypeError: Object of type set is not JSON serializable`.

Files:
- `src/LiuXin_alpha/core/proxies/remote.py:33`
- `src/LiuXin_alpha/core/proxies/remote.py:37`
- `src/LiuXin_alpha/core/proxies/remote.py:128`
- `src/LiuXin_alpha/core/proxies/remote.py:136`
- `tests/core/test_core_proxy_jobs_phase3.py:138`
- `tests/core/test_core_proxy_jobs_phase3.py:157`

Risk:
- The current test shape hides the bug because it monkeypatches `_http_json()` instead of exercising real serialization.

### 3. Medium: proxy write classification misses `bootstrap_storage_manager`

`looks_like_write_method()` currently reports `False` for `bootstrap_storage_manager`. That matters because `add store` calls that method after saving the row. Once that path moves behind the proxy boundary, it will be treated as a query instead of a command.

Files:
- `src/LiuXin_alpha/core/proxies/local.py:13`
- `src/LiuXin_alpha/core/proxies/local.py:41`
- `src/LiuXin_alpha/interfaces/terminal/commands/new_store.py:184`
- `src/LiuXin_alpha/interfaces/terminal/commands/new_store.py:189`

Validation:
- Confirmed directly by evaluating `looks_like_write_method("bootstrap_storage_manager")`.

### 4. Medium: terminal silently falls back out of core-backed mode

`TextDatabaseBrowser.__init__()` catches any exception from `_build_default_core_runtime()` and sets `_core_runtime = None`. From there, commands such as `jobs` and background `sync` quietly use local fallbacks.

Files:
- `src/LiuXin_alpha/interfaces/terminal/text_browser.py:58`
- `src/LiuXin_alpha/interfaces/terminal/text_browser.py:68`
- `src/LiuXin_alpha/interfaces/terminal/text_browser.py:481`
- `src/LiuXin_alpha/interfaces/terminal/text_browser.py:485`
- `src/LiuXin_alpha/interfaces/terminal/commands/jobs.py:123`
- `src/LiuXin_alpha/interfaces/terminal/commands/sync.py:798`

Risk:
- This makes it easy to think the terminal is exercising the RPC/core boundary when it is not.

### 5. Medium: windowed job/status panels hide core-query failures

The windowed status board and job panel swallow failures from the core query path and fall back to the local job manager instead. In a remote-client setup, that will show empty or stale local state instead of surfacing the RPC problem.

Files:
- `src/LiuXin_alpha/interfaces/terminal/windowed_ui.py:225`
- `src/LiuXin_alpha/interfaces/terminal/windowed_ui.py:235`
- `src/LiuXin_alpha/interfaces/terminal/windowed_ui.py:245`
- `src/LiuXin_alpha/interfaces/terminal/windowed_ui.py:249`

## Open Question

The terminal still appears to be local-DB-first for many mutating flows, with commands operating on `browser.db` directly rather than through the core boundary. If that is intentional for now, fine. If not, that looks like the next interface milestone before leaning harder on the RPC transition.

Representative files:
- `src/LiuXin_alpha/interfaces/terminal/text_browser.py:1596`
- `src/LiuXin_alpha/interfaces/terminal/commands/new_work.py:97`
- `src/LiuXin_alpha/interfaces/terminal/commands/link.py:246`
- `src/LiuXin_alpha/interfaces/terminal/commands/on.py:501`

## Test Note

I started a targeted test slice for terminal/core behavior:

```bash
pytest -q \
  /home/blackjane/LiuXin-alpha-wsl/tests/interfaces/test_text_browser.py \
  /home/blackjane/LiuXin-alpha-wsl/tests/core/test_core_runtime_phase1.py \
  /home/blackjane/LiuXin-alpha-wsl/tests/core/test_core_runtime_jobs_phase2.py \
  /home/blackjane/LiuXin-alpha-wsl/tests/core/test_core_http_daemon_phase2.py \
  /home/blackjane/LiuXin-alpha-wsl/tests/core/test_core_proxy_jobs_phase3.py
```

At the point I stopped watching, no failures had surfaced, but I do not have a completed pass/fail summary from that run.
