# LiuXin-alpha-wsl Context

Snapshot date: 2026-03-11

## What this repo is

`LiuXin-alpha-wsl` is an alpha development fork of LiuXin, which is effectively "calibre for large archival-scale datasets" with a stronger storage model and room for more backends.

The project framing from `dev-docs` is:

- metadata is owned by the database
- files are owned by storage / what exists on disk or remote storage
- the `Library` layer joins metadata + storage
- archival philosophy is conservative: original files should not be deleted

The top-level `README.md` is intentionally minimal. The real project explanation lives in `dev-docs/`.

## Session handoff

For short-term working notes and review findings, check:

- `working-memory/index.md`
- `working-memory/`

## High-signal directories

- `src/LiuXin_alpha/`: main application code
- `src/LiuXin_alpha/interfaces/terminal/`: text browser / terminal UI entrypoint and commands
- `src/LiuXin_alpha/storage/`: storage manager, storage backends, reconcile/sync code
- `tests/interfaces/test_text_browser.py`: best reference for terminal command behavior
- `tests/storage/`: backend and reconcile coverage
- `dev-docs/`: architecture notes, especially storage and schema
- `examples/`: runnable examples and quick smoke-test scripts
- `LiuXin_alpha_data/`: local fixture/test data

## Runtime / environment

- Python project with source layout under `src/`
- `pyproject.toml` declares `requires-python = ">=3.12"`
- minimal declared dependency is `lxml>=5.0`
- optional test dependency is `pytest>=7.4`
- there is no installed console script entrypoint in `pyproject.toml`

Practical launch pattern from repo root:

```bash
cd /home/blackjane/LiuXin-alpha-wsl
PYTHONPATH=src python3 -m LiuXin_alpha.interfaces.terminal --database /path/to/library.sqlite
```

Windowed UI:

```bash
cd /home/blackjane/LiuXin-alpha-wsl
PYTHONPATH=src python3 -m LiuXin_alpha.interfaces.terminal --database /path/to/library.sqlite --ui-mode windowed
```

Non-interactive command mode:

```bash
cd /home/blackjane/LiuXin-alpha-wsl
PYTHONPATH=src python3 -m LiuXin_alpha.interfaces.terminal --database /path/to/library.sqlite --command 'tables'
```

Important CLI behavior:

- `--command` bypasses the interactive shell and also bypasses the windowed UI
- if you want the windowed UI plus a background job panel, launch windowed first and then type commands inside the app

## Important docs to read first

- `dev-docs/01 - Introduction.md`
- `dev-docs/02 - Top Level Structure.md`
- `dev-docs/06 - Storage.md`
- `examples/README.md`

## Storage / sync model

The main storage sync command in the terminal UI is:

```text
sync store <store_id|store_name> [to-db] [options]
```

This is implemented in:

- `src/LiuXin_alpha/interfaces/terminal/commands/sync.py`

The text-browser entrypoint is implemented in:

- `src/LiuXin_alpha/interfaces/terminal/text_browser.py`

Background job inspection is implemented in:

- `src/LiuXin_alpha/interfaces/terminal/commands/jobs.py`

Supported sync targets observed in docs/code:

- local unmanaged disk stores
- `rclone_http_readonly`
- `wget_html_readonly`

Useful sync flags:

- `--background`
- `--job-panel`
- `--job-backend process|serial`
- `--job-timeout-s <sec|none>`
- `--job-output` / `--job-no-output`
- `--extensions epub,mobi,pdf`
- `--no-refresh`
- `--no-links`
- `--json` for foreground runs only

Important sync/job behavior:

- `--json` is not supported together with `--background`
- `--job-panel` requires `--background`
- after a background sync, inspect with `jobs list` and `jobs show <job_id> --wait`

## Current local working context

Verified during this session:

- repo root: `/home/blackjane/LiuXin-alpha-wsl`
- local database used in this workspace: `/home/blackjane/scratch_library.sqlite`
- store `1` exists in that database
- store `1` details:
  - `store_name`: `Faded Page (Wget)`
  - `store_kind`: `wget_html_readonly`
  - `store_root_uri`: `https://www.fadedpage.com/`

Known working launch command:

```bash
cd /home/blackjane/LiuXin-alpha-wsl
PYTHONPATH=src python3 -m LiuXin_alpha.interfaces.terminal --database /home/blackjane/scratch_library.sqlite --ui-mode windowed
```

Known working in-app sync command for store `1`:

```text
sync store 1 to-db --background --job-panel
```

If you need a non-windowed one-liner instead:

```bash
cd /home/blackjane/LiuXin-alpha-wsl
PYTHONPATH=src python3 -m LiuXin_alpha.interfaces.terminal --database /home/blackjane/scratch_library.sqlite --command 'sync store 1 to-db --background'
```

## Active workstreams / open questions

Inferred from the current worktree and local docs:

- terminal interface work is active: `src/LiuXin_alpha/interfaces/terminal/`, `src/LiuXin_alpha/interfaces/cli/`, job handling, and the core runtime/proxy plumbing are all in motion
- storage sync/reconcile work is active: local disk, `wget_html_readonly`, `rclone_http_readonly`, squashfs, and related tests/docs/examples have all been touched
- database API / metadata add-mixin work is active: a lot of `src/LiuXin_alpha/databases/api/` and `metadata_tools/add/` is currently changing
- file-format / conversion porting is active across many calibre-derived modules and tests
- tests are being expanded heavily across `tests/interfaces/`, `tests/storage/`, `tests/core/`, `tests/library/`, and `tests/file_formats/`

Known architectural open question from local docs:

- `dev-docs/global_todo.md` currently asks whether to adopt `python-event-bus` for a unified event system

Current local worktree caution:

- branch is `main`
- the repo is very dirty right now, with a large number of modified and untracked files
- do not assume a clean baseline before making changes; inspect local changes first and keep edits narrowly scoped

Current session note:

- a windowed terminal session was launched against `/home/blackjane/scratch_library.sqlite`
- store `1` background sync was started with `sync store 1 to-db --background --job-panel`
- if follow-up work depends on sync completion, check `jobs list` and `jobs show <job_id> --wait` inside that running terminal session

## Rolling handoff notes

Use this section as a lightweight append-only log for future sessions.

Suggested format:

```text
### YYYY-MM-DD - short title
- Goal:
- Database / store context:
- Files touched:
- Commands run:
- Tests run:
- Result:
- Next step:
```

Starter entry for this session:

```text
### 2026-03-11 - terminal sync bootstrap / handoff file
- Goal: identify the correct command for syncing store 1 to the database in the background from the windowed terminal UI
- Database / store context: /home/blackjane/scratch_library.sqlite, store 1 = Faded Page (Wget), kind = wget_html_readonly
- Files touched: claude.md
- Commands run: launched windowed terminal UI via PYTHONPATH=src python3 -m LiuXin_alpha.interfaces.terminal --database /home/blackjane/scratch_library.sqlite --ui-mode windowed
- Tests run: none; only command/path validation and source inspection
- Result: confirmed the sync must be started from inside the windowed UI; in-app command used was sync store 1 to-db --background --job-panel
- Next step: monitor the submitted background job and inspect results with jobs list / jobs show <job_id> --wait
```

## Examples / smoke tests

Examples are documented in `examples/README.md`.

High-value ones:

- `examples/quickstart.sh`: local non-network smoke tour
- `examples/library_register_unmanaged_disk_example.py`
- `examples/reconcile_with_database_path_example.py`
- `examples/storage_bootstrap_report_example.py`

General pattern:

```bash
python3 examples/<script>.py --help
```

## Testing guidance

Useful test areas when changing terminal or sync behavior:

- `tests/interfaces/test_text_browser.py`
- `tests/storage/reconcile/`
- `tests/storage/api/`
- `tests/library/`

## Fast re-orientation checklist for a future session

1. Confirm the repo root is `/home/blackjane/LiuXin-alpha-wsl`.
2. Confirm whether work is against `/home/blackjane/scratch_library.sqlite` or another DB.
3. If the task is about the terminal UI, start in `src/LiuXin_alpha/interfaces/terminal/`.
4. If the task is about sync/reconcile, start in `src/LiuXin_alpha/interfaces/terminal/commands/sync.py` and `src/LiuXin_alpha/storage/reconcile/`.
5. If behavior is unclear, read `tests/interfaces/test_text_browser.py` before guessing.

## Notes for future assistants

- Do not assume an installed CLI binary exists; use `PYTHONPATH=src python3 -m ...`.
- The project docs are spread across `dev-docs`; the root README is not enough.
- The terminal UI and sync workflow are already reasonably well covered by tests.
- This repo has a lot of calibre-derived code; keep changes narrow and grounded in existing patterns.
