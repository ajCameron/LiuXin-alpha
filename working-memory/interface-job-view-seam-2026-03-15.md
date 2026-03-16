# Interface Job View Seam

Date: 2026-03-15

Scope:
- Interface-only slice to reduce duplication between `jobs` commands and the windowed job output pane while core review is in progress.

What changed:
- Added shared terminal job snapshot/log helpers in `src/LiuXin_alpha/interfaces/terminal/job_view.py`.
- Moved interface-side job fetch/log resolution onto that helper in:
  - `src/LiuXin_alpha/interfaces/terminal/commands/jobs.py`
  - `src/LiuXin_alpha/interfaces/terminal/windowed_ui.py`
- Added `jobs tail <job_id> [lines] [--wait[=<sec|none>]]` as the textual counterpart to the windowed job output pane.

Why:
- The terminal previously had two separate interface paths that both understood:
  - how to fetch one job
  - how to resolve `log_path`
  - how to interpret missing / empty / unreadable logs
- This seam is the interface contract the future named core RPCs should satisfy, without changing `core/` during review.

Validation:
- `pytest -q tests/interfaces/test_text_browser.py tests/interfaces/test_windowed_ui.py -k 'jobs_tail or jobs_group_lists_subcommands or jobs_list_and_show or jobs_commands_route_via_core_when_available or jobs_panel_command_attach_and_detach or job_output or scrollback or focus'`
  - `13 passed`

Boundary:
- This slice stayed in `interfaces` plus tests and working-memory only.
