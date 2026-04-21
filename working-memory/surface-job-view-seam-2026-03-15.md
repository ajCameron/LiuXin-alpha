# Surface Job View Seam

Date: 2026-03-15

Scope:
- Surface-only slice to reduce duplication between `jobs` commands and the windowed job output pane while core review is in progress.

What changed:
- Added shared terminal job snapshot/log helpers in `src/LiuXin_alpha/surfaces/terminal/job_view.py`.
- Moved surface-side job fetch/log resolution onto that helper in:
  - `src/LiuXin_alpha/surfaces/terminal/commands/jobs.py`
  - `src/LiuXin_alpha/surfaces/terminal/windowed_ui.py`
- Added `jobs tail <job_id> [lines] [--wait[=<sec|none>]]` as the textual counterpart to the windowed job output pane.

Why:
- The terminal previously had two separate surface paths that both understood:
  - how to fetch one job
  - how to resolve `log_path`
  - how to interpret missing / empty / unreadable logs
- This seam is the surface contract the future named core RPCs should satisfy, without changing `core/` during review.

Validation:
- `pytest -q tests/surfaces/test_text_browser.py tests/surfaces/test_windowed_ui.py -k 'jobs_tail or jobs_group_lists_subcommands or jobs_list_and_show or jobs_commands_route_via_core_when_available or jobs_panel_command_attach_and_detach or job_output or scrollback or focus'`
  - `13 passed`

Boundary:
- This slice stayed in `surfaces` plus tests and working-memory only.
