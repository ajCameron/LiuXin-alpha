# Windowed Job Pane Scrollback

Date: 2026-03-15

Scope:
- Added job-pane scrollback/focus parity in the windowed terminal UI without changing terminal command semantics.

Behavior:
- `PgUp` / `PgDn` / `Home` / `End` now operate on the focused scroll target.
- Default scroll target remains the console pane.
- `F6` toggles scroll focus between the console pane and the job output pane when a job panel is attached.
- Job-pane scrollback preserves the current view when new log lines arrive, matching the existing console scrollback behavior.
- Status lines now surface:
  - active scroll focus
  - console scrollback offset
  - job scrollback offset

Implementation:
- Added job-pane scroll state and focus state to the curses UI driver in `src/LiuXin_alpha/surfaces/terminal/windowed_ui.py`.
- Split job output building into:
  - full content generation
  - wrapped line handling
  - visible line selection
- Reused the console scroll/clamp pattern for the job pane instead of adding a separate ad hoc render path.

Validation:
- `pytest -q tests/surfaces/test_windowed_ui.py -k 'job_output or scrollback or focus'`
  - `8 passed`
- `pytest -q tests/surfaces/test_windowed_ui.py`
  - `17 passed`

Notes:
- This slice is intentionally UI-local. It does not change the core API surface while the core review is underway.
