# Terminal Formatting - 2026-03-12

## Scope

Standardized the main terminal formatting surfaces around shared section/table renderers in `TextDatabaseBrowser`.

Also standardized the curses/windowed UI status board and job output panel onto compact section-based renderers so the split-pane UI no longer uses a separate ad hoc `field: value` style.

## Shared Renderers

Added shared browser helpers for:

- grouped single-row detail rendering
- generic titled detail sections
- reuse of the same ASCII table style across detail/report outputs

Primary implementation lives in:

- `src/LiuXin_alpha/interfaces/terminal/text_browser.py`

## Commands Migrated

Detail and report surfaces now use the shared section renderer:

- `row`
- `store show`
- `jobs show`
- `summary`
- `ingest disk`
- `sync store ...`

Windowed UI surfaces now use compact section renderers:

- status board
- job output panel

Wizard summary screens now use the same section renderer:

- `new creator`
- `new work`
- `new expression`
- `new manifestation`
- `new item`
- `new tag`
- `new genre`
- `new subject`
- `new series`
- `new organisation`
- `new publisher`
- `new title`

The interactive database creation wizard summary was also moved onto the same table style.

## Output Shape

- many-row views remain horizontal tables
- single-row inspection remains grouped vertical tables
- detail/report commands now render titled two-column sections instead of ad hoc `field: value` line blocks
- create-wizard summaries now match the same section/table style before confirmation
- windowed panes use the same section model, but rendered compactly to preserve space in curses layouts

## Validation

Passed slices:

- `pytest -q tests/interfaces/test_text_browser.py -k 'summary or jobs_commands or ingest_disk_registers_ebook_files or sync_store_registers_ebook_files or sync_store_compact_subcommand_ref or sync_store_background_submits_job or store_view or row_command_accepts_compact_table_id'`
- `pytest -q tests/interfaces/test_text_browser.py -k 'new_creator or new_work or new_expression or new_manifestation or new_item or new_tag or new_genre or new_subject or new_series or new_organisation or new_publisher or new_title'`
- `pytest -q tests/interfaces/test_text_browser.py -k 'main_create_new_db_wizard or jobs_commands or jobs_panel_command_attach_and_detach or sync_store_background_job_panel_attaches'`
- `pytest -q tests/interfaces/test_windowed_ui.py`

Also passed syntax checks with `python3 -m py_compile` over the touched terminal files.
