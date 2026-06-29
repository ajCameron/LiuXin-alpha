# Terminal Mutations - 2026-03-11

## Scope

Added the first generic row-mutation commands to the terminal interface:

- `set <table> <id> <column> <value...>`
- `set <table>:<id> <column> <value...>`
- `edit <table> <id> [column ...]`
- `edit <table>:<id> [column ...]`
- `delete <table> <id> [--force]`
- `delete <table>:<id> [--force]`

Aliases:

- `set` also answers to `update`
- `delete` also answers to `remove`

## Behavior

- `set` and `edit` accept both raw schema column names and the shortened column labels shown in the terminal UI.
- `edit` is a line-by-line wizard:
  - `Enter` keeps the current value
  - `null` clears the field
  - optional trailing columns restrict the wizard to a curated subset
- `delete` now performs a preflight impact query before removal.
- single-line row previews are now centralized in `TextDatabaseBrowser` and reused across delete previews, `links`, `show all`, and browse-style row listings.

## Delete Preflight

`delete` now shows:

- the target row summary
- interlinked row counts, when present
- direct reference counts for columns like `file_store_id`, `folder_store_id`, `book_work_id`, etc.
- capped sample affected rows under each section, rendered with the normal terminal row formatter
- those sample rows are now humanized for common tables, e.g. `#1 | Fish`, `#1 | root | delete-cancel-root`, `#1 | Delete Preview Work`
- a warning when dependents exist: delete may fail or cascade depending on schema constraints

`--force` skips the confirmation prompt but still prints the preview.

## Core Boundary

These terminal mutations now go through `library` methods when core is available:

- `library.get_row(...)`
- `library.update_row_fields(...)`
- `library.describe_row_delete_impact(...)`
- `library.delete_row(...)`

This keeps the terminal aligned with the in-process core runtime now, and remote/RPC later.

## Validation

Relevant test slices that passed during this session:

- `pytest -q tests/surfaces/test_text_browser.py -k 'set_command or edit_command or delete_command or command_completion_candidates'`
- `pytest -q tests/core/test_core_runtime_phase1.py`

## Follow-up

Likely next interface step:

- add a safer `delete` preview for interlinked rows by showing a capped sample of affected row labels/titles, not just counts
