# Metadata Write Refresh Hooks - 2026-05-11

Branch: `metadata-write-refresh-hooks`

Depends on PR #33 / `metadata-cache-freshness-after-writes`.

## Summary

Cache freshness after writes is now available beyond the read-write web
handlers. A shared surface helper refreshes any attached metadata/read source,
core API invoke commands emit a `write.completed` event, and terminal mutating
commands call the browser write-completed hook after successful execution.

## Details

- Added `surfaces.write_refresh.refresh_metadata_read_source_after_write()`.
  It checks `read_model`, `metadata_read_source`, then `read_source`, and
  treats refresh as best-effort.
- `ReadOnlyWebApplication.refresh_metadata_read_source()` and the read-write
  web `_refresh_read_source_after_write()` now reuse that helper.
- `CoreRuntime` emits `write.completed` after successful generic `invoke`
  commands, including `target`, `method`, and `command_id` when available.
- Terminal commands now have a `mutates_data` marker. The default mutating
  commands include row set/edit/delete, link/unlink, note/on/off metadata
  attachment, new-row wizards, ingest, and sync. `TextDatabaseBrowser` refreshes
  its attached metadata read source once after each successful mutating command.

## Validation

- `python3 -m py_compile src/LiuXin_alpha/surfaces/write_refresh.py src/LiuXin_alpha/core/runtime.py src/LiuXin_alpha/surfaces/web_readonly/app.py src/LiuXin_alpha/surfaces/web_readwrite/app.py src/LiuXin_alpha/surfaces/terminal/text_browser.py src/LiuXin_alpha/surfaces/terminal/commands/base.py src/LiuXin_alpha/surfaces/terminal/commands/__init__.py tests/core/test_core_runtime_phase1.py tests/core/test_core_http_daemon_phase2.py tests/surfaces/test_text_browser.py`
- `python3 -m pytest tests/core/test_core_runtime_phase1.py::test_core_runtime_emits_command_lifecycle_events tests/surfaces/test_text_browser.py::test_text_browser_mutating_commands_refresh_attached_metadata_read_source`
- `python3 -m pytest tests/core/test_core_runtime_phase1.py tests/surfaces/test_text_browser.py::test_text_browser_set_command_updates_row_with_display_column_token tests/surfaces/test_text_browser.py::test_text_browser_set_command_routes_via_core tests/surfaces/test_text_browser.py::test_text_browser_edit_command_updates_selected_columns tests/surfaces/test_text_browser.py::test_text_browser_delete_command_with_force_removes_row tests/surfaces/test_text_browser.py::test_text_browser_delete_command_routes_via_core tests/surfaces/test_web_readwrite.py::test_web_readwrite_cache_read_source_refreshes_after_metadata_write`
- `python3 -m pytest tests/surfaces/test_text_browser.py::test_text_browser_link_links_unlink_note_and_work tests/surfaces/test_text_browser.py::test_text_browser_on_tag_creates_and_links_tag tests/surfaces/test_text_browser.py::test_text_browser_off_tag_subcommand_supports_batch_targets`
- `python3 -m pytest tests/core/test_core_http_daemon_phase2.py::test_core_http_daemon_events_next_poll_scaffold`
- `git diff --check`
