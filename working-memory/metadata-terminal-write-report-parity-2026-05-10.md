# Metadata Terminal Write Report Parity - 2026-05-10

Branch: `metadata-terminal-write-report-parity`

Base: `main` after PR #30 and PR #31 were merged and pulled locally.

## Scope

Terminal metadata-specific `on` flows now share the same WEMI metadata
writer/report bridge used by the web read-write surface. Generic terminal links
continue to use direct database interlinks.

## Changes

- Updated `src/LiuXin_alpha/surfaces/terminal/commands/on.py` so
  `_link_one_value()` tries `write_wemi_metadata_relation_link()` before the
  direct `Database.interlink_rows()` path.
- Supported WEMI metadata relations now emit terminal output that keeps the
  old prefix (`Tag linked:`, `Genre linked:`, etc.) and appends
  `metadata writer; metadata report: ...` counts.
- Direct generic links, such as languages, remain on the direct database path
  and do not emit metadata reports.
- Preserved bulk rollback and best-effort behavior by resolving the generated
  interlink row after metadata-writer writes.
- Added/updated tests proving tag writes report metadata writer output and
  language links remain direct.

## Validation

```bash
python3 -m py_compile \
  src/LiuXin_alpha/surfaces/terminal/commands/on.py \
  tests/surfaces/test_text_browser.py

python3 -m pytest -q \
  tests/surfaces/test_text_browser.py::test_text_browser_on_tag_creates_and_links_tag \
  tests/surfaces/test_text_browser.py::test_text_browser_on_tag_bulk_atomic_rollback_on_error \
  tests/surfaces/test_text_browser.py::test_text_browser_on_tag_bulk_best_effort_keeps_successes \
  tests/surfaces/test_text_browser.py::test_text_browser_on_language_subcommand_style

python3 -m pytest -q tests/surfaces/test_text_browser.py \
  -k "on_note or on_tag or off_tag or on_language or on_series or on_genre or on_subject"

git diff --check
```

Results:
- focused terminal write-report tests: `8 passed`
- widened terminal metadata attach/detach slice: `36 passed, 307 deselected`
- `py_compile`: passed
- `git diff --check`: passed

## Follow-Ups

- Terminal `off` still unlinks directly. A true metadata report for removal
  likely needs either a dedicated remove bridge or a replace-mode writer helper
  with careful rollback semantics.
- Cache freshness after terminal metadata writes is represented by the metadata
  writer's dirty-record marking; a later pass should pin cache reload/invalidate
  behavior explicitly.
