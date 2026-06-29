# Metadata Cache Freshness After Writes - 2026-05-11

Branch: `metadata-cache-freshness-after-writes`

## Summary

Metadata read sources now expose a refresh hook so cache-backed surfaces can
reload their storage cache after successful writes. The read-write web surface
uses that hook after row creates, edits, deletes, file uploads, and interlink
add/create/edit/delete operations, appending `Read cache refreshed.` to the
notice only when a cache-backed source actually reloads.

## Details

- `DatabaseMetadataReadSource.refresh()` returns `False`; database-backed reads
  need no refresh.
- `CacheMetadataReadSource.refresh()` calls the wrapped cache `reload()` when
  available, falling back to `read()`, then checks readiness again.
- `ReadModelBackend.refresh_read_source()` and
  `ReadOnlyWebApplication.refresh_metadata_read_source()` provide the surface
  hook used by read-write handlers.
- The regression test builds `ReadWriteWebApplication` with
  `metadata_read_source="cache"` and database fallback disabled, writes a tag
  link, and proves the app read model sees the linked tag from the refreshed
  cache.

## Validation

- `python3 -m py_compile src/LiuXin_alpha/metadata/read_sources.py src/LiuXin_alpha/surfaces/read_model/api.py src/LiuXin_alpha/surfaces/web_readonly/app.py src/LiuXin_alpha/surfaces/web_readwrite/app.py tests/surfaces/test_web_readwrite.py`
- `python3 -m pytest tests/surfaces/test_web_readwrite.py::test_web_readwrite_cache_read_source_refreshes_after_metadata_write`
- `python3 -m pytest tests/surfaces/test_web_readwrite.py::test_web_readwrite_work_tag_links_use_metadata_write_reports tests/surfaces/test_web_readwrite.py::test_web_readwrite_work_pages_can_create_and_link_new_targets tests/surfaces/test_web_readwrite.py::test_web_readwrite_work_tag_create_uses_metadata_write_reports tests/surfaces/test_web_readwrite.py::test_web_readwrite_row_pages_can_add_edit_and_remove_interlinks`
- `python3 -m pytest tests/surfaces/test_read_model_api.py::test_read_model_can_use_cache_read_source_without_database_fallback`
- `git diff --check`
