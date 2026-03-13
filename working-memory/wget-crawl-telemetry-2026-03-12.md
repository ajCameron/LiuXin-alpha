# Wget Crawl Telemetry - 2026-03-12

## Scope

Added crawler-observation counters for `wget_html_readonly` sync so progress can move even when the database is not yet inserting many `files` rows.

This was aimed at the Faded Page case where the job was visibly active but file growth looked stalled.

## Added Counters

`UnmanagedDiskRegistrationReport` now tracks:

- `crawler_urls_observed`
- `crawler_html_seen`
- `crawler_book_like_found`
- `crawler_html_rejected`
- `crawler_rejection_counts`

These are unique observed URLs from the crawler path, not just inserted rows.

## Surfacing

The counters are now visible in:

- foreground `sync store ...` progress lines
- background job log output
- final `sync` completion summary under a new `Crawler` section

The `wget` backend now reports accept/reject decisions for observed URLs so reconcile can classify them without inserting everything into `files`.

## Metric Meaning

- `crawler_book_like_found` means the observed URL looked like a book asset by extension, even if it was later rejected for scope
- `crawler_html_rejected` counts observed HTML-like URLs that were not accepted by the crawler filter
- `crawler_rejection_counts` currently reflects reasons like `not_file_like` and `out_of_scope`

## Practical Rate-Limit Note

Current default for the `wget` backend is `1200` requests/hour, which is one request every `3` seconds.

For whole-site Faded Page crawls from `https://www.fadedpage.com/`, the bigger problem is scope, not only rate. Raising the limit helps only linearly if the spider is still walking a lot of non-book pages.

Practical guidance:

- `1200/hr`: very polite, but slow for whole-site discovery
- `1800/hr`: safer next step, about one request every `2` seconds
- `3600/hr`: reasonable upper bound for interactive testing, about one request every `1` second

Going above that is probably the wrong lever unless the crawl scope gets narrower first.

## Validation

Passed:

- `pytest -q tests/storage/store_backend_plugins/wget_html_readonly/test_wget_html_readonly_storage_backend.py tests/storage/reconcile/test_wget_html_store_db_sync.py -k 'observed_url_decisions or tracks_crawler_observation_counts or crawl_filters_scope_and_non_file_urls or incremental_writes_during_crawl or non_incremental_defers_writes'`
- `pytest -q tests/interfaces/test_text_browser.py -k 'wget_surfaces_crawler_observation_summary or sync_store_wget_uses_rate_limit_option or sync_store_wget_kind_takes_precedence_over_https_protocol or sync_store_wget_listing_flags_are_forwarded or sync_store_wget_no_verbose_flag_is_forwarded or sync_store_wget_timeout_option_is_forwarded'`
