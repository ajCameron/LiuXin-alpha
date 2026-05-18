# Metadata Web Sources Hardening - 2026-05-18

Branch: `metadata-web-sources-hardening`

## Context

After the WEMI relation-container hardening PRs landed, the latest saved full
coverage artifact still showed the largest untouched metadata gaps in
`metadata.web_sources`:

- `metadata/web_sources/identify.py`: 56.5%, 231 missing lines
- `metadata/web_sources/amazon.py`: 71.6%, 140 missing lines
- `metadata/web_sources/ozon.py`: 70.5%, 126 missing lines
- `metadata/web_sources/base.py`: 71.1%, 123 missing lines
- `metadata/web_sources/overdrive.py`: 70.9%, 117 missing lines

The strategy is to treat this as offline provider hardening first: shared
identify/merge behavior, Source utilities, provider parser contracts, and fake
browser responses before any optional live-backend checks.

## First Slice

Added offline coverage for the shared web-source spine:

- `identify.Worker` failure logging and elapsed-time recording
- xISBN exception recording
- ISBN-pool merge behavior with first-edition date propagation
- ISBN-backed vs ISBN-less duplicate-source result handling
- identifier-overlap metadata merging
- plugin ignore-field handling, cover-cache failures, duplicate filtering,
  HTML comment conversion, NFC normalization, tag truncation, and undefined
  date cleanup in `identify()`
- URL generation fallback behavior for custom id rules, broken providers,
  single-url provider APIs, iterable identifiers, URI-style identifiers, and
  file/HTTP links
- Source log levels, byte logging, traceback capture, browser headers/gzip/SSL
  context behavior, option normalization, config/default API paths, author/title
  token helpers, touched-field checks, metadata cleaning, cache dump/load, and
  cover download success/failure paths

Production fix found:

- `metadata.web_sources.identify.ISBNMerge.merge()` no longer calls
  `set_identifiers(..., update=True)` on legacy `calibreMetadata`. That method
  does not accept `update`, so merged web-source identifiers were silently
  dropped. The merge now applies identifiers through `set_identifier()`.

## Validation

- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_identify.py tests/metadata/web_sources/test_web_sources_base.py -q`
  - `30 passed`
- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_identify.py tests/metadata/web_sources/test_web_sources_base.py --cov=LiuXin_alpha.metadata.web_sources.identify --cov=LiuXin_alpha.metadata.web_sources.base --cov-branch --cov-report=term-missing:skip-covered -q`
  - `30 passed`
  - `identify.py`: 84%
  - `base.py`: 87%
- `.venv/bin/python -m pytest tests/metadata/web_sources -q`
  - `177 passed, 9 skipped`

## Amazon Provider Slice

Added offline coverage for `metadata.web_sources.amazon`:

- low-level text/identifier/date/language helper edge cases
- preferred-domain fallback, save-settings touched-field refresh, Amazon URL
  parsing across US/UK/JP/BR/DE domains, and query construction for invalid,
  BR, and JP domains
- downloaded metadata cleanup for title/authors/tags and ISBN normalization
- cover-cache fallback from ISBN to cached ASIN
- JSON-LD author/description parsing, meta-title/meta-author fallbacks,
  German detail rows, cover URL variants, localized rating formats, and
  CAPTCHA detection
- ISBN search miss followed by title/author retry
- aborted identify, empty detail pages, CAPTCHA logging, detail parse exception
  logging
- uncached cover discovery through identify, abort handling, no-cover logging,
  empty payload handling, download exception handling, and text decode/abort
  backoff paths

Validation:

- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_amazon.py -q`
  - `16 passed`
- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_amazon.py --cov=LiuXin_alpha.metadata.web_sources.amazon --cov-branch --cov-report=term-missing:skip-covered -q`
  - `16 passed`
  - `amazon.py`: 91%
- `.venv/bin/python -m pytest tests/metadata/web_sources -q`
  - `184 passed, 9 skipped`

## Ozon Provider Slice

Added offline coverage for `metadata.web_sources.ozon`:

- low-level text, first-value, identifier, JSON-LD, meta-content, date, series,
  and cover URL helper edge cases
- Ozon URL parsing, invalid identifier handling, empty query handling, duplicate
  search-result ID filtering, and search result limiting
- JSON-LD fallback contracts for string publishers, meta title/description,
  image mappings, comma-string keywords, mapping/list authors, string series,
  localized language names, aggregate ratings, and ISBN normalization
- sparse detail pages with unknown title/author defaults, rating regex fallback,
  fallback ISBN extraction, already-HTML comments, tag comma escaping, and
  optional field rejection
- ISBN-search miss followed by title/author retry, duplicate search IDs, empty
  detail pages, detail request exceptions, requested-ISBN filtering, early abort,
  and empty-query returns
- uncached cover discovery through identify, abort handling, no-cover logging,
  empty payload handling, download exception handling, and text decode/abort
  backoff paths

Validation:

- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_ozon.py -q`
  - `17 passed`
- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_ozon.py --cov=LiuXin_alpha.metadata.web_sources.ozon --cov-branch --cov-report=term-missing:skip-covered -q`
  - `17 passed`
  - `ozon.py`: 93%
- `.venv/bin/python -m pytest tests/metadata/web_sources -q`
  - `191 passed, 9 skipped`

## OverDrive Provider Slice

Checked the local Calibre clone at `/home/blackjane/calibre-master`; it has
current metadata-source references for shared source behavior, Amazon, Google,
OpenLibrary, and Edelweiss, but no matching OverDrive metadata source. This
slice therefore used the local OverDrive implementation directly.

Added offline coverage for `metadata.web_sources.overdrive`:

- low-level text, first-value, identifier, tag-stripping, ISBN, date, JSON-LD,
  meta-content, search-ID extraction, series, and cover URL helper edge cases
- invalid OverDrive identifiers, URL parsing misses, empty query handling, ISBN
  query construction, ISBN-to-media cover-cache fallback, duplicate search IDs,
  and search result limiting
- JSON-LD fallback contracts for string publishers, meta title/description,
  image mappings, comma-string keywords, mapping/list authors, string series,
  language normalization, and ISBN normalization
- sparse detail pages with unknown title/author defaults, absent media IDs,
  already-HTML comments, tag comma escaping, and optional field rejection
- ISBN-search miss followed by title/author retry, duplicate search IDs, empty
  detail pages, detail request exceptions, early abort, and empty-query returns
- uncached cover discovery through identify, abort handling, no-cover logging,
  empty payload handling, download exception handling, and text decode/abort
  backoff paths

Validation:

- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_overdrive.py -q`
  - `16 passed`
- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_overdrive.py --cov=LiuXin_alpha.metadata.web_sources.overdrive --cov-branch --cov-report=term-missing:skip-covered -q`
  - `16 passed`
  - `overdrive.py`: 93%
- `.venv/bin/python -m pytest tests/metadata/web_sources -q`
  - `198 passed, 9 skipped`

## Douban Provider Slice

Checked the local Calibre clone at `/home/blackjane/calibre-master`; it does
not include a matching Douban metadata source, so this slice used the local
Douban implementation directly.

Added offline coverage for `metadata.web_sources.douban`:

- low-level text, first-value, ISBN, date, float, Douban ID, API-key, cache,
  query, JSON payload, XML payload, XML text, and payload-dispatch helpers
- JSON metadata fallbacks for alternate URL IDs, string authors, empty
  publisher/summary, string tags, clamped ratings, mixed ISBN values, missing
  covers, and cover fallback fields
- XML metadata fallbacks for atom-author names, missing authors, publisher and
  pubdate attributes, comma-containing tags, clamped ratings, default-cover
  rejection, ISBN normalization, and missing XML text
- identify fallback from subject/isbn lookup to title/author search, early
  abort, insufficient-query logging, empty parsed items, parse exceptions, and
  `None` metadata results
- uncached cover discovery through identify, abort handling, no-cover logging,
  empty payload handling, download exception handling, and text decode/abort
  backoff paths

Production fix found:

- `metadata.web_sources.douban.Douban.download_cover()` now catches cover
  download exceptions and returns without enqueueing a payload, matching the
  safer behavior of the Amazon/Ozon/OverDrive providers.

Validation:

- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_douban.py -q`
  - `14 passed`
- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_douban.py --cov=LiuXin_alpha.metadata.web_sources.douban --cov-branch --cov-report=term-missing:skip-covered -q`
  - `14 passed`
  - `douban.py`: 95%
- `.venv/bin/python -m pytest tests/metadata/web_sources -q`
  - `205 passed, 9 skipped`

## Edelweiss Provider Slice

Checked the local Calibre clone at `/home/blackjane/calibre-master` for the
Edelweiss reference implementation. The local port is more dependency-light and
keeps query search enabled, but the reference still guided the comments, cover,
tag, identifier, and rating contracts.

Added offline coverage for `metadata.web_sources.edelweiss`:

- low-level text, first-value, identifier, tag stripping, cover URL, CSV-ish
  splitting, comment sanitization, retry, and text decode helpers
- empty identifier/query handling, deterministic query timestamping, direct
  browser byte reads, malformed search payload fallback parsing, priority/title
  DOM SKU extraction, and duplicate SKU preservation order
- parser fallbacks for title, authors, tags, publisher, pubdate, rating,
  comments, Open Graph covers, sparse detail pages, invalid fields, clamped
  ratings, and cover misses
- ISBN-search miss followed by title/author retry, no-query and empty-search
  returns, early abort, duplicate/blank SKU filtering, five-result limiting,
  empty detail pages, detail parse exceptions, and abort between detail pages
- uncached cover discovery through identify, abort handling, no-cover logging,
  empty payload handling, and download exception handling

Test stabilization found:

- `test_source_tokens_field_checks_and_cleaning()` no longer relies on
  two-element `frozenset` iteration order when checking `Source.test_fields()`.
  Identifier and publisher branches are now exercised separately.

Validation:

- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_edelweiss.py -q`
  - `20 passed`
- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_edelweiss.py --cov=LiuXin_alpha.metadata.web_sources.edelweiss --cov-branch --cov-report=term-missing:skip-covered -q`
  - `20 passed`
  - `edelweiss.py`: 96%
- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_base.py::test_source_tokens_field_checks_and_cleaning -q`
  - `1 passed`
- `.venv/bin/python -m pytest tests/metadata/web_sources -q`
  - `216 passed, 9 skipped`

## Next

Continue with provider modules using offline fake browser responses and compact
HTML/JSON fixtures. Highest old-coverage payoff is probably `amazon.py`,
`ozon.py`, `overdrive.py`, `douban.py`, `edelweiss.py`, and `isbndb.py`, with
`identify.py` branch gaps revisited only where provider tests naturally hit
them. After the Amazon, Ozon, OverDrive, Douban, and Edelweiss slices, the next
provider target should be `isbndb.py`.
