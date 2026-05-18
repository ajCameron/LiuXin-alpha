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

## Next

Continue with provider modules using offline fake browser responses and compact
HTML/JSON fixtures. Highest old-coverage payoff is probably `amazon.py`,
`ozon.py`, `overdrive.py`, `douban.py`, `edelweiss.py`, and `isbndb.py`, with
`identify.py` branch gaps revisited only where provider tests naturally hit
them. After the Amazon slice, the next provider target should be `ozon.py` or
`overdrive.py`.
