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

## ISBNDB Provider Slice

Checked the local Calibre clone at `/home/blackjane/calibre-master`; it does
not include a matching ISBNDB metadata source, so this slice used the local
ISBNDB implementation directly.

Added offline coverage for `metadata.web_sources.isbndb`:

- low-level text, first-value, ISBN, date, author-list, legacy XML, API-key,
  retry, header, and text decode helpers
- empty-key/query handling, invalid ISBN handling, ISBN and title/author query
  construction, JSON API headers, direct browser byte reads, and environment
  key precedence
- JSON payload fallbacks for list, `book`, `books`, and `data` shapes, malformed
  JSON, scalar payloads, mixed record lists, and unknown payload modes
- metadata fallbacks for unknown title/authors, comma-string authors,
  synopsis/summary/overview comments, `published_date`, language codes,
  alternate ISBN keys, `isbns` lists, sparse records, and audio-publisher
  rejection
- identify paths for early abort, missing configuration, insufficient metadata,
  query exceptions, legacy fallback, ISBN miss retry through title/author,
  abort between query attempts, duplicate metadata suppression, and clean result
  enqueueing

Validation:

- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_isbndb.py -q`
  - `20 passed`
- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_isbndb.py --cov=LiuXin_alpha.metadata.web_sources.isbndb --cov-branch --cov-report=term-missing:skip-covered -q`
  - `20 passed`
  - `isbndb.py`: 95%
- `.venv/bin/python -m pytest tests/metadata/web_sources -q`
  - `227 passed, 9 skipped`

## OpenLibrary Provider Slice

Checked the local Calibre clone at `/home/blackjane/calibre-master`; it includes
a matching cover-only OpenLibrary source. The local port intentionally adds ISBN
normalization and iterable/mapping identifier support around the same cover API
contract.

Added offline coverage for `metadata.web_sources.openlibrary`:

- low-level first-value, text coercion, ISBN normalization, and identifier ISBN
  extraction helper edge cases
- malformed, empty, byte, mapping, iterable, ISBN-10, ISBN-13, and missing
  identifier inputs
- no-ISBN book URL and cached-cover URL behavior

Validation:

- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_openlibrary.py -q`
  - `12 passed`
- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_openlibrary.py --cov=LiuXin_alpha.metadata.web_sources.openlibrary --cov-branch --cov-report=term-missing:skip-covered -q`
  - `12 passed`
  - `openlibrary.py`: 99%
- `.venv/bin/python -m pytest tests/metadata/web_sources -q`
  - `228 passed, 9 skipped`

## Big Book Search Provider Slice

Checked the local Calibre clone at `/home/blackjane/calibre-master`; it includes
a matching Big Book Search cover source. The local port intentionally adds a
fallback search path, retry/backoff plumbing, dependency-light image extraction,
and URL normalization.

Added offline coverage for `metadata.web_sources.big_book_search`:

- low-level text, image URL normalization, image extraction, query building, and
  search URL helper edge cases
- byte HTML payloads, empty HTML, relative/non-root image rejection, duplicate
  image filtering, empty-query returns, empty response fallback, and no-image
  fallback across both search paths
- retry policy, backoff, abort-aware wait helpers, token logging, browser
  timeout propagation, no-token image lookups, and no-title download noops

Validation:

- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_big_book_search.py -q`
  - `10 passed`
- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_big_book_search.py --cov=LiuXin_alpha.metadata.web_sources.big_book_search --cov-branch --cov-report=term-missing:skip-covered -q`
  - `10 passed`
  - `big_book_search.py`: 100%
- `.venv/bin/python -m pytest tests/metadata/web_sources -q`
  - `232 passed, 9 skipped`

## Google Provider Slice

Checked the local Calibre clone at `/home/blackjane/calibre-master`; it includes
the older Atom-feed Google Books provider. The local port uses the Google Books
Volumes JSON API and preserves the same key contracts: Google identifiers,
ISBN/cover caching, comment paragraph cleanup, metadata normalization, and
dummy-cover rejection.

Added offline coverage for `metadata.web_sources.google`:

- low-level text, first-value, identifier, ISBN, Google URL, comment formatting,
  API parameter, API URL, JSON request, retry, backoff, and abort-aware wait
  helpers
- query construction for ISBN, title-only, author-only, insufficient metadata,
  invalid-first ISBN fallback, Google API key injection, path quoting, and URL
  parse failures
- metadata fallbacks for unknown title/authors, string authors, sparse volume
  records, mapping/list/non-mapping industry identifiers, invalid ISBNs,
  custom identifiers, invalid dates, string categories, non-mapping image links,
  cover priority, missing covers, comments, language, publisher, and subtitle
  absence
- postprocessing for `None`, Google-ID-free metadata, comments, source
  relevance, ISBN-to-Google cache population, and cover cache population
- identify paths for early abort, direct Google-ID lookup, missing payloads,
  insufficient query data, JSON-query miss, ISBN miss retry with and without
  fallback queries, retry payload `None`, parse exceptions, abort between
  result items, and postprocess returning `None`
- cover-download paths for cache hits, uncached identify discovery, identify
  abort, no-cover logging, multi-result cover scanning, pre-download abort,
  dummy image rejection, empty payloads, download exceptions, existing `zoom=`
  URLs, and successful fallback zoom downloads

Validation:

- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_google.py -q`
  - `25 passed`
- `.venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_google.py --cov=LiuXin_alpha.metadata.web_sources.google --cov-branch --cov-report=term-missing:skip-covered -q`
  - `25 passed`
  - `google.py`: 99%
- `.venv/bin/python -m pytest tests/metadata/web_sources -q`
  - `244 passed, 9 skipped`

## Google Images Rendered Fallback Slice

After live probes showed static Google Images responses were returning guarded
or empty pages, added an optional rendered-browser fallback to
`metadata.web_sources.google_images`.

Implemented behavior:

- static scrape variants still run first
- guarded/no-result static responses fall back to rendered Google search pages
- Chrome/Edge browser detection supports Linux browser names and Windows
  Chrome/Edge under WSL
- `LIUXIN_GOOGLE_IMAGES_BROWSER` can override the browser path
- browser profiles are created under `.tmp` and ignored by git
- WSL Windows browser launches use Windows-accessible profile paths
- rendered DOM parsing accepts Google thumbnail URLs such as
  `encrypted-tbn*.gstatic.com/images?...`
- static `asearch=ichunk` was removed after repeated `404` responses
- diagnostics now mark Google guard/challenge signatures, including
  `enablejs`, `SG_REL`, `SG_SS`, and thumbnail URL counts

Validation:

- `python3 -m pytest tests/metadata/web_sources/test_web_sources_google_images.py -q`
  - `17 passed`
- `python3 -m pytest tests/metadata/web_sources -q`
  - `269 passed, 9 skipped`
- `LIUXIN_RUN_LIVE_WEB_TESTS=1 .venv/bin/python -m pytest tests/metadata/web_sources/test_web_sources_live_backends.py::test_live_google_images_search_and_download -q -s`
  - `1 passed`
- direct smoke for `The Hobbit` / `J. R. R. Tolkien`
  - rendered fallback returned `235` candidate image URLs

Operational note:

- The functional live path currently depends on a locally available Chrome/Edge
  binary. Static Google scraping remains guarded in this environment.

CI hardening follow-up:

- PR: `#63` (`Harden Google Images browser profile path test`)
- merge commit: `118bf0c6c0ce60ecd9890ef028d939d8098725ae`
- fixed a non-WSL Linux CI assertion in
  `test_google_images_windows_browser_profile_uses_windows_accessible_root`
- the test still covers `/mnt/c/...` to `C:\...` conversion directly, but only
  expects the full Windows profile path when the generated profile path is
  actually Windows-shaped
- GitHub Actions failure signature before the fix:
  `/tmp/pytest-of-runner/.../.tmp/google-images-rendered-browser/...`
  did not start with `C:\`

Validation:

- `python3 -m pytest tests/metadata/web_sources/test_web_sources_google_images.py::test_google_images_windows_browser_profile_uses_windows_accessible_root -q`
  - `1 passed`
- `python3 -m pytest tests/metadata/web_sources/test_web_sources_google_images.py -q`
  - `17 passed`
- `python3 -m pytest tests/metadata/web_sources -q`
  - `296 passed, 11 skipped`
- `git diff --check`
  - clean

## Library Of Congress Source Slice

Branch: `web-source-library-of-congress`

Added a new `metadata.web_sources.library_of_congress` provider and registered
it in `KNOWN_WEB_SOURCE_MODULES`.

Implemented behavior:

- `LibraryOfCongress` supports `identify` and `cover`
- search endpoint: `https://www.loc.gov/books/?fo=json`
- item endpoint: `https://www.loc.gov/item/<id>/?fo=json`
- identifiers: `loc`, `lccn`, `isbn`, and `oclc`
- fields: title, authors, comments, publisher, pubdate, language, tags, and
  cover URLs
- cover URLs are extracted from top-level `image_url` and nested `resources`
- successful metadata postprocessing populates ISBN-to-LoC and LoC-to-cover
  caches
- guarded/blocked responses are logged and treated as source misses instead of
  failing the full identify run
- live probe is gated behind `LIUXIN_RUN_LIVE_WEB_TESTS=1`

Validation:

- `python3 -m pytest tests/metadata/web_sources/test_web_sources_library_of_congress.py -q`
  - `12 passed`
- `python3 -m pytest tests/metadata/web_sources -q`
  - `281 passed, 10 skipped`
- `LIUXIN_RUN_LIVE_WEB_TESTS=1 python3 -m pytest tests/metadata/web_sources/test_web_sources_live_backends.py::test_live_library_of_congress_identify -q -s`
  - `1 skipped`
  - current environment receives `HTTP 403` from Cloudflare for the LoC JSON
    endpoint
- rendered Chrome check against the LoC JSON URL also returned the Cloudflare
  challenge page, so this first slice intentionally stops at static API parser
  and clean guarded-backend handling
- `git diff --check`
  - clean

## Internet Archive Source Slice

Branch: `web-source-internet-archive`

Added a new `metadata.web_sources.internet_archive` provider and registered it
in `KNOWN_WEB_SOURCE_MODULES`.

Implemented behavior:

- `InternetArchive` supports `identify` and `cover`
- discovery endpoint: `https://archive.org/advancedsearch.php?output=json`
- direct metadata endpoint: `https://archive.org/metadata/<identifier>`
- identifiers: `internet_archive`, `isbn`, `lccn`, `oclc`, and `openlibrary`
- aliases accepted for incoming archive IDs: `internet_archive`, `ia`,
  `archive`, `archive_org`, and `ocaid`
- fields: title, authors, comments, publisher, pubdate, language, tags, and
  cover URLs
- cover URLs prefer metadata thumbnail files such as `__ia_thumb.jpg`, falling
  back to `https://archive.org/services/img/<identifier>`
- successful metadata postprocessing populates ISBN-to-Internet-Archive and
  Internet-Archive-to-cover caches
- request failures are logged and treated as source misses instead of failing
  the full identify run
- live probe is gated behind `LIUXIN_RUN_LIVE_WEB_TESTS=1`

Validation:

- `python3 -m pytest tests/metadata/web_sources/test_web_sources_internet_archive.py -q`
  - `15 passed`
- `python3 -m pytest tests/metadata/web_sources -q`
  - `296 passed, 11 skipped`
- `scripts/run_live_web_sources.sh`
  - log: `working-memory/test-results/live-web-sources-2026-05-20-020101.log`
  - `6 passed, 5 skipped, 1 xfailed`
  - Internet Archive identify + cover passed live; it was the slowest backend
    in this run at `93.73s`
  - skips were expected backend/environment outcomes: LoC `403`, Big Book
    Search DNS failure, Douban `400`/`403`, OverDrive captcha/zero results,
    and Ozon redirect-loop signature
- `git diff --check`
  - clean

Durable doc:

- `docs/development/metadata-web-sources.md`

## Wikidata Source Slice

Branch: `web-source-wikidata`

Added a new `metadata.web_sources.wikidata` provider and registered it in
`KNOWN_WEB_SOURCE_MODULES`.

Implemented behavior:

- `Wikidata` supports `identify`
- Action API endpoint: `https://www.wikidata.org/w/api.php`
- narrow ISBN lookup endpoint: `https://query.wikidata.org/sparql`
- identifiers: `wikidata`, `qid`, `wd`, `isbn`, `lccn`, and `oclc`
- direct Wikidata item URLs such as `https://www.wikidata.org/wiki/Q15228`
- fields: title, authors, comments, publisher, pubdate, language, tags, and
  source identifiers
- exact ISBN lookup uses `P212`/`P957` via WDQS, while text discovery uses
  `wbsearchentities` and direct ID lookup uses `wbgetentities`
- search results are filtered to bookish entities before metadata emission
- successful metadata postprocessing populates ISBN-to-Wikidata caches
- no `cover` capability: Wikidata image statements are treated as unsafe for
  edition-cover use
- live probe is gated behind `LIUXIN_RUN_LIVE_WEB_TESTS=1`

Validation:

- `python3 -m pytest tests/metadata/web_sources/test_web_sources_wikidata.py -q`
  - `11 passed`
- `python3 -m pytest tests/metadata/web_sources -q`
  - `307 passed, 12 skipped`
- `git diff --check`
  - clean

## Next

The web-source unit suite is now broad enough that new provider work should be
chosen for user value rather than old coverage numbers alone.

Good next options:

- Split the Internet Archive live probe timing into identify and cover phases so
  future slow runs show which request path is dragging.
- A local/imported ISFDB-backed source for speculative-fiction quality, likely
  more robust than scraping the live site.
- Review the `5 XPASS` tests from the latest full coverage run and either remove
  stale xfails or record why they remain expected.
- Reduce warning noise clustered in `SQL/databasedriver/search_mixin.py`,
  `utils/date.py`, and multiprocessing fork warnings.
