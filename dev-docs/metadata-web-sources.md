# Metadata Web Sources

This document captures the local development contract for metadata web-source
plugins under `src/LiuXin_alpha/metadata/web_sources`.

## Scope

Web sources provide two kinds of data:

- `identify`: title, authors, identifiers, dates, publisher, comments, tags,
  language, ratings, and source-specific enrichment.
- `cover`: cached or discovered cover image URLs and downloaded image payloads.

The source layer is intentionally dependency-light. Provider modules should use
the shared `Source` base class and `http_client` retry/backoff helpers where
possible, and they should fail as source misses instead of taking down the whole
identify/cover pipeline.

## Current Sources

The current source set includes:

- Amazon
- Big Book Search
- Douban
- Edelweiss
- Google Books
- Google Images
- Internet Archive
- ISBNDB
- KDL
- Library of Congress
- LibraryThing
- Open Library
- OverDrive
- Ozon
- Wikidata
- xISBN

Calibre-derived first-party sources are covered by Amazon, Google Books, Google
Images, Open Library, Big Book Search, and Edelweiss. LiuXin also carries
additional provider-specific sources. Local dataset-backed sources are covered
in [Metadata local sources](metadata-local-sources.md).

## Provider Contract

Each source should prefer deterministic parser tests with compact inline HTML or
JSON fixtures over live-network assertions. Tests should cover:

- query construction from title/authors/identifiers
- provider-specific URL and identifier parsing
- sparse/alternate payload shapes
- ISBN and source-identifier normalization
- cover URL extraction and cache population
- abort handling
- retry/backoff logging
- guarded backend behavior such as `403`, `429`, CAPTCHA, Cloudflare, empty
  payloads, and malformed responses

Live tests are optional probes. They must be gated by
`LIUXIN_RUN_LIVE_WEB_TESTS=1`, and they should skip with diagnostics when a
backend blocks or flakes in a predictable way.

## Library Of Congress

`LibraryOfCongress` uses the official `https://www.loc.gov/books/?fo=json`
search endpoint and `https://www.loc.gov/item/<id>/?fo=json` item endpoint.
It currently supports:

- `identify` and `cover`
- `loc`, `lccn`, `isbn`, and `oclc` identifiers
- title, authors, comments, publisher, publication date, language, and tags
- cover URL extraction from `image_url` and nested `resources`
- identifier-to-cover and ISBN-to-LoC cache population

The LoC endpoint can return a Cloudflare browser challenge or `HTTP 403` from
some environments. The source therefore treats these responses as provider
misses, logs diagnostics, and lets the broader identify run continue.

The current live probe confirms this behavior: under the present environment,
LoC returns `HTTP 403`/Cloudflare and the test skips cleanly rather than
failing.

## Internet Archive

`InternetArchive` uses the official advanced search endpoint,
`https://archive.org/advancedsearch.php?output=json`, for discovery and
`https://archive.org/metadata/<identifier>` for direct item metadata lookup.
It currently supports:

- `identify` and `cover`
- `internet_archive`, `isbn`, `lccn`, `oclc`, and `openlibrary` identifiers
- title, authors, comments, publisher, publication date, language, and tags
- cover URL extraction from metadata thumbnail files, with
  `https://archive.org/services/img/<identifier>` as the fallback thumbnail URL
- identifier-to-cover and ISBN-to-Internet-Archive cache population

This source should be treated as an enrichment and cover fallback for digitized
or archived text items. It complements Open Library but does not replace
edition-level catalog sources.

## Wikidata

`Wikidata` uses the public Wikidata Action API,
`https://www.wikidata.org/w/api.php`, for entity search and direct entity
lookup. It uses the Wikidata Query Service endpoint,
`https://query.wikidata.org/sparql`, only for narrow ISBN lookups by exact
`P212`/`P957` values.

It currently supports:

- `identify`
- `wikidata`, `qid`, `wd`, `isbn`, `lccn`, and `oclc` identifiers
- title, authors, comments, publisher, publication date, language, and tags
- ISBN-to-Wikidata cache population
- direct Wikidata item URLs such as `https://www.wikidata.org/wiki/Q15228`

This source is intentionally conservative enrichment. It does not expose
`cover`: Wikidata image statements are often representative images rather than
edition covers.

## Google Images

Google Images has a static scrape path plus a rendered Chrome/Edge fallback.
The rendered fallback is optional and depends on an available browser binary.
It auto-detects common Linux browser names and Windows Chrome/Edge under WSL,
with `LIUXIN_GOOGLE_IMAGES_BROWSER` available as an override.

Static Google Images responses are often guard pages. The rendered fallback is
currently the functional path for live image discovery.

## Validation

For local web-source work, run:

```bash
python3 -m pytest tests/metadata/web_sources -q
```

For a single live probe, use:

```bash
LIUXIN_RUN_LIVE_WEB_TESTS=1 python3 -m pytest tests/metadata/web_sources/test_web_sources_live_backends.py::<test_name> -q -s
```

Live probes are diagnostic, not release blockers, unless a specific backend is
being hardened under controlled network conditions.
