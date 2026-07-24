# Metadata Local Sources

This document captures the local development contract for metadata sources
under `src/LiuXin_alpha/metadata/local_sources`.

## Scope

Local sources provide metadata from configured local datasets. They expose the
same source interface as web sources where possible, but they should not be
registered as web-source modules or depend on live network scraping.

Local sources should:

- fail as source misses when their dataset is unavailable or malformed
- keep configuration explicit through environment variables or source
  preferences
- prefer deterministic fixture-backed tests
- reuse the shared `Source` base class until the broader metadata source
  framework is split into web/local/common layers

## Current Sources

The current local source set includes:

- ISFDB

## ISFDB

`ISFDB` reads a local LiuXin ISFDB import database instead of scraping the live
site. Configure it with `LIUXIN_ISFDB_TEST_DB`, `LIUXIN_ISFDB_DB`, or the
source preferences `database_path`, `data_root`, and `bundle_name`.

It currently supports:

- `identify`
- `isfdb`, `isfdb_title`, `isfdb_pub`, `isbn`, and `asin` identifiers
- direct ISFDB title/publication URLs such as
  `https://www.isfdb.org/cgi-bin/title.cgi?1272`
- title, authors, comments, publisher, publication date, language, tags,
  series, rating, and source identifiers
- ISBN-to-ISFDB cache population

This source is intentionally identify-only. The imported ISFDB metadata is
valuable for speculative-fiction bibliographic enrichment, while cover support
needs a separate pass over the imported image tables before it should be trusted
as edition cover data.

## Validation

For local source work, run:

```bash
python3 -m pytest tests/metadata/local_sources -q
```
