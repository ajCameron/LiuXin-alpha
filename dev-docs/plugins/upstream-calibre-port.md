# Porting calibre's metadata readers

Written **2026-11-18**, at calibre 9.18.

`LiuXin_alpha.metadata.upstream_calibre` holds copies of current calibre's
metadata readers and the calibre code they use. They run in process, without
calibre installed, through the providers in
`file_formats.providers.ported_calibre` (priority −50, ahead of running an
installed calibre at −60). This guide covers what is ported, what LiuXin
substitutes for, and how to port more.

## The rules

- **Copy, don't rewrite.** A ported module is calibre's file with its imports
  changed. The other edits are mechanical, so the copies run on Python 3.12
  and 3.13 too: `from __future__ import annotations` is added, `except A, B:`
  clauses (Python 3.14 syntax) are parenthesised, and defaults are dropped
  from type parameters (`class C[T = str]`, Python 3.13 syntax), which only
  type checkers read. A header comment records the source path and the
  changes.
- **Fix only what hangs or reaches outside the book.** A reader that upstream
  leaves running for minutes, or for ever, on a damaged file, or that reads a
  file the book names outside itself, gets a fix marked `# LiuXin:`. In
  scripted ports the fixes live in `PATCHES` in the porting script, written
  against calibre's text, and a re-port stops if that text has changed. Hangs
  are recorded in `dev-docs/calibre_compat/upstream-calibre-bugs.md` (added by
  PR #181).
- **No calibre imports.** A test parses every module in the package and fails
  on an import from `calibre`, `calibre_extensions`, `polyglot`, `tzlocal` or
  `compression`.
- **Run calibre's tests.** Where calibre has tests for a module, port them as
  well and run them in the suite. calibre's OPF 2, OPF 3, author-sort, EPUB
  and damaged-ZIP tests run this way.

## What is ported

| Ported | From calibre | Notes |
| --- | --- | --- |
| Readers `txt`, `imp`, `rb`, `lrx`, `snb`, `rtf`, `topaz`, `pml`, `opf`, `epub` | `ebooks/metadata/` | `rtf`, `topaz` and `epub` carry marked fixes; `epub` also reads KEPUB |
| `snbfile` | `ebooks/snb/snbfile.py` | |
| `metadata_base` | `ebooks/metadata/__init__.py` | `MetaInformation`, `string_to_authors`, `check_isbn`, `title_sort` and so on |
| `book_fields`, `book_base`, `field_metadata`, `json_codec` | `ebooks/metadata/book/`, `library/field_metadata.py` | calibre's own `Metadata` class, used by every ported reader |
| `opf2`, `opf3`, `opf`, `metadata_utils`, `toc` | `ebooks/metadata/` | |
| `date`, `iso8601`, `cleantext`, `chardet`, `imghdr` | `utils/`, `ebooks/chardet.py` | |
| `pretty`, `config_base`, `meta` | parts of `ebooks/oeb/polish/pretty.py`, `utils/config_base.py`, `ebooks/metadata/meta.py` | only what the ported code uses |
| `localunzip` | `utils/localunzip.py` | Reads ZIP files whose central directory is missing or damaged, from their local headers |
| `ebooks`, `beautifulsoup` | `ebooks/__init__.py`, `ebooks/BeautifulSoup.py` | Finding a cover on a book's first page |

## What LiuXin substitutes

`_compat` holds the small helpers taken from modules that are not ported whole,
and its docstring lists each difference from calibre. In short:

| calibre | LiuXin | Effect |
| --- | --- | --- |
| C extensions `speedup` and `fast_html_entities` | Python ports in `_speedups` and `_compat` | Same answers: checked against calibre's compiled C on hundreds of thousands of random inputs, and pinned by tests |
| ICU collation and case mapping | Python's `casefold`, `lower`, `upper` | Sort order can differ from calibre's for some scripts |
| uchardet | the `chardet` package | Encoding guesses can differ; calibre only guesses when a file declares no encoding and is not UTF-8 |
| `mime.types` added to Python's global table, with the system's files | a private table of Python's built-in types plus calibre's `mime.types` | The same answers on every machine; other code is unaffected |
| ISO 639 and 3166 maps built at calibre's build time | `iso_codes.json.gz`, built from iso-codes 4.16.0 by `scripts/build_upstream_calibre_iso_codes.py` with calibre's rules | Languages read as ISO 639-3 codes, as calibre stores them (`fra`, not `fr`) |
| The `tzlocal` package, and `zoneinfo` with calibre's bundled tz data | `TZ` or `/etc/localtime`, falling back to UTC | Only dates written without a time zone are affected |
| Interface language from preferences or the system | English | Only the default language of OPF files the code writes |
| Template formatter (`SafeFormat`) | a stand-in that raises when used | Composite custom columns cannot be computed |
| calibre's own `zipfile`, a copy of an old Python one | Python's `zipfile`, with calibre's defaults and `writestr` keywords | Entry names are read the same way; Python's extraction makes them safe |
| html5-parser (C) building Beautiful Soup trees | Beautiful Soup's lxml builder; Beautiful Soup is optional | Without Beautiful Soup, an HTML first page holding only a cover image is not recognised; SVG cover pages still are |
| A worker process running Qt to draw a book's first page as its cover | not ported: `fork_job` raises `WorkerError`, as a failed worker does | A book with no cover image and a first page of text has no cover; calibre prints a line saying so |
| Qt image and date helpers, EPUB writing helpers | not ported; raise `NotImplementedError` | Readers do not call them |

The OPF and EPUB readers parse dates with `python-dateutil`, as calibre does,
so it is a LiuXin dependency. Their providers still check for it, and report
themselves unavailable, naming the package, where it is missing.

## What the providers add

The providers run calibre's code as calibre's plugins do, with two LiuXin
rules around it:

- **Archive limits.** Before calibre's EPUB reader opens a book, the provider
  applies the member-count, size and compression-ratio limits of LiuXin's own
  EPUB input (`file_formats.archive_preflight`). calibre reads the package
  document whole, and extracts the whole book when it looks for a cover on
  the first page, so a small file that expands enormously would otherwise
  fill memory or disk. Books whose central directory Python cannot read go to
  calibre's salvage reader, which inflates in bounded pieces.
- **Cover formats named by their bytes.** calibre's EPUB reader labels every
  cover `jpg`; the provider names each cover by what calibre's `imghdr` finds
  in it, so a PNG cover is a `png`.

## Not ported yet

The LIT reader needs calibre's LIT container reader and OEB reader and the
`lzx` and `msdes` C extensions; the fork's LIT reader still serves LIT files.

## Porting more

1. Add the module to `PORTS` in `scripts/port_upstream_calibre.py`, and run
   the script on a calibre checkout. An import its tables do not cover stops
   the run: decide whether to port that module too (add it to `MODULES` and
   `PORTS`), take a few names into `_compat` (`NAMES`), or leave it
   (`LEAVE`, for imports only unused functions make).
2. Port calibre's tests for the module, if it has any, and run them.
3. Add the reader to `PORTED_READERS` and `ported_reader()` in
   `file_formats.providers.ported_calibre`.
4. Try it on damaged and hostile input: truncated files, counts past the end,
   unclosed structures, nested entities, archives that expand enormously,
   paths that climb out of the book. Fix what hangs or reaches outside the
   book in `PATCHES`, marked `# LiuXin:`, and record hangs in the upstream bug
   notes.
5. Compare its readings with the fork's and LiuXin's readers for the same
   files, and note the differences in the pull request.

When calibre releases a new version, run the script again and review the diff.
The readers ported before the script existed (`txt` to `pml`) are not in
`PORTS`; re-port them by hand and keep their marked fixes.

Return to the [plugins index](README.md).
