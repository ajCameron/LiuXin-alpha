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
  changed. The only other edits are mechanical: `from __future__ import
  annotations` is added, and `except A, B:` clauses (Python 3.14 syntax) are
  parenthesised, so the copies run on Python 3.12 and 3.13 too. A header
  comment records the source path and the changes.
- **Fix only what hangs.** A reader that upstream leaves running for minutes,
  or for ever, on a damaged file gets a fix, marked `# LiuXin:` and listed in the
  header. Such bugs are recorded in
  `dev-docs/calibre_compat/upstream-calibre-bugs.md` (added by PR #181).
- **No calibre imports.** A test parses every module in the package and fails
  on an import from `calibre`, `calibre_extensions`, `polyglot` or `tzlocal`.
- **Run calibre's tests.** Where calibre has tests for a module, port them as
  well and run them in the suite. calibre's OPF 2, OPF 3 and author-sort tests
  run this way.

## What is ported

| Ported | From calibre | Notes |
| --- | --- | --- |
| Readers `txt`, `imp`, `rb`, `lrx`, `snb`, `rtf`, `topaz`, `pml`, `opf` | `ebooks/metadata/` | `rtf` and `topaz` carry marked fixes |
| `snbfile` | `ebooks/snb/snbfile.py` | |
| `metadata_base` | `ebooks/metadata/__init__.py` | `MetaInformation`, `string_to_authors`, `check_isbn`, `title_sort` and so on |
| `book_fields`, `book_base`, `field_metadata`, `json_codec` | `ebooks/metadata/book/`, `library/field_metadata.py` | calibre's own `Metadata` class, used by every ported reader |
| `opf2`, `opf3`, `opf`, `metadata_utils`, `toc` | `ebooks/metadata/` | |
| `date`, `iso8601`, `cleantext`, `chardet`, `imghdr` | `utils/`, `ebooks/chardet.py` | |
| `pretty`, `config_base`, `meta` | parts of `ebooks/oeb/polish/pretty.py`, `utils/config_base.py`, `ebooks/metadata/meta.py` | only what the ported code uses |

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
| Qt image and date helpers, EPUB writing helpers | not ported; raise `NotImplementedError` | Readers do not call them |

`python-dateutil` is not a runtime dependency of LiuXin. The OPF reader parses
dates with it, so its provider reports itself unavailable, naming the package,
when it is not installed.

## Porting more

1. Add the module to `PORTS` in `scripts/port_upstream_calibre.py`, and run
   the script on a calibre checkout. An import its tables do not cover stops
   the run: decide whether to port that module too (add it to `MODULES` and
   `PORTS`), take a few names into `_compat` (`NAMES`), or leave it
   (`LEAVE`, for imports only unused functions make).
2. Port calibre's tests for the module, if it has any, and run them.
3. Add the reader to `PORTED_READERS` and `ported_reader()` in
   `file_formats.providers.ported_calibre`.
4. Try it on damaged input: truncated files, counts past the end, unclosed
   structures, nested entities. Fix what hangs, marked `# LiuXin:`, and record it
   in the upstream bug notes.
5. Compare its readings with the fork's and LiuXin's readers for the same
   files, and note the differences in the pull request.

When calibre releases a new version, run the script again and review the diff.
The readers ported before the script existed (`txt` to `pml`) are not in
`PORTS`; re-port them by hand and keep their marked fixes.

Return to the [plugins index](README.md).
