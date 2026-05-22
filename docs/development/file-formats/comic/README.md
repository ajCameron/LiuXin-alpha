# Comic File Format Notes

## Status

Comic conversion enters through `ComicInput` in
`src/LiuXin_alpha/file_formats/conversion/plugins/comic_input.py`. The lower
level CBZ/CBR helpers live under `src/LiuXin_alpha/file_formats/comic/input.py`.

The current hardening coverage spans ZIP-backed comic inputs and CBR/RAR
preflight. ZIP-backed paths cover direct `.cbz` archives and `.cbc` comic
collections containing listed CBZ archives. CBR/RAR paths are preflighted with
the vendored RAR header parser where possible, falling back to the existing
`unrar` listing path for externally supported RAR variants.

The focused test fixtures live in:

- `tests/support/file_format_comic.py`
- `tests/file_formats/comic/test_comic_container_framework.py`
- `tests/file_formats/comic/test_comic_malformed_hostile.py`
- `tests/file_formats/comic/test_comic_rar_extraction_boundary.py`

The reusable fixture builds multilingual CBZ and CBC archives with non-ASCII
titles, collection entries, page paths, rewrite helpers for malformed or
hostile archive cases, and fake CBR/RAR headers for deterministic preflight
tests that do not require a system `unrar` binary. It also references the
vendored `rarfile` package's small `unicode.rar` fixture to pin real RAR header
listing behavior, and monkeypatches the names-only `unrar` listing fallback for
RAR variants the header parser cannot inspect.

## Container Contract

Default CBZ and CBC conversion is strict before extraction for archive safety
and for the required comic-image payload:

- require a readable ZIP archive for direct CBZ and CBC input
- require a readable RAR archive listing for CBR input
- require CBC `comics.txt` to decode as UTF-8 or UTF-16
- reject CBZ and CBC archive member names that can escape or confuse the
  conversion work directory
- reject CBR/RAR archive member names that can escape or confuse the
  conversion work directory
- lower-level RAR extraction also rejects unsafe member paths and explicitly
  advances past skipped unsafe entries
- preflight nested CBZ archives inside CBC before page extraction
- no more than `4096` archive members
- no member expanding beyond `256 MiB`
- no archive expanding beyond `512 MiB` total
- no member at or above `1 MiB` with a compression ratio above `1000`
- no non-empty member reporting a zero compressed size
- reject CBR/RAR entries that require a password
- require at least one image page in the resulting comic stream

CBC entries listed in `comics.txt` but missing from the archive warn and are
skipped. A CBC where every listed comic is missing still fails because it cannot
produce a comic stream.

When the RAR listing backend only exposes member names, CBR preflight still
enforces readability, member count, path safety, and the required output-product
invariants. Size and compression-ratio checks apply when the available RAR
listing exposes uncompressed and compressed sizes.

## Unicode And Locale Coverage

The current fixtures exercise multilingual comic titles, CBC collection titles,
listed comic titles, nested CBZ paths, CBR page names, image output names, OPF
metadata, generated wrapper XHTML, and generated table-of-contents content.
The CBR preflight tests include a real RAR listing with non-ASCII filenames from
the vendored `rarfile` fixture corpus and a names-only external listing
fallback with multilingual member names.

Tests assert the generated OPF, TOC, XHTML wrappers, and copied image output so
replacement characters, dropped combining marks, broken non-ASCII filenames,
and lost collection titles stay visible.

## Hostile Corpus

The checked-in hostile corpus currently covers:

- non-ZIP `.cbz` payloads
- wrong-format `.cbz` archives without comic image pages
- CBZ archives with no image pages
- CBC archives missing `comics.txt`
- CBC archives with invalid `comics.txt` encoding
- CBC archives with one missing listed comic, preserving the remaining comics
- CBC archives with all listed comics missing
- parent traversal, normalized traversal, absolute-looking, and Windows
  drive-looking CBZ member names
- parent traversal, normalized traversal, absolute-looking, and Windows
  drive-looking CBC member names
- unsafe nested CBZ page paths inside CBC
- non-RAR `.cbr` payloads
- CBR/RAR payloads whose parser returns an empty or truncated member listing
- parent traversal, normalized traversal, absolute-looking, Windows
  drive-looking, and backslash-drive-looking CBR member names
- unsafe names from the names-only external RAR listing fallback
- lower-level RAR extraction safe-path checks for traversal, absolute-looking,
  drive-looking, empty, and dot member names
- lower-level RAR extraction of unsafe useful entries, confirming the iterator
  advances and later safe entries are still extracted
- lower-level RAR extraction of non-useful entries such as directories,
  symlinks, and password-protected entries
- archive member-count budget failures
- oversized expanded members
- excessive total expanded archive size
- suspicious compression-ratio payloads
- password-protected CBR entries

Future regressions should be added here when real-world comics expose new edge
cases, especially around newer RAR variants, malformed image payloads, encoding
oddities in `comics.txt`, and collection ordering.

## Salvage And Reporting Direction

There is no separate comic salvage mode today. The default CBC behavior already
salvages missing listed comics by warning and continuing when at least one
listed comic can still be converted.

If future trusted-input overrides are added, they should only raise bounded
archive budgets. They must not bypass path safety, unreadable ZIP structure,
invalid `comics.txt` decoding, or the invariant that conversion must produce at
least one comic page.

Diagnostics for future recovery work should record:

- skipped CBC member and listed title, when applicable
- rejected nested CBZ member and reason
- CBR/RAR listing backend failures, including both vendored parser and
  external listing fallback errors when both fail
- relaxed archive limit, observed value, and active trusted-input profile
- dropped image payloads or image-decoding failures, when image validation is
  performed before output
- CBR/RAR extraction failure details after preflight succeeds but extraction or
  image discovery fails
