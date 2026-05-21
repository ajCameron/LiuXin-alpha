# Comic File Format Notes

## Status

Comic conversion enters through `ComicInput` in
`src/LiuXin_alpha/file_formats/conversion/plugins/comic_input.py`. The lower
level CBZ/CBR helpers live under `src/LiuXin_alpha/file_formats/comic/input.py`.

The current hardening pass covers ZIP-backed comic inputs: direct `.cbz`
archives and `.cbc` comic collections containing listed CBZ archives. CBR/RAR
inputs are intentionally outside this pass because they do not use the ZIP
preflight path.

The focused test fixtures live in:

- `tests/support/file_format_comic.py`
- `tests/file_formats/comic/test_comic_container_framework.py`
- `tests/file_formats/comic/test_comic_malformed_hostile.py`

The reusable fixture builds multilingual CBZ and CBC archives with non-ASCII
titles, collection entries, page paths, and rewrite helpers for malformed or
hostile archive cases.

## Container Contract

Default CBZ and CBC conversion is strict before extraction for archive safety
and for the required comic-image payload:

- require a readable ZIP archive for direct CBZ and CBC input
- require CBC `comics.txt` to decode as UTF-8 or UTF-16
- reject CBZ and CBC archive member names that can escape or confuse the
  conversion work directory
- preflight nested CBZ archives inside CBC before page extraction
- no more than `4096` archive members
- no member expanding beyond `256 MiB`
- no archive expanding beyond `512 MiB` total
- no member at or above `1 MiB` with a compression ratio above `1000`
- no non-empty member reporting a zero compressed size
- require at least one image page in the resulting comic stream

CBC entries listed in `comics.txt` but missing from the archive warn and are
skipped. A CBC where every listed comic is missing still fails because it cannot
produce a comic stream.

## Unicode And Locale Coverage

The current fixtures exercise multilingual comic titles, CBC collection titles,
listed comic titles, nested CBZ paths, page names, image output names, OPF
metadata, generated wrapper XHTML, and generated table-of-contents content.

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
- archive member-count budget failures
- oversized expanded members
- excessive total expanded archive size
- suspicious compression-ratio payloads

Future regressions should be added here when real-world comics expose new edge
cases, especially around RAR-backed CBR input, malformed image payloads,
encoding oddities in `comics.txt`, and collection ordering.

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
- relaxed archive limit, observed value, and active trusted-input profile
- dropped image payloads or image-decoding failures, when image validation is
  performed before output
- CBR/RAR extraction failure details once that path has equivalent hardening
