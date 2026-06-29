# Comic Container Hardening Handoff - 2026-05-21

Branch: `file-formats-comic-container-hardening`

## Scope

This branch hardens ZIP-backed comic input conversion after the ODT, EPUB,
DOCX, and HTMLZ container passes. The implementation covers direct CBZ input
and CBC collections that list nested CBZ archives. CBR/RAR is explicitly left
for a later pass because it does not share the ZIP preflight path.

## Implemented

- Added reusable comic fixtures in `tests/support/file_format_comic.py`.
- Added valid fixture and conversion coverage in
  `tests/file_formats/comic/test_comic_container_framework.py`.
- Added malformed and hostile coverage in
  `tests/file_formats/comic/test_comic_malformed_hostile.py`.
- Hardened `ComicInput` in
  `src/LiuXin_alpha/file_formats/conversion/plugins/comic_input.py`.
- Updated the existing modernized glue test to use a minimal valid ZIP-backed
  fake CBZ now that direct CBZ input is preflighted.
- Added durable docs in `docs/development/file-formats/comic/README.md`.
- Updated `docs/development/file-format-unicode-conversion.md` to include
  comic CBZ/CBC in the container-format direction.

## Fixture Contract

The reusable builders can create:

- CBZ archives with caller-selected page names and PNG payloads
- CBC archives with decodable `comics.txt`, collection title entries, and
  nested CBZ members
- non-ASCII titles, collection names, nested paths, and page filenames
- extra archive members for resource and edge-case coverage
- rewrite helpers for removing, replacing, and adding archive members in
  malformed-container tests

## Conversion Contract

`ComicInput` now preflights CBZ/CBC ZIP archives before extraction:

- invalid ZIPs fail before partial output
- unsafe archive member paths fail before extraction
- nested CBZ archives inside CBC are preflighted before page extraction
- member count, per-member expanded size, total expanded size, suspicious
  compression ratio, and invalid compressed-size shapes are rejected
- direct CBZ conversion requires at least one image page
- CBC `comics.txt` must decode as UTF-8 or UTF-16
- missing listed CBC members warn and are skipped when another listed comic can
  still be converted
- a CBC where every listed comic is missing fails instead of producing empty
  output

Preflight failures are logged as
`Comic preflight rejected <path>: <reason>`.

## Validation

Latest validation on this branch:

- `python3 -m py_compile tests/support/file_format_comic.py tests/file_formats/comic/test_comic_container_framework.py`
- `python3 -m pytest tests/file_formats/comic/test_comic_container_framework.py -q` -> `7 passed`
- `python3 -m py_compile src/LiuXin_alpha/file_formats/conversion/plugins/comic_input.py tests/support/file_format_comic.py tests/file_formats/comic/test_comic_malformed_hostile.py`
- `python3 -m pytest tests/file_formats/comic/test_comic_malformed_hostile.py -q` -> `23 passed`
- `python3 -m pytest tests/file_formats/comic -q` -> `36 passed`
- `python3 -m pytest tests/file_formats/conversion/plugins tests/file_formats/conversion/test_conversion_top_level_smoke.py -q` -> `10 passed`
- `python3 -m pytest tests/file_formats -q` -> `722 passed, 1 skipped, 127 warnings`
- `git diff --check`

## Follow-Ups

- Commit and PR this branch when ready.
- CBR/RAR still needs equivalent archive hardening, diagnostics, and malformed
  input coverage.
- Consider a shared archive-preflight helper later; ODT, EPUB, DOCX, HTMLZ, and
  comic CBZ/CBC now carry very similar path-safety and archive-budget checks.
- If trusted-input overrides are added, keep path safety, unreadable archive
  structure, invalid collection decoding, and required conversion-product
  checks non-overridable.
- Consider image payload validation before output once the conversion path needs
  stronger diagnostics for damaged or hostile image bytes.
