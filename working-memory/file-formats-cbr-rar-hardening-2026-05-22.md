# File Formats CBR/RAR Hardening Slice

Branch: `file-formats-cbr-rar-hardening`

Started after PR #81 merged and after the database review branch landed on
main. This branch continues the file-format hardening lane while leaving the
databases/catalog split to the separate database workstream.

## Scope

CBR/RAR was the explicit follow-up from the comic CBZ/CBC container pass. The
goal is equivalent pre-extraction safety for RAR-backed comic input without
requiring a system `unrar` binary in unit tests.

## Implemented

Stage 1 added plugin-level CBR/RAR preflight:

- Added CBR/RAR preflight to
  `src/LiuXin_alpha/file_formats/conversion/plugins/comic_input.py`.
- Preflight uses the vendored RAR header parser first, then falls back to the
  existing `unrar` listing path for externally supported RAR variants.
- CBR preflight now rejects unreadable RAR input, unsafe member paths, too many
  members, oversized members, excessive total expansion, suspicious compression
  ratios, invalid compressed-size shapes, and password-protected entries when
  the listing backend exposes the relevant fields.
- Added deterministic fake-RAR fixture helpers in
  `tests/support/file_format_comic.py`.
- Added CBR conversion and malformed/hostile tests in:
  - `tests/file_formats/comic/test_comic_container_framework.py`
  - `tests/file_formats/comic/test_comic_malformed_hostile.py`

Stage 2 added lower-level RAR extraction-boundary coverage:

- Hardened `src/LiuXin_alpha/utils/decompression/unrar.py` so `safe_path`
  rejects traversal, absolute-looking paths, drive-looking paths, empty names,
  and dot names before extraction.
- Fixed `stream_extract` so unsafe useful file entries are explicitly skipped
  and advanced instead of leaving the RAR iterator on the same item.
- Added `tests/file_formats/comic/test_comic_rar_extraction_boundary.py`.
- The new tests cover safe-path rejection/acceptance, unsafe useful entry
  skipping with continued extraction of later safe entries, and skipping
  non-useful directory/symlink/password entries.

Stage 3 added real vendored RAR listing coverage:

- Added `vendored_rar_fixture()` to `tests/support/file_format_comic.py`.
- Added a positive CBR preflight test against the vendored `unicode.rar`
  header listing, asserting non-ASCII RAR filenames are seen by the preflight
  layer without requiring extraction or a system `unrar` binary.
- Hardened `ComicInput.validate_rar_archive_members` to reject empty RAR
  listings as `CBR has no archive members`, covering truncated RAR-like input
  that the header parser can otherwise reduce to an empty listing.
- Added malformed CBR coverage for that empty/truncated listing boundary.

Stage 4 added names-only external listing fallback coverage:

- Added `patch_rarfile_failure()` and `patch_unrar_names()` helpers to
  `tests/support/file_format_comic.py`.
- Added a positive preflight test where the vendored parser fails and the
  `unrar.names()` fallback supplies multilingual member names with no size
  metadata.
- Added a hostile test where the names-only fallback supplies an unsafe member
  path; `ComicInput` rejects it before extraction and logs the preflight
  rejection.

## Database Import Unblocker

The database review merge introduced an import-time cycle that blocked
`ComicInput` import:

`customize -> databases -> metadata -> databases.row -> databases.DatabaseAPI`

The immediate trigger was a redundant package-root import in
`src/LiuXin_alpha/databases/row.py`; the same symbol was imported directly from
`LiuXin_alpha.databases.api` immediately afterward. This branch removes only
that redundant root import so file-format tests can run. The broader
databases/catalog split remains out of scope for this branch.

## Validation

- `python3 -m pytest tests/file_formats/comic -q`
  - `65 passed`
- `python3 scripts/run_file_formats_lane.py --lane fast`
  - `625 passed, 1 skipped`
- `git diff --check`
  - clean

## Open

- Add real CBR fixture coverage later if a small redistributable RAR-backed
  comic fixture becomes available.
- RAR variants whose listing backend exposes only names cannot get size/ratio
  budget checks before extraction; they still get readability, member-count,
  path-safety, password where available, and required output-product checks.
- Consider moving archive preflight into a shared helper now that ODT, EPUB,
  DOCX, HTMLZ, comic ZIP, and comic RAR paths have the same policy shape.
