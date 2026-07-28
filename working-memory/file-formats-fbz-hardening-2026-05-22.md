# File Formats: FBZ Hardening

Date: 2026-05-22
Branch: `file-formats-zipped-fb2-hardening`

## Scope

This branch promotes zipped FB2 from a deferred follow-up to first-class `.fbz`
coverage for conversion and metadata.

The work follows the same container-hardening pattern used for ODT, EPUB,
HTMLZ, DOCX, and comics:

- reusable fixture helpers first
- valid multilingual conversion-product coverage
- hostile archive rejection before partial output
- shared source policy for conversion and metadata
- durable docs for future format/conversion work

## Source Changes

- Added `src/LiuXin_alpha/file_formats/fb2/archive.py`.
  - Shared FBZ archive policy for conversion and metadata.
  - Enforces safe member paths.
  - Requires exactly one non-directory `.fb2` member.
  - Rejects no-FB2 and multiple-FB2 archives.
  - Applies member-count, per-member expanded size, total expanded size,
    invalid compressed-size, and suspicious compression-ratio budgets.
- Updated `FB2Input`.
  - Registers `{"fb2", "fbz"}`.
  - Reads `.fbz` through the shared archive selector before XML parsing.
  - Logs preflight rejection reasons.
  - Passes only the selected FB2 payload to metadata extraction.
  - Does not extract unrelated zip members into the conversion work directory.
- Updated FB2 metadata.
  - Reuses the shared archive selector for zipped payloads.
  - Treats `.fbz` as forced zip.
  - Keeps safe unrelated zip members during metadata writes.
- Registered `.fbz` across metadata readers/writers, archive dispatch, worker
  priority, and known extension lists.

## Tests And Fixtures

- Added reusable zip fixture support in `tests/support/file_format_zip.py`.
- Extended `tests/support/file_format_fb2.py` with:
  - `build_zipped_fb2`
  - `fb2_zip_bytes`
  - `read_zipped_fb2_member`
  - `parse_zipped_fb2`
  - `rewrite_zipped_fb2`
  - `zipped_fb2_members`
- Added `tests/file_formats/fb2/test_fb2_zip_framework.py`.
  - Valid `.fbz` shape and unicode payload.
  - UTF-16 FB2 inside `.fbz`.
  - extra archive members.
  - archive rewrite helper.
  - `FB2Input` `.fbz` registration.
  - `.fbz` conversion products: OPF, XHTML, CSS, extracted binaries.
- Expanded `tests/file_formats/fb2/test_fb2_malformed_hostile.py`.
  - corrupt/non-zip `.fbz`.
  - no `.fb2` member.
  - multiple `.fb2` members.
  - unsafe member paths.
  - member count, size, total expansion, and compression-ratio limits.
- Updated metadata FB2 tests for strict zip selection, `.fbz` plugin
  registration, and hostile zip payload rejection.

## Docs

Updated:

- `dev-docs/file-formats/fb2/README.md`
- `dev-docs/file-format-unicode-conversion.md`

The docs now describe `.fb2` as XML-backed by default and `.fbz` as the strict
single-FB2-member archive-backed variant.

## Verification

Commands run on this branch:

- `python3 -m py_compile tests/support/file_format_zip.py tests/support/file_format_fb2.py tests/file_formats/fb2/test_fb2_zip_framework.py`
- `python3 -m pytest tests/file_formats/fb2/test_fb2_zip_framework.py -q` -> `7 passed`
- `python3 -m pytest tests/file_formats/fb2 -q` -> `50 passed`
- `python3 -m pytest tests/metadata/file_sources/test_fb2_metadata_source.py tests/metadata/file_sources/test_fb2_edge_cases.py -q` -> `26 passed`
- `python3 -m pytest tests/metadata/file_sources/test_fb2_metadata_source.py tests/metadata/file_sources/test_fb2_edge_cases.py tests/metadata/file_sources/test_malformed_input_fuzzing.py -q` -> `159 passed`
- `python3 -m pytest tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py tests/file_formats/conversion/test_conversion_top_level_smoke.py tests/utils/test_mine_types.py -q` -> `10 passed`
- `python3 -m pytest tests/metadata/file_sources/test_malformed_input_fuzzing.py tests/metadata/file_sources/test_archive_metadata_source.py tests/metadata/file_sources/test_zip_metadata_source.py tests/metadata/file_sources/test_archive_container_edge_cases.py -q` -> `153 passed`
- `python3 -m pytest tests/file_formats/fb2 tests/metadata/file_sources/test_fb2_metadata_source.py tests/metadata/file_sources/test_fb2_edge_cases.py -q` -> `76 passed`
- `git diff --check`

## Follow-Ups

- Commit and PR after the final doc/test pass.
- Consider whether other duplicated zip preflight implementations should be
  consolidated around a shared archive policy helper later. This branch keeps
  the new shared code scoped to FB2/FBZ to avoid churn in already-merged
  container formats.
