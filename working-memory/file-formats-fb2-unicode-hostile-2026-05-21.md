# FB2 Unicode And Hostile Input Handoff - 2026-05-21

Branch: `file-formats-fb2-unicode-hostile`

## Scope

This branch hardens FB2 input/output conversion after the archive/container
format passes. FB2 is XML-backed, so the branch focuses on reusable fixtures,
unicode and encoding coverage, embedded base64 binary extraction safety,
malformed XML behavior, and output serialization boundaries.

## Implemented

- Added reusable FB2 fixtures in `tests/support/file_format_fb2.py`.
- Added fixture and valid input conversion coverage in
  `tests/file_formats/fb2/test_fb2_unicode_framework.py`.
- Added malformed and hostile input coverage in
  `tests/file_formats/fb2/test_fb2_malformed_hostile.py`.
- Added output-side unicode/loss-boundary coverage in
  `tests/file_formats/fb2/test_fb2_output_unicode_framework.py`.
- Extended `tests/support/file_format_oeb.py` with richer reusable OEB
  metadata/manifest helpers for output-format tests.
- Hardened `FB2Input` in
  `src/LiuXin_alpha/file_formats/conversion/plugins/fb2_input.py`.
- Added durable docs in `docs/development/file-formats/fb2/README.md`.
- Updated `docs/development/file-format-unicode-conversion.md` to include FB2
  in the XML/input-output hardening direction.

## Fixture Contract

The reusable FB2 builder can create:

- multilingual FB2 documents with title, authors, description, keywords,
  publisher metadata, body text, and sections
- UTF-8 and UTF-16 XML payloads
- optional cover binaries and extra embedded binaries
- odd but valid unicode binary IDs
- text rewrites for malformed XML, bad encoding, and corrupted base64 cases
- body-text and embedded-binary inspection helpers

The reusable OEB extension can create output books with richer metadata,
manifest `ids`/`hrefs`, TOC, body XHTML, and embedded images for output
serializers.

## Conversion Contract

`FB2Input` now:

- detects XML encoding before stripping decoded NUL characters
- preserves UTF-16 input through conversion
- sanitizes embedded binary IDs before writing extracted files
- maps XHTML image refs and OPF cover refs to sanitized filenames
- skips corrupted base64 binaries with warnings rather than aborting otherwise
  usable text conversion
- warns when unsafe cover references cannot be used

Malformed XML remains recoverable only when the recovery parser can produce a
usable document. Wrong-format and unrecoverable non-XML payloads fail before
partial output.

`FB2Output`/`FB2MLizer` now have focused coverage for:

- multilingual metadata and body serialization
- styled text serialization
- embedded image binary serialization
- valid UTF-8 XML output
- explicit surrogate replacement behavior at the output boundary

## Validation

Latest validation on this branch:

- `python3 -m py_compile tests/support/file_format_fb2.py tests/file_formats/fb2/test_fb2_unicode_framework.py`
- `python3 -m pytest tests/file_formats/fb2/test_fb2_unicode_framework.py -q` -> `7 passed`
- `python3 -m py_compile src/LiuXin_alpha/file_formats/conversion/plugins/fb2_input.py tests/support/file_format_fb2.py tests/file_formats/fb2/test_fb2_malformed_hostile.py tests/file_formats/fb2/test_fb2_unicode_framework.py`
- `python3 -m pytest tests/file_formats/fb2/test_fb2_malformed_hostile.py -q` -> `13 passed`
- `python3 -m py_compile tests/support/file_format_oeb.py tests/file_formats/fb2/test_fb2_output_unicode_framework.py`
- `python3 -m pytest tests/file_formats/fb2/test_fb2_output_unicode_framework.py -q` -> `3 passed`
- `python3 -m pytest tests/file_formats/fb2 -q` -> `30 passed`
- `python3 -m pytest tests/file_formats/conversion/plugins tests/file_formats/conversion/test_conversion_top_level_smoke.py -q` -> `10 passed`
- `python3 -m pytest tests/file_formats -q` -> `742 passed, 1 skipped, 127 warnings`
- `git diff --check`

## Follow-Ups

- Commit and PR this branch when ready.
- Add conversion-report integration for skipped binaries, sanitized IDs, parser
  recovery, and output character replacement.
- Consider zipped `.fb2.zip` coverage separately if conversion or ingest paths
  treat it as a first-class FB2 container.
- Reuse the richer OEB output fixture for future output formats that need
  metadata/manifest/image coverage.
