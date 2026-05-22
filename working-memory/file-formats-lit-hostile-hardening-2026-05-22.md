# File Formats LIT Hostile Hardening Slice

Branch: `file-formats-lit-hostile-hardening`

Started after PR #80 merged. LIT is the next legacy binary/container-ish target
after the archive/XML and FB2 passes. The goal is to make LIT parser behavior
safe, explicit, and reusable before adding broader conversion-product unicode
coverage.

Durable docs:

- `docs/development/file-format-unicode-conversion.md`
- `docs/development/file-formats/lit/README.md`

## Plan

1. Inspect current LIT parser/converter/tests.
2. Add reusable LIT parser fixture/test harness.
3. Add malformed, wrong-format, and truncated payload coverage.
4. Add conversion-facing unicode/foreign-language assertions around existing
   LIT input/output behavior.
5. Document contract and boundaries.

## Progress

Stage 1 inspected the LIT surface:

- input plugin: `src/LiuXin_alpha/file_formats/conversion/plugins/lit_input.py`
- output plugin: `src/LiuXin_alpha/file_formats/conversion/plugins/lit_output.py`
- parser: `src/LiuXin_alpha/file_formats/lit/reader.py`
- writer: `src/LiuXin_alpha/file_formats/lit/writer.py`
- existing tests under `tests/file_formats/lit/`

Stage 2 added reusable parser-facing fixture support:

- `tests/support/file_format_lit.py`
- `tests/file_formats/lit/test_lit_parser_framework.py`
- shared `LitLog`, `lit_options`, in-memory `LitFile` helpers, manifest and
  namelist builders, sized UTF-8 helpers, and `UnBinary` binary XHTML builders
- existing LIT tests now share the support module instead of duplicating local
  log/options helpers

Stage 3 added hostile/malformed coverage:

- `tests/file_formats/lit/test_lit_malformed_hostile.py`
- wrong-format payloads
- truncated primary and secondary headers
- unknown secondary-header block behavior
- truncated manifests and namelists
- invalid UTF-8 in manifest strings and binary markup
- unterminated encoded integers
- truncated `UnBinary` control sequences
- truncated atom table headers

Stage 3 also hardened `src/LiuXin_alpha/file_formats/lit/reader.py`:

- bounded primitive unpack helpers now raise `LitError`
- `encint` rejects unterminated encoded integers
- `msguid` rejects truncated GUIDs
- secondary-header parsing validates short and unknown blocks without hanging
- manifest and namelist parsing reject short fields before raw unpack failures
- `UnBinary` reports truncated control state as `LitError`
- the observed real fixture `ITSF` block shape remains accepted

Stage 4 added conversion-facing unicode coverage:

- `tests/file_formats/lit/test_lit_conversion_unicode_framework.py`
- optional real-corpus `LITInput` conversion products are checked for
  replacement characters
- `LITInput.postprocess_book` preserves the shared multiscript corpus while
  converting single-`pre` bodies into XHTML paragraph content
- output-side `ReBinary` preserves shared OEB XHTML unicode text, anchors, and
  styled fragments
- `LitWriter` manifest serialization preserves non-ASCII item IDs and nested
  paths

The full `LITOutput` archive path still cannot be exercised in this
environment because the LZX compressor backend is unavailable. The current
coverage stops at `ReBinary` and manifest serialization, which are the
conversion-facing surfaces before compression/storage assembly.

Stage 5 added durable docs and recorded the current LIT contract:

- `docs/development/file-formats/lit/README.md`
- `docs/development/file-format-unicode-conversion.md`
- `docs/development/file-formats/README.md`

## Validation

- `python3 -m pytest tests/file_formats/lit -q`
  - `58 passed`
- `python3 scripts/run_file_formats_lane.py --lane fast`
  - `596 passed, 1 skipped`
- `git diff --check`
  - clean

## Open

- Full `.lit` archive output coverage remains blocked until a testable LZX
  compressor backend or a bounded compressor/storage seam is available.
- Add transform/decompression-focused malformed tests later, especially around
  LZX reset tables and section offsets.
- Add real-fixture regressions here when more LIT samples expose malformed OPF
  binary markup, atom table damage, or DRM fallback quirks.
