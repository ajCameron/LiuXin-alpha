# File Formats Text Unicode Framework Slice

Branch: `file-formats-text-unicode`

Started after PR #68 merged, as the first systematic file-format text/unicode
hardening pass.

## Scope

Build a reusable test framework before going format-by-format. The immediate
consumer is TXT input/processing, but the support layer is intentionally
format-neutral so it can be reused for Markdown, Textile, RTF, PML, ODT, OPF,
and later conversion matrix work.

## Framework

- Added `tests/support/file_format_unicode.py`.
- Shared fixtures include:
  - stable multiscript corpus cases
  - common text fragments for lossy-conversion checks
  - deterministic unicode fuzz generation
  - BOM-bearing encoded payload cases for UTF-8 and UTF-16
  - generic assertions for fragment preservation, replacement-character
    absence, and deterministic renderer output
- Added `tests/file_formats/test_unicode_framework.py` as a contract test for
  the reusable support layer.
- Added `tests/support/file_format_conversion.py`.
- Shared conversion fixtures include:
  - stable text-output matrix cases for UTF-8, UTF-8-SIG, UTF-16 native,
    UTF-16 LE, and UTF-16 BE
  - unix, Windows, and old-Mac newline expectations
  - strict decode/newline-style assertions for output conversion bytes
- Added `tests/file_formats/test_conversion_framework.py` as a contract test
  for the reusable conversion matrix support.
- Added `tests/support/file_format_oeb.py`.
- Shared OEB fixtures include:
  - a minimal XHTML spine/manifest/TOC model for output serializers
  - a neutral stylizer that avoids requiring the full CSS/stylizer stack
  - reusable text-output options and logging helpers

## First Consumer

Added `tests/file_formats/txt/test_txt_unicode_framework.py` covering:

- `txt.processor.convert_basic` against the shared multiscript corpus
- `convert_markdown` and `convert_textile` against shared unicode fragments
- `clean_txt` behavior for valid UTF-8 plus malformed trailing bytes
- `detect_formatting_type` determinism under shared unicode fuzz
- `TXTInput.convert` decoding for UTF-8, UTF-8 with BOM, UTF-16 LE with BOM,
  and UTF-16 BE with BOM
- extension-forced Markdown/Textile routing for `.md`, `.markdown`, and
  `.textile` without losing non-ASCII text
- `TXTOutput.convert` determinism and unicode preservation across the shared
  encoding/newline output matrix

## Additional Consumers

Added framework-backed Markdown/Textile coverage without removing the older
bespoke torture tests:

- `tests/file_formats/markdown/test_markdown_unicode_framework.py`
  - shared corpus preservation through `markdown.markdown`
  - bytes, bytearray, and memoryview payloads through `Markdown.convert`
  - UTF-8, UTF-8 BOM, UTF-16 LE BOM, and UTF-16 BE BOM file conversion through
    `markdownFromFile`
- `tests/file_formats/textile/test_textile_unicode_framework.py`
  - shared corpus preservation through `textile`
  - restricted-mode escaping/nofollow behavior while preserving unicode text
  - heading offset with non-ASCII heading content
  - deterministic unicode fuzz stability

## Output Serializers

Added `tests/file_formats/txt/test_txt_output_serializers_unicode_framework.py`
to exercise the real TXT output serializers against the shared OEB fixture:

- `TXTMLizer` extracts the shared unicode corpus from an OEB spine with inline
  TOC enabled.
- `MarkdownMLizer` preserves unicode through headings, styled text, links,
  images, lists, tables, and preformatted text.
- `TextileMLizer` covers the same OEB fixture, including link/image retention.
- `TXTOutput.convert` now uses the real plain/Markdown/Textile serializers
  against the shared unicode OEB fixture.

This uncovered and fixed a Python 3 regex construction bug in
`src/LiuXin_alpha/file_formats/txt/textileml.py`: the Textile cleanup regex for
styled bracket spans opened a capture group without closing it before the
literal closing bracket. Full Textile output conversion now reaches cleanup
without a `re.error`.

## RTF Slice

Added `tests/file_formats/rtf/test_rtf_unicode_framework.py` as the first step
up from plain text serializers:

- `txt2rtf` now uses the shared unicode corpus and checks deterministic,
  ASCII-safe RTF escape output.
- `RTFMLizer` serializes the shared minimal OEB fixture, including unicode
  metadata, styled text, links, image placeholders, and embedded raster output.
- `RTFOutput.convert` runs through the real serializer and validates
  deterministic ASCII bytes.
- `tests/support/file_format_oeb.py` now includes minimal `guide` and
  `metadata` surfaces for richer output serializers, plus tag-aware fake styles
  for bold/italic elements.

This uncovered a metadata-loss risk in
`src/LiuXin_alpha/file_formats/rtf/rtfml.py`: the RTF header inserted title and
author values raw, then `RTFOutput` encoded the whole document as ASCII with
replacement. Header metadata now goes through `txt2rtf`, matching body text
escaping and preserving non-ASCII title/creator values.

## RTF Malformed/Hostile Slice

Added `tests/file_formats/rtf/test_rtf_malformed_hostile.py` for bounded hostile
RTF inputs before moving away from the format:

- signed unicode control words such as `\u-945`
- malformed signed control words, excessive numeric arguments, and trailing
  control characters
- parser invalid-RTF exceptions wrapped as user-facing `ValueError`s by
  `RTFInput.convert`
- corrupt/odd/non-hex `\pict` payloads with `ignore_wmf=True`
- corrupt OEB image payloads dropped by `RTFMLizer` without leaking internal
  placeholders into output

Fixes from that pass:

- `src/LiuXin_alpha/file_formats/rtf/preprocess.py` now parses optional signs
  in control-word numeric arguments and raises deterministic errors for missing
  digits/end delimiters.
- `src/LiuXin_alpha/file_formats/conversion/plugins/rtf_input.py` now catches
  both parser invalid-RTF and invalid-code exceptions, while remaining tolerant
  of older/fake parser modules that only expose `RtfInvalidCodeException`.

## Validation

- `python3 -m py_compile tests/support/file_format_unicode.py tests/file_formats/test_unicode_framework.py tests/file_formats/txt/test_txt_unicode_framework.py`
  - clean
- `python3 -m pytest tests/file_formats/test_unicode_framework.py tests/file_formats/txt/test_txt_unicode_framework.py -q`
  - `14 passed`
- `python3 -m py_compile tests/support/file_format_conversion.py tests/support/file_format_unicode.py tests/file_formats/test_conversion_framework.py tests/file_formats/test_unicode_framework.py tests/file_formats/txt/test_txt_unicode_framework.py`
  - clean
- `python3 -m pytest tests/file_formats/test_conversion_framework.py tests/file_formats/test_unicode_framework.py tests/file_formats/txt/test_txt_unicode_framework.py -q`
  - `26 passed`
- `python3 -m py_compile tests/file_formats/markdown/test_markdown_unicode_framework.py tests/file_formats/textile/test_textile_unicode_framework.py`
  - clean
- `python3 -m pytest tests/file_formats/markdown/test_markdown_unicode_framework.py tests/file_formats/textile/test_textile_unicode_framework.py -q`
  - `12 passed`
- `python3 -m py_compile src/LiuXin_alpha/file_formats/txt/textileml.py tests/support/file_format_oeb.py tests/file_formats/txt/test_txt_output_serializers_unicode_framework.py`
  - clean
- `python3 -m pytest tests/file_formats/txt/test_txt_output_serializers_unicode_framework.py -q`
  - `6 passed`
- `python3 -m pytest tests/file_formats/txt -q`
  - `33 passed`
- `python3 -m py_compile src/LiuXin_alpha/file_formats/rtf/rtfml.py tests/support/file_format_oeb.py tests/file_formats/rtf/test_rtf_unicode_framework.py`
  - clean
- `python3 -m pytest tests/file_formats/rtf/test_rtf_unicode_framework.py -q`
  - `3 passed`
- `python3 -m pytest tests/file_formats/rtf -q`
  - `32 passed`
- `python3 -m py_compile src/LiuXin_alpha/file_formats/rtf/preprocess.py src/LiuXin_alpha/file_formats/conversion/plugins/rtf_input.py tests/file_formats/rtf/test_rtf_malformed_hostile.py`
  - clean
- `python3 -m pytest tests/file_formats/rtf/test_rtf_malformed_hostile.py -q`
  - `7 passed`
- `python3 -m pytest tests/file_formats/rtf -q`
  - `39 passed`
- `python3 -m pytest tests/file_formats/txt tests/file_formats/textile tests/file_formats/markdown -q`
  - `76 passed`
- `git diff --check`
  - clean
- `python3 -m pytest tests/file_formats -q`
  - `583 passed, 1 skipped, 124 warnings`

## Next

- Reuse the same framework in existing Markdown/Textile tests instead of adding
  more inline unicode corpora.
- Add a thin conversion-combinatorics harness that can express
  source-format/input-encoding/output-surface expectations without duplicating
  corpus data.
- Work through the rich text formats one family at a time, with particular
  attention to normalization, bidi/ZWJ preservation, and explicit malformed
  encoding behavior.
