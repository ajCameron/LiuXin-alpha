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

## Validation

- `python3 -m py_compile tests/support/file_format_unicode.py tests/file_formats/test_unicode_framework.py tests/file_formats/txt/test_txt_unicode_framework.py`
  - clean
- `python3 -m pytest tests/file_formats/test_unicode_framework.py tests/file_formats/txt/test_txt_unicode_framework.py -q`
  - `14 passed`
- `python3 -m py_compile tests/support/file_format_conversion.py tests/support/file_format_unicode.py tests/file_formats/test_conversion_framework.py tests/file_formats/test_unicode_framework.py tests/file_formats/txt/test_txt_unicode_framework.py`
  - clean
- `python3 -m pytest tests/file_formats/test_conversion_framework.py tests/file_formats/test_unicode_framework.py tests/file_formats/txt/test_txt_unicode_framework.py -q`
  - `26 passed`
- `python3 -m pytest tests/file_formats/txt tests/file_formats/textile tests/file_formats/markdown -q`
  - `58 passed`
- `git diff --check`
  - clean
- `python3 -m pytest tests/file_formats -q`
  - `555 passed, 1 skipped, 124 warnings`

## Next

- Reuse the same framework in existing Markdown/Textile tests instead of adding
  more inline unicode corpora.
- Add a thin conversion-combinatorics harness that can express
  source-format/input-encoding/output-surface expectations without duplicating
  corpus data.
- Work through the rich text formats one family at a time, with particular
  attention to normalization, bidi/ZWJ preservation, and explicit malformed
  encoding behavior.
