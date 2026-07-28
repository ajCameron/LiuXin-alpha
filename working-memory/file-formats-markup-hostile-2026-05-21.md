# File Formats Markup Hostile Hardening Slice

Branch: `file-formats-markup-hostile-hardening`

Started after PR #71 merged. This is the optional lightweight-markup pass
before moving into ODT/container conversion.

Durable doc: `dev-docs/file-format-unicode-conversion.md`.

## Scope

This slice covers hostile Markdown/Textile/TXT markup without changing source
behavior. The intent is to lock down recovery and determinism around malformed
lightweight markup before the conversion work moves into archive/XML formats.

## Changes

- Added `tests/support/file_format_markup.py`.
- Added reusable hostile-markup cases for:
  - malformed Markdown links, images, tables, reference links, footnotes, raw
    HTML, and delimiter-heavy input
  - malformed Textile links, images, tables, references, no-textile regions,
    raw HTML, and delimiter-heavy input
- Added `tests/file_formats/markdown/test_markdown_malformed_hostile.py`.
- Added `tests/file_formats/textile/test_textile_malformed_hostile.py`.
- Added `tests/file_formats/txt/test_txt_markup_hostile.py`.
- Exercised `TXTInput` for `.md`, `.markdown`, and `.textile` extension-forced
  formatting with invalid UTF-8 bytes and multilingual text around hostile
  markup.
- Updated the durable file-format unicode/conversion doc to record:
  - delimiter-heavy hostile lightweight-markup tests as part of the reusable
    contract
  - future conversion planning as a capability graph, including direct
    `A -> B` paths and external-tool adapters alongside the legacy
    `input -> OEB -> output` path
- Added the conversion-pipeline graph follow-up to `dev-docs/global_todo.md`.

## Validation

- `python3 -m py_compile tests/support/file_format_markup.py tests/file_formats/markdown/test_markdown_malformed_hostile.py tests/file_formats/textile/test_textile_malformed_hostile.py tests/file_formats/txt/test_txt_markup_hostile.py`
  - clean
- `python3 -m pytest tests/file_formats/markdown/test_markdown_malformed_hostile.py tests/file_formats/textile/test_textile_malformed_hostile.py tests/file_formats/txt/test_txt_markup_hostile.py -q`
  - `12 passed, 3 warnings`
- `python3 -m pytest tests/file_formats/markdown tests/file_formats/textile tests/file_formats/txt -q`
  - `88 passed, 3 warnings`
- `python3 -m pytest tests/file_formats -q`
  - `604 passed, 1 skipped, 127 warnings`

## Next

- Move into ODT next. Use it to extend the reusable framework into archive/XML
  container fixtures: `content.xml`, manifest/content-file absence, malformed
  XML/UTF-8, embedded assets, generated `metadata.opf`, and `index.xhtml`.
- Later pipeline design should make direct format-to-format and external-tool
  conversions explicit capability edges, with version/discovery/timeout and
  loss-reporting behavior tested against the same unicode/malformed corpus.
