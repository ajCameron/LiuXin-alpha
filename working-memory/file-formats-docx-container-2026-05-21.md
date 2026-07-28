# File Formats DOCX Container Hardening Slice

Branch: `file-formats-docx-container-hardening`

Started after PR #75 merged. DOCX is the next complex container target after
ODT and EPUB because it exercises ZIP package preflight, package relationships,
content types, WordprocessingML, document properties, styles, images, unicode
paths, and hostile archive behavior.

Durable docs:

- `dev-docs/file-format-unicode-conversion.md`
- `dev-docs/file-formats/docx/README.md`

## Plan

1. Start `file-formats-docx-container-hardening`.
2. Add reusable multilingual DOCX fixture framework.
3. Cover valid `DOCXInput`/`Convert` output with non-ASCII body, metadata,
   styles, and asset paths.
4. Cover malformed containers: missing `[Content_Types].xml`, missing
   `_rels/.rels`, missing main document, and malformed core XML.
5. Add hostile archive checks for path traversal, absolute/drive paths, member
   count, expanded-size, total-size, and suspicious compression-ratio limits.
6. Add durable DOCX format docs and salvage/reporting policy.

## Progress

The first slice created the branch from `main` after PR #75 merged.

The second slice adds reusable DOCX fixture infrastructure:

- `tests/support/file_format_docx.py` builds a minimal real DOCX ZIP package
- fixtures include `[Content_Types].xml`, `_rels/.rels`,
  `word/document.xml`, `word/_rels/document.xml.rels`, `word/styles.xml`,
  `docProps/core.xml`, and `docProps/app.xml`
- core/app metadata carries multilingual title, authors, description,
  publisher, subject, and keywords
- body text uses the shared multilingual corpus, including combining marks,
  bidirectional marks, CJK, Thai, Devanagari, Hebrew, Arabic, Cyrillic, Greek,
  and emoji
- optional media includes a nested non-ASCII PNG member at
  `word/media/深/cover_世界.png`
- helpers expose ZIP member reads, document text extraction, and rewrite
  support for later malformed/hostile tests
- `tests/file_formats/docx/test_docx_container_framework.py` pins the fixture
  package shape, XML parseability, unicode payload, optional extra assets, and
  rewrite helper behavior

The third slice uses that fixture through real DOCX conversion paths:

- `docx.to_html.Convert` converts the multilingual fixture into `index.html`,
  `docx.css`, extracted image assets, and `metadata.opf`
- `DOCXInput.convert` preserves the same unicode body and metadata through the
  plugin entry point and current working directory output path
- assertions cover title, split authors, description, publisher, normalized
  subject tags, unicode keywords, body fragments, image alt text, and nested
  PNG media extraction
- `src/LiuXin_alpha/file_formats/docx/to_html.py` now avoids unnecessary
  paragraph-margin comparison for multi-paragraph runs without visible borders,
  fixing a crash exposed by realistic multi-paragraph fixtures

The fourth slice covers malformed DOCX package structure:

- `tests/file_formats/docx/test_docx_malformed_hostile.py` checks missing
  `[Content_Types].xml`, missing `_rels/.rels`, missing `word/document.xml`,
  empty/wrong-root required XML parts, malformed main document XML, and
  malformed `docProps/core.xml`
- malformed conversion failures assert no `index.html`, `metadata.opf`,
  `docx.css`, or `images/` partial output is created
- `src/LiuXin_alpha/file_formats/docx/container.py` now reports malformed
  required package parts and missing main document paths as named `InvalidDOCX`
  failures instead of raw parser/key errors

The fifth slice adds hostile archive preflight before extraction:

- `DOCX.validate_container_members` rejects unreadable ZIP files, unsafe member
  paths, missing core package members, too many members, oversized members,
  excessive total expansion, invalid non-empty zero-compressed-size entries, and
  suspicious compression ratios before `extractall`
- path checks reject parent traversal, absolute-looking paths, Windows
  drive-looking paths, and raw backslash names when surfaced by the ZIP reader
- archive budgets match the ODT/EPUB defaults: `4096` members, `256 MiB` per
  member, `512 MiB` total expansion, and compression ratio `1000` for members
  at or above `1 MiB`
- hostile tests use strict subclasses to keep payloads small while exercising
  production budget checks

The sixth slice adds durable DOCX format docs:

- `dev-docs/file-formats/docx/README.md` records converter entry points,
  fixture locations, archive/package contract, current budgets, guarded
  override policy, unicode coverage, hostile corpus, and salvage/reporting
  direction
- `dev-docs/file-format-unicode-conversion.md` now records DOCX as the
  third archive/XML container pass after ODT and EPUB
- DOCX has no silent salvage mode today; future recovery should be opt-in,
  bounded, and report relaxed checks, selected package relationships, skipped
  media, ignored metadata, and unicode/markup loss

## Validation

- `python3 -m py_compile src/LiuXin_alpha/file_formats/docx/to_html.py tests/support/file_format_docx.py tests/file_formats/docx/test_docx_container_framework.py`
  - clean
- `python3 -m py_compile src/LiuXin_alpha/file_formats/docx/container.py src/LiuXin_alpha/file_formats/docx/to_html.py tests/support/file_format_docx.py tests/file_formats/docx/test_docx_container_framework.py tests/file_formats/docx/test_docx_malformed_hostile.py`
  - clean
- `python3 -m pytest tests/file_formats/docx/test_docx_container_framework.py -q`
  - `5 passed`
- `python3 -m pytest tests/file_formats/docx/test_docx_malformed_hostile.py -q`
  - `19 passed`
- `python3 -m pytest tests/file_formats/docx -q`
  - `30 passed`
- `python3 -m pytest tests/file_formats/conversion/plugins tests/file_formats/conversion/test_conversion_top_level_smoke.py -q`
  - `10 passed`
- `python3 -m pytest tests/file_formats -q`
  - `668 passed, 1 skipped, 127 warnings`

## Next

- Run final validation, commit, and PR the DOCX branch.
