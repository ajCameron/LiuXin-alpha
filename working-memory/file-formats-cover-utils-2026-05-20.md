# File Formats Cover Utility Slice

Branch: `file-formats-utils-covers`

Started as a lateral pass from the metadata-source hardening work into
`file_formats`.

## Scope

Focused on deterministic cover-extraction helpers rather than broad legacy
reader/writer churn:

- `LiuXin_alpha.file_formats.extract_calibre_cover`
- `LiuXin_alpha.file_formats.render_html_svg_workaround`
- `LiuXin_alpha.file_formats.utils.extract_calibre_cover`
- `LiuXin_alpha.file_formats.utils.render_html_svg_workaround`

## Changes

- Added `tests/file_formats/test_cover_extraction_utils.py` covering both the
  top-level package helper surface and the `file_formats.utils` module.
- Fixed the top-level `extract_calibre_cover` path to delegate to
  `file_formats.utils` so the two import surfaces no longer drift.
- Fixed Python 3 cover extraction for byte-string HTML by decoding bytes before
  parsing.
- Replaced the inert vendored BeautifulSoup primary parse path with an
  `lxml.html` parse, retaining BeautifulSoup as a fallback.
- Covered:
  - raster image detection
  - embedded SVG image extraction
  - Calibre-style `<img alt="cover">` pages
  - body-only single-image cover pages
  - text-bearing pages that should not be treated as covers
  - static-cover precedence in `render_html_svg_workaround`
  - Qt renderer fallback dispatch without requiring PyQt
- Added `tests/file_formats/test_utils_parity.py` to keep top-level
  `file_formats` helpers and `file_formats.utils` helpers behaviorally aligned
  for format sniffing, CSS unit parsing/conversion, XPath escaping, and
  normalization.
- Extended `tests/file_formats/test_toc_corner_cases.py` with deterministic TOC
  tree and OPF-path coverage:
  - `count`, `purge`, `remove`, `depth`, `flat`, `top_level_items`, and
    `abspath`
  - guide-based HTML TOC discovery
  - NCX manifest item discovery
- Fixed a stale PML test assumption by explicitly hiding `PIL` during the
  "Pillow unavailable" test.

## Validation

- `python3 -m py_compile src/LiuXin_alpha/file_formats/__init__.py src/LiuXin_alpha/file_formats/utils.py tests/file_formats/test_cover_extraction_utils.py`
  - clean
- `python3 -m pytest tests/file_formats/test_cover_extraction_utils.py tests/file_formats/test_top_level_file_formats_helpers.py -q`
  - `24 passed`
- `python3 -m pytest tests/file_formats/test_cover_extraction_utils.py tests/file_formats/test_top_level_file_formats_helpers.py tests/metadata/file_sources/test_epub_edge_cases.py -q`
  - `29 passed`
- `python3 -m pytest tests/file_formats/pml/test_pml_modernized.py tests/file_formats/test_cover_extraction_utils.py -q`
  - `19 passed`
- `python3 -m pytest tests/file_formats/test_utils_parity.py tests/file_formats/test_toc_corner_cases.py tests/file_formats/test_cover_extraction_utils.py -q`
  - `56 passed`
- `python3 -m pytest tests/file_formats -q`
  - `529 passed, 1 skipped`

## Next

Good next file-format options:

- Continue the same low-risk utility lane with `file_formats.toc` /
  `file_formats.tweak` edge cases.
- Pick one legacy conversion plugin family at a time. Avoid broad OEB/MOBI/LRF
  sweeps unless there is a specific bug or fixture target.
- If coverage is the goal, `file_formats.utils`, `covers`, `txt.textileml`,
  and selected conversion plugins still have large deterministic gaps.
