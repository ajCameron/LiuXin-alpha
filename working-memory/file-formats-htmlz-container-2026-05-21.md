# HTMLZ Container Hardening Handoff - 2026-05-21

Branch: `file-formats-htmlz-container-hardening`

## Scope

This branch hardens HTMLZ input conversion after the ODT, EPUB, and DOCX
container passes. HTMLZ is ZIP-backed, but its contract is intentionally smaller
than EPUB: a top-level HTML/XHTML file is required; OPF metadata and cover
references are optional enrichment.

## Implemented

- Added reusable HTMLZ fixtures in `tests/support/file_format_htmlz.py`.
- Added fixture and valid conversion coverage in
  `tests/file_formats/htmlz/test_htmlz_container_framework.py`.
- Added malformed and hostile coverage in
  `tests/file_formats/htmlz/test_htmlz_malformed_hostile.py`.
- Hardened `HTMLZInput` in
  `src/LiuXin_alpha/file_formats/conversion/plugins/htmlz_input.py`.
- Added durable docs in `docs/development/file-formats/htmlz/README.md`.
- Updated `docs/development/file-format-unicode-conversion.md` to include
  HTMLZ in the container-format direction.

## Fixture Contract

The reusable builder can create `.htmlz` archives with:

- top-level `index.html` or caller-selected top-level HTML/XHTML members
- optional top-level `metadata.opf`
- optional CSS and cover image assets
- non-ASCII nested asset paths
- multilingual title, authors, description, publisher, subject, body text, and
  image alt text
- rewrite helpers for removing, replacing, and adding archive members
- a recorded downstream HTML-input stub for focused `HTMLZInput` tests

## Conversion Contract

`HTMLZInput` now preflights before extraction:

- invalid ZIPs fail before partial output
- missing top-level HTML fails before the downstream HTML plugin is called
- unsafe archive member paths fail before extraction
- member count, per-member expanded size, total expanded size, suspicious
  compression ratio, and invalid compressed-size shapes are rejected
- preflight failures are logged as
  `HTMLZ preflight rejected <path>: <reason>`

After extraction:

- empty selected top-level HTML remains a hard failure
- multiple top-level HTML files warn and only the selected file is used
- malformed optional OPF logs a warning and conversion continues
- missing optional cover files log a warning and conversion continues
- unsafe optional OPF cover references are ignored with a warning
- valid non-ASCII cover paths are attached to the OEB manifest/guide

## Validation

Latest validation on this branch:

- `python3 -m py_compile src/LiuXin_alpha/file_formats/conversion/plugins/htmlz_input.py tests/file_formats/htmlz/test_htmlz_malformed_hostile.py tests/file_formats/htmlz/test_htmlz_container_framework.py tests/support/file_format_htmlz.py`
- `python3 -m pytest tests/file_formats/htmlz -q` -> `31 passed`
- `python3 -m pytest tests/file_formats/conversion/plugins tests/file_formats/conversion/test_conversion_top_level_smoke.py -q` -> `10 passed`
- `python3 -m pytest tests/file_formats -q` -> `692 passed, 1 skipped, 127 warnings`
- `git diff --check`

## Follow-Ups

- Commit and PR this branch when ready.
- Consider a shared archive-preflight helper later; ODT, EPUB, DOCX, and HTMLZ
  now carry very similar member-count, expansion, path-safety, and compression
  ratio checks.
- If trusted-input overrides are added, keep path safety and required
  conversion-product checks non-overridable.
- If HTMLZ gains deeper linked-resource recovery, report selected top-level
  HTML, ignored OPF/cover metadata, and dropped resources explicitly.
