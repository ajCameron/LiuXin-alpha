# File Formats ODT Container Hardening Slice

Branch: `file-formats-odt-container-hardening`

Started after PR #72 merged. This is the first archive/XML container pass after
the text, markup, RTF, and PML unicode/malformed-input work.

Durable docs:

- `dev-docs/file-format-unicode-conversion.md`
- `dev-docs/file-formats/odt/README.md`

## Scope

The ODT slice extends the shared file-format test framework into container
behavior:

- reusable ODT fixture creation
- archive member rewriting for malformed-container cases
- multilingual metadata/body text through `Extract` and `ODTInput.convert`
- generated `metadata.opf`, `index.xhtml`, `odfpy.css`, and copied picture
  assets
- required-member validation and hostile asset paths
- bounded zip-bomb-shaped archive preflight checks
- ODT-specific durable docs for container policy and future guarded overrides

## Changes

- Added `tests/support/file_format_odt.py`.
- Added `tests/file_formats/odt/test_odt_container_framework.py`.
- Added `tests/file_formats/odt/test_odt_malformed_hostile.py`.
- Hardened `Extract.validate_container_members` so conversion rejects ODT
  archives missing `META-INF/manifest.xml`, `meta.xml`, or `content.xml`
  before generating partial output.
- Added bounded archive preflight checks for member count, per-member expanded
  size, total expanded size, and suspicious compression ratios.
- Added `dev-docs/file-formats/odt/README.md` and
  `dev-docs/file-formats/README.md` so future file-format work can keep
  one durable folder per format.
- Documented the future guarded override direction: trusted modes may raise
  bounded archive budgets, but must not bypass path traversal, unreadable ZIP,
  or undefined-product failures.
- Hardened `Extract.extract_pictures` so:
  - nested `Pictures/...` members are copied with directories created
  - non-ASCII nested asset paths are preserved
  - `Pictures/../...`, `Pictures/../../...`, and absolute-looking archive
    names are skipped instead of being written outside the intended tree

## Validation

- `python3 -m py_compile src/LiuXin_alpha/file_formats/odt/input.py tests/support/file_format_odt.py tests/file_formats/odt/test_odt_container_framework.py tests/file_formats/odt/test_odt_malformed_hostile.py`
  - clean
- `python3 -m pytest tests/file_formats/odt/test_odt_container_framework.py tests/file_formats/odt/test_odt_malformed_hostile.py -q`
  - `13 passed`
- `python3 -m pytest tests/file_formats/odt tests/file_formats/odf -q`
  - `23 passed`
- `python3 -m pytest tests/file_formats -q`
  - `617 passed, 1 skipped, 127 warnings`

## Next

- Consider OPF or EPUB next. OPF is narrower and metadata-heavy; EPUB is the
  natural next archive/container step if we want spine/manifest/resource
  behavior immediately.
- When a guarded override is implemented, make it a named trusted-input budget
  profile with explicit diagnostics rather than a broad safety bypass.
