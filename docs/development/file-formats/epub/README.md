# EPUB File Format Notes

## Status

EPUB is the next archive/XML container hardening target after ODT. The main
conversion entry point is `EPUBInput` in
`src/LiuXin_alpha/file_formats/conversion/plugins/epub_input.py`; output uses
`EPUBOutput` in the sibling plugin module.

The first slice establishes reusable EPUB fixture builders under
`tests/support/file_format_epub.py`. Those fixtures should be reused for valid
unicode conversion, malformed archive, OPF, spine, manifest, and asset-path
tests rather than creating one-off ZIP payloads in each test.

## Planned Sequence

1. EPUB container fixture framework.
2. Valid multilingual EPUB conversion/read coverage.
3. Missing or malformed `mimetype`, `META-INF/container.xml`, OPF package,
   manifest, and spine coverage.
4. Hostile archive members and zip-bomb-shaped limits, using the ODT policy as
   the model.
5. Asset path and unicode filename coverage.
6. Loss/reporting TODOs for malformed-but-salvageable books.

## Container Contract Direction

Default EPUB conversion should eventually be strict before extraction or partial
output:

- require a readable ZIP archive
- require the EPUB `mimetype` member and OCF `META-INF/container.xml`
- resolve the package document through `container.xml`
- require a package-shaped OPF with manifest and spine entries
- reject archive member names that can escape the conversion work directory
- support valid nested and non-ASCII resource paths
- reject bomb-shaped archives with bounded member count, per-member expanded
  size, total expanded size, and suspicious compression-ratio checks

As with ODT, future trusted-input overrides should raise bounded archive budgets
only. They should not bypass path safety, unreadable archive structure, missing
core container files, or failures that leave the conversion product undefined.

## Fixture Requirements

The reusable EPUB fixtures should preserve:

- `mimetype` as the first archive member and stored without compression
- `META-INF/container.xml`
- an OPF package below a nested content directory
- XHTML spine content containing the shared multilingual corpus
- NCX, CSS, and optional image assets
- nested unicode asset paths
- rewrite helpers for removing, replacing, and adding archive members in
  malformed-container tests
