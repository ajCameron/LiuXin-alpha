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

Current focused coverage uses those fixtures through `EPUBInput` and the EPUB
metadata reader path, including path, stream, quick metadata, inplace metadata,
reader-plugin dispatch, and raster cover extraction for a nested non-ASCII
asset path.

`EPUBInput` now preflights the core OCF/OPF structure before extracting the
archive into the conversion workdir. Missing or malformed `mimetype`,
`META-INF/container.xml`, OPF package root, OPF manifest, or OPF spine inputs
fail before partial output appears.

Archive preflight also rejects member names that could escape or confuse the
conversion workdir, plus bomb-shaped archives using bounded checks for member
count, per-member expanded size, total expanded size, and suspicious
compression ratios.

Positive-path asset coverage now includes a deeply nested non-ASCII OPF path
and extra manifest assets with spaces, combining marks, Arabic text, CJK path
segments, and mixed media types. Those paths are extracted and preserved in the
normalized `content.opf`.

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

Default EPUB conversion is strict before extraction or partial output:

- require a readable ZIP archive
- require the EPUB `mimetype` member and OCF `META-INF/container.xml`
- resolve the package document through `container.xml`
- require a package-shaped OPF with manifest and spine entries
- reject archive member names that can escape the conversion work directory
- support valid nested and non-ASCII resource paths
- reject bomb-shaped archives with bounded member count, per-member expanded
  size, total expanded size, and suspicious compression-ratio checks

Current archive budgets match the ODT defaults:

- no more than `4096` archive members
- no member expanding beyond `256 MiB`
- no archive expanding beyond `512 MiB` total
- no member at or above `1 MiB` with a compression ratio above `1000`
- no non-empty member reporting a zero compressed size

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
- optional extra manifest assets for deeper resource-path coverage
- rewrite helpers for removing, replacing, and adding archive members in
  malformed-container tests

## Current Gaps

The generated fixture includes OPF description, publisher, and subject metadata,
but the current EPUB metadata reader path only surfaces title, authors, and
cover data from that fixture. Broader OPF field parity should be handled as a
separate metadata reader/writer refinement rather than hidden inside container
hardening.
