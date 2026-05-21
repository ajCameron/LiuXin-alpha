# File Formats EPUB Container Hardening Slice

Branch: `file-formats-epub-container-hardening`

Started after PR #73 merged. EPUB is the next archive/XML container target
after ODT because it exercises ZIP/OCF preflight, OPF package discovery,
manifest/spine correctness, XHTML/XML parsing, CSS/images/fonts, unicode paths,
and hostile archive behavior.

Durable docs:

- `docs/development/file-format-unicode-conversion.md`
- `docs/development/file-formats/epub/README.md`

## Plan

1. EPUB container fixture framework.
2. Valid multilingual EPUB conversion/read coverage.
3. Missing or malformed `mimetype`, `META-INF/container.xml`, OPF package,
   manifest, and spine coverage.
4. Hostile archive members and zip-bomb-shaped limits, using the ODT policy as
   the model.
5. Asset path and unicode filename coverage.
6. Loss/reporting TODOs for malformed-but-salvageable books.

## Progress

The first slice built reusable fixture infrastructure:

- generate a minimal real EPUB container with OCF `container.xml`
- write `mimetype` first and stored
- create a nested OPF package, XHTML spine item, NCX, CSS, and optional image
  asset
- carry the shared multilingual corpus through OPF metadata and XHTML content
- preserve nested non-ASCII resource paths
- provide archive rewrite helpers for later malformed-container tests

The second slice uses that fixture through real read/conversion paths:

- `EPUBInput.convert` extracts the fixture, normalizes a root `content.opf`,
  keeps multilingual OPF metadata, preserves nested non-ASCII resource hrefs,
  and leaves the XHTML/body corpus readable in the workdir.
- `metadata.file_sources.epub` reads the generated fixture from path and
  stream, preserves title/authors, rewinds streams, extracts the nested PNG
  cover, returns the inplace LiuXin metadata container, and works through the
  metadata reader plugin dispatch.
- Current known reader boundary: fixture OPF description/publisher/subject are
  present in the archive but are not surfaced by `get_metadata`; treat broader
  OPF field parity as a later metadata reader/writer refinement.

## Validation

- `python3 -m py_compile tests/file_formats/epub/test_epub_container_framework.py tests/metadata/file_sources/test_epub_metadata_source.py tests/support/file_format_epub.py`
  - clean
- `python3 -m pytest tests/file_formats/epub/test_epub_container_framework.py tests/metadata/file_sources/test_epub_metadata_source.py::test_epub_metadata_reads_generated_multilingual_fixture_path_stream_and_plugin -q`
  - `4 passed`
- `python3 -m pytest tests/file_formats/epub -q`
  - `11 passed`
- `python3 -m pytest tests/metadata/file_sources/test_epub_metadata_source.py tests/metadata/file_sources/test_epub_edge_cases.py -q`
  - `28 passed`

## Next

- Add strict missing/malformed container cases before changing source behavior.
