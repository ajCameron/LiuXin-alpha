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

## Current Scope

The first slice builds reusable fixture infrastructure:

- generate a minimal real EPUB container with OCF `container.xml`
- write `mimetype` first and stored
- create a nested OPF package, XHTML spine item, NCX, CSS, and optional image
  asset
- carry the shared multilingual corpus through OPF metadata and XHTML content
- preserve nested non-ASCII resource paths
- provide archive rewrite helpers for later malformed-container tests

## Next

- Use the fixture through `EPUBInput.convert` and metadata readers to assert
  multilingual content survives real conversion/read paths.
- Add strict missing/malformed container cases before changing source behavior.
