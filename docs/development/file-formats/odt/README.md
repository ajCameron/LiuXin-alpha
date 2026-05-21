# ODT File Format Notes

## Status

ODT conversion currently runs through `Extract` in
`src/LiuXin_alpha/file_formats/odt/input.py`, with the plugin path entering
through `ODTInput`. The conversion product is a work directory containing
`metadata.opf`, `index.xhtml`, `odfpy.css`, and copied `Pictures/...` assets.

The focused test fixtures live in:

- `tests/support/file_format_odt.py`
- `tests/file_formats/odt/test_odt_container_framework.py`
- `tests/file_formats/odt/test_odt_malformed_hostile.py`

## Container Contract

Default ODT conversion is strict at the archive boundary. It should reject an
input before partial conversion output is produced when the file is not a
credible ODT container.

Current preflight requires:

- a readable ZIP archive
- `META-INF/manifest.xml`
- `meta.xml`
- `content.xml`
- no more than `4096` archive members
- no member expanding beyond `256 MiB`
- no archive expanding beyond `512 MiB` total
- no member at or above `1 MiB` with a compression ratio above `1000`
- no non-empty member reporting a zero compressed size

Embedded picture extraction should accept valid nested and non-ASCII
`Pictures/...` paths, create needed subdirectories, and reject archive member
names that would escape the output `Pictures` directory.

## Guarded Override Direction

Some real-world ODT files may exceed conservative archive budgets without being
malicious. Future override support should make those cases possible for trusted
input, but it should not be a blanket archive-safety bypass.

Do not make these checks overridable:

- path traversal or absolute-path extraction attempts
- destination paths outside the conversion work directory
- unreadable ZIP structure
- structural failures that make the conversion product undefined

These limits may be overridable only through explicit trusted-input profiles:

- archive member count
- maximum expanded size for a single member
- maximum total expanded size
- compression-ratio threshold

The intended shape is:

- `default`: conservative limits for normal ingestion
- `relaxed`: larger bounded limits for unusual but trusted books
- `trusted-custom`: caller-provided bounded limits, recorded in diagnostics

Even trusted modes should stay bounded. Error messages should report the
observed value, the active limit, and which profile or setting would be relevant
for a user trying to convert a known-safe file.

## Unicode And Locale Coverage

The current fixtures exercise multilingual title, author, body text, comments,
and nested picture names. Tests should continue to assert generated XHTML and
OPF bytes directly enough to catch replacement characters, dropped combining
marks, and broken non-ASCII asset paths.

## Hostile Corpus

The checked-in hostile corpus currently covers:

- missing required archive members
- malformed `content.xml`
- invalid UTF-8 in declared UTF-8 XML
- non-ZIP payloads sent through `ODTInput`
- nested valid picture paths
- picture path traversal and absolute-looking archive names
- archive member-count budget failures
- oversized expanded members
- excessive total expanded archive size
- suspicious compression-ratio payloads

Future ODT work should add named regressions here when a real fixture exposes a
new failure mode.
