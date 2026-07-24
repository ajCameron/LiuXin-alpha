# FB2 File Format Notes

## Status

FB2 conversion enters through `FB2Input` and `FB2Output` in
`src/LiuXin_alpha/file_formats/conversion/plugins/`. The output serializer is
`FB2MLizer` in `src/LiuXin_alpha/file_formats/fb2/fb2ml.py`.

FB2 is XML-backed by default. `.fbz` is the archive-backed FB2 variant and is
handled as a strict single-FB2-member zip container. The current hardening pass
therefore covers XML encoding, parser recovery, embedded base64 binaries, safe
extraction into the conversion work directory, archive preflight, and
unicode-preserving input/output conversion.

The focused test fixtures live in:

- `tests/support/file_format_fb2.py`
- `tests/support/file_format_zip.py`
- `tests/file_formats/fb2/test_fb2_unicode_framework.py`
- `tests/file_formats/fb2/test_fb2_malformed_hostile.py`
- `tests/file_formats/fb2/test_fb2_zip_framework.py`
- `tests/file_formats/fb2/test_fb2_output_unicode_framework.py`

The reusable fixture builds multilingual FB2 documents with title/author,
description, keywords, publisher metadata, body text, optional cover images,
extra embedded binaries, UTF-8 or UTF-16 XML encodings, zipped `.fbz`
containers, and text/archive rewrite helpers for malformed-input cases.

## Input Contract

Default FB2/FBZ input conversion should produce `metadata.opf`, `index.xhtml`,
`inline-styles.css`, and any extracted embedded binaries in the conversion work
directory.

Current input behavior:

- XML encoding detection happens before decoded NUL cleanup, so UTF-16 input is
  not corrupted by raw byte filtering
- wrong-format or unrecoverable non-XML payloads fail before partial output
- malformed-but-recoverable XML can still convert when the recovery parser can
  produce a usable document
- embedded binary IDs are sanitized before writing files to the conversion work
  directory
- parent traversal, normalized traversal, absolute-looking, Windows
  drive-looking, slash-containing, and backslash-containing binary IDs are
  rewritten to local `fb2_binary_....<ext>` names
- XHTML image references and OPF cover references use the sanitized filename
  map, not the original hostile ID
- invalid embedded base64 payloads are skipped with a warning instead of
  aborting otherwise usable text conversion

## FBZ Archive Contract

`.fbz` input and metadata paths use the same archive-safety shape as the other
container formats:

- non-zip `.fbz` payloads and corrupt zip payloads fail before conversion
  output is created
- exactly one non-directory `.fb2` member is required
- archives with no `.fb2` member or multiple `.fb2` members fail clearly
- archive member names are rejected if they contain backslashes, parent
  traversal, absolute paths, or Windows drive-looking paths
- preflight rejects excessive member counts, oversized members, excessive
  total expansion, invalid compressed sizes, and suspicious compression ratios
- non-FB2 extra members are not extracted into the conversion work directory
- metadata read/write preserves unrelated safe zip members while replacing the
  selected FB2 member

The archive checks live in
`src/LiuXin_alpha/file_formats/fb2/archive.py` so conversion and metadata share
the same member selection and budget policy.

## Unicode And Locale Coverage

The current fixtures exercise multilingual title, author names, publisher,
description, keywords, body text, embedded binary IDs, output XHTML, OPF, CSS,
and output FB2 serialization. UTF-16 input is included as a regression case for
the input plugin. `.fbz` conversion coverage repeats the generated-product
assertions for UTF-8 and UTF-16 zipped payloads, including OPF/XHTML/CSS output
and extracted embedded binaries.

Output-side tests use a richer reusable OEB fixture in
`tests/support/file_format_oeb.py` to drive `FB2MLizer` and `FB2Output` with
multilingual metadata, styled XHTML body text, table-of-contents data, and an
embedded image. Tests assert valid UTF-8 XML output, base64 image serialization,
and no replacement characters in normal multilingual content.

## Hostile Corpus

The checked-in hostile corpus currently covers:

- wrong-format non-XML FB2 payloads
- bad declared XML encoding
- recoverable malformed XML containing multilingual text
- corrupted embedded base64 payloads
- parent traversal, normalized traversal, absolute-looking, Windows
  drive-looking, slash-containing, and backslash-containing embedded binary IDs
- unsafe cover IDs reaching both XHTML and OPF generation
- odd but valid unicode binary IDs
- corrupt/non-zip `.fbz` payloads
- `.fbz` archives with no FB2 member or multiple FB2 members
- unsafe `.fbz` member paths
- zip-bomb-shaped `.fbz` member count, size, total expansion, and compression
  ratio limits
- output serialization with an unserializable surrogate metadata boundary

Future regressions should be added here when real-world FB2 files expose new
edge cases, especially around unusual declared encodings, deeply nested
sections, notes/citations, binary MIME mismatches, and metadata reader/writer
parity.

## Loss And Reporting Direction

There is no FB2-specific conversion report today. Current recovery is visible
through warnings for skipped corrupted binaries, unsafe embedded binary IDs, and
cover references that cannot be used.

The output plugin writes with UTF-8 replacement for unserializable content. The
surrogate boundary is covered so the behavior stays explicit, but broader loss
reporting should eventually record:

- phase, for example `fb2-input-binary` or `fb2-output-serialize`
- original embedded binary ID and sanitized filename
- skipped binary ID, MIME type, and decode failure reason
- XML parser recovery path used and whether unrecoverable fragments were
  dropped
- number and sample of characters replaced during output serialization

## Sign-Off Status

FB2/FBZ is signed off for the current format scope as of 2026-05-31.

The signed-off scope includes:

- XML-backed `.fb2` input conversion for UTF-8 and UTF-16 fixtures
- strict single-FB2-member `.fbz` archive selection and preflight
- metadata read/write for plain and zipped FB2 payloads
- `FB2MLizer`/`FB2Output` unicode serialization
- hostile XML, archive, embedded-binary ID, and corrupt-base64 boundaries
- generated OPF, XHTML, CSS, extracted-binary, and output-FB2 product
  assertions

Focused sign-off validation:

```text
python3 -m pytest tests/file_formats/fb2 tests/metadata/file_sources/test_fb2_metadata_source.py tests/metadata/file_sources/test_fb2_edge_cases.py -q
76 passed in 14.17s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/fb2/test_fb2_malformed_hostile.py tests/file_formats/fb2/test_fb2_zip_framework.py -q
48 passed in 12.08s
```

The broader pipeline goal of structured `ConversionReport` events for every
recoverable warning remains outside this format sign-off row. Future real-corpus
FB2/FBZ defects should be added as regressions against this contract.
