# FB2 File Format Notes

## Status

FB2 conversion enters through `FB2Input` and `FB2Output` in
`src/LiuXin_alpha/file_formats/conversion/plugins/`. The output serializer is
`FB2MLizer` in `src/LiuXin_alpha/file_formats/fb2/fb2ml.py`.

FB2 is XML-backed rather than archive-backed. The current hardening pass
therefore focuses on XML encoding, parser recovery, embedded base64 binaries,
safe extraction into the conversion work directory, and unicode-preserving
input/output conversion.

The focused test fixtures live in:

- `tests/support/file_format_fb2.py`
- `tests/file_formats/fb2/test_fb2_unicode_framework.py`
- `tests/file_formats/fb2/test_fb2_malformed_hostile.py`
- `tests/file_formats/fb2/test_fb2_output_unicode_framework.py`

The reusable fixture builds multilingual FB2 documents with title/author,
description, keywords, publisher metadata, body text, optional cover images,
extra embedded binaries, UTF-8 or UTF-16 XML encodings, and text rewrite helpers
for malformed-input cases.

## Input Contract

Default FB2 input conversion should produce `metadata.opf`, `index.xhtml`,
`inline-styles.css`, and any extracted embedded binaries in the conversion
work directory.

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

FB2 currently has no archive-bomb budget because the default format is a single
XML payload. Zipped FB2 reader/writer paths should be covered separately if
they become part of conversion hardening.

## Unicode And Locale Coverage

The current fixtures exercise multilingual title, author names, publisher,
description, keywords, body text, embedded binary IDs, output XHTML, OPF, CSS,
and output FB2 serialization. UTF-16 input is included as a regression case for
the input plugin.

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
- output serialization with an unserializable surrogate metadata boundary

Future regressions should be added here when real-world FB2 files expose new
edge cases, especially around zipped `.fb2.zip` payloads, unusual declared
encodings, deeply nested sections, notes/citations, binary MIME mismatches, and
metadata reader/writer parity.

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
