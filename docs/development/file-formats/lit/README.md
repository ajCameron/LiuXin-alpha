# LIT File Format Notes

## Status

LIT input conversion enters through `LITInput` in
`src/LiuXin_alpha/file_formats/conversion/plugins/lit_input.py`. The parser is
`LitReader` and `LitFile` in `src/LiuXin_alpha/file_formats/lit/reader.py`.
Output conversion enters through `LITOutput` and serializes with `LitWriter` in
`src/LiuXin_alpha/file_formats/lit/writer.py`.

LIT is a legacy Microsoft Reader binary container. It is not ZIP-backed, but it
has equivalent structural risk: a primary header, secondary header blocks,
directory entries, a manifest, section namelist data, transformed sections, and
binary OPF/XHTML markup decoded by `UnBinary`.

The focused test fixtures live in:

- `tests/support/file_format_lit.py`
- `tests/file_formats/lit/test_lit_conversion_unicode_framework.py`
- `tests/file_formats/lit/test_lit_parser_framework.py`
- `tests/file_formats/lit/test_lit_malformed_hostile.py`
- `tests/file_formats/lit/test_lit_modernized.py`
- `tests/file_formats/lit/test_lit_end_to_end_and_unicode_torture.py`

The reusable LIT support module intentionally builds small parser-facing
payloads rather than full valid LIT books. It covers in-memory `LitFile`
objects, manifest payloads, namelist payloads, sized UTF-8 strings, binary HTML
fragments for `UnBinary`, and log/options helpers shared by the LIT tests.
Output-side conversion tests use the shared minimal OEB fixtures and stop at
`ReBinary`/manifest serialization because this environment does not currently
provide the LZX compressor backend needed to emit a complete `.lit` archive.

## Parser Contract

Default LIT input is strict at the binary parser boundary. Wrong-format,
truncated, or structurally incoherent data should raise `LitError` or a named
domain exception such as `DRMError`, not raw `struct.error`, `IndexError`, or
an unbounded loop.

Current parser behavior:

- wrong magic raises `LitError("Not a valid LIT file")`
- truncated primitive fields raise `LitError` with a truncated-field message
- unterminated encoded integers raise `LitError`
- malformed or unknown secondary-header blocks raise `LitError`
- unknown secondary-header blocks do not loop forever
- truncated manifest and namelist payloads raise `LitError`
- truncated binary markup raises `LitError`
- invalid UTF-8 in LIT-sized strings or binary markup raises `LitError`
- truncated atom table headers raise `LitError`
- DRM level 5 remains a hard `DRMError`
- lower DRM fallback remains best-effort for unencrypted sections, but
  encrypted sections without a title key remain blocked

The reader allows the observed real-corpus `ITSF` secondary-header shape where
the fields used by the parser occupy 32 bytes, even though legacy code advances
by the older 48-byte block stride.

## Unicode And Locale Coverage

Current parser-level coverage exercises:

- multilingual sized UTF-8 strings
- valid and invalid UTF-8 scalar boundaries
- manifest paths and IDs with non-ASCII text
- namelist section names with non-ASCII text
- `UnBinary` XHTML text and attributes with the shared multiscript corpus
- internal LIT href resolution through manifest items
- optional real-corpus LIT conversion when `LiuXin_alpha_data` fixtures are
  present
- optional real-corpus conversion-product checks for replacement characters
- `LITInput.postprocess_book` preservation of multiscript `<pre>` text while
  rewriting it to XHTML paragraphs
- output-side `ReBinary` serialization of shared OEB XHTML with non-ASCII text,
  anchors, and styles
- `LitWriter` manifest serialization of non-ASCII item IDs and nested paths

## Hostile Corpus

The checked-in hostile corpus currently covers:

- wrong-format whole-file payloads
- truncated primary headers
- truncated secondary headers
- unknown secondary-header blocks
- truncated manifest file-count and entry fields
- invalid UTF-8 inside manifest strings
- truncated namelist length/name fields
- unterminated variable-length encoded integers
- truncated `UnBinary` control sequences
- invalid UTF-8 inside binary-markup text
- truncated atom table headers
- invalid hostile UTF-8 byte sequences at the low-level reader boundary

Future regressions should be added here when real LIT files expose new failure
modes, especially around transform lists, LZX reset tables, encrypted sections,
broken OPF binary markup, atom tables, malformed anchors, and suspiciously large
declared section sizes.

## Salvage And Reporting Direction

There is no general LIT salvage mode today. Default parsing should remain
strict when structure is undefined, because partial binary-container recovery
can easily hide dropped sections or corrupt spine order.

The existing DRM fallback is a narrow best-effort path: if lower-level DRM
setup cannot initialize, the reader may continue for unencrypted sections, but
encrypted content without a title key still fails. That behavior should remain
visible through warnings.

If future recovery is added for malformed-but-readable LIT files, it should be
explicit and reportable:

- recovery must be opt-in or limited to a clearly named trusted-input profile
- unknown header pieces, impossible offsets, missing manifest/namelist data,
  and unavailable encrypted content must remain hard failures unless a specific
  recovery rule can prove the conversion product is still defined
- diagnostics should record secondary-header quirks, skipped directory entries,
  skipped manifest items, transform/decompression failures, atom-table damage,
  and any text replacement during binary-markup decoding
- conversion tests should assert both outcomes: a usable conversion product and
  visible reporting of every relaxed or skipped piece

## Validation

High-value focused commands:

- `python3 -m pytest tests/file_formats/lit -q`
- `python3 scripts/run_file_formats_lane.py --lane fast`

The current LIT hardening branch last validated:

- `python3 -m pytest tests/file_formats/lit -q` -> `58 passed`
- `python3 scripts/run_file_formats_lane.py --lane fast` -> `596 passed, 1 skipped`
