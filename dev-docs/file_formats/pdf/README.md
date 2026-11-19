# PDF File Format Notes

Reviewed **2026-11-19**. The metadata reader, its limits and the PDF metadata corpus were reviewed. See [next tasks](../next_tasks.md). Sign-off sections describe their dated fixture scope; they do not certify every input, optional dependency or installed provider today.

## Status

PDF metadata reading enters through `get_metadata` in
`src/LiuXin_alpha/metadata/file_sources/pdf.py`, registered as the
"Read PDF metadata" reader plugin. That module maps Info and XMP onto LiuXin's
metadata record. The PDF itself is read by a bounded, pure-Python object reader:

| Module | Role |
| --- | --- |
| `file_formats/pdf/objects.py` | `read_metadata`, `PdfDocument`: cross-reference data, object loading, the fallback scan, Info and XMP |
| `file_formats/pdf/syntax.py` | The lexer and object parser; `decode_text` for PDF text strings |
| `file_formats/pdf/filters.py` | Flate (with PNG and TIFF predictors), ASCIIHex and ASCII85 |
| `file_formats/pdf/security.py` | The Standard security handler, opened with an empty password |
| `file_formats/pdf/errors.py` | `PdfReadError` and `PdfEncryptedError` |

Writing metadata (`set_metadata`) still uses pypdf when it is installed. pypdf is not a
declared dependency.

calibre's own PDF reader is ported too (#202), as two providers:

- **`calibre-port.read-pdf-metadata`** (priority −50) runs poppler's `pdfinfo`
  for the fields and `pdftoppm` for a cover, as calibre does. It is available
  where `pdfinfo` is on the PATH.
- **`calibre-port.read-pdf-metadata-without-poppler`** (−55) runs the same
  calibre code on fields this reader gives, shaped as `pdfinfo` prints them, so
  PDFs are read everywhere, without a cover.

The port runs poppler through `utils/bounded_process.py`, with these limits:

| | `pdfinfo` | `pdftoppm` |
| --- | --- | --- |
| Timeout | 30 s | 60 s |
| CPU time | 30 s | 60 s |
| Address space | 1 GiB | 1 GiB |
| Output | 8 MiB | 64 MiB |

A page larger than 20 inches is drawn at 3,000 pixels on its longest side. See
the [port status page](../../plugins/upstream-calibre-port-status.md#hostile-input)
for what that fixes.

## How the reader finds objects

The reader follows the cross-reference data as a viewer does. It handles:

- classic tables, read entry by entry from the file when their lines are the regular
  20 bytes, and entry by entry from a window when they are not;
- cross-reference streams, with their PNG predictors;
- hybrid files (a table plus `/XRefStm`);
- incremental updates chained by `/Prev`, newest first;
- object streams;
- files with junk before `%PDF-`, whose offsets are relative to the header.

When that data is missing or wrong, one linear scan indexes every `N G obj` and a few
landmarks: `trailer` keywords, cross-reference streams, object streams, catalogs and
dictionaries with Info keys. The trailers it finds are then merged newest first. If the
file has no trailer at all, the reader takes the last catalog and the last dictionary
that looks like Info. An outline item has a `/Title` too, but its `/Parent`, `/Dest` and
similar keys rule it out.

Only the objects metadata needs are loaded: the trailer, the Info dictionary, the
catalog, its metadata stream, the Encrypt dictionary, and any object streams that hold
them. Paths and seekable binary streams are read range by range, so a 500 MB scan does
not enter memory.

## Encryption

Many PDFs are encrypted only to carry permission flags. Their user password is empty,
but every string and stream is still ciphertext. The old reader returned that ciphertext
as the title and authors. `security.py` derives the file key as a viewer does:

1. It tries the empty user password.
2. If that fails, it tries the empty owner password.

| Revision | Cipher | Needs |
| --- | --- | --- |
| 2–4 | RC4, 40 to 128 bits | nothing (pure Python, at most `rc4_stream_bytes` per stream) |
| 4 | AES-128 | the `cryptography` package (the `encryption` extra) |
| 5–6 | AES-256 | the `cryptography` package |

When no empty password opens the file, the reader raises `PdfEncryptedError`, which the
metadata source turns into `PdfParseError`. It does the same when AES is needed and
`cryptography` is missing. It never returns ciphertext. Metadata streams are left alone
when `/EncryptMetadata` is false, as are streams whose first filter is the Identity crypt
filter, and cross-reference streams.

## Text strings

`decode_text` follows the specification. A string with a UTF-16 byte order mark is
UTF-16, and PDF 2.0 language escapes are removed from it. A string with a UTF-8 byte
order mark is UTF-8. Every other string is PDFDocEncoding, which differs from Latin-1 in
0x18–0x1F and 0x80–0xA0: 0x84 is an em dash, for example, and 0x93 is the "fi"
ligature. The old reader decoded such strings as UTF-8 with replacement, so "café"
became "caf�".

There is one deliberate exception: non-ASCII bytes that are valid UTF-8 are read as
UTF-8. Many tools write raw UTF-8 into Info strings, and valid multi-byte UTF-8 is rarely
meant as PDFDocEncoding.

## Limits

`PdfLimits` bounds the work any one file can cause:

| Limit | Default | What happens when it is reached |
| --- | --- | --- |
| `object_bytes` | 1 MiB | An object outside stream data larger than this is refused |
| `stream_bytes` | 64 MiB | A stream's raw data larger than this is refused |
| `decoded_bytes` | 32 MiB | A stream decoding larger than this is refused |
| `total_decoded_bytes` | 128 MiB | Decoding stops for the document |
| `rc4_stream_bytes` | 4 MiB | Larger RC4 streams are refused (pure Python is slow) |
| `depth` | 64 | Deeper nesting is refused, with no recursion error |
| `xref_sections` | 256 | `/Prev` is followed no further |
| `xref_subsections` | 65,536 | The table is abandoned for a scan |
| `irregular_table_bytes` | 8 MiB | An irregular table larger than this is abandoned for a scan |
| `objects` | 4,096 | No more objects are loaded |
| `xmp_bytes` | 4 MiB | The XMP packet is dropped |

A limit reached while reading Info or XMP costs that part only. `PdfMetadata.problems`
records what was lost, and the metadata source logs it at DEBUG.

## Hostile inputs

| Input | Old reader | Now |
| --- | --- | --- |
| Many `N 0 obj` without `endobj` (the `.*?endobj` regex) | Quadratic: 72 KB took 3.9 s, so 1.8 MB would take about 40 minutes | Linear: 1.8 MB in 0.12 s |
| A Flate bomb as the metadata stream | Inflated without bound | Stops at 32 MiB; Info is still read |
| An unterminated string or 3 MB of whitespace in an object | Scanned to the end of the file, once per object | Stops at `object_bytes` |
| 100,000 nested arrays | Read without bound | Refused at depth 64 |
| A `/Prev` loop, a reference loop or chain, a table claiming 4 billion entries, an object stream claiming a billion objects | Not reachable (no xref support) | Each terminates; the file still reads where it can |
| A file that looks encrypted but has no trailer | Returned ciphertext | `PdfEncryptedError` |

## Agreement with poppler

`tests/fixtures/pdf_metadata` holds one small PDF per storage layout. Each records what
poppler's `pdfinfo` 24.02 read from it. `scripts/make_pdf_metadata_corpus.py` regenerates
the files, which needs pypdf, qpdf and poppler; the tests need none of these.

The layouts are:

- plain files and object streams;
- incremental updates, with and without object streams;
- linearized files;
- RC4-40, RC4-128, AES-128 and AES-256 encryption;
- a user password, an empty owner password, and cleartext XMP;
- UTF-16 strings and raw UTF-8 strings;
- XMP only, and XMP alongside Info;
- broken and hybrid cross-reference data;
- indirect Info values;
- leading and trailing junk;
- a wrong stream length;
- a 200-inch page.

The reader agrees with `pdfinfo` on every Info field of every case, except in three
cases where it reads more than poppler does:

- **A hybrid file whose table lists an object as free while `/XRefStm` lists it.**
  poppler does not consult the stream; qpdf, pypdf and this reader do.
- **Raw UTF-8 Info strings.** poppler shows them as PDFDocEncoding mojibake.
- **AES-128 with `/EncryptMetadata false`.** The XMP packet is stored in clear text, but
  poppler decrypts it anyway and returns garbage.

The old reader found nothing in files that keep their Info dictionary in an object
stream, as many writers have done since PDF 1.5. It returned ciphertext for every
encrypted file, and garbled PDFDocEncoding text.

## Tests

- `tests/file_formats/pdf/test_pdf_object_reader.py`: corpus agreement with `pdfinfo`,
  RC4 and AES paths with and without `cryptography`, the lexer, and hostile inputs with
  time bounds.
- `tests/metadata/file_sources/test_pdf_storage_layouts.py`: the metadata source on
  every layout, by path and by stream.
- `tests/file_formats/providers/test_ported_pdf_reader.py`: the calibre port with
  and without poppler on every layout, covers, fake tools that hang, print
  forever or crash, and the XMP fixes.
- `tests/metadata/file_sources/test_pdf_edge_cases.py`,
  `tests/metadata/file_sources/test_pdf_metadata_source.py` and
  `tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py`: mapping, fallback
  policy and writing.

## calibre's handling

These are things the provider without poppler does differently from calibre with
poppler:

- **Values with a line break.** `pdfinfo` prints a value with a line break across
  lines, and calibre reads its output line by line. A title keeps only its first
  line, and a later line shaped like `Keywords: …` becomes that field. The
  provider without poppler joins the lines with spaces.
- **Date formats.** calibre reads dates in `pdfinfo -isodates` form; the provider
  without poppler writes them the same way (`2019-06-01T12:00:00+02`), keeping
  text that is not a date.

## Open follow-ups

- AES without `cryptography` is refused. A pure-Python AES would read those files
  everywhere, at the cost of carrying cipher code; the provider that runs poppler covers
  them when poppler is installed.
- The reader takes no cover. A scanned book's first page is often a single JPEG image
  XObject that could be returned as-is without rendering.
- Writing still needs pypdf; see the project's open items about making it an optional
  dependency.
