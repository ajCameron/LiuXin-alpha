# PDF File Format Notes

Reviewed **2026-11-19**. The metadata reader, its limits and the PDF metadata corpus were reviewed. See [next tasks](../next_tasks.md). Sign-off sections describe their dated fixture scope; they do not certify every input, optional dependency or installed provider today.

## Status

PDF metadata reading enters through `get_metadata` in
`src/LiuXin_alpha/metadata/file_sources/pdf.py`, registered as the
"Read PDF metadata" reader plugin. That module maps Info and XMP onto LiuXin's
metadata record, combining the two by a policy you choose (see
[combining Info and XMP](#combining-info-and-xmp)). The PDF itself is read by a
bounded, pure-Python object reader:

| Module | Role |
| --- | --- |
| `file_formats/pdf/objects.py` | `read_metadata`, `PdfDocument`: cross-reference data, object loading, the fallback scan, Info and XMP |
| `file_formats/pdf/syntax.py` | The lexer and object parser; `decode_text` for PDF text strings |
| `file_formats/pdf/filters.py` | Flate (with PNG and TIFF predictors), ASCIIHex and ASCII85 |
| `file_formats/pdf/security.py` | The Standard security handler, opened with an empty password |
| `file_formats/pdf/errors.py` | `PdfReadError` and `PdfEncryptedError` |

Writing metadata (`set_metadata`) uses pypdf 6.1.3 or later, from the `pdf` extra
(`pip install 'liuxin-alpha[pdf]'`), and keeps everything else the file holds (see
[writing](#writing)). The full test suite installs it, and uses it to check this
reader against a second implementation (see
[agreement with poppler and pypdf](#agreement-with-poppler-and-pypdf)).

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

LiuXin's PDF conversion (`file_formats/pdf/pdftohtml.py`) runs poppler's `pdftohtml`
the same way since #221. It writes a whole book's HTML and images, so its limits are
wider: 600 s, 2 GiB of address space, and 512 MiB of output or for any one file. It
runs in its output folder, without changing this process's folder. Before #221 it had
never run: it named `isbsd` without importing it.

## Covers without poppler

LiuXin draws no pages without poppler. Its own reader (`liuxin.read-pdf-metadata`)
takes a scanned book's cover as the scanner wrote it (#221,
`file_formats/pdf/scanned_cover.py`): the JPEG the first page draws over the whole
page. It takes it only when all of these hold:

- the first page draws exactly one image, upright and not mirrored, over at least 85%
  of its crop box (else its media box), and the page is not rotated;
- the image is a JPEG (`DCTDecode`, perhaps behind other filters, which are undone);
- it is in grey or RGB, at least 150 pixels a side, with no decode array or mask.

Anything else gives no cover rather than a wrong one. CMYK JPEGs are left alone because
many are stored inverted. A JPEG 2000 image would need decoding. Only the first page's
own content streams are read, at most 1 MiB of them, within the reader's limits; a
damaged page costs only the cover.

## Combining Info and XMP

A PDF can say the same things twice, in its Info dictionary and in its XMP packet.
Writers that keep both write the same values, but an editor that updates one leaves the
other stale, and the two disagree. `metadata/file_sources/pdf_merge.py` offers three
ways to combine them. The `pdf_metadata_merge` preference chooses one for the
"Read PDF metadata" plugin:

| Policy | Which source wins | Info `Subject` | Tags |
| --- | --- | --- | --- |
| `newest` (default) | The one changed last wins every field it has; the other fills the gaps | The description (`dc:description`) | The winner's |
| `calibre` | XMP, except that a newer Info dictionary wins the title, authors and tags | The first tag | The winner's |
| `union` | XMP for title, authors, publisher and description | A tag | Info's and XMP's together |

`newest` follows the specifications. The XMP specification maps `Subject` to
`dc:description`, `Keywords` to `pdf:Keywords`, `Creator` to `xmp:CreatorTool` and
the dates to `xmp:CreateDate` and `xmp:ModifyDate`. The PDF specification says to prefer
XMP unless the Info dictionary's `ModDate` is later than the packet's
`xmp:MetadataDate`. The packet's date is the later of `xmp:MetadataDate` and
`xmp:ModifyDate`, as calibre takes it. When either date is missing, XMP wins, since
PDF 2.0 makes it the main record.

`calibre` is calibre's `consolidate_metadata`, applied to the fields LiuXin reads. A
packet whose title is an escaped PDF string (Nitro PDF writes `\376\377…`) is ignored
whole, as calibre ignores it; `newest` drops only the escaped fields. For calibre's own
reading of the file, poppler included, use the ported providers.

`union` is how LiuXin read PDFs before. It reads XMP properties written as elements
only, and keeps dates as the file writes them.

`newest` and `calibre` also read properties written as attributes of
`rdf:Description`, a short form many writers use. They read `dc:language`, DOIs and
ISBNs from `prism:` and `pdfx:` properties and `dc:identifier`, and give dates in ISO 8601.

A preference set to anything else means `newest`. Each policy also has a reader plugin
that is off by default: "Read PDF metadata (newest source wins)",
"Read PDF metadata (calibre's rule)" and "Read PDF metadata (Info and XMP together)".
Enabled, one runs before "Read PDF metadata" whatever the preference says. Callers can
pass `merge=` to `get_metadata` too.

## Writing

`set_metadata` is in `metadata/file_sources/pdf_write.py`. It writes the title, authors,
comments (the Subject), tags (the Keywords), producer, publisher and series. Fields the
metadata leaves empty keep what the file says.

- **Appended, not rewritten.** The change is appended to the file as an incremental
  update, the way PDF editors save. The original bytes stay as they were, so outlines,
  forms, attachments and the earlier revision are kept, and so are the bytes a digital
  signature over that revision covers. Given a path, only the update is written to the file.
  `tests/metadata/file_sources/test_pdf_signature_kept.py` signs a PDF
  (`adbe.pkcs7.detached`), writes metadata into it, and checks that the signature still
  verifies, with the signer's key and with poppler's `pdfsig` (#221).
- **Info and XMP together.** The Info dictionary gets the new values and a new
  `ModDate`. When the file has an XMP packet, the same fields are updated there, and
  `xmp:MetadataDate` and `xmp:ModifyDate` get the same moment. No merge policy then
  finds stale values in either source. The packet's other properties stay, as do a
  title's or description's other languages. No packet is added to a file without one.
- **Checked before it is written.** Every result is read back with this reader. It must
  find what was written through the cross-reference data alone, without a repair scan.
  Nothing is written to the file until a result passes.
- **Damaged files are rewritten whole, with a warning.** An update appended to a file
  whose cross-reference data is broken chains on to the damage. Given the corpus file
  `broken_xref`, poppler could then no longer find the catalog at all. Such a file
  (`broken_xref` and `leading_junk` in the corpus) is instead cloned whole by pypdf,
  which keeps its pages and document structure but not its original bytes.
- **A failure part-way leaves the file as it was.** A full disk or an error mid-write
  must not cost the file we were careful with.
  - **Appended update to a path:** one that fails is cut off again.
  - **Whole rewrite of a path:** it goes to a new file beside the old one, which is
    renamed into place only once complete and synced. It keeps the old file's
    permissions. A symbolic link stays a link, and the file it names is the one
    written. Other hard links keep the old version.
  - **A file changed by another program meanwhile:** it is not written.
  - **A stream:** it cannot be swapped like that, so after a failed write its original
    bytes are put back.
- **Encrypted files are refused,** and left unchanged. pypdf's updates to them do not
  read back reliably: some readers then see garbage, and one AES file became
  unreadable to poppler.
- **An XMP packet that cannot be edited safely is left, with a warning.** This covers a
  packet that is malformed, declares a document type, is over 4 MiB, or is stored with
  a filter other than Flate (pypdf cannot re-encode it). The Info dictionary is still
  written.

`set_metadata` returns a `PdfWriteReport` saying whether the file was appended to or
rewritten, and whether the packet was updated. Each warning is also logged.

The mapping is the one the reader reads back:

- authors are joined with ` & `, as calibre joins them, an ampersand in a name
  doubled;
- the author sort is not written to `/Creator`, which names the application that made
  the PDF;
- the series index is the series' own, not its name.

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

| Revision | Cipher | How |
| --- | --- | --- |
| 2–4 | RC4, 40 to 128 bits | pure Python, at most `rc4_stream_bytes` per stream |
| 4 | AES-128 | the `aes` utils plugin |
| 5–6 | AES-256 | the `aes` utils plugin |

The `aes` plugin (see [utils plugin layers](../../plugins/utils-plugin-layers.md#aes-cryptography-then-poppler-then-pure-python))
has three layers, best first:

1. **The `cryptography` package** (the `encryption` extra).
2. **Poppler.** Where `cryptography` is missing and `pdfinfo` is on the PATH, the
   reader hands an AES file to `pdfinfo` (`file_formats/pdf/poppler.py`), which
   decrypts it itself; `PdfMetadata.read_by` then says so. `pdfinfo` runs within the
   same limits as the ported calibre reader's, on a copy of the file.
3. **Pure-Python AES**, checked against FIPS 197, NIST SP 800-38A and `cryptography`.
   Deriving an AES-256 key takes 0.3 to 0.8 s; the few strings and streams the reader
   decrypts take milliseconds.

When no empty password opens the file, the reader raises `PdfEncryptedError`, which the
metadata source turns into `PdfParseError`; `pdfinfo` reports the same. It never returns
ciphertext. Metadata streams are left alone when `/EncryptMetadata` is false, as are
streams whose first filter is the Identity crypt filter, and cross-reference streams.
Poppler decrypts such a cleartext packet anyway (see below), so on the poppler layer
that file's XMP is lost.

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

## Agreement with poppler and pypdf

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

pypdf 6 agrees with this reader on the Info dictionary and the XMP packet of every case
but two, raw UTF-8 strings and cleartext XMP, where it reads as poppler does. It agrees
on the hybrid file that poppler reads less of. Both refuse the file with a user
password. `tests/file_formats/pdf/test_pdf_reader_agrees_with_pypdf.py` checks this
when pypdf is installed.

The old reader found nothing in files that keep their Info dictionary in an object
stream, as many writers have done since PDF 1.5. It returned ciphertext for every
encrypted file, and garbled PDFDocEncoding text.

## Tests

- `tests/file_formats/pdf/test_pdf_object_reader.py`: corpus agreement with `pdfinfo`,
  RC4, and AES through each layer of the `aes` plugin, the lexer, and hostile inputs with
  time bounds.
- `tests/file_formats/pdf/test_pdf_reader_agrees_with_pypdf.py`: corpus agreement with
  pypdf, when the `pdf` extra is installed.
- `tests/metadata/file_sources/test_pdf_storage_layouts.py`: the metadata source on
  every layout, by path and by stream.
- `tests/metadata/file_sources/test_pdf_safe_writing.py`: writing into every corpus
  file, when the `pdf` extra is installed. It checks that each update is appended after
  the original bytes, and that outlines, attachments and the earlier revision survive.
  It checks that every merge policy reads the new values, and that poppler, pypdf and
  this reader all read the result. It also covers damaged files rewritten with a
  warning, encrypted files refused, and packets left with a warning.
- `tests/metadata/file_sources/test_pdf_merge_policies.py`: each merge policy on files
  whose Info and XMP disagree and are dated either way, the preference, and the
  plugins.
- `tests/file_formats/providers/test_ported_pdf_reader.py`: the calibre port with
  and without poppler on every layout, covers, fake tools that hang, print
  forever or crash, and the XMP fixes.
- `tests/metadata/file_sources/test_pdf_edge_cases.py`,
  `tests/metadata/file_sources/test_pdf_metadata_source.py` and
  `tests/metadata/file_sources/test_pdf_golden_corpus_scaffold.py`: mapping, fallback
  policy and writing. The golden manifest records what the default policy reads, and
  what each other policy reads differently.

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

- Writing into encrypted PDFs would need the update encrypted with the file's key; pypdf
  does not do that reliably yet.
