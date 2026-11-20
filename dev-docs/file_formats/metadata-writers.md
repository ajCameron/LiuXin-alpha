# Metadata writers: audit and contract

Reviewed **2026-11-20**. This covers LiuXin's metadata writers for DOCX, EPUB, FB2,
HTMLZ, LRF, MOBI (with AZW, AZW3 and PRC), PDB, RTF, Topaz and TXTZ, and the
`metadata.file.write` service that runs them. The PDF writer was reworked separately,
in #205, and its notes are in the PDF dossier that change adds.

## How they were checked

Every book in `LiuXin_alpha_data/md_test_books` was put through each writer, plus
calibre's quick-start EPUB and a built Topaz file. Each book was written twice through
the writer plugins, as the application writes. The checks:

- **Both readers.** Each result was read back with LiuXin's reader and with calibre's
  ported reader, and the fields compared with what was written.
- **Everything outside the metadata.** That was compared with the original:
  - ZIP members, other than the OPF and document properties;
  - PDB records other than the header;
  - LRF objects, by ID;
  - FB2 XML outside `<description>`, compared canonically;
  - RTF outside `{\info}`.
- **Covers and partial records.** A new cover was written into each format that holds
  one. A title-only record was written the way the service builds one.

`tests/metadata/file_sources/test_metadata_writer_interchange.py` repeats the audit
on every data-repository book when `LiuXin_alpha_data` is checked out.

## What holds

- **Nothing outside the metadata changes**, in any format, even after two writes in a
  row. A new cover replaces the old cover image only. Where a book has none, it is added:
  - in EPUB, as a new image;
  - in FB2, as a new binary.
- **A write changes only the fields it names.** Given a plain metadata mapping,
  `metadata.file.write` starts from the file's current metadata (without its cover,
  so an unchanged cover is not re-encoded). Writing only a title keeps the authors,
  publisher, tags and the rest. An explicitly empty author list is written as
  "Unknown", as calibre writes it.
- **A failed write leaves the file as it was.**
  - The service's writer works on a copy in memory.
  - A path is replaced only when the writer reported nothing, by a new file written
    beside it, synced and renamed into place.
  - A file another program changed meanwhile is not written.
  - Symbolic links stay links; other hard links keep the old file.
- **Both readers agree** on every field a format stores, except the format limits below.

Calling a writer plugin directly on a stream still rewrites that stream in place:
the plugins truncate it and write the new book. Only the service, and the command
line (which sends bytes and writes the result back atomically), protect the file.

## Fixed by the audit

- **Title-only writes replaced the authors with "Unknown".** This hit eight writers:
  DOCX, EPUB, LRF, MOBI and AZW3, RTF, Topaz and TXTZ. DOCX also cleared the
  publisher, and RTF the tags. The writers behave as calibre's do: they write whatever
  record they are given. The service gave them one with calibre's defaults in place
  of the fields the caller left out. It now fills those from the file.
- **Path writes truncated the book before writing it.** A writer that reported a
  failure, or a full disk, left it damaged. The service's own documentation said so.
- **DOCX tags were written with `_-_` for each space.** The point was to stop LiuXin's
  reader, which split keywords on spaces as calibre's does, from splitting a tag that
  holds one. Word and calibre showed the token. Tags are now plain comma-separated
  keywords. The reader splits on commas or semicolons when the keywords have any,
  and on spaces otherwise; it still turns `_-_` back into a space.
- **Languages were written as `fre`, `ger`, `eng`, where `fr`, `de`, `en` belong.**
  This reached DOCX `dc:language` and the MOBI language record (EXTH 524).
  `lang_as_iso639_1` falls back to LiuXin's ISO 639-2 table, because calibre's
  `iso639.pickle` does not ship with LiuXin. That fallback returned the three-letter
  code; it now returns the two-letter one, as calibre's does.
- **LRF authors carried LiuXin's private token `,0420,` for the ampersand.** The XML
  field escapes it already. Readers other than LiuXin's showed the token. LiuXin's
  reader still decodes it in books written before.
- **RTF authors were joined with `& `** (no space before), so calibre read
  `Ann Writer& Jürgen Ölmann` as one author. They are now joined with commas, as
  calibre writes and reads them; LiuXin's reader splits on commas too.

## Format limits shared with calibre

These match calibre's writers, so they are left as they are:

| Format | Limit |
| --- | --- |
| MOBI, AZW, PRC | Text is encoded in the book's code page. In a cp1252 book, characters outside it become `?`. |
| MOBI, AZW, AZW3, PRC | A cover replaces an existing cover record; a book without one gets none. No series is stored. |
| PDB | Only the title is written, into the 31-byte header name, with characters outside `A–Z a–z 0–9 -` and space replaced by `_`. Some PDB types' readers do not read that name. |
| Topaz | Only the title and authors are written. |
| FB2 | No language or ISBN is written. |
| DOCX | No series, ISBN or cover is written. A missing publisher clears the document's company. |
| HTMLZ | A book without an OPF is refused and left unchanged. |
| RTF | The comments go into `\subject`, the publisher into `\manager`. |

## Found, not fixed

- **Language handling without `iso639.pickle`** (fixed in #207). Every language
  function in `utils/localization` depended on calibre's table, which LiuXin did not
  ship, so readers reported one language as `fr`, `fra` or `French`. See
  [language codes](../metadata/languages.md).
- **EPUB covers added at a doubled folder.** A cover added to an EPUB whose OPF is in
  `OEBPS/` goes to `OEBPS/OEBPS/cover.jpg`. The OPF points to it correctly and both
  readers find it; calibre puts it beside the OPF.
- **calibre's RTF reader and negative `\u` escapes.** RTF writes characters above
  U+7FFF as negative numbers (`測試` is `\u28204?\u-30106?`). calibre's reader leaves
  those undecoded. LiuXin's reader and writer are right; this is a calibre bug.
- **Author names holding `&`.** LiuXin's metadata record splits such a name when it is
  set, so "Bob & Co" becomes two authors, though the files carry it correctly.

## Tests

- `tests/core/test_metadata_file_write_safety.py`: the service.
  - Title-only and named-field writes, by path and as bytes, on EPUB and MOBI.
  - A failed replace, a file changed meanwhile, links and permissions.
- `tests/metadata/file_sources/test_metadata_writer_interchange.py`: the writers.
  - DOCX keywords and language; the keyword splitting, including LiuXin's old `_-_`.
  - LRF and RTF authors in both readers.
  - The data-repository audit.
- `tests/core/test_program_service_contracts.py`: a writer that reports a failure
  leaves the file unchanged. This test used to pin the partial rewrite.
