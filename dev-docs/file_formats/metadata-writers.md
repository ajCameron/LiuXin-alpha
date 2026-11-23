# Metadata writers: audit and contract

Reviewed **2026-11-20**. This covers LiuXin's metadata writers for DOCX, EPUB, FB2,
HTMLZ, LRF, MOBI (with AZW, AZW3 and PRC), PDB, RTF, Topaz and TXTZ, and the
`metadata.file.write` service that runs them. The PDF writer was reworked separately,
in #205, and its notes are in the PDF dossier that change adds. LiuXin had no ODT
writer; one was added afterwards (#212), checked the same way. See [ODT](#odt) below.

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
  - RTF outside `{\info}`;
  - ODT members other than `meta.xml`, byte for byte, in their order and compression.
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
| RTF | The publisher goes into `\manager`. With the `read_subject_fields_as` preference at `calibre`, the comments go into `\subject`, as in calibre. At its default, `subjects`, `\subject` holds the subjects and the comments go into `\doccomm`, which calibre does not read (see [subjects](../metadata/subjects.md#a-documents-subject-field)). |
| ODT | No cover is written. The language is written as ISO 639-1; one with no such code is not written. The publication date is written as a date, in UTC. A tag holding a comma is split when read (by LiuXin's reader, a semicolon too). Subjects share the one Subject, joined by semicolons, so a subject holding a semicolon is split when read; calibre does not read the Subject. With the `read_subject_fields_as` preference at `calibre`, the Subject is neither read nor written, and the subjects go among the keywords. |

## Found, not fixed

- **LiuXin's ODT reader reads some fields from other places than calibre's.** It
  takes the last-saved date as the publication date and prefers the creator to the
  initial creator, where calibre reads neither. The writer leaves such values as they
  are unless the caller changes them, so each reader keeps its own reading.

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

## ODT

The writer replaces `meta.xml` and copies every other member as it was, so `mimetype`
stays first and uncompressed. It does not write through the fork's odfpy, whose
writer has a known fault (see the [ODT notes](odt/README.md)). It parses `meta.xml`
without loading DTDs or resolving entities, and drops characters XML cannot hold.
A `meta.xml` that is missing, not well-formed, or without `office:meta` is refused,
and the file is left as it was.

It writes as calibre's ODT writer does:

- the title, authors, comments, tags (the keywords) and language go into the ODF
  elements;
- the rest goes into calibre's `opf.*` user-defined properties, with `opf.metadata` set.

It also writes the subjects, which calibre's record does not have: LiuXin keeps
subject headings apart from tags (see [subjects](../metadata/subjects.md)). They go
into the document's Subject, joined by semicolons, and LiuXin's reader reads the
Subject as subjects; it used to count it as tags. calibre does not read the Subject.

Both LiuXin's reader and calibre's read every field back, apart from the subjects,
which calibre does not read. LiuXin's reader now also reads `opf.identifiers` and
`opf.rating`, which calibre writes and reads.

Two rules keep a write to the fields it names:

- **Properties a reader prefers are replaced.** Writing a field removes any property
  either reader would read in its place, in any letter case:
  - `opf.title`, `opf.subject` and `opf.series_index` (LiuXin's reader);
  - `opf.language` (both readers);
  - `opf.isbn`, and `meta:keywords` (calibre's reader).

  calibre's own writer leaves `opf.language`, so a book that has one keeps reading in
  its old language in calibre.
- **A field that does not change is not written.** The writer first reads the file
  with LiuXin's reader. A field whose value matches that reading is left as the file
  has it.

  LiuXin's reader takes some values from places calibre's writer does not write:

  - the creator before the initial creator;
  - the last-saved `dc:date` as the publication date;
  - plain `series` properties;
  - `dc:identifier`.

  The service fills omitted fields from that reading. Without this rule, a title-only
  write would have copied those values where calibre reads them.

## Tests

- `tests/core/test_metadata_file_write_safety.py`: the service.
  - Title-only and named-field writes, by path and as bytes, on EPUB and MOBI.
  - A failed replace, a file changed meanwhile, links and permissions.
- `tests/metadata/file_sources/test_metadata_writer_interchange.py`: the writers.
  - DOCX keywords and language; the keyword splitting, including LiuXin's old `_-_`.
  - LRF and RTF authors in both readers.
  - The data-repository audit.
- `tests/metadata/file_sources/test_odt_metadata_writer.py`: the ODT writer.
  - Every field in both readers.
  - What the package keeps.
  - Null and unchanged fields; replaced properties; subjects in the Subject.
  - Characters XML cannot hold; refusals; entities.
  - Title-only writes through the service, on built books and on every data-repository
    ODT.

  The interchange audit above includes ODT.
- `tests/core/test_program_service_contracts.py`: a writer that reports a failure
  leaves the file unchanged. This test used to pin the partial rewrite.
