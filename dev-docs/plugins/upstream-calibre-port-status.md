# Ported calibre readers: status and findings

Written **2026-11-18** and updated **2026-11-19**, at calibre 9.18. The work is
in PRs #177 to #188, #195, #197, #200 and #202. [Porting calibre's metadata readers](upstream-calibre-port.md) explains
how the ports are made; this page records what has been ported, how the ports
compare with the readers LiuXin already had, what hostile input turned up, and
what is left.

## What is ported

Twenty of calibre's metadata reader plugins run in process through
`file_formats.providers.ported_calibre`, at priority −50: above an installed
calibre (−60), below native LiuXin providers (0). The PDF port needs poppler's
`pdfinfo`; a second PDF provider, at −55, runs the same calibre code on fields
read by LiuXin's own PDF reader, so PDFs are read without poppler, though
without a cover.

| calibre plugin | Formats | Ported in | calibre's own tests run | LiuXin fixes |
| --- | --- | --- | --- | --- |
| Read TXT, IMP, RB, LRX metadata | `txt`, `imp`, `rb`, `lrx` | #177 | — | — |
| Read SNB, PML metadata | `snb`, `pml`, `pmlz` | #179 | — | — |
| Read RTF metadata | `rtf` | #179 | — | an unclosed `{\info` group; field patterns made linear |
| Read Topaz metadata | `tpz`, `azw1` | #179 | — | counts past the end of the file |
| Read OPF metadata | `opf` | #182 | OPF 2 (7), OPF 3 (15), author sort (17) | — |
| Read EPUB metadata | `epub`, `kepub` | #184 | EPUB (4), damaged ZIPs (8) | a first page outside the extracted book is not its cover |
| Read HTMLZ, TXTZ metadata | `htmlz`, `txtz` | #185 | — | — |
| Read HTML metadata | `html` | #185 | HTML (5) | comment pattern made linear |
| Read FB2 metadata | `fb2`, `fbz` | #185 | FB2 round trip (1), where html2text is installed | — |
| Read ODT metadata | `odt` | #185 | — | — |
| Read MOBI metadata | `mobi`, `prc`, `azw`, `azw3`, `azw4`, `pobi` | #188 | language codes (2), PalmDOC (4), page breaks (1) | see [the Kindle family](#the-kindle-family) |
| Read PDB metadata | `pdb`, `updb` | #195 | — | a section table whose offsets go back and forth |
| Read LRF metadata | `lrf` | #197 | — | meta info and image streams that unpack without limit |
| Read DOCX metadata | `docx` | #200 | — | a cover search that slows down faster than the drawings squared |
| Read PDF metadata | `pdf` | #202 | — | poppler run with limits, in its folder and without a worker; giant pages drawn at 3,000 pixels; a cubic XMP pattern; deeply nested XMP |

The readers ported before the porting script (#177 and #179) were copied by
hand; from #182 on, `scripts/port_upstream_calibre.py` makes every copy and
re-running it reproduces them byte for byte. Since #195 it can also port just
some of a module's top-level names, with the imports they use (`PARTS`): the
PDB readers need a header class or a few constants from calibre's conversion
readers, not the conversion code.

### Dependencies

- **python-dateutil** became a runtime dependency on 2026-11-18 (#184). calibre's
  date parsing uses it, and with it only a test dependency the OPF and EPUB
  providers were unavailable in ordinary installs. Since #187 the ported code
  reaches it through the `dateutil_parser` utils plugin, which falls back to a
  vendored copy of python-dateutil 2.9.0.post0 where the package is missing, so
  the readers always run. See [utils plugin layers](utils-plugin-layers.md);
  that page also records why LiuXin's older `liuxin_dateutil` 1.5 was not used
  as the backup.
- **Beautiful Soup** stays optional. Without it the EPUB reader does not
  recognise an HTML first page holding only a cover image; SVG cover pages
  still work.
- **html2text** stays optional. Only writing FB2 comments needs it.
- **Python 3.12** must still parse every copy, as CI runs on it. calibre 9.18
  uses Python 3.13 and 3.14 syntax, which the porter rewrites (#182).
- **Pillow** stays optional. The MOBI reader converts GIF images with it while
  reading an old book's text; without it they are skipped, which the metadata
  does not need.
- **poppler-utils** stays optional, as calibre bundles it. Where `pdfinfo` is
  not on the PATH the PDF port reports itself unavailable and the provider
  without poppler reads PDFs instead. CI installs poppler so that both run.
- **cryptography** stays optional (the `encryption` extra). LiuXin's PDF reader
  takes AES from the `aes` utils plugin: `cryptography`, else poppler, else
  pure-Python AES (see [utils plugin layers](utils-plugin-layers.md)).

## How the ports compare

Each batch was read through every provider for the format: the port, the fork's
calibre-path reader and the LiuXin reader. The fork's and LiuXin's readers here
were the ones on the stacked branches, without #178's fixes for OPF 2 fields.

| Format | Samples | Findings |
| --- | --- | --- |
| OPF | OPF 2 and OPF 3 packages | The port keeps author sort, publication date, rating and rights, which the fork's and LiuXin's readers dropped before #178. They agree on OPF 3 apart from languages. |
| EPUB, KEPUB | calibre's six quick-start EPUBs, OPF 2 and 3 books, a KEPUB | Same titles, authors, identifiers and cover bytes. The port also reads the OPF 2 fields the fork dropped, finds covers on the first page (a JPEG first page, as in manga, or an SVG cover page), and names covers by their bytes. |
| HTMLZ, TXTZ | sample books | The port reads the whole OPF and the cover; LiuXin's reader kept only title, author and ISBN. |
| HTML | the six real-world fixtures in `tests/fixtures/html_ingest`, a sample page | The same answers as LiuXin's reader, comment tags winning over meta tags in both. |
| FB2, FBZ | sample books | The same fields as LiuXin's reader, and the cover. |
| ODT | sample documents | The same as the fork's and LiuXin's readers, plus calibre's OPF rating. |
| DOCX | a book with core and app properties in several scripts, and books whose drawings show a wide image then a portrait one | The same as LiuXin's reader for title, authors, publisher, tags and comments, and for the cover. The language comes out as calibre's `eng` where LiuXin's reader gives `en` twice |
| LRF | a book with meta info, a thumbnail, and an image object whose stream is plain or zlib-packed | The same as LiuXin's reader: title, authors, sort fields, publisher, tags, comments and cover. Neither reads the language, which calibre leaves out too. |
| PDB | eReader books with a metadata record and a cover; a Plucker book with a charset, authors, a title and a date; Haodoo books in cp950 and UTF-16; a PalmDOC book | The same as LiuXin's reader for eReader, Haodoo and other PDB books, covers included. LiuXin's reader found nothing in the Plucker book's metadata until #196 (see [found along the way](#found-along-the-way)); the port reads it all. |
| PDF | the 25 files of `tests/fixtures/pdf_metadata`: object streams, incremental updates, hybrid cross-reference data, RC4 and AES encryption, XMP, UTF-16 and raw UTF-8 strings | With poppler, the same fields as without it, and a cover, except in the three layouts where poppler 24.02 reads less (see the [PDF notes](../file_formats/pdf/README.md)). Against LiuXin's own reader: both take XMP's title and authors over the Info ones, but calibre takes XMP's tags in place of the Info tags (unless the Info is newer), where LiuXin's reader adds them to the Info tags. A `pdfinfo` value with a line break keeps only its first line in calibre, and later lines shaped like `Name: value` become fields; the provider without poppler joins the lines |
| MOBI, AZW3, PRC | books with EXTH and JPEG or PNG covers; an old book without EXTH, with metadata in its markup | Same titles, authors, publishers, identifiers and descriptions as the fork's reader. The port keeps the EXTH publication date, which the fork drops, and tags in the book's order where the fork sorts them; it returns languages as `fra` where the fork says `French`, and the cover's own bytes. From the old book it also reads the publisher. |

### Differences to decide on

- **Language codes.** The ports return calibre's ISO 639-3 codes (`fra`,
  `deu`); the fork's and LiuXin's readers return what the file says (`fr`,
  `de`), or, for MOBI, a language name (`French`). The WEMI mapping, or the pool, should settle on one form.
- **calibre's "undefined" date.** calibre writes `0101-01-01` into books for an
  unknown date, and reads it back as a date. Every calibre-model provider
  passes it on as a real date, because `serialised_calibre` does not treat it
  as empty. Two of the quick-start EPUBs show it.
- **Year-only dates.** Given only a year, calibre's FB2 reader fills in the
  15th of the *current* month, so the same file reads differently from month
  to month. LiuXin's reader fills in 2 June. Neither says the day and month
  were invented.
- **Encoding guesses.** Where a file declares no encoding and is not UTF-8,
  the ports guess with the `chardet` package where calibre uses uchardet. The
  windows-1252 HTML fixture with no declared charset was mis-decoded by both
  the port and LiuXin's reader with chardet 7; LiuXin's own test of it passes
  in CI, so the result depends on the chardet version.
- **Cover format names.** The ported providers name covers by their bytes
  (`jpeg`, `png`). calibre's EPUB reader labels every cover `jpg`, and its
  HTMLZ/TXTZ reader uses the file extension, dot included. Other providers
  still pass calibre's label on.
- **MOBI covers.** calibre re-encodes every MOBI cover as a JPEG of at most
  1650x2200 pixels, through Qt. The port keeps the image the book holds, as
  the EPUB reader does.
- **Plucker dates.** calibre and LiuXin's reader both take a Plucker book's
  publication date as seconds since 1970, while the
  [Plucker format](https://hwiegman.home.xs4all.nl/fileformats/P/pdb/The%20Plucker%20Document%20Format.htm)
  counts from 1904; a date written to the format would come out 66 years late.
  calibre also gives a book with no date record 1 January 1970.
- **Haodoo authors in UTF-16.** calibre and LiuXin's reader both strip
  trailing zero bytes from the name field before decoding it, so a UTF-16
  author ending in an ASCII letter loses half of that letter. Haodoo names are
  usually Chinese, whose characters do not end in a zero byte.

## Hostile input

Each batch was tried on damaged and crafted files: truncated files, counts past
the end, unclosed structures, external and nested entities, archives that
expand enormously, paths that climb out of the book, and long runs that make
patterns backtrack.

| Found in | Problem | Fixed in |
| --- | --- | --- |
| calibre's Topaz reader | A 9-byte file ran for 7.8 minutes and took 2.2 GB | The port (#179); LiuXin's reader had it too (#180) |
| calibre's RTF reader | A cut-off `{\info` group never returned | The port (#179); LiuXin's reader (#180) |
| calibre's RTF reader | Field patterns quadratic, cubic once the above is fixed | The port (#179); LiuXin's reader (#180) |
| calibre's HTML reader | The comment pattern is quadratic: a 40,000-character comment without spaces took 14–32 s, the 150,000 calibre reads several minutes | The port (#185); LiuXin's reader had the same pattern with no length cap, so a 1 MB comment would have taken hours (#186) |
| LiuXin's EPUB cover search | A cover page could name any image on the disk, which then became the book's cover, in metadata reading and in EPUB and OEB conversion. calibre 9.18 already confines these images to the book | #183 |
| calibre's EPUB, HTMLZ, TXTZ, FBZ and ODT readers | They read the members they need whole, and the EPUB reader extracts the whole book to look for a first-page cover: a 1 GB OPF took 13 s and 5 GB of memory | The providers apply LiuXin's EPUB-input archive limits before calibre's code runs (#184, #185) |
| calibre's encoding detection (`chardet.py`) | Two of its encoding-declaration patterns retry every quote after every `encoding=` or `content=` against the rest of the tag. 8 KB of unclosed `<?…encoding="` took 9 s, and the cost grows with the cube: 50 KB, the most calibre strips, over half an hour. `xml_to_unicode` searches the whole file, so every reader that decodes with it was exposed: OPF, EPUB, HTML, FB2, MOBI | The port (#188), which finds the same matches by hand. LiuXin's own `utils/libraries/calibre_chardet.py` had the same patterns (8 KB: 6.7 s), and a quadratic entity pattern too; #190 fixes them on `main` |
| calibre's DOCX reader | The cover search is an XPath for the first image in each drawing, which libxml2 merges in time that grows faster than the drawings squared, and the image was read whole again for each drawing that named it: 64,000 drawings naming a 1 MB image took 16 s, and a 100 KB book can hold a million | The port (#200) finds the images in one pass with `linear_time.first_images` and reads each once; a million drawings take about 5 s, mostly parsing. The providers also apply LiuXin's archive limits to DOCX. LiuXin's own DOCX reader had both; #199 fixes it |
| calibre's LRF reader | A book's meta info is zlib-packed XML of up to 64 KB, which can unpack to 64 MB, and calibre unpacked and parsed it with minidom once for every field it read: 1.6 million empty elements in a 6.4 KB book took 48 s. An image stream's zlib is unpacked whole before its declared size is checked: a 408 KB book took the process to 1.2 GB | The port (#197) unpacks the meta info to at most 1 MiB and parses it once, and unpacks an image stream to no more than it declares and no more than 32 MiB. LiuXin's own LRF reader had both; #198 fixes it |
| calibre's PDB header reader | A section runs to the next section's offset, and `read()` of a negative length reads to the end of the file, so a table whose offsets go back names the whole file once per section. calibre's Haodoo reader keeps every section: 4,000 sections in a 536 KB book took 1.9 GB, and a table can name 65,535 | The port (#195) refuses sections that together hold more than twice the file, as the MOBI port does. LiuXin's own PDB header reader already refused offsets that do not increase |
| calibre's XMP reader | Its XML-declaration pattern is bug 5's with `xml` after `<?`, and cubic: 10 KB took 8.2 s, 20 KB about a minute. Its lookups read every match's descendants, so a field nested in itself 250 deep is read 250 times: 0.9 MB took 4.3 s | The port (#202) finds the declarations by hand in linear time, and refuses packets nested more than 32 deep |
| calibre's PDF reader | It runs `pdfinfo` and `pdftoppm` with no memory or output limit (its worker times out at 300 s), and draws the first page at 150 dpi whatever its size: a 172-inch page came out 25,834 pixels square, 2 GB of memory and a cover Pillow refuses | The port (#202) runs both tools with a timeout and memory, CPU and output limits (`utils/bounded_process.py`), and draws pages over 20 inches at 3,000 pixels. It runs them in their folder instead of changing the process's, and in process instead of a worker |
| calibre's MOBI readers | See [the Kindle family](#the-kindle-family) | The port (#188). LiuXin's own MOBI 6 reader (`file_formats/mobi/reader/mobi6.py`) had the same patterns, XPath, scans and inserts; #191 fixes them on `main`. Its PalmDB reader already refused offsets that go back, its HUFF/CDIC reader caps each record's output, and its EXTH reader refuses a record of size 0 |

The calibre problems are written up for reporting upstream, with reproducers
and suggested fixes, in
[upstream-calibre-bugs.md](../calibre_compat/upstream-calibre-bugs.md): bugs 1 to
10 from these ports, 11 from LiuXin's conversion code (#193), 12, the PDB
section table (#195), 13, LRF's unpacking (#197), 14, the DOCX cover search
(#200), and 15 to 17, the XMP and PDF readers (#202). None has been reported yet.

### The Kindle family

calibre reads EXTH-headed books from their headers alone. A book under 4 MB
without EXTH has its whole text decompressed and run through calibre's MOBI 6
reader, the first half of its MOBI conversion, to find metadata in the markup.
That path held most of what hostile input found. Timings are for calibre 9.18's
code as ported, before the fixes, against books of 4 MB or less; after the
fixes none of these took over 2.5 s.

| Problem | Before | Fix |
| --- | --- | --- |
| An EXTH record claiming size 0 is read again for every record the header claims, up to 2³², each kept | 1 million records: 1.7 s and growing; 2³² would take hours and fill memory | Stop at a record of size 0, as LiuXin's own MOBI reader does |
| HUFF/CDIC dictionary entries can unpack to other entries | A 4.7 KB book unpacks to 1 GB | Refuse a book once its text has unpacked to 32 MiB, far beyond what a book under 4 MB holds |
| A section runs to the next section's offset, so a table whose offsets go back and forth copies the same bytes for every entry | A 1 MB book with 600 sections held 325 MB; 65535 entries would hold about 100 GB | Refuse sections that together hold more than twice the file |
| The guide's toc and cover hrefs are pasted into XPath | A quote in one fails the read; the rest runs as XPath of the book's choosing, 15 s on 50 KB with a slow expression | Pass them as XPath variables |
| Nine patterns in the text clean-up backtrack: unclosed `<p`, `</p`, `<?xml`, `<o:p` and namespaced tags, runs of style tags, runs of spaces after `<`, unclosed page breaks, `filepos=` links in an unclosed tag, entities around anchors, `<metadata>` and `<guide>` without their end tag | From 10 s on 3.4 KB (page breaks) and 9 s on 42 KB (`filepos=` links) to hours on 4 MB | `_linear_time` tries each pattern only where a match can start and skips tries that must fail, or reads runs possessively; the results are calibre's |
| Anchor placement searches from every link target to the next `<` | 4.3 s on 3.8 MB, growing with the square, and with the decompressed text up to five times the file | Searches from targets in order reuse what the last one found |
| Each styled tag's CSS rule is compared with every rule so far | 470 KB of differently styled tags: 5.9 s; 4 MB: about 7 minutes | Look rules up in a dictionary |
| Images moved out of each `<svg>`, and anchors after page breaks, are placed by counting the parent's children | 1 MB of page-break anchors: 13.5 s; 4 MB of svgs: about 2 minutes | Place them beside the element itself |

Each linear-time replacement is compared with calibre's original by the tests,
on random text built from the pattern's pieces; during the port they were also
checked on 40,000 random texts each, and the patched and unpatched MOBI 6 readers
gave identical HTML, CSS, OPF, NCX and metadata on 9,000 random books.

Two costs remain. libxml2 checks each attribute of a tag against the others,
so a tag with 20,000 attributes takes a second to parse, and four times as
long for twice as many, anywhere LiuXin reads HTML with lxml. And a 3.3 MB PalmDOC book can decompress to 16 MB of markup
that takes about 10 s and 500 MB to read: linear, but the most the 4 MB limit
allows.

Entities are left alone: OPF, FB2, ODT `meta.xml` and EPUB containers are
parsed with calibre's `safe_xml_fromstring`, and odfpy parses ODT content with
Python's SAX parser, which fetches no external entities. OPF, FB2 and ODT have
tests with external and nested entities.

## Found along the way

- **LiuXin's pure-Python PalmDOC codec differed from calibre's C.** Its
  decompressor stopped at the first malformed literal run, where calibre's
  copies what is there and carries on, and its compressor never packed a space
  and a letter into one byte, so its output was about 6.5% larger. Both now
  match calibre's compiled `palmdoc.c`: compression byte for byte on 4,000
  random texts, decompression on 200,000 random byte strings, identical
  except for back-references of distance 0, where calibre's C copies bytes of
  its output buffer it never wrote and LiuXin writes zero bytes (#188). LiuXin's MOBI writer uses the codec, so the books it
  writes changed, and so did the hash its golden test pins.
- **The fork's odfpy writes ODT files that are not well-formed XML.** Writing a
  frame with `svg:width` and an image with `xlink:href` through
  `file_formats.odf` leaves the `svg` and `xlink` prefixes undeclared in
  `content.xml`. odfpy 1.4.1 from PyPI declares them. No LiuXin code writes ODT
  through it today; the ODT reader and converter only load. See the
  [ODT notes](../file_formats/odt/README.md).

- **LiuXin's own Plucker reader misread the metadata records.** Each record
  is a type, a length and an argument, and the length counts the argument's
  16-bit words, as the Plucker format describes and calibre reads it.
  `metadata/file_sources/pdb/plucker.py` took the length to include the
  4-byte type and length, so on a standard book it stopped at the charset
  record, whose argument is one word, and found no title, author or date.
  LiuXin's Plucker conversion reader refused the same record. #196 fixes both.

## What is left

In rough order of value against effort:

| Batch | calibre code | What it needs |
| --- | --- | --- |
| ZIP, RAR, comics | `ebooks/metadata/zip.py`, `rar.py`, `archive.py` | These hand each book inside an archive to the other readers. RAR needs calibre's unrar extension or a substitute. |
| CHM, LIT | `ebooks/chm/`, `ebooks/lit/` | C extensions (chmlib; `lzx` and `msdes`). The fork's readers still serve both. |
| Metadata writers | `set_metadata` in each module | calibre's own `zipfile` writer (`safe_replace`) and an image library for covers; today these raise `NotImplementedError`. |

Return to the [plugins index](README.md).
