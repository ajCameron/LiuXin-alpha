# Ported calibre readers: status and findings

Written **2026-11-18**, at calibre 9.18, with the work in PRs #177 to #188 still
open. [Porting calibre's metadata readers](upstream-calibre-port.md) explains
how the ports are made; this page records what has been ported, how the ports
compare with the readers LiuXin already had, what hostile input turned up, and
what is left.

## What is ported

Sixteen of calibre's metadata reader plugins run in process through
`file_formats.providers.ported_calibre`, at priority −50: above an installed
calibre (−60), below native LiuXin providers (0).

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

The readers ported before the porting script (#177 and #179) were copied by
hand; from #182 on, `scripts/port_upstream_calibre.py` makes every copy and
re-running it reproduces them byte for byte.

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
| calibre's MOBI readers | See [the Kindle family](#the-kindle-family) | The port (#188). LiuXin's own MOBI 6 reader (`file_formats/mobi/reader/mobi6.py`) had the same patterns, XPath, scans and inserts; #191 fixes them on `main`. Its PalmDB reader already refused offsets that go back, its HUFF/CDIC reader caps each record's output, and its EXTH reader refuses a record of size 0 |

The calibre hangs before #188 are written up for reporting upstream, with
reproducers and suggested fixes, in
`dev-docs/calibre_compat/upstream-calibre-bugs.md` (PR #181); the ones #188
found are being added there. None has been reported yet.

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

## What is left

In rough order of value against effort:

| Batch | calibre code | What it needs |
| --- | --- | --- |
| PDB family: eReader, Plucker, Haodoo, zTXT | `ebooks/metadata/pdb.py`, `ereader.py`, `plucker.py`, `haodoo.py`, `ebooks/pdb/header.py` | Small and pure Python. |
| LRF | `ebooks/lrf/meta.py` | About 800 lines, pure Python, few imports. |
| DOCX | `ebooks/metadata/docx.py`, `ebooks/docx/container.py` | The DOCX container; the writer half needs `ebooks/docx/writer`. |
| ZIP, RAR, comics | `ebooks/metadata/zip.py`, `rar.py`, `archive.py` | These hand each book inside an archive to the other readers. RAR needs calibre's unrar extension or a substitute. |
| PDF | `ebooks/metadata/pdf.py`, `xmp.py` | Runs poppler's `pdfinfo` and `pdftoppm` in a worker process, so it needs those tools and a stand-in for the worker. |
| CHM, LIT | `ebooks/chm/`, `ebooks/lit/` | C extensions (chmlib; `lzx` and `msdes`). The fork's readers still serve both. |
| Metadata writers | `set_metadata` in each module | calibre's own `zipfile` writer (`safe_replace`) and an image library for covers; today these raise `NotImplementedError`. |

Return to the [plugins index](README.md).
