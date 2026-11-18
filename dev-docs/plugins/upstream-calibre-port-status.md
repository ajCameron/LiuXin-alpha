# Ported calibre readers: status and findings

Written **2026-11-18**, at calibre 9.18, with the work in PRs #177 to #186 still
open. [Porting calibre's metadata readers](upstream-calibre-port.md) explains
how the ports are made; this page records what has been ported, how the ports
compare with the readers LiuXin already had, what hostile input turned up, and
what is left.

## What is ported

Fifteen of calibre's metadata reader plugins run in process through
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

The readers ported before the porting script (#177 and #179) were copied by
hand; from #182 on, `scripts/port_upstream_calibre.py` makes every copy and
re-running it reproduces them byte for byte.

### Dependencies

- **python-dateutil** became a runtime dependency on 2026-11-18 (#184). calibre's
  date parsing uses it, and with it only a test dependency the OPF and EPUB
  providers were unavailable in ordinary installs. The providers still check
  for it.
- **Beautiful Soup** stays optional. Without it the EPUB reader does not
  recognise an HTML first page holding only a cover image; SVG cover pages
  still work.
- **html2text** stays optional. Only writing FB2 comments needs it.
- **Python 3.12** must still parse every copy, as CI runs on it. calibre 9.18
  uses Python 3.13 and 3.14 syntax, which the porter rewrites (#182).

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

### Differences to decide on

- **Language codes.** The ports return calibre's ISO 639-3 codes (`fra`,
  `deu`); the fork's and LiuXin's readers return what the file says (`fr`,
  `de`). The WEMI mapping, or the pool, should settle on one form.
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

The four calibre hangs are written up for reporting upstream, with reproducers
and suggested fixes, in `dev-docs/calibre_compat/upstream-calibre-bugs.md` (PR
#181). None has been reported yet.

Entities are left alone: OPF, FB2, ODT `meta.xml` and EPUB containers are
parsed with calibre's `safe_xml_fromstring`, and odfpy parses ODT content with
Python's SAX parser, which fetches no external entities. OPF, FB2 and ODT have
tests with external and nested entities.

## Found along the way

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
| Kindle family: MOBI, PRC, AZW, AZW3, AZW4, POBI | `ebooks/metadata/mobi.py`, `ebooks/mobi/reader/headers.py`, parts of `ebooks/mobi/` | The MOBI and EXTH headers (about 350 lines) and the MOBI 6 reader, which pulls in conversion code; Topaz is already ported. The most valuable batch for an archive. |
| PDB family: eReader, Plucker, Haodoo, zTXT | `ebooks/metadata/pdb.py`, `ereader.py`, `plucker.py`, `haodoo.py`, `ebooks/pdb/header.py` | Small and pure Python. |
| LRF | `ebooks/lrf/meta.py` | About 800 lines, pure Python, few imports. |
| DOCX | `ebooks/metadata/docx.py`, `ebooks/docx/container.py` | The DOCX container; the writer half needs `ebooks/docx/writer`. |
| ZIP, RAR, comics | `ebooks/metadata/zip.py`, `rar.py`, `archive.py` | These hand each book inside an archive to the other readers. RAR needs calibre's unrar extension or a substitute. |
| PDF | `ebooks/metadata/pdf.py`, `xmp.py` | Runs poppler's `pdfinfo` and `pdftoppm` in a worker process, so it needs those tools and a stand-in for the worker. |
| CHM, LIT | `ebooks/chm/`, `ebooks/lit/` | C extensions (chmlib; `lzx` and `msdes`). The fork's readers still serve both. |
| Metadata writers | `set_metadata` in each module | calibre's own `zipfile` writer (`safe_replace`) and an image library for covers; today these raise `NotImplementedError`. |

Return to the [plugins index](README.md).
