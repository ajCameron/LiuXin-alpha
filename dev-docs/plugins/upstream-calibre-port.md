# Porting calibre's metadata readers

Written **2026-11-18**, at calibre 9.18.

`LiuXin_alpha.metadata.upstream_calibre` holds copies of current calibre's
metadata readers and the calibre code they use. They run in process, without
calibre installed, through the providers in
`file_formats.providers.ported_calibre` (priority −50, ahead of running an
installed calibre at −60). This guide covers what is ported, what LiuXin
substitutes for, and how to port more. The [status page](upstream-calibre-port-status.md)
records how the ports compare with LiuXin's other readers, what hostile input
turned up, and which readers are left.

## The rules

- **Copy, don't rewrite.** A ported module is calibre's file with its imports
  changed. The other edits are mechanical, so the copies run on Python 3.12
  and 3.13 too: `from __future__ import annotations` is added, `except A, B:`
  clauses (Python 3.14 syntax) are parenthesised, and defaults are dropped
  from type parameters (`class C[T = str]`, Python 3.13 syntax), which only
  type checkers read. A header comment records the source path and the
  changes.
- **Port only what the readers use.** Where a reader needs a few names from a
  large module, `PARTS` in the porting script lists them. The script keeps
  those top-level definitions, the comment lines just above them and the
  imports they use, and stops if a kept definition uses another module-level
  name that is not listed. The header comment says what was kept.
- **Fix only what hangs or reaches outside the book.** A reader that upstream
  leaves running for minutes, or for ever, or filling memory, on a damaged
  file, that reads a file the book names outside itself, or that runs the
  book's text as code (XPath, here), gets a fix marked `# LiuXin:`. So does a
  reader that throws away what the book stores: the MOBI cover is kept as the
  book holds it rather than re-encoded. In scripted ports the fixes live in
  `PATCHES` in the porting script, written against calibre's text, and a
  re-port stops if that text has changed. Hangs are recorded in
  `dev-docs/calibre_compat/upstream-calibre-bugs.md` (added by PR #181).
- **A faster pattern must give calibre's results.** Where a pattern
  backtracks, the fix calls a helper in `_linear_time` that gives exactly
  what `re.sub` or `finditer` gives with calibre's pattern, or rewrites the
  pattern with possessive quantifiers that can only end one way. Tests compare
  each with calibre's original on random text built from the pattern's pieces.
- **No calibre imports.** A test parses every module in the package and fails
  on an import from `calibre`, `calibre_extensions`, `polyglot`, `tzlocal` or
  `compression`.
- **Run calibre's tests.** Where calibre has tests for a module, port them as
  well and run them in the suite. calibre's OPF 2, OPF 3, author-sort, EPUB
  and damaged-ZIP tests run this way.

## What is ported

| Ported | From calibre | Notes |
| --- | --- | --- |
| Readers `txt`, `imp`, `rb`, `lrx`, `snb`, `rtf`, `topaz`, `pml`, `opf`, `epub`, `extz`, `html`, `fb2`, `odt`, `mobi`, `pdb`, `ereader`, `plucker`, `haodoo` | `ebooks/metadata/` | `rtf`, `topaz`, `epub`, `html` and `mobi` carry marked fixes; `epub` also reads KEPUB, `extz` reads HTMLZ and TXTZ, `fb2` reads FBZ, `mobi` reads PRC, AZW, AZW3, AZW4 and POBI, and `pdb` reads UPDB and hands eReader, Plucker and Haodoo books to their readers |
| `mobi_headers`, `mobi6`, `huffcdic`, `palmdoc`, `mobi_package`, `mobi_utils`, `mobi_langcodes` | `ebooks/mobi/`, `ebooks/compression/palmdoc.py` | The MOBI and EXTH headers; the MOBI 6 reader, which reads the markup of books without EXTH; their decompressors. `mobi6`, `headers` and `huffcdic` carry marked fixes |
| `pdb_header`, `pdb_formatreader`, `pdb_ereader_reader132`, `pdb_plucker_reader`, `pdb_haodoo_reader`, `txt_processor` | `ebooks/pdb/`, `ebooks/txt/processor.py` | The PDB header reader, which carries a marked fix, and what the PDB readers use from calibre's conversion readers. `reader132` (its `HeaderRecord`), the Plucker reader (two constants and `SectionHeader`) and the TXT processor (`HTML_TEMPLATE`, `opf_writer`) are ported in part |
| `docx`, `docx_container`, `docx_names`, `docx_package`, `docx_writer_container` | `ebooks/metadata/docx.py`, `ebooks/docx/container.py`, `names.py`, `__init__.py`, `writer/container.py` | The DOCX reader, which carries a marked fix, and the container it reads through. The namespace table (all but `generate_anchor`, which needs calibre's transliterator) and the writer's container (`xml2str`, `update_doc_props`) are ported in part |
| `lrf`, `lrf_objects`, `lrf_tags`, `lrf_package`, `html_entities` | `ebooks/lrf/meta.py`, `objects.py`, `tags.py`, `__init__.py`, `ebooks/html_entities.py` | The LRF reader and the object and tag parsers its cover needs. `lrf` and `lrf_objects` carry marked fixes. The LRF package (`LRFParseError`, `PRS500_PROFILE`) and the entity table (`html5_entities`, `entity_to_unicode_in_python`) are ported in part |
| `pdf`, `xmp` | `ebooks/metadata/pdf.py`, `xmp.py` | The PDF reader and calibre's XMP reader, both with marked fixes. `xmp` reads the packet `pdfinfo -meta` prints |
| `_poppler` | LiuXin's own | Stands in for `ebooks/pdf/pdftohtml.py` (where poppler's tools are), and for the worker the PDF reader forks: runs `pdfinfo` and `pdftoppm` within limits, in their folder, in process. Inside `without_poppler()` it gives calibre's code the same fields from LiuXin's PDF reader |
| `snbfile` | `ebooks/snb/snbfile.py` | |
| `metadata_base` | `ebooks/metadata/__init__.py` | `MetaInformation`, `string_to_authors`, `check_isbn`, `title_sort` and so on |
| `book_fields`, `book_base`, `field_metadata`, `json_codec` | `ebooks/metadata/book/`, `library/field_metadata.py` | calibre's own `Metadata` class, used by every ported reader |
| `opf2`, `opf3`, `opf`, `metadata_utils`, `toc` | `ebooks/metadata/` | |
| `date`, `iso8601`, `cleantext`, `chardet`, `imghdr` | `utils/`, `ebooks/chardet.py` | `chardet` carries a marked fix |
| `_linear_time` | LiuXin's own | Passes the marked fixes the linear-time stand-ins in `utils/linear_time.py`, which LiuXin's own readers share, and adds `linear_pattern`, which recognises calibre's encoding-declaration patterns by their source |
| `pretty`, `config_base` | parts of `ebooks/oeb/polish/pretty.py`, `utils/config_base.py` | only what the ported code uses |
| `meta` | `ebooks/metadata/meta.py`, in part | `get_metadata`, with its filename pattern, and `opf_metadata`: what the archive readers and calibre's OPF 2 tests use |
| `comic`, `zip`, `rar`, `archive`, `libunzip`, `seven_zip` | the comic reader plugin in `customize/builtins.py`, `ebooks/metadata/zip.py`, `rar.py`, `archive.py` (in part), `libunzip.py` (in part), `utils/seven_zip.py` | The comic, ZIP and RAR readers. `comic` is the plugin's `get_metadata`, ported by hand as a function; the plugin's setting (volume or issue as the series index) is its argument |
| `_readers` | LiuXin's own | Stands in for `customize.ui.get_file_type_metadata`: the archive readers hand each book inside to the ported reader for its type, as calibre's plugin for it calls it |
| `_unrar` | LiuXin's own | Stands in for `utils/unrar.py`, which wraps calibre's `unrardll`: the same functions on `rarfile`, refusing members past LiuXin's archive limits |
| `lit`, `lit_reader`, `lit_package`, `lit_mssha1`, `lit_maps`, `lit_maps_html`, `lit_maps_opf` | `ebooks/metadata/lit.py`, `ebooks/lit/` | The LIT reader and the container reader it uses; `lit_reader` is calibre's `reader.py` without `LitReader`, its conversion reader |
| `chm`, `chm_reader` | `ebooks/chm/metadata.py`, `reader.py` | The CHM reader; it runs where pychm is installed |
| `localunzip` | `utils/localunzip.py` | Reads ZIP files whose central directory is missing or damaged, from their local headers |
| `ebooks`, `beautifulsoup` | `ebooks/__init__.py`, `ebooks/BeautifulSoup.py` | Finding a cover on a book's first page |
| `fb2_package`, `html_to_text` | `ebooks/fb2/__init__.py`, `utils/html2text.py` | FB2's tolerant base64 decoder; HTML to text, which only FB2 writing uses |

## What LiuXin substitutes

`_compat` holds the small helpers taken from modules that are not ported whole,
and its docstring lists each difference from calibre. In short:

| calibre | LiuXin | Effect |
| --- | --- | --- |
| C extensions `speedup` and `fast_html_entities` | Python ports in `_speedups` and `_compat` | Same answers: checked against calibre's compiled C on hundreds of thousands of random inputs, and pinned by tests |
| C extension `cPalmdoc` | the `cPalmdoc` utils plugin: an installed extension, or LiuXin's pure-Python fallback | The fallback compresses byte for byte as calibre's C does, and decompresses the same apart from back-references of distance 0, where the C copies bytes it never wrote |
| ICU collation and case mapping | Python's `casefold`, `lower`, `upper` | Sort order can differ from calibre's for some scripts |
| uchardet | the `chardet` package | Encoding guesses can differ; calibre only guesses when a file declares no encoding and is not UTF-8 |
| `mime.types` added to Python's global table, with the system's files | a private table of Python's built-in types plus calibre's `mime.types` | The same answers on every machine; other code is unaffected |
| ISO 639 and 3166 maps built at calibre's build time | `utils/libraries/iso639/iso_codes.json.gz`, built from iso-codes 4.16.0 by `scripts/build_upstream_calibre_iso_codes.py` with calibre's rules; LiuXin's own language functions read it too | Languages read as ISO 639-3 codes, as calibre stores them (`fra`, not `fr`) |
| python-dateutil, imported directly | the `dateutil_parser` utils plugin: the installed package, or LiuXin's vendored copy | None: the same parser either way |
| The `tzlocal` package, and `zoneinfo` with calibre's bundled tz data | `TZ` or `/etc/localtime`, falling back to UTC | Only dates written without a time zone are affected |
| Interface language from preferences or the system | English | Only the default language of OPF files the code writes |
| Template formatter (`SafeFormat`) | a stand-in that raises when used | Composite custom columns cannot be computed |
| calibre's own `zipfile`, a copy of an old Python one | Python's `zipfile`, with calibre's defaults and `writestr` keywords | Entry names are read the same way; Python's extraction makes them safe |
| html5-parser (C) parsing HTML | lxml's HTML parser | Tags and comments come back the same on ordinary pages; badly broken pages can be repaired differently |
| html5-parser (C) building Beautiful Soup trees | Beautiful Soup's lxml builder; Beautiful Soup is optional | Without Beautiful Soup, an HTML first page holding only a cover image is not recognised; SVG cover pages still are |
| calibre's copy of odfpy | the copy LiuXin ships in `file_formats.odf` | The same library: the ODT reader uses it only to find the cover's frame |
| The html2text package, which calibre bundles | not a LiuXin dependency | Only writing FB2 comments needs it; calibre's FB2 test runs where it is installed |
| A worker process running Qt to draw a book's first page as its cover | not ported: `fork_job` raises `WorkerError`, as a failed worker does | A book with no cover image and a first page of text has no cover; calibre prints a line saying so |
| A worker process running the PDF reader's `read_info`, with poppler's tools bundled | `_poppler.run_here`, in process; poppler from the PATH, run through `utils/bounded_process.py` | The same fields and cover where poppler is installed. Without it, the provider `calibre-port.read-pdf-metadata-without-poppler` gives calibre's code the fields LiuXin's PDF reader reads, and there is no cover |
| podofo, which writes PDF metadata | not ported; raises `NotImplementedError` | Only writing calls it |
| Qt re-encoding a MOBI cover as a JPEG of at most 1650x2200 | not ported: the cover is kept as the book stores it | The cover is the book's own image, named by its bytes |
| Qt saving the JPEG and BMP images of an old MOBI book while reading its text; Pillow converting its GIFs | not ported; Pillow where installed | Those images are skipped (PNGs are written as they are); the metadata does not need them |
| Qt image and date helpers, EPUB writing helpers, the MOBI writer's colour and guide helpers | not ported; raise `NotImplementedError` | Readers do not call them; calibre's tests of the MOBI writer helpers are not run |
| `unrardll`, calibre's binding to RARLAB's library | `_unrar`, on the `rarfile` package (the `archives` extra) or LiuXin's vendored copy | Names are read the same way; contents need an extraction tool on the PATH, so the RAR port is unavailable without one, and a CBR's cover cannot be read |
| `py7zr`, which calibre bundles | optional (the `archives` extra) | Without it a CB7 is not read |
| C extensions `lzx` and `msdes`, for LIT | `_compat.lzx` and `_compat.msdes`, from the utils plugins of those names: the compiled modules where installed, else LiuXin's pure-Python ports | The same metadata; the pure-Python LZX decoder cannot decode LIT text (see the status page) |
| pychm, the binding to chmlib, which calibre bundles | optional, in no extra; `_compat.CHMFile` refuses to open a file without it | Without it the CHM port is unavailable |
| ICU's numeric collation, for page order | `_compat.numeric_sort_key`: numbers by value, the rest case-folded | The same order for page names; accented letters can sort differently |
| calibre's plugin dispatch (`customize.ui`) | `_readers.get_file_type_metadata`, which runs the ported readers; `file_formats.providers.ported_calibre` gives it the readers when it runs one | Books inside archives are read by the ported readers only |
| calibre's preferences (`prefs`) | `_compat.prefs`, at calibre's defaults | File names are matched against calibre's default pattern; author names are not swapped |
| The `regex` package | used where installed (the `search` and `conversion` extras), else `re` | calibre's default filename pattern reads the same either way |

The ported readers parse dates with `python-dateutil`'s parser, as calibre
does. It comes through the utils plugin system's `dateutil_parser` plugin:
python-dateutil itself, a LiuXin dependency, where it is installed, and
otherwise the copy LiuXin vendors, which gives the same answers. See
[utils plugin layers](utils-plugin-layers.md).

## What the providers add

The providers run calibre's code as calibre's plugins do, with two LiuXin
rules around it:

- **Archive limits.** Before calibre's EPUB, HTMLZ, TXTZ, FBZ, ODT, DOCX, ZIP or
  comic reader opens a book, the provider applies the member-count, size and
  compression-ratio limits of LiuXin's own EPUB input
  (`file_formats.archive_preflight`), to 7-Zip comics as well as ZIP files; RAR
  members are checked against the same limits before they are read. calibre reads the members it needs
  whole, and the EPUB reader extracts the whole book when it looks for a cover
  on the first page, so a small file that expands enormously would otherwise
  fill memory or disk. Books whose central directory Python cannot read go to
  calibre's salvage reader, which inflates in bounded pieces.
- **Cover formats named by their bytes.** calibre's EPUB reader labels every
  cover `jpg`, and its HTMLZ and TXTZ reader uses the file's extension, dot
  included; the provider names each cover by what calibre's `imghdr` finds in
  it, so a PNG cover is a `png`.
- **Availability.** The PDF port reports itself unavailable where poppler's
  `pdfinfo` is not on the PATH. The provider below it needs no poppler. The RAR
  port reports itself unavailable where `rarfile` finds no tool to extract with.

## Not ported yet

Every calibre metadata reader is ported. What is left, the writers, is on the
[status page](upstream-calibre-port-status.md#what-is-left).

## Porting more

1. Add the module to `PORTS` in `scripts/port_upstream_calibre.py`, and run
   the script on a calibre checkout. An import its tables do not cover stops
   the run: decide whether to port that module too (add it to `MODULES` and
   `PORTS`), take a few names into `_compat` (`NAMES`), or leave it
   (`LEAVE`, for imports only unused functions make).
2. Port calibre's tests for the module, if it has any, and run them.
3. Add the reader to `PORTED_READERS` and `ported_reader()` in
   `file_formats.providers.ported_calibre`.
4. Try it on damaged and hostile input: truncated files, counts past the end,
   unclosed structures, nested entities, archives that expand enormously,
   paths that climb out of the book. Fix what hangs or reaches outside the
   book in `PATCHES`, marked `# LiuXin:`, and record hangs in the upstream bug
   notes.
5. Compare its readings with the fork's and LiuXin's readers for the same
   files, and note the differences in the pull request.

When calibre releases a new version, run the script again and review the diff.
The readers ported before the script existed (`txt` to `pml`) are not in
`PORTS`; re-port them by hand and keep their marked fixes.

Return to the [plugins index](README.md).
