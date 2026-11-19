# file_formats top-level review — 2026-11-14

Reviewed checkpoint: the 16 modules directly in `src/LiuXin_alpha/file_formats/`,
plus the callers cited below. At write-up time `HEAD` was
**`5d5c0c88fb8f9e9e4a3ff14ef15c79b4b27806ba`** and none of these files had
uncommitted changes. The SHA-256 hashes of all 23 cited files matched the
snapshot the reproductions ran against. Subpackages (`conversion/`, `oeb/` and
the per-format packages) were not reviewed, except where they call a top-level
helper. No production code, library or test was changed.

**The top level of `file_formats` is mostly calibre-derived glue: cover
sniffing, CSS unit conversion, the file-type plugin runtime, a TOC model and the
tweak/rebuild helpers. The archive boundary is in good shape: `archive_preflight`
rejects unsafe member names and oversized archives before anything is extracted.
The weak boundary is one step later. Once a book is unpacked, paths taken from its
content are trusted: two helpers follow `../` out of the book today, and a third
would as soon as its existing bug is fixed. Separately, the helper that rebuilds
an EPUB overwrites the original before it knows the rebuild will succeed.** No
finding is P1 today. The traversal only returns raster images at paths an
attacker must guess, and the in-place rebuild has no surface caller yet. Three
findings are P2.

## Scope and evidence

| Depth | Areas |
| --- | --- |
| Read in full | `__init__.py`, `utils.py`, `archive_preflight.py`, `plugin_runtime.py`, `import_plugins.py`, `toc.py`, `tweak.py`, `rendering.py`, `plugin_defaults.py`, `api.py`, `constants.py`, `html_entities.py`. |
| Read selectively | `covers.py` (1,929 lines): import-time configuration, the Pillow fallback, `generate_masthead`, `calibre_cover2` and the formatting escapes. The Qt drawing paths were skimmed, because PyQt5 is not a declared dependency. |
| Checked for use only | `hyphenate.py`, `sgmllib.py`, `markupbase.py`: vendored modules with no production importer (FFT09). |

Evidence comes in two kinds, and each finding names its kind:

- **Executed:** a disposable probe that calls the package's own code, run on a
  snapshot of the repository in a Python 3.13.18 sandbox (lxml 6.1.3,
  Pillow 12.3.0; PyQt5, cssutils and `regex` not installed). The development
  machine's virtual environment is Python 3.10 and cannot import the package,
  which requires 3.12 or later.
- **Source-traced:** a code path followed by reading, without execution.

The probes, their output (`probes.log`) and a rendered image are kept locally in
the ignored `.tmp/file-formats-top-level-review-2026-11-14/` directory. Re-run any
of them with `HOME=$(mktemp -d) python3 probe_<name>.py <repo>/src`. The results
that matter are reproduced below.

## Findings and priorities

P1 means resolve before running the affected operation on a library that
matters. P2 means a reproducible correctness or operational gap to close in a
focused change. P3 means contained hardening or consistency debt.

| ID | Priority | Finding | Evidence |
| --- | --- | --- | --- |
| FFT01 | P2; P1 before serving covers or bulk-ingesting untrusted EPUBs | Cover sniffing follows `../` in book content and returns any raster image on disk. EPUB metadata ingest reaches it when the OPF names no cover image. | Executed (helpers and `render_html_svg_workaround`); source-traced (ingest call chain). |
| FFT02 | P2 | `zip_rebuilder` truncates the target first. A failure partway leaves a valid-looking, content-short EPUB where the original was. | Executed. |
| FFT03 | P2 | The TXT→TXTZ import plugin never runs. The obvious one-line fix makes it copy files from outside the book's folder into the library copy. | Executed, with and without the fix. |
| FFT04 | P3 | `get_file_type_metadata(path)` opens the path in text mode and silently returns empty metadata. | Executed. |
| FFT05 | P3 | File-type plugins are not chained, `except BaseException` swallows Ctrl-C, and per-call options live on shared plugin instances. | Executed (first two); source-traced (third). |
| FFT06 | P3 | TOC hrefs are percent-decoded before splitting, so `%23` and `%3F` in file names break them. `render()` does not clean author or description text. | Executed. |
| FFT07 | P3 | `UNIT_RE` accepts `-`, `.` and `--5` as numbers, and `float()` then raises. Valid `+3px` and `12PX` are not parsed. | Executed. |
| FFT08 | P3 | Broken or dead paths: `calibre_cover`, the Kindle tools in `tweak`, the Qt renderer, the `fork_job` fallback and `calibre_cover2`. | Mixed; marked individually. |
| FFT09 | P3 | `__init__.py` duplicates 514 lines of `utils.py`. Four modules (2,335 lines) have no production importer. | Executed (AST comparison and import search). |
| FFT10 | P3 | The Pillow fallback cover, used whenever PyQt5 is absent, renders CJK titles as boxes and cannot wrap titles without spaces. | Executed, with rendered output. |

### FFT01. Confine paths taken from book content to the book

[`extract_cover_from_embedded_svg`](../../src/LiuXin_alpha/file_formats/utils.py)
(line 130) and `extract_calibre_cover` (line 160) build image paths with
`os.path.join(base, *src.split("/"))`, at lines 156, 189, 200, 211 and 225. The
duplicate in [`__init__.py`](../../src/LiuXin_alpha/file_formats/__init__.py)
does the same at line 284. Nothing checks that the result stays under `base`.
`return_raster_image` (line 106) then returns the bytes of any file that sniffs
as a raster image.

These helpers run on book content in three places:

- EPUB metadata ingest. In [`epub.py`](../../src/LiuXin_alpha/metadata/file_sources/epub.py),
  `get_cover` (line 875) falls back to `_render_cover_from_spine` (line 840) when
  the OPF names no raster cover. That function extracts the archive (line 858)
  and calls `render_html_svg_workaround` on the first spine page (line 864).
- EPUB conversion: [`epub_input.py`](../../src/LiuXin_alpha/file_formats/conversion/plugins/epub_input.py)
  (line 438).
- The OEB reader: [`oeb/reader.py`](../../src/LiuXin_alpha/file_formats/oeb/reader.py)
  (line 1012).

The probe extracted a book under a temporary root and placed a PNG at
`home/user/Pictures/passport.png` under the same root. The book's page used
twelve `../` hops, enough to reach `/`, followed by the absolute path:

```text
extract_calibre_cover returns the private image: True
extract_cover_from_embedded_svg returns it too:  True
render_html_svg_workaround (EPUB ingest path):    True
```

The stolen image becomes the book's cover. It leaves the machine whenever covers
do: through export, a shared library or a served catalogue. Only files that sniff
as raster images are returned, and the attacker must guess an absolute path,
which is why this is P2 rather than P1. The archive member names themselves are
already handled: `archive_preflight` rejects `../` members, and the
[EPUB dossier](../file_formats/epub/README.md) requires that rejection to stay.
That contract does not yet cover references inside extracted content. The join
pattern is inherited from calibre's helper.

**Next:** add one helper that resolves a book-relative reference with `realpath`
and returns `None` unless `commonpath` with the book root equals the root. Use it
at all six joins (five in `utils.py`, one in `__init__.py`) and in FFT03. Add
regression tests for `<img src>`, SVG `xlink:href` and the spine render path.

### FFT02. Make `zip_rebuilder` atomic

[`zip_rebuilder`](../../src/LiuXin_alpha/file_formats/tweak.py) (line 180) opens
the target with `ZipFile(path, "w")` (line 197), which truncates it, and then
walks the working tree. If anything fails partway, `ZipFile.__exit__` still
writes a central directory. The result is a well-formed archive that holds only
the members written so far.

The probe exploded an EPUB with `zip_exploder` and then added one unreadable
entry to the working tree. A dangling symlink stood in for a permission error,
an I/O error or a file removed by another process:

```text
zip_rebuilder raises FileNotFoundError: [Errno 2] No such file or directory: '.../exploded/zzz_style.css'
original before: 120372 bytes
original after : 244 bytes  changed=True
original still opens as zip; members: ['mimetype', 'content.opf']
```

The chapter was lost, and a "does it open as a zip?" check still passes. This
conflicts with the archival rule that original files are never destroyed.

Callers:

- `tweak()` (line 245).
- [`EpubContainer.commit`](../../src/LiuXin_alpha/file_formats/oeb/polish/container.py)
  (line 2157), which defaults `outpath` to the source EPUB (lines 2182–2188).
- Polish (`oeb/polish/main.py`, line 495), which commits to the output path it is
  given. Its command line accepts an explicit `inbook outbook` pair, and the two
  can name the same file.

No terminal or library surface calls these today, which keeps this at P2. Fix it
before any surface does.

**Next:** write to a temporary file in the target's directory, fsync it,
`os.replace` it over the target, then fsync the directory (the same rule DBR13
asks for in SQLite publication). Remove the temporary file on failure. Consider
making `EpubContainer.commit` require an explicit `outpath`. Turn the probe into a
regression test.

### FFT03. Repair the TXT→TXTZ import plugin and confine its references

[`TXT2TXTZ.run`](../../src/LiuXin_alpha/file_formats/import_plugins.py)
(line 218) reads the file as bytes (line 233) and passes them to
`_get_image_references` (line 237), whose regular expressions are `str` patterns.
Every call raises `TypeError`. `_run_filetype_plugins` catches it, prints the
traceback and returns the original path. `.txt` imports therefore still succeed,
but always as plain TXT without the images they reference.

The reference filter accepts any relative path that exists, checked with
`not os.path.isabs(path)` and `os.path.exists(os.path.join(base_dir, path))`
(lines 173–175, 189–191 and 209–211). That allows `../`. Line 263 then writes each
reference into the archive under its relative path. The probe ran the shipped
plugin, and then the same plugin with only a UTF-8 decode added before the
regular expressions:

```text
(a) as shipped
  run() raises TypeError: cannot use a string pattern on a bytes-like object
  through run_plugins_on_import: returned original path unchanged: True
(b) with the decode fix applied
  (b1) no metadata.opf beside the txt
    run() raises AttributeError: Metadata object has no attribute named: 'creator_sort'
  (b2) metadata.opf beside the txt
    members: ['notes.txt', 'metadata.opf', '../private/secret.png']
    bytes match the private file: True
```

Once decoded, a text file can pull a file from outside its folder into the
library's TXTZ. The filter checks only that the file exists, so this is not
limited to images (source-traced; the probe used a PNG). The archive also carries a `../`
member that `archive_preflight` would reject when the book is read back. Case
(b1) is a separate bug: when no `metadata.opf` sits beside the text file, the
generated-OPF branch (lines 256–259) fails in `metadata_to_opf`.

**Next:** fix all three together. Decode the text (UTF-8 with replacement, or the
TXT reader's encoding detection), confine references with the FFT01 helper, and
repair the metadata-to-OPF call. Add tests for a TXT with an image beside it and
for one with a `../` reference.

### FFT04. Open paths in binary mode in `get_file_type_metadata`

[`get_file_type_metadata`](../../src/LiuXin_alpha/file_formats/plugin_runtime.py)
(line 594) accepts a path as well as a stream. It opens a path with
`open(stream)` (line 632), which is text mode, and never closes it. Readers that
need bytes fail, the exception is printed, and an empty `Metadata` is returned:

```text
stream -> title: Probe Title
path   -> title:
stderr tail: EPubException: Unable to open EPUB container as ZIP data.
```

No caller in the repository passes a path today. The function is public through
[`plugins/application.py`](../../src/LiuXin_alpha/plugins/application.py)
(line 94).

**Next:** open with `"rb"` inside a `with` block, and add a test that passes a
path.

### FFT05. Chain file-type plugins and stop swallowing interrupts

- `_run_filetype_plugins` (line 182) runs each plugin on `path_to_file`, not on
  the previous plugin's output (line 223). The executed probe used two import
  plugins for one file type:

  ```text
  stage1 got in.probe
  stage2 got in.probe
  final path: in.probe (stage1 output discarded)
  ```

  The two built-in import plugins (`PML2PMLZ`, `TXT2TXTZ`) handle different
  types, so this affects third-party plugins.
- `except BaseException` at lines 226, 287, 653 and 699 catches `KeyboardInterrupt`
  and `SystemExit`. Executed: a plugin that raised `KeyboardInterrupt` was
  swallowed, and `run_plugins_on_import` returned normally. The rollback in
  `replace_plugins` (line 70) re-raises and is fine.
- The `quick`, `apply_null` and `force_identifiers` switches are module-global
  context managers (lines 417–591). Their values, and `site_customization`, are
  copied onto shared plugin instances on each call (lines 217, 648 and 695–696).
  Two threads reading or writing metadata at once can see each other's settings.
  Source-traced.

**Next:** use `nfp = plugin.run(nfp) or nfp`, catch `Exception`, and pass
per-call options as arguments or through `contextvars`.

### FFT06. Split TOC hrefs before decoding them

[`_parse_href`](../../src/LiuXin_alpha/file_formats/toc.py) (line 57) calls
`urlparse(unquote(raw_href))` (line 73). It decodes percent-escapes before
splitting off the fragment and query, so an escaped `#` or `?` in a file name
splits the path. `read_from_opf` decodes in the same order at line 370.

| Input | `_parse_href` returns | Expected |
| --- | --- | --- |
| `Text/ch%231.xhtml#p3` | `('Text/ch', '1.xhtml#p3')` | `('Text/ch#1.xhtml', 'p3')` |
| `Text/what%3F.xhtml` | `('Text/what', None)` | `('Text/what?.xhtml', None)` |
| `Text/50%25.xhtml#a` | `('Text/50%.xhtml', 'a')` | the same |
| `Text/a%20b.xhtml#x` | `('Text/a b.xhtml', 'x')` | the same |

`read_ncx_toc` on a real NCX produced the same broken entries.

`render()` (line 544) passes entry text through `clean_xml_chars` (line 595), but
not the author or description (lines 605–612). One control character in an
author name raises
`ValueError: All strings must be XML compatible`.

**Next:** split first, then decode the path and the fragment separately. Clean the
author and description the same way as the text. Add both cases to
`tests/file_formats/test_toc_corner_cases.py`.

### FFT07. Tighten the CSS length pattern

[`UNIT_RE`](../../src/LiuXin_alpha/file_formats/utils.py) (line 489) is
`^(-*[0-9]*[.]?[0-9]*)\s*(%|em|ex|en|px|mm|cm|in|pt|pc|rem)$`. The number group
can match `-`, `.` or `--5`, and `unit_convert` (line 519) and `parse_css_length`
(line 622) then call `float()` on it. Valid CSS such as `+3px` or `12PX` does not
match. `unit_convert` returns those values as the original string, which callers
may then treat as a number. The `__init__.py` copies behave identically.

| Input | `unit_convert` | `parse_css_length` |
| --- | --- | --- |
| `12pt`, `-.5em` | `12.0`, `-6.0` | `(12.0, 'pt')`, `(-0.5, 'em')` |
| `-px`, `.em`, `--5pt` | `ValueError` | `ValueError` |
| `+3px`, `12PX` | returned as the string | `(None, None)` |

`docx/writer/styles.py` calls `parse_css_length` without exception handling
(lines 1179 and 1200). The MOBI reader guards its call. Whether malformed values
can reach these functions after cssutils normalisation was not tested, because
cssutils was absent.

**Next:** use `^([-+]?(?:\d+(?:\.\d*)?|\.\d+))\s*(units)$` with
`re.IGNORECASE`, and extend `tests/file_formats/test_utils_parity.py` with the
cases above.

### FFT08. Fix or remove broken and dead paths

| Path | Evidence | Next |
| --- | --- | --- |
| [`calibre_cover`](../../src/LiuXin_alpha/file_formats/utils.py) (line 421, and the `__init__.py` copy at line 501) imports `LiuXin_alpha.utils.wrappers.magick.draw` (line 449), which does not exist. It then imports `regex` (line 450), which is not a declared dependency, and ignores its `output_format` argument (line 482). Its only caller, `replace_wmf` in [`rtf_input.py`](../../src/LiuXin_alpha/file_formats/conversion/plugins/rtf_input.py) (line 267), runs inside error handling. RTF conversion therefore keeps the raw WMF instead of inserting a placeholder. | Executed | Reimplement it on the Pillow drawing in `covers.py`, or delete it together with the WMF placeholder branch. |
| [`tweak.get_tools`](../../src/LiuXin_alpha/file_formats/tweak.py) for MOBI, AZW and AZW3 imports `LiuXin_alpha.utils.calibre.ebooks.mobi.tweak` (lines 145 and 234). The real module is `LiuXin_alpha.file_formats.mobi.tweak`. The `ModuleNotFoundError` escapes before `tweak()` (line 245) reaches its error handling. The unsupported-format message also prints a literal `%s` (line 263). | Executed (import); source-traced (message) | Fix the import path and add a smoke test for each format. |
| `render_html` (line 317) returns `None` unless `rendering.is_ok_to_use_qt()` is true, and that function always returns `False`. The unreachable `HTMLRenderer` would store `str(bytes)` (line 98), and it sets `self.traceback` (line 101) while initialising `self.tb` (line 63). | Source-traced | Delete it, or fix it before Qt rendering is re-enabled. |
| `render_html_svg_workaround` (line 229) falls back to `fork_job` (line 282). That starts a worker process to call `render_html_data`, which can only return `None`. | Executed: about 10 ms per text-first page, result `None` | Skip the fork when Qt is unavailable. |
| [`calibre_cover2`](../../src/LiuXin_alpha/file_formats/covers.py) has no callers. It divides by `prefs["cover_height"]` (line 1718) before `_normalize_cover_prefs` validates the value. | Source-traced | Delete it, or normalise before dividing. |

### FFT09. Collapse the `__init__.py` duplicate and drop unused modules

[`__init__.py`](../../src/LiuXin_alpha/file_formats/__init__.py) lines 166–705
repeat 17 definitions from `utils.py`, 514 lines in all. An AST comparison with
docstrings removed found:

- 12 definitions identical;
- four that differ only in annotations, an `object` base class, or the
  `fork_job` target;
- `extract_calibre_cover`, which already delegates to `utils` (from the May 2026
  cover-utilities work in `working-memory/file-formats-cover-utils-2026-05-20.md`).

`tests/file_formats/test_utils_parity.py` keeps the copies aligned, but every fix
in FFT01, FFT07 and FFT08 still has to be made twice.

`hyphenate.py`, `sgmllib.py`, `markupbase.py` and `api.py` (2,335 lines together)
have no importer outside the import smoke tests. `sgmllib.py` duplicates
`utils/libraries/sgmllib.py`, which the vendored BeautifulSoup uses with the
standard library's `_markupbase`. `api.py` contains only a module docstring and a
`__future__` import.

**Next:** make `__init__.py` re-export the helpers from `utils.py`, keeping the
exception classes and `BOOK_EXTENSIONS` where they are. Do this before FFT01, and
check the [layer-direction](layer-direction-ratchet.md) and
[import-footprint](import-footprint-ratchet.md) ratchets afterwards. Delete the
four unused modules, or move them to a vendored location, once no plugin is
known to rely on them.

### FFT10. Make the fallback cover usable for CJK and long titles

When PyQt5 cannot be imported (`HAS_QT`, [`covers.py`](../../src/LiuXin_alpha/file_formats/covers.py)
lines 49–51), and it is not a declared dependency, covers come from
`_draw_fallback_cover_with_pillow` (line 1345):

- The font lookup tries DejaVu Sans, Liberation Sans and Arial by file name, then
  `ImageFont.load_default()` (lines 1392–1397). None of these has CJK glyphs, so
  Chinese, Japanese and Korean titles render as boxes.
- Wrapping splits on whitespace only (line 1448), so a long title without spaces
  runs off the canvas.
- The header band is fixed at 18% of the height (line 1376), so a three-line
  title spills below it.

Executed on a 600 × 800 cover with 540 px of usable width:

```text
font the fallback will pick: DejaVuSans.ttf
latin           single unwrapped title width  1099px  wraps on spaces: True
cjk             single unwrapped title width   475px  wraps on spaces: False  (glyphs render as boxes)
unspaced_latin  single unwrapped title width  1223px  wraps on spaces: False  (text cut off)
```

The rendered covers are in `fallback_covers_side_by_side.png` in the evidence
directory.

**Next:** add CJK-capable families to the lookup (for example Noto Sans CJK,
Source Han Sans, WenQuanYi or Microsoft YaHei), preferably through a
configurable font path. Fall back to wrapping by measured character width when a
title has no spaces, and size the band from the line count.

## Checked without new findings

These areas were examined for this review's failure modes and held up:

- `archive_preflight.py` rejects backslashes, absolute paths, drive letters and
  `..` segments. It caps the member count (4,096), the size of one member
  (256 MiB), the total size (512 MiB) and the compression ratio (1,000 for
  members of 1 MiB or more). HTMLZ, comic and EPUB input, DOCX, FB2 and ODT all
  use it. Two hardening notes, source-traced: duplicate normalised names collapse
  silently (the last one wins), and members whose names differ only in case are
  not reported. Either can make two tools read different members.
- Importing `covers.py` constructs `JSONConfig("cover_generation")` but creates
  nothing on disk (executed with a scratch `HOME`).
- `replace_plugins` restores the previous registry if the builder fails, and
  registry changes hold a re-entrant lock.
- `plugin_defaults.py`, `rendering.py`, `constants.py` and `html_entities.py` are
  small and match their contracts.
- `check_ebook_format`, `normalize` and `escape_xpath_attr` raised no issues.

## Validation performed

- Focused tests: the 12 top-level modules in `tests/file_formats/test_*.py` ran
  in the Python 3.13.18 sandbox, with 131 passing in about 4 seconds. The
  per-format suites, the full suite and the quality gate were **not** run. No
  static analysis was run.
- Eight probes ran against the package's own code, using disposable temporary
  directories. No repository file, library or test database was modified.
- At write-up time, the SHA-256 hashes of the 23 files cited above matched
  between the probed snapshot and the working tree at `5d5c0c88`.

## Suggested next sequence

1. **Collapse the duplicate first:** the FFT09 re-export, so that later fixes land
   once.
2. **Close the content-path boundary:** FFT01's helper, then FFT03 built on it.
   Each needs a regression test made from its reproduction.
3. **Make rebuilds atomic:** FFT02, before any surface exposes tweak or polish on
   library files.
4. **Hardening:** FFT04–FFT08 and FFT10 as small focused changes. FFT10 matters
   most for CJK collections.

New fixes should cite the relevant FFT ID. Record implementation, verification
and inclusion in `main` separately, as the [review index](README.md) asks.
