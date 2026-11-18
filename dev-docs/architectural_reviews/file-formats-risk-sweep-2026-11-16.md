# file_formats risk sweep

Date: 2026-11-16

Status: **Open findings from a static sweep.** See the
[follow-up](#follow-up-2026-11-16) for a correction to FFS06, three executed
reproductions and the proposed fixes for FFS01 to FFS04.
Source checkpoint: `main` at `2d612283`.

This is the first of three passes over `src/LiuXin_alpha/file_formats` proposed
in the [global review](global-review-2026-11-16.md): a pattern sweep for the
ways a hostile input file can harm the host. Subpackage triage and deep review
of individual formats are separate, later passes. The top-level review of
2026-11-14 covers the sixteen top-level modules and is not repeated here.

## Scope and evidence

Every tracked Python module under `file_formats` except the `unihandecode`
code-point tables was parsed with `ast` and searched for calls in these
classes: dynamic evaluation and deserialisation, subprocesses, XML parser
construction, archive extraction, raw decompression, network fetches and
temporary-file creation. Each hit that could take attacker-controlled input was
then read in context. Output paths built from names inside a container were
checked by reading the writers for the formats that extract files.

The sweep was also compared with calibre 9.18. LiuXin forked from a much older
calibre, so input-plugin security fixes listed in calibre's changelog were
checked one by one against the corresponding LiuXin code.

**Limits.** This was static: no dependency could be installed, so **no finding
was executed** and no test was run. A finding marked *source-traced* means the
path from input to effect was read end to end; it has not been reproduced.
Line numbers are for the checkpoint above. The sweep finds known patterns; it
does not show that a module is safe.

## Findings and priorities

P1 needs fixing before untrusted files are converted on a host that holds
anything private. P2 should be fixed before a release. P3 is hardening.

| ID | Priority | Finding | Evidence |
| --- | --- | --- | --- |
| FFS01 | P1 where untrusted HTML is converted; otherwise P2 | HTML input copies any readable local file that the document links to into the output book. Upstream fixed this as CVE-2023-46303. | Source-traced; upstream fix absent. |
| FFS02 | P2 | EPUB input applies font de-obfuscation to a path taken from `encryption.xml` without confining it to the book, so a crafted EPUB can corrupt a file outside it. Upstream: CVE-2026-25636. | Source-traced; upstream fix absent. |
| FFS03 | P2 | CHM extraction writes each entry to `output_dir` joined with the entry's own path. Only a leading `/` is removed, so `..` segments escape. Upstream: CVE-2026-25635. | Source-traced; depends on the CHM library returning such names. |
| FFS04 | P2 | Stored conversion options are read back with `eval()`. A catalogue or defaults file from an untrusted source executes code when its options are loaded. Upstream switched to `ast.literal_eval`. | Source-traced. |
| FFS05 | P3 | There is no shared safe XML parser. Fifteen `XMLParser` constructions and 33 default-parser calls rely on lxml 5's default to block external entities; five parsers also omit `no_network`. | Counted by AST. |
| FFS06 | P3 | Ten raw `zlib`/`bz2` calls and six extraction call sites have no size limit, so a small file can exhaust scratch space or memory. Corrected in the follow-up: several inputs already have a preflight. | Counted by AST. |
| FFS07 | P2 (process) | Four of the findings above are fixed upstream. The fork has no routine for carrying calibre's security fixes forward. | Changelog comparison. |

### FFS01. Confine HTML input resources to the document root

`conversion/plugins/html_input.py:341` (`link_to_local_path`) and `:381`
(`resource_adder`) resolve every link in the input to a local path and, if the
file is readable, add it to the book. There is no check that the path lies
under the folder of the input document. An HTML file containing
`<img src="/etc/passwd">` or a link to a private key therefore produces an
EPUB that contains that file.

This matters most for any workflow that converts HTML obtained from elsewhere,
such as pages acquired by web ingest. calibre fixed the same code by recording
`root_dir_of_input`, rejecting links that resolve outside it, and adding an
`allow_local_files_outside_root` option that defaults to off.

Port that check, with `os.path.realpath` so a symlink cannot defeat it, and
default to refusing. Add a regression test with a link to a file in a sibling
folder.

### FFS02. Confine font de-obfuscation to the EPUB

`conversion/plugins/epub_input.py:344-349` reads `CipherReference/@URI` from
`META-INF/encryption.xml`, builds
`os.path.abspath(os.path.join(dirname(encfile), "..", *uri.split("/")))` and,
if that path exists, calls `decrypt_font` (`:64-83`), which rewrites the file in
place with its leading bytes XOR-ed. A URI of `../../../home/user/notes.txt` reaches outside the
extraction folder. The effect is corruption of an existing file the process can
write, not disclosure.

calibre's current code checks the path with `is_existing_subpath` against the
container folder first. Do the same using the confinement helper proposed as
FFT01 in the 2026-11-14 review.

### FFS03. Reject CHM entries that leave the output folder

`chm/reader.py:381-400` joins `output_dir` with each name from `Contents()`.
`Contents()` (`:606-620`) strips a leading `/` and nothing else. A name with
`..` segments, or on Windows a drive-qualified name, is written outside
`output_dir`.

Normalise each name, drop any that do not resolve under `output_dir`, and log
them. calibre's fix for CVE-2026-25635 ignores such entries.

### FFS04. Stop evaluating stored conversion options

`conversion/config.py:242` (`GuiRecommendations.from_string`) runs `eval(raw)`.
`raw` comes from two places:

- `load_specifics` (`:137-139`): the per-book options stored in the catalogue;
- `load_defaults` (`:85-98`): a defaults file in the configuration folder, read
  during MOBI and Topaz metadata extraction
  (`metadata/file_sources/mobi.py:686`, `topaz.py:598`).

Anyone who can supply a catalogue, for example a library imported from another
machine, can therefore run code. The serialised form is a `repr` of a plain
dictionary, so `ast.literal_eval` reads every legitimate value. Replace the
call, accept only a `dict` result, and treat anything else as absent. This is
the one-line change calibre made.

### FFS05. Route XML parsing through one safe parser

No parser in `file_formats` sets `resolve_entities=False`. Protection against
external-entity file disclosure currently comes from lxml 5.0 changing its
default to `resolve_entities='internal'`, together with the `lxml>=5.0` floor
in `pyproject.toml`. That is a sound floor but a single point of reliance:
internal entities are still expanded, and a parser subclass or entry point with
a different default would not be covered. lxml's `iterparse` and
`ETCompatXMLParser` kept the unsafe default until 6.1.0; neither is used in
`src/LiuXin_alpha` today.

Parsers that also omit `no_network=True`: `opf/__init__.py:1368`,
`opf/opf2.py:1607`, `opf/utils.py:29`, `pdf/reflow.py:1423` and
`oeb/polish/check/parsing.py:669`. `conversion/plugins/lrf_input.py:91,96` sets
`huge_tree=True`, which removes libxml2's size limits; its input is XML
generated by the LRF reader, so the exposure is size rather than entities.

Add one helper equivalent to calibre's `safe_xml_fromstring`
(`resolve_entities=False`, `no_network=True`, no DTD loading), use it for every
parse of file content, and add a check that fails on a bare
`etree.fromstring` or `XMLParser(` in this package.

### FFS06. Bound extraction and decompression

`file_formats/archive_preflight.py` already bounds member count, member size,
total size and compression ratio, and the EPUB, HTMLZ, FB2, comic, DOCX and ODT
inputs call it before extracting. The first version of this finding missed
that module and overstated the gap.

What remains: `ZipFile.extractall` itself places no limit, and six call sites
extract without a preflight: `conversion/plugins/pml_input.py`,
`conversion/plugins/txt_input.py`, `conversion/plugins/recipe_input.py`,
`oeb/polish/container.py` (two calls), `tweak.py` and the developer tool
`docx/dump.py`. Ten call sites call `zlib.decompress` or `bz2.decompress` on data
from the file with no output limit: `lrf/meta.py:972`, `lrf/objects.py:488`,
`mobi/utils.py:993`, `pdb/ereader/reader132.py:233`,
`pdb/plucker/reader.py:921,1024`, `rb/reader.py:295` and
`snb/snbfile.py:383,392,444`.

The project goals call for bounded memory, scratch space and expansion. Route
the remaining extraction sites through the existing preflight, and replace one-shot
decompression with a `decompressobj` loop that stops at a limit.

### FFS07. Track upstream security fixes

FFS01 to FFS04 are all fixed in calibre. Some other upstream fixes are already
matched here by independent hardening (see below), so the gap is uneven rather
than total. Without a routine, each new upstream fix is found only by accident.

Keep a short table of calibre's security-relevant changelog entries with the
LiuXin status of each, and review new calibre releases against it. The entries
checked in this sweep are a starting point.

## Upstream fixes checked

| calibre fix | LiuXin status |
| --- | --- |
| HTML input: files outside the document root (CVE-2023-46303) | **Absent**: FFS01. |
| EPUB input: font obfuscation path traversal (CVE-2026-25636) | **Absent**: FFS02. |
| CHM input: paths outside the container (CVE-2026-25635) | **Absent**: FFS03. |
| Stored conversion options: unsafe de-serialisation | **Absent**: FFS04. |
| ODT input: images outside the container (CVE-2026-26064) | Present: `odt/input.py:138-146` confines to `Pictures` with `commonpath`. |
| PDB input: images outside the container (CVE-2026-26065) | Present for eReader: `pdb/ereader/__init__.py:38` reduces names to a basename. Other PDB readers not checked. |
| RB input: files outside the container (CVE-2026-30853) | Present: `rb/reader.py:239` (`_item_output_path`). Not read in full. |
| FB2 input: unknown namespaces and embedded binaries | Partly: binary names go through `safe_embedded_binary_filename`, but the namespace is not validated (`conversion/plugins/fb2_input.py:359-366`). Needs the deep pass. |
| TXT input: resources outside the book (CVE-2026-33206) | **Not determined.** The corresponding code was not located in `conversion/plugins/txt_input.py`; see FFT03 in the 2026-11-14 review. |
| Python templates when reading metadata (CVE-2026-53511) | Not applicable: no Python template mode was found in the formatter. |

## Checked without findings

- **Archive member names.** Both extraction implementations drop empty, `.` and
  `..` segments and drive prefixes before joining
  (`calibre_zipfile.py:1794-1800`, `localunzip.py:314-318`).
- **Names from containers.** SNB uses basenames or generated names; LIT strips
  leading `../`; MOBI and KF8 generate names; DOCX images and fonts pass
  through `unique_name` and `ascii_filename`.
- **Dynamic evaluation.** `mobi/writer8/header.py:127` evaluates a constant
  class definition, not file content.
- **Deserialisation.** The `pickle` and `marshal` loads in
  `unihandecode/pykakasi/jisyo.py` read bundled resources. Whoever controls the
  resource overlay folder controls them, which is an operator-trust matter.
- **Subprocesses.** No call uses `shell=True` and there is no `os.system`. The
  calls are developer helpers and the user's own editor command in `tweak.py`.
- **Network.** `textile/functions.py:132` fetches remote image sizes only when
  `get_sizes` is set, and nothing sets it. `Plumber.download_cover` fetches a
  URL given as a conversion option.
- **Temporary files.** No `mktemp`.

## Not covered

Reads and writes that follow `href` values inside the OEB and EPUB pipeline
beyond the 2026-11-14 findings; the PDF, DjVu and comic readers; resource
limits on images; anything outside `file_formats`, including
`utils/decompression` beyond the two extraction functions; and the test
fixtures. These belong to the deep pass.

## Suggested sequence

1. **FFS04** first: it is a one-line change with the widest reach.
2. **One confinement helper** (FFT01), then **FFS01, FFS02 and FFS03** built on
   it, each with a regression test that uses a crafted input.
3. **FFS05**, then **FFS06**, as mechanical changes behind a check.
4. **FFS07** alongside, so the table exists before the next calibre release.

## Follow-up, 2026-11-16

Source checkpoint: `main` at `2d2a9cc3`.

| Finding | Status | Evidence and remainder |
| --- | --- | --- |
| FFS01 | **open**, fix proposed in PR #168 | Reproduced: on unfixed `main`, `HTMLInput.link_to_local_path("../secret.txt")` returns the path of a file outside the document folder. |
| FFS02 | **open**, fix proposed in PR #168 | Still source-traced. Its regression test needs `lxml` and had not been executed when this was written. |
| FFS03 | **open**, fix proposed in PR #168 | Reproduced with a stand-in for the CHM library: entries named `../escaped.txt` and an absolute path are written outside the output folder on unfixed `main`. Whether the real library yields such names from a crafted file is not shown. |
| FFS04 | **open**, fix proposed in PR #168 | Reproduced: on unfixed `main`, `GuiRecommendations.from_string` executes an expression that writes a file. |
| FFS06 | **corrected** | The original text said extraction had no size limits. `archive_preflight.py` provides them for six inputs; the finding now covers only the call sites without it and the raw decompression calls. |

The reproductions were run by calling the functions directly, because `pytest`
could not be installed. They are the regression tests in
`tests/file_formats/test_hostile_input_confinement.py` on the PR #168 branch.

Return to the [architecture index](README.md).
