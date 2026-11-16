# HTMLBook specification

Date: 2026-11-16

Status: **Draft for review. Not implemented.**
Source checkpoint: `main` at `2d2a9cc3`.

`HTMLBook` is the HTML intermediate named in the
[capability providers proposal](../plugins/format-capability-providers.md). This
document defines it precisely enough to implement and test: the data model, the
rules a value must satisfy, how one is loaded and saved, and what a conforming
implementation must do with hostile input.

The rules are derived from what `file_formats/html/input.py`,
`conversion/plugins/html_input.py`, `htmlz_input.py`, `htmlz_output.py`,
`html_output.py` and `Plumber.find_html_index` do today.
[Differences from current behaviour](#differences-from-current-behaviour) lists
every place where this specification deliberately departs from that code.

In this document **must** marks a requirement for conformance, **should** a
strong recommendation, and **may** a permitted option. Type and function names
are proposed. Values marked *proposed* are open to change in review.

## Purpose

An `HTMLBook` describes a set of HTML pages and the files they use, held in one
folder. It serves three uses:

1. the value passed between `HTMLImporter`, `HTMLTransform` and `HTMLExporter`
   providers;
2. the join between those providers and file-based `Converter` providers, which
   read and write the folder directly;
3. the description of an HTML document the library holds, such as a page
   acquired by web ingest.

Out of scope: provider signatures beyond the type they pass, execution policy,
rendering, and running scripts. How a held document maps onto storage is
described in a non-normative section.

## Terms

| Term | Meaning |
| --- | --- |
| Root | The folder that contains every file of the book. |
| Member | A file that belongs to the book: a page, a resource, the cover or the metadata sidecar. |
| Page | A member that is an HTML document a reader moves through. |
| Resource | Any other member a page uses or links to: an image, stylesheet, font, script or linked file. |
| Entry | The page a reader opens first. |
| Reading order | The sequence of pages. HTML has links, not a sequence, so the order is the result of a stated rule. |
| Local reference | A link or resource reference that names a file. |
| Remote reference | A reference that names a network location. |
| Outside reference | A local reference that resolves outside the root. |
| Described folder | A root the book was loaded from and does not own, such as a held document. |

## Data model

```python
@dataclass(frozen=True)
class HTMLPage:
    path: PurePosixPath
    encoding: str
    title: str | None = None


@dataclass(frozen=True)
class HTMLTocEntry:
    label: str
    page: PurePosixPath
    fragment: str | None = None
    children: tuple["HTMLTocEntry", ...] = ()


@dataclass(frozen=True)
class HTMLBook:
    root: Path
    entry: PurePosixPath
    pages: tuple[HTMLPage, ...]
    resources: tuple[PurePosixPath, ...] = ()
    toc: tuple[HTMLTocEntry, ...] = ()
    toc_is_derived: bool = True
    cover: PurePosixPath | None = None
    metadata: PurePosixPath | None = None

    @property
    def title(self) -> str | None:
        """The entry page's title."""
```

| Field | Meaning |
| --- | --- |
| `HTMLPage.path` | The page's path relative to the root. |
| `HTMLPage.encoding` | The character encoding of the page's bytes, as a Python codec name. |
| `HTMLPage.title` | The page's title, or `None` if it has none. |
| `HTMLTocEntry.label` | The text shown for the entry. |
| `HTMLTocEntry.page`, `fragment` | Where the entry points: a page, and optionally an anchor within it. |
| `HTMLTocEntry.children` | Nested entries. |
| `HTMLBook.root` | The folder that contains every member. |
| `HTMLBook.entry` | The path of the first page. |
| `HTMLBook.pages` | The pages in reading order. |
| `HTMLBook.resources` | The resources, in a stable order. |
| `HTMLBook.toc` | The table of contents. |
| `HTMLBook.toc_is_derived` | `True` when the table of contents was built by the rule in this document, and `False` when a provider or manifest supplied it. |
| `HTMLBook.cover` | The cover image, if any. |
| `HTMLBook.metadata` | An OPF metadata sidecar, if the source had one. |

Page titles and the table of contents are part of the type at the
maintainer's direction, so a held document can be browsed without opening its
files. There is no separate book-level title field: `HTMLBook.title` is the
entry page's title, so there is one source for it. Catalogue metadata is not stored on the
type; the proposal carries it outside the route.

The proposed home is a new module under `file_formats/html/`.

## Invariants

A value that breaks any of these must not be constructed.

Structural invariants need no filesystem access:

| ID | Rule |
| --- | --- |
| S1 | `root` is an absolute path. |
| S2 | Every member path is relative, uses `/` as its separator, and has no empty, `.` or `..` segment, no drive or anchor, no backslash and no NUL character. |
| S3 | `pages` is not empty and `pages[0].path` equals `entry`. |
| S4 | No path appears twice among the members. `cover`, when set, is one of `resources`. `metadata`, when set, is neither a page nor a resource. |
| S5 | Each `encoding` is a codec that `codecs.lookup` accepts, stored under the name that lookup returns. |
| S6 | Each `label` contains a visible character. Each table-of-contents `page` is the path of one of `pages`. |

Filesystem invariants are checked by `load`, `save` and an explicit
`validate`:

| ID | Rule |
| --- | --- |
| F1 | `root` exists and is a directory. |
| F2 | Each member, after symbolic links are followed, is a regular file inside `root`. |

F2 uses the containment check introduced for finding FFS01 in the
`file_formats` risk sweep of 2026-11-16: both paths are made absolute,
resolved through links and case-normalised before comparing.

## Pages

**What is a page.** The entry is always a page. Any other local file is a page
when its first 4096 bytes, decoded with its detected encoding, contain an
`<html` tag in any letter case. This is today's test.

**Encoding.** A page's encoding is the first of these that applies:

1. an encoding supplied by the caller;
2. a byte-order mark for UTF-8 or UTF-16;
3. a declaration in the first 4096 bytes: an XML declaration, a
   `meta charset`, or an `http-equiv` content type;
4. statistical detection over the same bytes;
5. UTF-8.

The load report names every page whose encoding came from step 4 or 5, since
those can be wrong. Bytes that do not decode are replaced, not rejected.

**Title.** A page's title is the text of its first `title` element, with
character references resolved and runs of whitespace collapsed to one space. An
empty result is `None`. Where a title is needed for display and there is none,
the file name without its extension is used.

## References

Loading classifies every reference found in a page or stylesheet.

| Reference | Class | Outcome |
| --- | --- | --- |
| A fragment only, such as `#notes` | Internal | Ignored. |
| No scheme and no host, or the `file` scheme with no host | Local | Resolved as below. |
| A host with no scheme, such as `//cdn.example/a.css` | Remote | Reported; never fetched. |
| `http`, `https` or `ftp` | Remote | Reported; never fetched. |
| `data` | Inline | Ignored. |
| Any other scheme, such as `mailto` | Other | Ignored. |

A local reference is resolved to a file like this:

- The path, with any parameters and query string, is percent-decoded and used
  as the file name, as today. Mirroring tools commonly save pages under names
  that include the query.
- A path with no leading `/` is resolved against the folder of the page or
  stylesheet that contains the reference.
- A path with a leading `/` is resolved against the root. *Proposed*; today it
  is treated as an absolute filesystem path.
- A `file` URL is resolved as a filesystem path.
- A `base` element is not applied, as today.

The resolved file then has one of three outcomes:

| Resolved file | Outcome |
| --- | --- |
| Inside the root and present | It becomes a member. |
| Inside the root and absent | Reported as a missing reference. |
| Outside the root | Reported as an outside reference. It must not be read, listed or tested for existence beyond what resolving the path requires. |

Anchors (`a href`) name candidate pages. Every other reference names a
resource, including references inside stylesheets, which are followed
recursively. A conforming implementation must find at least `a href`,
`img src`, `link href`, `script src`, and `url()` and `@import` in CSS. The
reference implementation should use the same link iteration `html_input` uses
today.

A local file that an anchor points to and that is not a page of the book,
because it fails the page test or lies beyond the depth limit, is a resource.

## Reading order

Pages are discovered from the entry by following anchors, breadth first, to at
most `max_depth` links away. The default is 5, today's `max_levels`. Each page
is discovered once; cycles end at the first repeat.

The discovered pages are then ordered by one of three rules:

| Rule | Order |
| --- | --- |
| `depth_first` (default) | A walk from the entry that visits each page before the pages it links to, taking links in the order they appear. |
| `breadth_first` | The order of discovery. |
| `given` | An order supplied by the provider that built the book. |

The same folder, entry, rule and depth must always give the same order.

## Table of contents

A table of contents is either supplied or derived.

**Supplied.** A provider or a manifest gives the entries, which may be nested.
`toc_is_derived` is `False`.

**Derived.** There is one top-level entry per page, in reading order, pointing
at the page with no fragment. `toc_is_derived` is `True`. Labels are chosen for
the whole book by one rule:

1. If every page has a title and no two titles are equal, each label is the
   page's title.
2. Otherwise each label is the text of a heading in the page, with whitespace
   collapsed. The element types are tried in this order, and the first type
   the page contains is used: `h1`, `h2`, `h3`, `h4`, `h5`, `strong`.
3. A page with no such element takes its display title, which is its title or
   else its file name without the extension.

A consumer may replace a derived table of contents, as structure detection
does today for an automatically generated one. It must not silently replace a
supplied one.

## Loading

```python
def load_html_book(
    source: Path,
    *,
    entry: PurePosixPath | None = None,
    members: Literal["linked", "all"] = "linked",
    order: Literal["depth_first", "breadth_first"] = "depth_first",
    max_depth: int = 5,
    encoding: str | None = None,
    limits: ArchiveLimits = DEFAULT_LIMITS,
) -> HTMLLoadResult:  # .book and .report
```

`source` is one of four things:

| Source | Root | Notes |
| --- | --- | --- |
| A folder | The folder | It is a described folder. |
| An HTML file | The file's folder | The file is the entry. |
| A ZIP archive | A fresh scratch folder | Extracted after the archive preflight. |
| An HTMLZ archive | A fresh scratch folder | Extracted after the archive preflight; see [HTMLZ](#the-htmlz-profile). |

Loading proceeds in this order:

1. Establish the root.
2. If the root holds a valid [manifest](#manifest), the manifest is the book.
   `entry`, `members`, `order` and `max_depth` are not applied, and files the
   manifest does not list are reported.
3. Otherwise find the entry: the `entry` argument if given, else the rule for
   the source kind.
4. Discover and order the pages, collect the resources, read titles and derive
   the table of contents.
5. Check every invariant.

**Finding the entry in a folder or ZIP.** Among the HTML files, taken from
smallest to largest, the first whose name without extension is `toc`, then the
first whose name is `index`; otherwise the largest HTML file. This is
`Plumber.find_html_index`. Ties are broken by path so the result is
deterministic. The report records that the entry was guessed.

**Which files become members.**

| `members` | Pages | Resources |
| --- | --- | --- |
| `linked` | Pages discovered from the entry | Files the pages and their stylesheets refer to |
| `all` | As `linked`, followed by every other file under the root that has an HTML extension and passes the page test, in path order | Every other regular file under the root |

`linked` matches a conversion today. `all` is for describing a held document,
where no file should be left out.

Loading must not write inside a described folder, must not fetch anything from
a network, and must not run scripts.

### Load report

Every load returns a report beside the book.

| Field | Content |
| --- | --- |
| `source_kind` | Folder, file, ZIP or HTMLZ. |
| `entry_source` | Given, manifest or guessed. |
| `order`, `max_depth` | The rule and depth that produced the reading order. |
| `uncertain_encodings` | Pages whose encoding was detected statistically or assumed. |
| `remote_references` | Each remote reference and the page that holds it. |
| `outside_references` | Each outside reference and the page that holds it. |
| `missing_references` | Each missing reference and the page that holds it. |
| `skipped` | Files not taken as members, with the reason: a link leading outside the root, not a regular file, unreadable, or over a limit. |
| `unlisted` | Files under the root that are not members. |
| `case_collisions` | Member paths that differ only by letter case. |
| `is_self_contained` | `True` when there are no remote, outside or missing references. |

When a load happens inside a conversion, the runner should record missing,
outside and remote references and uncertain encodings as loss events on the
existing `ConversionReport`, so that they reach the job result with the rest.

## Saving

```python
def save_html_book(
    book: HTMLBook,
    destination: Path,
    *,
    form: Literal["folder", "zip", "htmlz"] = "folder",
    write_manifest: bool = True,
) -> Path
```

- Saving copies each member's bytes unchanged to the same relative path. It
  must not re-encode or rewrite a page.
- `destination` must not exist, or must be an empty folder. It must not be
  inside the book's root, and the root must not be inside it.
- The result must appear complete or not at all: write beside the destination
  and move it into place.
- A `folder` or `zip` save writes the manifest unless `write_manifest` is
  false. If a member already uses the manifest's path, the save fails.
- A saved ZIP should be the same bytes for the same book: members in a fixed
  order with fixed timestamps. Assets are identified by digest, so a save that
  varies between runs would create a new asset each time.
- A book loaded from a folder or ZIP that was saved with a manifest must equal
  the book that was saved, apart from `root`.

### The HTMLZ profile

HTMLZ is a narrower form, not the general packed form. It holds one top-level
HTML page, its resources, and optionally a top-level OPF file and a cover.

- **Loading.** The entry is the top-level `index.html`, else `index.xhtml`,
  else `index.htm`, else the first top-level HTML file in path order. Only the
  entry is a page; other top-level HTML files are reported as skipped. A
  top-level `.opf` file becomes `metadata`. The cover named by that file
  becomes `cover` when it is a safe path to a present file, following the
  existing HTMLZ policy for unsafe and missing covers.
- **Saving.** The book must have exactly one page, at the top level, and any
  `metadata` must be a top-level `.opf` file. Otherwise the save fails. No
  manifest is written.
- **Round trip.** A supplied table of contents does not survive HTMLZ. On
  reload it is derived, giving one entry.

### Manifest

A manifest records a book so that loading reproduces it exactly, including a
supplied reading order and table of contents that could not be derived again.

- It is a UTF-8 JSON file named `htmlbook.json` at the root. *Proposed.*
- It carries `"format": "liuxin-htmlbook"` and `"version": 1`, followed by the
  fields of `HTMLBook` other than `root`.
- A file of that name without the format marker is an ordinary file.
- A manifest is untrusted input. Its paths pass through every invariant, and a
  manifest that lists an absent file is rejected.
- A manifest is never written into a described folder. For a held document the
  same information belongs in the catalogue.

## Transforms and copies

An `HTMLTransform` receives a book and returns a book.

- It must not write under the root of the book it receives.
- It writes its result under a fresh folder owned by the run and returns a
  book whose root is that folder.
- A helper, `copy_html_book(book, new_root)`, copies the members and returns
  the new book, so a transform that changes two files does not have to rebuild
  the rest.

Re-encoding pages to UTF-8 is a transform in the `normalise` stage. HTMLZ input
does this today before it hands the page on.

## Errors

| Error | Raised when |
| --- | --- |
| `HTMLBookError` | Base class; a `ValueError`. |
| `HTMLBookPathError` | A member path breaks S2 or F2. |
| `HTMLBookStructureError` | Any other invariant is broken, or a manifest is malformed or lists an absent file. |
| `HTMLBookEntryError` | No entry can be found, or the entry is missing or empty. |
| `HTMLBookProfileError` | A book cannot be saved in the requested form. |
| `ArchivePreflightError` | An archive fails the existing preflight; raised unchanged. |

Problems with individual references are reported, not raised. A missing image
must not stop a book from loading.

## Security considerations

- **Containment.** No member and no reference may lead outside the root. This
  closes, for every user of the type, the class of defect fixed for HTML input
  as FFS01.
- **Links on disk.** A symbolic link is accepted only when its target is inside
  the root.
- **Archives.** Packed forms are read only after `archive_preflight`, with its
  current defaults: at most 4096 members, 256 MiB per member, 512 MiB in total,
  and a compression ratio of at most 1000 for members of 1 MiB or more.
- **Folders.** A folder load takes the same limits. Suitable defaults for a
  large crawled site are an open question.
- **Bounded work.** Page detection reads 4096 bytes. Discovery is bounded by
  `max_depth` and visits each page once.
- **No network and no execution.** Loading never fetches a remote reference and
  never runs a script. Scripts are resources like any other file.
- **Manifests.** A manifest is checked like any other input and cannot widen
  what a load may read.

## Differences from current behaviour

| Topic | Today | This specification |
| --- | --- | --- |
| Linked files outside the document's folder | Included on `main`; refused by the fix in PR #168 | Never included; reported |
| A reference with a host and no scheme | Treated as a local absolute path | Remote |
| A path with a leading `/` | An absolute filesystem path | Relative to the root |
| Labels when a page has no title | The remaining titles shift onto the wrong pages and the last page gets no entry | Each page keeps its own label |
| Label when a page has no heading | The text `(unlabled)` | The page's display title |
| Caller-supplied encoding during link discovery | Overwritten by detection in `HTMLFile` | Takes precedence |
| Character references such as `&amp;` in `HTMLFile.title` | Kept as written | Resolved |
| HTMLZ entry when no page is named `index` | The first HTML file in directory-listing order | The first in path order |

The label shift follows from how `HTMLInput.create_oebbook` pairs a list of
non-empty titles with the full list of pages. It was confirmed by replicating
that logic with three pages, the second untitled; the function itself was not
run. The same code is in calibre 9.18.

## Conformance tests

An implementation conforms when it passes at least these cases. Each builds a
small input and asserts on the book, the report and the filesystem.

| ID | Case | Expected |
| --- | --- | --- |
| T1 | One page, one image | One page, one resource, self-contained |
| T2 | Three pages linked in a cycle | Three pages, each once, in the stated order |
| T3 | Depth-first and breadth-first on the same folder | Two different, repeatable orders |
| T4 | A link to `../secret.txt` | Not a member; one outside reference; the file is not read |
| T5 | A `file` URL to a path outside the root | Not a member; one outside reference |
| T6 | A link through a symbolic link that leaves the root | Skipped and reported |
| T7 | `http`, `//host` and `data` references | Two remote references; the `data` reference ignored; nothing fetched |
| T8 | A reference to an absent image | One missing reference; the book loads |
| T9 | A page declaring `windows-1252` | That encoding recorded; the bytes unchanged after a save |
| T10 | A page with no declaration and no byte-order mark | Listed under uncertain encodings |
| T11 | Three pages, the second untitled | Three entries, each pointing at its own page |
| T12 | Two pages with the same title | Labels taken from headings |
| T13 | A folder with `toc.html`, `index.html` and a larger page | Entry is `toc.html`; reported as guessed |
| T14 | A leading-slash reference to a file under the root | A member |
| T15 | `members="all"` with an unlinked page and an unlinked image | Both are members; nothing unlisted |
| T16 | Save to a folder, then load | Equal to the original apart from the root |
| T17 | Save to ZIP twice | Identical bytes |
| T18 | A manifest listing `../x` | Rejected with a path error |
| T19 | A manifest listing an absent file | Rejected with a structure error |
| T20 | An HTMLZ with two top-level pages | One page; the other skipped and reported |
| T21 | Saving a two-page book as HTMLZ | Profile error; nothing written |
| T22 | An archive over a preflight limit | `ArchivePreflightError`; nothing extracted |
| T23 | Two members differing only by letter case | Loaded; reported as a case collision |
| T24 | A transform that writes under the input root | Detected by the test harness as a failure |

## Holding HTML documents in the library

This section is not normative. It is a suggestion for the
[storage](../storage_and_backup/README.md) owner.

The storage schema has composite assets for assemblies of several files. Their
membership carries a sequence number, a required flag and a role. An HTML
document fits that shape:

- each page is a required member, with its position in the reading order as
  the sequence number;
- each resource is a member with a role;
- the entry, encodings, titles and table of contents, which a manifest would
  hold for a saved copy, are stored with the composite.

An `HTMLBook` is then the in-code description of such a composite, built with
`members="all"` so that nothing in the folder is left out.

## Open questions

1. Is `htmlbook.json`, written inside a saved folder or ZIP, an acceptable
   manifest name and place?
2. Should a leading-slash path be resolved against the root? It suits mirrored
   sites and can never leave the root, but it changes today's behaviour.
3. The page test misses a valid HTML5 document that omits the `html` tag.
   Should a file with an HTML extension count as a page without it?
4. What limits should a folder load apply by default? The archive defaults may
   be too small for a large crawled site.
5. Should a case collision be an error instead of a report entry, given that
   such a book cannot be saved on Windows?
6. Is a default depth of 5 right for a held document, or should `members="all"`
   imply no depth limit?

Return to the [file format notes](README.md).
