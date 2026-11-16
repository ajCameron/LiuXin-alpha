# Capability providers for file formats

Date: 2026-11-16

Status: **Proposal for review. Nothing here is implemented.**
Source checkpoint: `main` at `2d2a9cc3`.

This briefing records an architecture discussion with the maintainer on
2026-11-16. It proposes replacing the inherited one-plugin-per-format model in
`file_formats` with typed capability providers, conversions built from
composable steps, and recorded provenance. Decisions the maintainer has made
are marked **decided**; the rest are proposals to accept, change or reject.

Code and SQL in this guide are sketches that show shape. Names, signatures and
columns are open to change in review.

## The short version

- Every kind of operation gets **one base class**. A plugin subclasses the
  capabilities it provides, and the registry keeps a typed list per capability.
- **Several providers** can serve the same capability and format. Settings
  choose the default; a run can override it; a fallback chain is allowed.
- A **conversion is a route through providers**, not a fixed pair of plugins.
  A provider can import to an intermediate, export from one, or do a whole
  format-to-format job, and routes can chain.
- **OEB and HTML are both intermediates.** More can be added by name.
- **Routes have names**, such as `pdf.compress`. A route may start and end in
  the same format, so compression, OCR and repair pipelines fit.
- **Transforms** between import and export are their own capability and run in
  fixed, named stages.
- Each run records **which provider ran on which file**, and each metadata
  value records **which run claimed it**.

## Why change

The format plugins still follow calibre's model. Three properties of it limit
what the project can do without dropping formats.

1. **One implementation per format.** `plugin_for_input_format`
   (`file_formats/plugin_runtime.py:756`) returns the first plugin whose
   `file_types` contains the extension. Two readers for one format cannot
   coexist as choices.
2. **Conversion is one fixed box.** `Plumber.run`
   (`file_formats/conversion/plumber.py:1549`) takes one input plugin and one
   output plugin and always goes through OEB. A tool that performs a whole
   conversion, or a route through HTML, has no place in it.
3. **Nothing records what ran.** A converted file or an extracted author
   carries no record of the plugin, version or options that produced it.

A fourth, shared state on plugin instances (`site_customization` is assigned
per call), belongs to execution and lifetime, which this proposal leaves to
[PL-02](next_tasks.md).

## Decisions so far

| Topic | Position | Status |
| --- | --- | --- |
| One base class per capability; a class may inherit several | Typing and the registry both stay simple | **Decided** |
| Several providers per capability and format | Default in settings, per-run override, fallback chain | **Decided** |
| Results record the provider that ran | Part of every result | **Decided** |
| Input and output providers chosen independently | Each with its own default settings | **Decided** |
| Whole-conversion providers and chains | First-class, alongside import and export | **Decided** |
| Intermediates | OEB and HTML to start; not OEB alone | **Decided** |
| Named routes | Every route has a name; a format pair's default points at one | **Decided** |
| Same-format routes (loop-backs) | Supported in all three forms | **Decided** |
| Output of a same-format route | Stays within the same Manifestation, recorded as derived; the original stays | **Decided** |
| Strict WEMI for derived files | No project-specific exceptions | **Decided** |
| Output of a format conversion | A derived Manifestation, linked to its source | **Decided** |
| Provenance of metadata claims | A new table | **Decided** |
| Transforms | Own capability, run in fixed stages | **Decided**; how stages are added is still open |
| Capability names; `CoverExtractor` separate from `MetadataReader` | As listed below | **Decided** |
| Import and export classes | A separate class per intermediate | **Decided** |
| OCR output | A separate Item of the same Manifestation | **Decided** |
| Route names | Free-form | **Decided** |
| Transform classes | One per intermediate: `OEBTransform`, `HTMLTransform` | **Decided** |
| HTML intermediate | Defined as a concrete type, also usable to represent HTML documents | **Decided**; definition below is a proposal |
| Intermediates on disk | Each has a saved form with load and save; an intermediate is a valid start or end of a route | **Decided** |
| calibre's newer transforms | Supported: port them into the stages | **Decided** |
| Execution policy (in-process, worker, limits) | Decide after the architecture, possibly per plugin | **Parked** |

## Capabilities

Each capability is one abstract base class with one typed entry point.

| Base class | Turns | Replaces today |
| --- | --- | --- |
| `MetadataReader` | a file into metadata claims | `MetadataReaderPlugin` |
| `MetadataWriter` | metadata into a file | `MetadataWriterPlugin` |
| `CoverExtractor` | a file into a cover image | cover logic inside readers |
| `OEBImporter`, `HTMLImporter` | a format into that intermediate | `InputFormatPlugin` |
| `OEBExporter`, `HTMLExporter` | that intermediate into a format | `OutputFormatPlugin` |
| `Converter` | a format into a format, the whole job | nothing; new |
| `OEBTransform`, `HTMLTransform` | that intermediate into itself | calls inside `Plumber.run` and output plugins |

Import, export and transform have a separate class per intermediate, so a
provider's base class states exactly what it produces or consumes. `Importer`,
`Exporter` and `Transform` remain as abstract parents that carry the shared
record and are never subclassed directly by a provider. Adding an intermediate
means adding its three classes.

An exporter also declares the package-stage transforms it needs, as a class
attribute, so the pipeline runs them instead of the exporter calling them.

More capabilities can be added when a real operation does not fit, for example
text extraction for search or validation.

```python
class Provider(ABC):
    """Common record every provider carries as class attributes."""

    provider_id: ClassVar[str]          # stable, for example "pdf.reader.builtin"
    version: ClassVar[str]
    priority: ClassVar[int] = 0
    requires: ClassVar[tuple[str, ...]] = ()   # extras or external tools
    licence: ClassVar[str]


class MetadataReader(Provider):
    formats: ClassVar[frozenset[str]]

    @abstractmethod
    def read_metadata(self, source: BinaryIO, fmt: str) -> MetadataClaims: ...


class Importer(Provider):
    """Abstract parent; providers subclass one of the classes below."""

    source_formats: ClassVar[frozenset[str]]


class OEBImporter(Importer):
    @abstractmethod
    def import_book(self, source: Path, options: Options) -> OEBBook: ...


class HTMLImporter(Importer):
    @abstractmethod
    def import_book(self, source: Path, options: Options) -> HTMLBook: ...


class Converter(Provider):
    source_formats: ClassVar[frozenset[str]]
    target_formats: ClassVar[frozenset[str]]

    @abstractmethod
    def convert(self, source: Path, target: Path, options: Options) -> None: ...
```

One class can provide several capabilities by inheriting several bases. The
registry files it under each. A provider that needs something missing reports
itself unavailable with the reason, which is the outcome
[PL-01](next_tasks.md) asks for.

## Intermediates

An intermediate is a book held in a form that providers pass between them. Two
are defined to start. Each has an in-memory type and a saved form on disk, with
one function to load it and one to save it.

| Intermediate | In memory | Saved form | Code that already does this |
| --- | --- | --- | --- |
| OEB | `OEBBook` (`file_formats/oeb/base.py`) | A folder holding an OPF file and the book's files | `oeb_output.py` writes it; HTML input reads an `opf` |
| HTML | `HTMLBook`, proposed below | A folder holding an entry page and its resources; HTMLZ is the same thing zipped | `html_input.py` reads it; `htmlz_input.py` and `htmlz_output.py` handle the zipped form |

Because every intermediate can be saved and loaded, it is also a valid place
for a route to start or end. A route may stop at OEB and leave the folder as
its result, which the inherited "OEB Output" plugin already does today, and a
file-based `Converter` can hand its output to an exporter:

- a `Converter` produces or consumes **files**;
- an importer, exporter or transform produces or consumes the **in-memory**
  form;
- where a route joins the two, the runner loads or saves the intermediate.
  Providers never do this themselves.

### The HTML intermediate

Proposed definition, to be settled before `HTMLImporter` and `HTMLExporter`
have real signatures:

```python
@dataclass(frozen=True)
class HTMLBook:
    root: Path                    # folder that contains everything
    entry: PurePosixPath          # the first page, relative to root
    pages: tuple[PurePosixPath, ...]      # reading order, entry first
    resources: tuple[PurePosixPath, ...]  # images, styles, fonts, scripts
```

Every path is relative to `root` and must resolve inside it. That is the same
confinement rule as the HTML input fix for finding FFS01 in the
`file_formats` risk sweep of 2026-11-16, applied to the type instead of to one
plugin.

The same type can represent an HTML document held in the library, for example
a page acquired by web ingest, without converting it to anything.

## Registration

Subclassing a capability base class is what makes a provider. The record is
the class attributes above, so there is no separate manifest to keep in step.

- **Bundled providers** are listed by module in one table, as
  `file_formats.plugin_defaults` does today. Modules must stay cheap to import
  and load heavy dependencies inside methods, which is already this package's
  habit.
- **Third-party providers** register through a Python entry-point group, in
  addition to the existing ZIP loader.
- The registry answers typed queries, for example every `MetadataReader` for
  `pdf`, in selection order.

## Selection

For a capability and format the registry may hold several providers.

1. A **per-run override** names the provider to use.
2. Otherwise **settings** name the default.
3. Otherwise the highest **priority** available provider is used.
4. A configured **fallback chain** is tried in order when a provider fails.

The result always states which provider ran and which were tried first.

## Conversion as a route

Treat every format and every intermediate as a node, and every conversion
provider as an edge between two nodes.

```mermaid
flowchart LR
    PDF -->|OEBImporter| OEB
    OEB -->|OEBExporter| EPUB
    PDF -->|Converter| HTML
    HTML -->|Converter| EPUB
    PDF -->|Converter, whole job| EPUB
```

calibre's pipeline is one route in this picture: an `OEBImporter` in, then
an `OEBExporter` out. A `Converter` is a direct edge. A chain is a longer route.
Choosing the input and output providers independently means choosing the edge
used at each step.

Rules that keep this manageable:

- **Intermediates are a short named list.** OEB and HTML to start. A provider
  cannot introduce one silently, so the data passed along each edge has a
  known type.
- **Routes are named and curated before they are searched.** A route is an
  ordered list of providers with a name, for example `pdf.to-epub.via-html`.
  Settings map each pair of formats to its default route. Automatic
  route-finding is a fallback, limited to short routes, and the plan is shown
  before it runs.
- **Metadata is not a provider's job to carry.** It is read once from the
  source. On a leg that passes through OEB it is merged into the book at the
  `structure` stage, as `MergeMetadata` does today. On a route made only of
  `Converter` steps it is written to the result at the end by a
  `MetadataWriter`. No provider in the middle has to preserve it.

Later, each edge should declare what it preserves (structure, images, fixed
layout) so that a fallback does not choose a lossy hop. That can wait until
there are competing routes to choose between.

### Same-format routes

A route may end in the format it started from. Compression, OCR, repair and
similar pipelines are routes of this kind, and all three forms are supported:

| Form | Example |
| --- | --- |
| A `Converter` whose source and target are the same format | A PDF compressor: PDF to PDF |
| A round trip through an intermediate | EPUB to OEB, `resources`-stage transforms such as image rescaling and font subsetting, then OEB to EPUB |
| A chain out and back | PDF to page images, then images to PDF |

Two formats no longer identify what is wanted, since PDF to PDF could mean
compress, OCR or repair. That is why routes carry names: the request is for
`pdf.compress`, not for a pair of formats.

Rules:

- **The planner never loops by itself.** Automatic route-finding skips
  same-format edges and never revisits a format. A same-format route runs only
  when it is requested by name.
- **The result is a new file, never a replacement.** The original stays, in
  line with the project's preservation principles.
- **The new file belongs to the same Manifestation** and is recorded as
  derived from the original through `digital_asset_derivations`. The schema's
  comments already list derivation kinds such as `repacked` and `repaired`.

Whether the result is a new Asset or a new Item follows the policy in the
next section.

## Where derived files sit in WEMI

The maintainer's decision is to follow WEMI strictly, with no project-specific
exceptions. Format is a Manifestation attribute, both in WEMI and in this
schema, whose guide describes a Manifestation as carrying edition and format.
An EPUB and a MOBI of one book are therefore different Manifestations, and a
local conversion creates one.

The level of a new record is decided by what changed:

| What happened | New record | Example | Status |
| --- | --- | --- | --- |
| Same bytes, another place | Replica | A backup copy | Existing behaviour |
| Different bytes, same content and format | Asset on the same Item | Lossless recompression, repack, repair, rewritten metadata | Proposed |
| Content changed, same format | Item of the same Manifestation | OCR text added | **Decided** for OCR |
| Content changed, same format | Item of the same Manifestation | Lossy compression that reduces image quality | Proposed |
| Format changed | Manifestation, marked as derived and linked to its source | A local EPUB to MOBI conversion | **Decided** |

Two rules stop the catalogue filling with records:

- **An Item is created only by acquiring a copy or by a derivation that changes
  content.** Copying and lossless re-encoding never create one.
- **A derived Manifestation is marked as derived** and linked to the
  Manifestation it was made from, so it never reads as something a publisher
  issued. Their number is bounded by formats, not by files.

What a conversion writes, in one transaction owned by Catalog:

1. a derived Manifestation under the same Expression;
2. a link from it to the source Manifestation;
3. an Item for the new copy;
4. the new Asset, with a derivation edge to the source Asset and the run that
   made it.

### Consequence for calibre import

The same policy gives the calibre import its default shape, as the maintainer
noted: one calibre book becomes one Work and one Expression, and **each format
file becomes a Manifestation of that Expression**, with its own Item and
Asset. This may be wrong for individual books, and is acceptable as an import
default that can be corrected afterwards.

Points for the import design, which belongs with the
[calibre mapping guide](<../calibre_compat/mapping calibre to LiuXin.md>):

- calibre does not record whether a format was acquired or converted, so
  imported Manifestations are not marked as derived. Their origin is the
  import.
- calibre keeps the pre-conversion file as an `ORIGINAL_<FORMAT>` entry when it
  converts or polishes a book in place. Such a pair is the same format, so it
  belongs to one Manifestation, with the plain entry derived from the
  original.
- Two formats of one calibre book can be different editions. Splitting them
  into separate Expressions is a later, manual correction.
- The rationale text for manifestation links in `intralink_table_requests.toml`
  used to say only that everything is assumed to be under one Manifestation.
  It now states that this matching default applies within one edition and
  format, and that different formats are separate Manifestations. The text is
  documentation; the generator does not read it.

The schema already has manifestation-to-manifestation links
(`intralink_table_requests.toml`), with one type, `user_marked_different`.
A `derived_from` type would be an addition to that declaration. Items have no
field marking a copy as locally made; the per-copy provenance columns in
`4-items.sql` are the place to look first.

## Transforms and stages

An `OEBTransform` or `HTMLTransform` takes its intermediate and returns the
same kind. Transforms are
not routing choices, so they are not edges: a route decides how to get from
one format to another, and transforms decide what happens to the book on the
way.

The order is fixed by **stage**, not by one list in the core. The core defines
a small set of named stages per intermediate, in a fixed order. Each transform
declares its stage and a priority within it. A later plugin can then join an
existing stage without editing the core, and runs stay deterministic. The
result lists the transforms applied, in order.

### Stages for OEB

These six stages are read from the current code. Stages 1 to 5 are the order
`Plumber.run` already uses, so adopting them changes no behaviour.

| Stage | Purpose | In LiuXin today | In calibre 9.18, to port |
| --- | --- | --- | --- |
| 1 `normalise` | Make the book consistent after import | `Clean` (guide); each input plugin's `postprocess_book` and `specialize` | `DataURL` |
| 2 `structure` | Establish what the book is and how it is organised | `MergeMetadata`, `DetectStructure`, `Jacket` | `RemoveFirstImage` |
| 3 `content` | Rewrite text and markup | `LinearizeTables`, `UnsmartenPunctuation` | `AddAltText` |
| 4 `style` | Resolve CSS for the target | `CSSFlattener`, `RemoveFakeMargins`, `RemoveAdobeMargins` | |
| 5 `resources` | Settle fonts, images and the manifest | `EmbedFonts`, `SubsetFonts`, `ManifestTrimmer` | |
| 6 `package` | Shape the book for one output format | `Split`, `CaseMangler`, `HTMLTOCAdder`, `UniqueFilenames`, `FlatFilenames`, `CoverManager` | |

The three calibre transforms in the last column do not exist in this fork.
The maintainer wants calibre's newer work supported, so they are part of the
plan. calibre's code is GPL v3, the same licence as this project, so they can
be ported with their upstream notices.

### What the current code forces the design to handle

- **Stage 6 is hidden inside the output plugins.** Each output plugin calls
  `Split`, `CaseMangler` and the others itself; `Split` is called from four of
  them. An `Exporter` should instead declare which package transforms it
  needs, and the pipeline should run them. `RescaleImages` and `SVGRasterizer`
  are also called from output plugins and belong in `resources`.
- **Conditions are scattered.** Whether a transform runs is decided by tests on
  options and on the output format inside `Plumber.run`. Each `Transform`
  should carry its own applicability test.
- **One transform is coupled to the exporter.** `CSSFlattener` receives a
  callback from the output plugin (`specialize_css_for_output`). That needs a
  defined hook.
- **Order inside a stage can matter.** `MergeMetadata` must precede `Jacket`,
  which builds a page from the metadata. Priority covers it. An explicit
  "after" rule should be added only when priority stops being enough.

### Stages for HTML

HTML has one transform-like step today, the `HTMLPreProcessor` clean-up. Give
`HTMLTransform` a single `normalise` stage and add others when something needs
them.

### Adding stages later

Still under discussion. The aim is room to grow without building machinery
nobody uses. Three options, from least to most flexible:

| Option | How a stage is added | Trade-off |
| --- | --- | --- |
| A. Closed list | The stage list is one tuple per intermediate in the core; adding a stage is a reviewed core change | Simplest and fully predictable; a plugin cannot add a stage |
| B. Closed list with slots | As A, and a transform also declares `early`, `normal` or `late` within its stage | Eighteen positions from six names, with no new concept for plugin authors |
| C. Plugin-declared stages | A plugin registers a stage anchored before or after an existing one | Most flexible; two plugins can disagree about order, and the pipeline is harder to explain |

Recommended: start with A, add B when a transform first needs to run before or
after its stage-mates, and adopt C only if a real plugin cannot be expressed
with B.

### Costs

- Stage names become a contract once plugins rely on them. Start with these six
  and add a stage only when a real transform does not fit.
- Moving the package transforms out of the output plugins is the largest piece
  of migration work in this proposal.

## Provenance

### What exists

The schema already models runs on files:

- `transform_runs`: type, tool, profile, parameters, parameter hash, actor,
  status, error and timings.
- `transform_run_inputs` and `transform_run_outputs`: the digital assets a run
  read and wrote.
- `digital_asset_derivations`: parent-to-child edges, with an optional run.

A conversion step fits these tables. A chain is several runs joined by
derivation edges. Outside the schema these tables are referenced only in
column metadata and one web surface, so no conversion code appears to write to
them yet. `transform_run_tool` is free text; a provider id and version would
be better as their own columns.

### What is missing

Nothing records where a metadata value came from. Identifiers have a free-text
`entity_identifier_provenance` column; links such as work-to-creator carry no
source. The question "which plugin run, on which file, said this PDF has this
author" cannot be answered.

### Proposed

A metadata read is a run with an input asset and no output asset, so it can
reuse `transform_runs`. Two new tables hold the claims and tie catalogue facts
to them.

```sql
-- One row per assertion a provider made about a file.
CREATE TABLE metadata_claims (
  metadata_claim_id               INTEGER PRIMARY KEY,
  metadata_claim_run_id           INTEGER NOT NULL,  -- transform_runs
  metadata_claim_digital_asset_id INTEGER NOT NULL,  -- the file read
  metadata_claim_field            TEXT NOT NULL,     -- 'creator', 'title', ...
  metadata_claim_value            TEXT NOT NULL,     -- JSON
  metadata_claim_status           TEXT NOT NULL      -- 'proposed','accepted','rejected'
);

-- Which claims support a fact stored in the catalogue.
CREATE TABLE metadata_claim_applications (
  metadata_claim_application_id       INTEGER PRIMARY KEY,
  metadata_claim_application_claim_id INTEGER NOT NULL,
  metadata_claim_application_table    TEXT NOT NULL,
  metadata_claim_application_row_id   INTEGER NOT NULL
);
```

### What a claim is, and the two open choices

A **claim** is evidence: a named source said this value about this file. A
**catalogue fact** is what the library currently holds to be true. Keeping the
two apart is what allows the questions below to be answered:

- Why does the catalogue say this? Follow the fact to its claims.
- A reader had a bug. Which facts rest only on claims from that provider and
  version?
- The file, an online source and a person disagree. What did each say?
- A better reader exists now. What would change if it were used?

**Choice 1: how much to record.**

| Option | Stored | Good for | Cost |
| --- | --- | --- | --- |
| Every value as a row | One row per field per read | Full evidence, queryable | About ten to twenty rows per file read; tens of millions of rows for a large archive |
| Accepted values only | One row per value that reached the catalogue | Small | Rejected alternatives are lost; re-deciding means reading the files again, which is expensive when they are offline or on tape |
| Reading kept whole, accepted values as rows | One JSON document per read, plus a row per accepted value | Full evidence kept cheaply; explanations for every stored fact | Rejected values are searchable only by opening the document |

Recommended: the third. It keeps everything a reader said, at one row per
read, and gives row-level links only where the catalogue needs them.

**Choice 2: who writes claims.**

If only file readers write claims, a fact with no claim is ambiguous: it could
be a manual edit, an import or a legacy row. If manual edits and online
sources write claims too, every fact has at least one, and a precedence rule
becomes possible, for example a person's edit outranks an online source, which
outranks the file. That rule is what stops a re-read from overwriting a manual
correction.

The cost is that every catalogue write path has to create a claim, which
touches the [catalog write contract](../catalog/catalog-write-validation-contract.md).

Recommended: design the tables for all three sources now, write claims from
file readers first, and add manual edits and online sources afterwards. Until
then a fact without a claim means "origin not recorded".

The schema change belongs to the
[databases](../databases/README.md) and [catalog](../catalog/README.md)
owners; the sketch above is input to them, not a finished design.

## Compatibility and migration

- **Existing plugins keep working through an adapter.** `InputFormatPlugin`
  becomes an `OEBImporter`, `OutputFormatPlugin` an `OEBExporter`,
  and the metadata plugins their matching capabilities. Nothing is rewritten
  up front, and formats move to native providers one at a time.
- **Third-party calibre plugins** are not promised to load unchanged, as the
  [calibre compatibility guide](<../05 - Calibre compat.md>) already says.
- **Existing follow-ups still apply.** Availability reporting is
  [PL-01](next_tasks.md), instance and registry lifetime is PL-02, and keeping
  standalone formats independent of the application is PL-04.

## Proposed build order

| Step | Work | Done when |
| --- | --- | --- |
| 1 | Capability base classes and a typed registry | The registry returns typed providers for a capability and format, with tests |
| 2 | Adapter for today's plugins | Every bundled plugin is reachable through the registry and existing conversions give the same output |
| 3 | Selection: settings default, per-run override, fallback | Two providers for one format can be chosen between, and the result names the one that ran |
| 4 | `Converter` capability, the `HTMLBook` type with load and save, named routes and route planning | A two-hop route and a same-format route each run by name and are recorded; automatic planning never selects a same-format edge |
| 5 | `Transform` capability and stages, including the three calibre transforms | `Plumber.run` contains no direct transform calls, and output plugins declare their package transforms |
| 6 | Provenance: runs written for conversions; claims tables | The run and file behind a stored author can be queried |

Steps 1 to 3 change no conversion behaviour. Step 5 is the largest.

### Proving the bases before freezing names

Three routes exercise every base and every join between them. If all three
run without a special case, the bases are right:

1. a round trip: EPUB to OEB and back to EPUB;
2. a chain through HTML: a file-based `Converter` produces HTML, and an
   `HTMLExporter` or a second `Converter` produces the final format;
3. a single `Converter` wrapping one external tool.

## Parked

- **Execution policy.** In-process or worker, resource limits and isolation.
  It may differ per plugin and may need more base classes.
- **Typed options.** Replacing `OptionRecommendation` sets with a validated
  schema per provider, stored as JSON.
- **Loss declarations** on conversion edges.

## Questions for review

Answered on 2026-11-16 and folded into the text above: capability names, a
separate class per intermediate, transforms as a staged capability, OCR output
as a separate Item, and free-form route names.

Still open:

1. How are stages added later: option A, B or C under
   [Adding stages later](#adding-stages-later)?
2. How much of a read is recorded as claims: choice 1 under
   [What a claim is](#what-a-claim-is-and-the-two-open-choices)?
3. Do manual edits and online sources write claims: choice 2 in the same
   section?
4. Is the split for compression right: a lossless result is a new Asset on the
   same Item, and a lossy result is a new Item?

Return to the [plugins index](README.md).
