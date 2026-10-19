# Documentation and project structural review

**Follow-up — 2026-10-19:** PR #125 consolidated `customize` into `plugin_system`;
that package is now named `plugins`. Both former namespaces are retired.
Application assembly lives in `plugins.application`, with module-level dependency
and cold-import checks separating it from neutral mechanics. Earlier package
names and counts below describe their dated checkpoints. See the current
[plugin composition contract](../plugins/plugin-composition.md).


Date: **2026-10-18**

Status: **Review complete; proposed restructuring has not been implemented.**

Scope: developer documentation, repository organisation, package responsibilities,
and the relationship between documented and implemented architecture. This is a
structural review, not a new certification of every subsystem's behaviour.

## Assessment

The move towards subsystem folders is sound. Keep it. The next change should make
authority and navigation explicit: readers need to distinguish a current contract,
an accepted decision awaiting implementation, a proposal, and historical evidence.
Moving files alone does not establish those distinctions.

The project also has a defensible package structure. Core, Library, Catalog,
metadata, databases and storage represent useful separate responsibilities. Recent
changes have made those boundaries substantially more real. There is no evidence
here for replacing the package tree wholesale. The remaining architectural work
is concentrated in particular ownership and safety boundaries, especially jobs,
ingest, registry composition, low-level storage operations and compatibility code.

The immediate documentation problem is concrete: the in-progress folder migration
has broken navigation and leaves executable consumers pointing to old paths. Some
otherwise readable guides describe owners that have already moved. Complete that
migration and correct the current contracts before undertaking another broad move.

## Evidence and checkpoints

Two different trees were deliberately examined:

| Tree | Checkpoint | Purpose and limits |
| --- | --- | --- |
| Main working checkout, `catalog-write-consolidation` | HEAD `af2cebd03945aa635d5d617d7f725d75ac4659f0`, plus pending edits and 80 staged documentation renames | The documentation layout the user is actively building. Counts describe the working files, not HEAD alone. Existing source edits were not treated as the latest integrated implementation. |
| Isolated review branch, `architecture-review-followup` | `8c20f7d27c234c6c5ca7f85f8695eb414e7639b8` | The implementation checkpoint for the architecture assessment, including the configuration work and the dated follow-up to the September review. Its documentation still largely uses the earlier flat layout. |

The source tree at the second checkpoint is identical to `ce239fd4`:
`a0f10664959619804dd07e1ab549c8a4b7adf2a7`. The previous source-checked architecture
follow-up therefore applies to the same implementation. Its concrete findings
are incorporated below without pretending that a second complete behavioural
audit or full test run occurred during this documentation review.

The documentation inventory was captured at **20:41 UTC / 21:41 BST on
2026-10-18**, before adding this report. The working checkout is changing; these
are dated measurements, not a claim that a later working tree has identical counts.
The latest implementation is ahead of the main working checkout. The integration
and configuration work is recorded in PRs #122 and #123; implementation at the
review checkpoint must not be described as already present on `main`.

The September review has two relevant versions: the older copy now moved into
`architectural_reviews/` in the working checkout, and the annotated version in
commit `8c20f7d2`. A migration must preserve both the new location and the newer
annotations. Blindly choosing either branch's file would lose useful work.

## Documentation inventory

The working layout contains **116 files: 112 Markdown, one JSON and three SQL**.
There are 19 files directly under `dev-docs`, compared with 68 in the earlier-layout
review tree. These snapshots also differ in content, so this is not a controlled
before/after measure of the renames alone.

Counts below include descendants. Paths in this inventory describe the working
layout; suggested destinations later in this report are proposals.

| Location | Files | Assessment and disposition |
| --- | ---: | --- |
| `dev-docs/` root | 19 | Entry points, numbered overviews, conventions, current contracts and old thoughts are mixed. Keep the index and project goals prominent; distribute maintained topic guides and label the remaining notes. |
| `architectural_reviews/` | 8 | Useful home for dated findings. Currently also contains current enforcement guides and target decisions, which need a different authority label and eventually a different home. |
| `calibre_compat/` | 2 | Keep the compatibility policy and mapping rationale together; add an entry point distinguishing supported adapters from historical mappings. |
| `catalog/` | 8 | Strong subject grouping. API, mutation and matching contracts need clear precedence over roadmap and migration history. |
| `conversion_pipeline/` | 3 | Architecture, sign-off and remaining-work records have distinct purposes, but overlap the format guides. Give them a shared navigation route with `file_formats`. |
| `core/` | 2 | Too little current coverage for lifecycle, configuration, public operations, jobs and read-model composition. Correct existing ownership text and add focused contracts. |
| `databases/` | 16 | Good grouping, but drafts, schema explanations, connection contracts and operational backend instructions need status and task-based navigation. |
| `file_formats/` | 14 | The index and per-format dossiers are a useful pattern. Make dossier names actual links and separate format behaviour from campaign status. |
| `hacking/` | 7 | Appropriate home for contributing, packaging and testing. A large historical inventory currently dominates its bytes; conventions and enforcement guides are elsewhere. |
| `metadata/` | 7 | Mixes acquisition, write coverage, CLI use and projections. Distinguish value contracts from application orchestration and Catalog reads. |
| `metadata_container/` | 11 | Valuable detailed contracts, but a separate top-level taxonomy obscures their relationship to metadata values. One source-layer guide names a retired owner. |
| `random_sql/` | 3 | Design sketches, not installed schema authority. Preserve as explicitly historical material with a pointer to the packaged schema. |
| `storage_and_backup/` | 11 | A useful existing subsystem index. Preserve the grouping; distinguish byte operations, policy, operator procedures and historical design. |
| `surfaces/` | 5 | Sensible home for CLI, terminal and GUI composition. Startup and read-model guides still sit at the root. |

The Markdown totals **1,088,831 bytes**. The JSON inventory is **7,697,398 bytes**
(about 7.34 MiB), approximately **87%** of all bytes in this documentation tree.
The three SQL sketches total 56,530 bytes. This is a content-lifecycle issue, not
an argument for an arbitrary document-size limit or a material explanation of
the earlier multi-gigabyte storage pressure.

### Navigation and portability measurements

| Check | Working layout | Earlier-layout review tree |
| --- | ---: | ---: |
| Parsed local Markdown link occurrences, including root README | 316 | 357 |
| Missing local target occurrences | 47 | 86 |
| Documents containing those missing links | 9 | 2 |
| Distinct missing targets | 29 | 86 |
| Markdown documents unreachable from the two README entry points | 28 | 9 |
| Markdown documents with no inbound Markdown link | 25 | 9 |
| Documentation directories containing Markdown but lacking a README | 11 | 4 |
| Existing file-link targets absent from the Git index | 86 | 0 |

The 86 earlier-layout failures are local evidence outputs referenced by the
docstring completion plan and shim-removal plan. Those outputs exist in the main
working checkout, so an existence-only check there accepts them; they remain
untracked and unavailable to a fresh clone. The smaller number of missing links
in the working layout therefore does **not** demonstrate improved portability.

Of the 47 missing links in the working layout, 16 are in `dev-docs/README.md`, 11
in the target architecture, ten in the September review, and four in the earlier
top-level review. Examples include `database/` after the move to `databases/`, the
old style-guide name, and links whose relative depth was not updated when a guide
moved into `architectural_reviews/`.

The nine per-format dossiers are unreachable in both trees because the format
index lists directory names as code text rather than navigable Markdown links.
Unreachable does not mean useless: it means readers must know the path or browse
the filesystem to discover the document.

## Structural findings

Priority **P1** means a blocker to completing the documentation migration safely;
**P2** means the next content/ownership work; **P3** means useful subsequent cleanup.
These priorities are local to this review, not replacements for the severity of
the unresolved production findings in the September review.

### DS-01 — Finish path migration as a repository change (P1)

Evidence: 47 missing local links and 29 lines containing old full documentation
paths in 11 files outside `dev-docs`. The latter comprise nine script lines,
eight source lines, seven root-README lines and five test lines. This search is a
lower bound: paths assembled from several strings are additional consumers.

In particular, [deployment packaging](../../scripts/build_deployment_package.py)
requires the old metadata CLI, operational CLI, PostgreSQL and mixed-ingest guide
paths. Its tests assert those locations. Package docstrings still point to old
Catalog, storage and format guides. Both documentation test modules also encode
old names, including constructed `Path` expressions.

**Action:** preserve an old-to-new path map and update incoming links, outgoing
relative links, code-text references, package docstrings, tests and deployment
bundle metadata together. A passing Markdown-only checker is insufficient.
Do not retain duplicate guide copies to satisfy stale consumers.

### DS-02 — Establish document authority explicitly (P1)

`00 - This Document.md` describes a grab bag or lab book. The main index promises
maintained guides. Both are useful purposes, but readers currently have to infer
which one applies. The architecture-review folder likewise mixes reviews,
accepted decisions, unfinished proposals and active CI rules.

**Action:** keep exploratory writing, while marking its status and placing current
contracts on an explicit route through the index. Use the lifecycle vocabulary
below. A filename containing “architecture” or an incoming README link must not
be interpreted as evidence of implementation.

### DS-03 — Reconcile current contracts with implemented owners (P1)

There are specific semantic contradictions, beyond broken links:

| Document or claim in the working layout | Implementation evidence | Required correction |
| --- | --- | --- |
| README describes the target architecture as “not yet implemented” | Lifecycle was implemented in `472d71d0`; subsequent Catalog work has also landed in the reviewed source | Describe status per decision, retaining the historical problem statements as dated evidence. |
| `core/core-program-workflows.md` says `ingest/mixed_application.py` owns subsystem composition | Core [LibraryServices](../../src/LiuXin_alpha/core/library_services.py) now composes the graph; mixed ingest consumes an injected Library | Keep the workflow extraction explanation, but correct who creates and owns collaborators. |
| `metadata_container/metadata_db_source_layer.md` calls `metadata.api.metadata_db_source` current infrastructure | That package is absent at the reviewed implementation checkpoint; contracts and hydrators belong under Catalog retrieval | Update the owning paths and distinguish passive metadata values from persistence-aware hydration. |
| `catalog/catalog-roadmap.md` retains older migration-complete language | Supported compatibility entry points remain adapters to canonical writers; not all compatibility code has disappeared | Specify which independent write policies were retired, which adapters remain, and what the guarantee excludes. |
| The working tree has no configuration ownership guide | The configuration branch adds a detailed contract with `d59407f3` | Carry that guide forward when reconciling branches; do not reconstruct it from obsolete configuration prose. |

**Action:** update these contracts against the implementation checkpoint before
rewriting overview diagrams. Record implemented, remaining and excluded behaviour
separately. The September follow-up supplies the change trail; it should not become
the permanent place readers must visit to learn the current API.

### DS-04 — Provide navigation by reader task and subsystem (P2)

Eleven documentation directories lack an index. The root index is useful but
long, and mixes first-use guidance with detailed historical material. Twenty-eight
Markdown documents cannot be reached by following its valid links or the root
README's links.

**Action:** use short subsystem READMEs where a folder contains several independent
guides. Each should identify the owner, primary contract, common tasks, relevant
tests and deliberately historical material. Make the root index a route into
those indexes. Single-purpose folders do not require ceremonial extra pages.

### DS-05 — Retire the misleading numbered reading sequence (P2)

The root `00`–`08` sequence looks like an onboarding course, but it is not one.
The schema overview begins with a rewrite TODO. The storage overview is explicitly
deprecated and also contains newer explanations. The Calibre overview is sparse.
The top-level structure document combines orientation, design proposals, migration
instructions and text intended for source docstrings.

**Action:** extract a concise current architecture map and a short onboarding
route. Move maintained schema, test-database and cache guidance to their topic
owners. Preserve useful older explanations with a historical status and successor
pointer. Do not delete a mixed document until its still-current content is assigned.

### DS-06 — Give each policy and work queue one maintained owner (P2)

Coding Guide, Style Guide, project goals, the two separation-of-concerns guides,
target architecture and package docstrings overlap. Some duplication is valuable
orientation; independent normative rules are not. `global_todo.md` also retains
work already covered by later CI, indexing and workflow-extraction changes.

**Action:** keep goals as principles, one contributing guide as the owner of
coding conventions, and one responsibility guide as the owner of package policy.
Package roots should summarise and link that policy. Keep a short active work list
whose entries link to the responsible plan or issue; completed campaign lists are
historical records. Review specific convention conflicts during consolidation,
including inconsistent docstring blank-line instructions, instead of concatenating
the guides unchanged.

### DS-07 — Separate metadata values, catalogue reads and acquisition (P2)

The `metadata/` and `metadata_container/` split reflects an earlier implementation
shape. Hydration, projections, dirtied queues, pure containers and metadata source
orchestration sit next to one another without a sufficiently visible owner map.
The source-layer contradiction in DS-03 is a consequence of that ambiguity.

**Action:** make metadata values and container contracts one documentation family;
put persistence-aware read composition in Catalog/Core guidance; retain acquisition
provider contracts with their implementation owner. Initially link the existing
files correctly. A later move of container guides under `metadata/containers/` is
reasonable, but must not imply that Catalog hydration moved back into metadata.

### DS-08 — Connect conversion, format and plugin documentation (P2)

The format dossiers, conversion sign-off matrix, Unicode guide and remaining-work
list repeat status and command sequences. The source now distinguishes neutral
plugin contracts, native format runtime, metadata readers/writers and application
fleet composition. Documentation should make those different responsibilities
visible instead of treating everything named “plugin” as one subsystem.

**Action:** give format behaviour one owner per dossier, conversion contracts one
cross-format owner, and verification history a dated location. Link them from one
format index. Consolidating the pipeline guides beneath `file_formats/conversion/`
can follow once references are repaired; it is not a prerequisite to correctness.

### DS-09 — Preserve history without making it required runtime knowledge (P2)

Dated reviews and working-memory records contain important decisions, but current
behaviour is too often recoverable only by reading several successive handoffs.
Conversely, an old review's original line numbers or measurements can look current
after a file move that preserves its title but loses its context.

**Action:** preserve original review evidence and append explicitly dated
dispositions. Promote stable conclusions into current guides. Keep working memory
for session continuity and detailed evidence; maintained contracts must state the
behaviour themselves and link to history for rationale.

### DS-10 — Make evidence portable and generated content intentional (P2)

The 86 locally available but untracked evidence targets are not reproducible links
for another checkout. The 7.34 MiB docstring inventory records a completed campaign
and file hashes, so normal edits naturally age it. Rewriting it repeatedly to make
the documentation look current would create work without establishing behaviour.

**Action:** summarize decisive results in tracked Markdown and identify local-only
raw outputs as such. Link to durable published artifacts only when they actually
exist. Preserve the inventory as a dated campaign artifact; establish its consumers
before moving it to an archive/tooling location. Do not regenerate historical
review hashes merely because files moved. Label the SQL sketches with their scope
and link readers to the installed-schema owner.

### DS-11 — Extend documentation checks to the actual failure modes (P1/P2)

The [link checks](../../tests/scripts/test_developer_documentation_links.py)
reject absolute file links throughout developer documentation, but check target
existence for a fixed subset of guides. The
[public-boundary checks](../../tests/scripts/test_public_documentation_boundaries.py)
protect useful ownership statements, while also embedding old paths. Neither
proves whole-tree reachability, clean-checkout evidence availability, or semantic
agreement between a document and its named owner.

**Action:** first migrate the existing assertions with the files. Then add a small
whole-tree local-link check, a tracked-target/declared-local-artifact rule, and a
navigation check for maintained guides. Keep behavioural contracts in relevant
subsystem tests. Do not substitute phrase-presence tests for actual ownership or
put flaky external-link requests on the critical CI path.

### DS-12 — Avoid a second uncontrolled restructuring (P1)

The main checkout already contains 80 staged moves and additional documentation,
source and working-memory edits. The reviewed implementation branch contains
newer contracts and annotations at old paths. This is an integration problem as
well as an information-design problem.

**Action:** reconcile content and destination deliberately, then use focused
changes with explicit path maps. Preserve current folder naming where it is
serviceable. Do not rename `storage_and_backup`, `databases` or `file_formats`
again merely for stylistic uniformity. This report adds a review, not a competing
mass move or a replacement for the existing user edits.

## Project structure and implementation assessment

### Repository-level boundaries

| Area | Current role | Structural recommendation |
| --- | --- | --- |
| `src/LiuXin_alpha/` | Installable application and reusable subsystems | Retain the `src` layout and explicit public boundaries. Fix ownership crossings inside it before considering top-level repackaging. |
| `src/past/` | Packaged compatibility support, included by setuptools | Document why it is shipped and its containment; do not treat all imported compatibility code as dead. |
| `tests/` | Behavioural, integration and structural checks | Keep tests with explicit subsystem scopes; documentation should name the smallest useful check and any live-service requirement. |
| `scripts/` | Builds, quality gates, audits and test orchestration | Retain as tool entry points. Documentation moves must preserve packaging and gate consumers. Runtime business policy should stay in the package. |
| `.github/workflows/` | Automated validation and live-storage lanes | Link exact workflow and command owners from contributing guidance. Required checks and the known full-suite failures remain separate operational work. |
| `examples/` | Usage demonstrations | Treat runnable examples as public consumers. Document required fixtures and avoid a second implementation of policy. |
| `dev-docs/` | Maintained guidance, decisions and review history | Separate authority and reader routes as proposed below. |
| `working-memory/` | Handoffs, investigations and local evidence | Preserve useful tracked history; keep transient output explicitly local and promote lasting contracts into `dev-docs`. |
| `LiuXin_alpha_data`, `LiuXin_data`, root plugin/preferences directories and local caches | Data, compatibility/development resources and operational state | Make fixture, packaged-resource and instance-state ownership explicit. Existence in a checkout must not make a file an installed runtime dependency. |
| `pyproject.toml`, `pytest.ini`, requirements and packaging manifests | Executable build/test policy | Let documentation explain these sources of truth rather than copy configuration tables that drift independently. |

Installed resources are already enumerated in [pyproject.toml](../../pyproject.toml),
including SQL schema trees and format resources. `random_sql/` is not that owner.
Wheel/deployment verification should continue to test the artifact users receive,
not merely whether the source checkout contains a file.

### Package ownership map

The reviewed source contains **1,798 Python modules** under `LiuXin_alpha`.
Counts identify scale, not complexity or quality; generated Unicode tables and
vendored implementations make physical line counts particularly misleading here.

| Package(s) | Modules | Intended/current responsibility | Structural disposition |
| --- | ---: | --- | --- |
| `core` | 59 | Application contract, composition, configuration, process services | Keep. `LibraryServices` now owns the resource graph. Finish job/ingest composition and capability boundaries. |
| `library` | 37 | Workflows across Catalog and storage; supported Library facade | Keep the facade and useful workflows. Retired `legacy`/`backend` modules must not be restored merely to match old documentation. Contain remaining cache/metadata compatibility. |
| `catalog` | 137 | Bibliographic operation semantics, identity, supported writes and persistence-aware retrieval | Keep as the semantic persistence boundary. Document canonical writers, adapters and excluded raw access precisely. |
| `metadata` | 194 | Pure values, vocabularies, normalization, codecs, file metadata providers | Keep pure contracts low in the dependency graph. Acquisition orchestration and compatibility islands still need explicit containment. |
| `databases` | 175 | Drivers, SQL, transactions, constraints and schema | Keep persistence separate from bibliographic meaning. Transaction/create safety and remaining storage-specific mutation ownership need follow-up. |
| `storage` | 197 | Bytes, stores, replicas, integrity, placement and backup | Keep. Enforce destructive/read-only policy at the public operation boundary, not only in one higher-level workflow. |
| `caches` | 63 | Derived lookup/read acceleration | Keep subordinate to canonical semantics. It must not become an independent bibliographic writer or category-count authority. |
| `surfaces` | 169 | CLI, terminal, GUI and web input/output adapters | Keep adapters thin. Read-only presentation must be backed by a constrained capability contract. |
| `ingest` | 15 | Discovery, extraction, candidates and provenance | Keep acquisition separate from acceptance and durable writes. Consolidate application entry points and cancellation/resource ownership. |
| `jobs` | 9 | Durable execution records and scheduling contracts | Establish one authoritative job lifecycle; distinguish it from low-level execution helpers and domain workflows. |
| `plugin_system` | 5 | Neutral lifecycle, hook/stream contracts and loading mechanics | Keep passive and small. It should not select application provider fleets. |
| `file_formats` | 463 | Conversion contracts, format implementations and native format runtime | Preserve the recent containment boundary. Broad numerical size is not a reason to mix it back into application composition. |
| `customize` | 24 | Application provider composition and compatibility exports | Make initialization, registration order and ownership explicit. It remains a distinct composition concern after plugin contract extraction. |
| `utils` | 235 | General primitives, vendored helpers and historical mixed responsibilities | Avoid making this the default destination for shared domain code. Separate reusable primitives from remaining policy/config/job compatibility as concrete callers migrate. |
| `constants`, `startup_scripts`, `resources`, package-root modules | 16 | Constants, startup integration, resources and root configuration modules | Keep root imports passive; finish scratch-directory/startup-hook containment. Configuration changes do not automatically solve every startup side effect. |

This map describes responsibility, not a strict single call chain. Core composes
services; Library coordinates workflows; Catalog and storage are collaborators
with different responsibilities. Metadata supplies pure rules used by consumers.
A diagram that puts every package in one vertical stack would obscure composition,
data flow and allowed imports. Future architecture diagrams should label those
relationships separately.

### What the concrete changes have already achieved

| Concern from the earlier reviews | Implemented at the reviewed checkpoint | What still needs to happen |
| --- | --- | --- |
| Nobody owns opening and closing services | `472d71d0`: Core-owned `LibraryServices`, rollback on partial startup, teardown of all owned services; Database construction defaults to persistence only | Retire remaining lower-layer path-opening wrappers and resolve maintenance scheduling. Resource lifetime is not transaction rollback policy. |
| Several write paths and incompatible normalization | `2709f504`, `862576cb`, `9ff593a2`, `5f085a13`, `18040e46`, `3a4934cc`, `f40881b6`: pure metadata policy, canonical Catalog mutations, declared stored identities, adapter routing and real-backend rollback coverage | Keep documenting advanced/raw exclusions and retained adapters. Finish any independently identified compatibility ownership debt without claiming every old entry point is gone. |
| Catalog/cache/metadata dependency confusion | Hydration moved into `catalog.retrieval`; package roots became more passive; no current SCC contains both Catalog and caches | Legacy metadata/cache islands remain. Continue removing concrete reverse dependencies; a smaller cycle is not proof of complete layering. |
| Two catalogue category models disagree | `daa6a0ed`: Core category source, count, ordering and membership semantics; bounded adapter pages and direct/HTTP parity | Broader read projections are still separate. Do not describe this as unifying every catalogue read model. |
| Two application configuration worlds | `d59407f3`: Core selection, `ConfigurationPaths`, one lazy Preferences view, INI write ownership and worker propagation | Component namespaces and lifetimes remain distinct; worker propagation does not unify durable job ownership. Startup/scratch paths are separate remaining concerns. |
| Format/plugin code reaches into application composition | `f8723b96`, `f1bb7622`: neutral plugin contracts, native format runtime, containment and concrete runtime fixes | Application registry initialization/order and metadata acquisition composition remain. Specific conversion defects and reporting gaps are still open. |
| Large bodies of unusable or duplicate code | `06bf503d`, `af2cebd0`: proven dead definitions/copies and broken Library facades removed; fixture caller migrated | Retained compatibility can be live. Static unreachability alone is insufficient evidence for deletion, particularly with plugins and dynamic imports. |
| Test runs accumulate storage/resource pressure | `152991fa` and telemetry/cleanup follow-ups close auxiliary handles, reclaim completed scratch and retain diagnostics | Preserve the operational runbook and headroom checks. These observations support the storage-pressure diagnosis without proving the cause of the original VM crash. |

The import analysis for this exact source tree reports **15 backward edges** and
**28 unresolved dynamic imports**. Largest domain/runtime/combined strongly
connected components are **9 / 217 / 217**. The protected-cycle scope contains
243 modules; the direction inventory covers all 1,798. Do not repeat the older
“12% covered” claim as though it described the entire direction checker.

The project has therefore made meaningful structural progress, but its accepted
dependency graph still contains debt. Maintain exact reviewed exceptions and
fresh-process import checks. Neither a clean scoped gate nor a successful import
establishes all runtime capability, transaction or deletion guarantees.

### Architectural work that documentation must leave visibly open

1. **Transport and capabilities:** HTTP invocation still needs the trust boundary
   identified in CO-4/SU-6. Read-only applications retain broader Core access than
   their presentation suggests. Transport stability is not authorization.
2. **Destructive storage operations:** last-copy/read-only enforcement, compatibility
   backend flag propagation and ownership of staging-directory cleanup remain
   distinct public-boundary concerns. Safer evacuation behaviour does not establish
   those guarantees for every deletion route.
3. **Persistence safety:** exception-sensitive transaction handling and destructive
   blank-database creation remain separate from successful lifecycle teardown.
4. **Jobs, ingest and registries:** there are still multiple execution/composition
   owners. Define durable state, restart/cancellation behaviour, resource ownership,
   provider selection and ordering before merging structures with similar names.
5. **Compatibility and formats:** retained metadata/cache implementations, concrete
   DOCX/HTML conversion defects, process-wide conversion effects and loss reporting
   require bounded follow-ups with real callers and regression evidence.
6. **Repository and CI hygiene:** the preceding audit found 47 tracked ignored
   artifacts totalling 128.4 MiB. Known full-suite failures and required-check
   enforcement remain open. Documentation-only checks do not close those findings.

These are implementation tasks with documentation consequences. They should not
be disguised as folder cleanup or silently declared solved by a new architecture
diagram. The detailed finding IDs and source evidence remain in the September
review's 2026-10-18 follow-up at `8c20f7d2`.

## Recommended documentation structure

Keep Markdown and the existing repository workflow. The evidence does not require
a new documentation platform. The following is a destination model, not a request
to create empty directories or move every file in one commit:

```text
dev-docs/
  README.md                         task routes and subsystem index
  project-level-goals.md             enduring project principles
  architecture/
    README.md                       current map and how to read decisions
    responsibilities.md             canonical ownership and dependency policy
    decisions/                      accepted decisions, each with migration status
  architectural_reviews/
    README.md                       dated reviews and their follow-ups
    ...-YYYY-MM-DD.md
  hacking/
    README.md                       contributor route
    coding-guide.md                  consolidated conventions
    packaging.md
    testing/                        CI, streams, telemetry, fixtures, fuzzing
    quality/                        active type/import/layer/documentation gates
  core/                             lifecycle, settings, operations, job composition
  catalog/                          writes, identity, matching and persistent reads
  metadata/
    containers/                     passive value and container contracts
    ...                             source/provider and pure conversion guidance
  databases/                        schema, backend, transaction and repair contracts
  storage_and_backup/               storage contracts and operator procedures
  surfaces/                         external adapters and startup procedures
  file_formats/
    conversion/                     shared pipeline contract and verification routes
    <format>/                       individual format behaviour
  calibre_compat/                   supported compatibility boundaries
  proposals/                        exploratory designs that are not accepted contracts
  archive/                          superseded guides and completed campaign artifacts
```

`architecture/` and `architectural_reviews/` deliberately answer different
questions: “what should I build against now?” and “what did we find at a particular
checkpoint?”. Accepted decisions can retain historical rationale while stating
which parts are implemented. Reorganising the existing review folder is not needed
to add the present dated report.

Do not create a separate documentation folder for every Python package. A Core
composition guide can initially explain jobs and registry ownership, with links to
their implementation contracts. Introduce dedicated topic homes when there is
enough maintained content and a real reader task to justify them.

### Canonical content and proposed destinations

| Current material | Proposed maintained owner | Treatment of the rest |
| --- | --- | --- |
| `00 - This Document.md`, introduction, root README/index | Root index plus a short explanation of documentation status | Keep motivation in project goals or a clearly labelled note; preserve the lab-book role under proposals/history. |
| `02 - Top Level Structure.md`, `04 - Seperation of Concerns.md`, older separation/module notes | `architecture/README.md` and `architecture/responsibilities.md` | Extract current contracts; archive superseded sketches with successor links. Update all package-root pointers in the same migration. |
| Target architecture | `architecture/decisions/` | Keep problem/rationale history, but give each decision an implementation checkpoint and remaining migration work. |
| Dated architecture and lifecycle reviews | `architectural_reviews/` | Preserve original findings plus dated dispositions; add an index. |
| Maintainability, layer and import ratchets | `hacking/quality/` | Explain active commands/scopes here and link architectural rationale; these are not merely historical reviews. |
| Coding Guide and Style Guide | `hacking/coding-guide.md` | Reconcile rules and examples before removing duplicated text. Goals remain a separate principles document. |
| Schema overview and database notes | `databases/README.md` plus focused schema/backend/transaction guides | Mark drafts and SQL sketches historical/proposed as appropriate; installed schema remains code-owned. |
| Deprecated storage overview | Current storage index and contracts | Transfer any still-current explanations, then archive the deprecated overview. |
| Test databases and artifact guidance | `hacking/testing/` | Keep fixture/data contracts distinct from installed application resources. |
| Storage Cache Backends, Catalog/cache and read-model failure guidance | Catalog read/cache contract, with Core/surface failure behaviour linked separately | Make the word “cache” refer to a defined responsibility; byte-storage caches and catalogue projections need distinct explanations. |
| Metadata containers and DB source guide | Metadata container contracts; Catalog retrieval contract for DB sources | Preserve the value/persistence boundary established by the code extraction. |
| Conversion pipeline and format dossiers | Shared format index and conversion guide | Keep dated sign-off evidence separately from evergreen behaviour. |
| Read-only surface startup and metadata CLI | `surfaces/` task guides linked from the relevant subsystem | Operational use belongs with interfaces; provider/value contracts remain with metadata. |
| Docstring inventory, completion plan, shim campaign and old TODO lists | Dated archive or retained campaign record with an explicit status | Keep only current actionable work in the active index. Do not turn historic hash coverage into a permanent maintenance requirement. |

### Required reader routes

| Reader task | Route the indexes should provide |
| --- | --- |
| Set up the project and make a first change | Root README → hacking → relevant subsystem contract → smallest useful test command |
| Add or change a bibliographic mutation | Architecture responsibilities → Catalog write contract → metadata value rules → real-backend transaction tests |
| Understand lifecycle or add an application operation | Core index → LibraryServices/operation contract → workflow owner → failure/teardown checks |
| Configure a deployment or diagnose worker settings | Core configuration contract → surface selection procedure → precedence/worker tests and known limits |
| Operate ingest, backup or recovery | Surface runbook → storage contract and safety limits → bounded dry-run/verification procedure |
| Add a format or metadata provider | Format/metadata index → provider contract → composition owner → representative fixtures and containment checks |
| Diagnose slow or space-intensive tests | Hacking/testing → streams, telemetry and cleanup procedure → retained evidence policy |
| Understand an architectural exception | Current ownership guide → exact gate exception/decision → dated review and remaining action |

## Document lifecycle and maintenance policy

Use a small common vocabulary, applied to the document's actual role:

| Status | Meaning |
| --- | --- |
| **Current contract** | Supported behaviour or a rule for new work, with implementation scope and exclusions stated. |
| **Accepted decision — migration incomplete** | Chosen direction; implemented portions and remaining callers are explicit. |
| **Proposal** | An option or design under discussion, not an obligation on current callers. |
| **Historical review/record** | Evidence at a named date and source checkpoint; later dispositions are separately dated. |
| **Generated inventory** | Tool-produced data with generator, inputs/checkpoint and regeneration purpose recorded. |

A primary contract should identify its owner, status, last meaningful verification
checkpoint, and relevant code/tests. Use a date when behaviour was checked, not a
mechanical timestamp on every formatting edit. State exclusions where readers
could otherwise infer a guarantee the implementation does not provide.

Short package docstrings and overview summaries are useful duplication when they
link the canonical rule. Long copied policies, precedence tables and mutable TODO
lists create competing owners. Consolidate those by subject; do not remove useful
worked examples merely because they exercise the same API.

Historical local output should be described as unavailable local evidence rather
than published as a broken Markdown link. Summarise the result needed to assess
the claim in tracked text. A fresh clone must be able to navigate current guidance
without copying another developer's `working-memory/test-results` directory.

## Implementation order and acceptance criteria

The following sequence respects the existing preference for focused PRs. Each
step should have a reviewable result before the next larger consolidation begins.

| Step | Scope | Completion evidence |
| --- | --- | --- |
| 0. Reconcile the two documentation checkpoints | Preserve pending moves, newer configuration/read contracts and the annotated September review; record old-to-new paths | Reviewed content/path reconciliation, with no lost user edits or duplicate authorities. |
| 1. Complete the current folder migration | Repair links and relative depths; migrate package pointers, deployment paths and tests; add useful folder indexes and format-dossier links | All intended local targets resolve in a clean checkout; existing documentation and deployment-package tests pass with new locations. |
| 2. Establish current architecture authority | Publish current map/responsibilities and decision statuses; fix lifecycle/hydration/configuration/category-read claims | Each principal concept has one named contract, source owner, verified scope and explicit remainder. |
| 3. Consolidate topic guidance | Conventions, metadata/container roles, format/conversion guidance, root numbered guides and surface runbooks | Reader tasks above are navigable; no policy is lost; superseded documents have a deliberate disposition. |
| 4. Separate historical and generated material | Archive completed plans/inventory and obsolete sketches after checking consumers; summarise portable evidence | Maintained guides do not depend on ignored local outputs; generated artifacts have a stated purpose and regeneration rule. |
| 5. Keep the structure healthy | Extend whole-tree link/portability checks and maintained-guide navigation coverage; document contribution rules | A representative new guide can be added through one index and checked without a full application suite or manual path archaeology. |

Step 1 is the recommended next change. It is concrete, bounded and prerequisite
to judging the reorganised documentation fairly. Some step-5 link checking belongs
in step 1 so the migration is verifiable; wider lifecycle/navigation policy can
follow once document statuses are assigned.

For the final structure, every maintained guide should be reachable from the
developer index through a short, intentional route, normally a subsystem index
and then the guide. Historical collections can have deeper navigation. Check local
targets against tracked content, not just the author's filesystem. Check headings
using the actual renderer's conventions before making anchor validation a gate.

Run the documentation and deployment-consumer tests for path changes, plus the
appropriate source/static gate if package docstrings change. Use disposable data
for executable runbook examples. The full application suite is not required merely
to move Markdown; it remains necessary when implementation behaviour changes.
Keep the telemetry and serial execution practice for costly suites.

## Audit method, evidence and limits

The audit enumerated every file in both `dev-docs` trees, extracted every Markdown
heading, parsed local links from those documents and the root README, and built
a navigation graph from the two README entry points. It compared local file
targets with the Git index, captured document hashes, read the staged rename map,
and searched executable consumers and source signposts for old paths. Relevant
guides were compared with current source owners and the concrete implementation
history. This is whole-tree structural coverage, not a claim that every sentence
in all 112 documents was independently behaviour-tested.

CommonMark parsing detects ordinary inline/reference links and images. Code-text
paths, dynamically assembled paths and raw HTML need separate checking. Directory
links count as navigation to a README when one exists. The heading-slug heuristic
reported no candidates, but is not proof of GitHub anchor parity. Exact repeated
text blocks were used only to locate overlap; their count is not a measure of
unnecessary duplication. External websites were not fetched.

Raw audit outputs are **local-only**, under
`working-memory/test-results/documentation-structure-20261018/`:

- `main-working-tree.json` and `review-branch.json`: inventory, hashes, links,
  missing/untracked targets, status-header observations and navigation results.
- `main-working-tree-outline.txt` and `review-branch-outline.txt`: complete heading
  inventories for the two snapshots.
- `migration-references.json` and `old-doc-paths.txt`: staged rename mapping and
  literal old-path consumers outside developer documentation.
- `code-structure.json`: package module counts and identical source-tree IDs.
- `audit-summary.txt`: measurement summary.

These paths are intentionally not presented as portable evidence links. The
measurements and decisive examples needed to review the recommendations are in
this tracked report. The audit script was exploratory; a maintained checker should
be introduced with the focused migration, rather than committing an ad hoc scan
as a new permanent subsystem.

This change creates the review only. It does not alter production code, rename
existing guides, claim a clean full-suite baseline, or close any unresolved safety
finding by documentation alone.

Validation of this report: all five relative links resolve in both checkouts;
the Markdown parses with all 11 tables intact; the existing developer-documentation
and public-documentation-boundary tests pass (**25 passed**). Those tests ran in
the isolated review checkout. Their success does not mean the pending folder
migration in the main checkout passes its old path assertions. No application
suite was run for this documentation-only addition.
