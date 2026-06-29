# Catalog Layer: Database + Metadata

## Purpose

The catalog layer is the place where raw database persistence becomes meaningful metadata.

It sits between the low-level database module and the higher-level library workflows. 
Its job is to combine database access with LiuXin's metadata semantics: WEMI objects, agents, identifiers, titles, notes, matching rules, metadata retrieval, and metadata mutation policy.

The short version:

```text
databases  = raw persistence machinery
metadata   = meanings, dataclasses, field semantics, WEMI concepts
catalog    = metadata-aware persistence API
storage    = files, replicas, stores, packs, physical locations
library    = catalog + storage + workflows
surfaces   = human/program interfaces on top
```

The catalog is not a second database layer. It is the domain-facing API backed by the database layer.

## Dependency hierarchy

Preferred dependency direction:

```text
surfaces
   |
library
   |
   +-- catalog
   |      |
   |      +-- metadata
   |      |
   |      +-- databases
   |
   +-- storage
          |
          +-- databases, where needed
```

A simpler way to remember it:

```text
databases -> metadata -> catalog -> library -> surfaces
```

This is not a strict import graph in every case, but it is the architectural direction. Higher layers may call lower layers. Lower layers should not know about higher-layer policy.

## Layer responsibilities

### databases

The `databases` module owns raw persistence machinery.

It should contain:

- database connections
- driver wrappers
- SQL execution
- schema creation
- migrations
- triggers
- generic row handling
- transactions
- low-level views
- generic link-table helpers
- database repair and integrity utilities

It should not know:

- what a Work title means
- how an Agent should be resolved
- how an imported candidate should be matched
- which identifier type is bibliographically stronger
- how to build a human-facing item summary

The database layer should be boring, durable, and predictable.

### metadata

The `metadata` module owns metadata meanings and value types.

It should contain:

- dataclasses
- protocols
- WEMI concepts
- field semantics
- identifier types
- title semantics
- language metadata structures
- agent metadata structures
- validation helpers that do not require persistence

It should not execute SQL or know where records are physically stored.

### catalog

The `catalog` module owns metadata-aware persistence.

It should contain:

- repositories
- metadata matching
- metadata mutation APIs
- WEMI-aware retrieval
- display-neutral projections
- catalog-side mutation policy
- duplicate resolution and merge helpers
- metadata dirty-marking hooks, where appropriate

It answers questions like:

- Which Work does this identifier belong to?
- Does this candidate match an existing Work?
- What titles are attached to this Expression?
- Which Agents are linked to this Work?
- What coherent WEMI metadata bundle describes this Item?
- What metadata records need to be updated after this merge?

### storage

The `storage` module owns physical and logical file storage.

It should contain:

- digital assets
- digital asset replicas
- composite digital assets
- stores
- physical paths
- backup packs
- storage policies
- replica validation
- pack creation and verification

It should not decide bibliographic identity.

### library

The `library` module owns high-level orchestration.

It combines catalog and storage operations into actual workflows:

- ingest
- import
- enrichment
- indexing
- backup planning
- repair workflows
- sidecar generation
- user-facing library operations

A useful slogan:

```text
catalog = database + metadata
library = catalog + storage
```

### surfaces

The `surfaces` module owns interfaces to people and external callers.

It may contain:

- terminal UI
- GUI UI
- HTTP/API surfaces
- RSS interfaces
- report rendering
- human display formatting
- command-line tools

It may call into `library`, `catalog`, `storage`, or lower layers as appropriate, but display-specific rules should not leak downward into catalog objects.

## Repository pattern

A repository is a catalog-facing object that provides a clean API for one metadata concept or aggregate.

Repositories are not table classes.

A repository may use several SQL tables internally if that is what the domain concept requires. For example, `WorkRepository` may touch:

```text
works
work_titles
work_identifiers
agent__works__links
work__expression__links
notes
```

The caller should not need to know that.

Instead of this:

```python
db.select("works", ...)
db.join("agents", "agent__works__links", ...)
db.query_identifier_tables(...)
db.hand_roll_wemi_bundle(...)
```

callers should do this:

```python
catalog.works.get(work_id)
catalog.agents.resolve_name("Mary Shelley")
catalog.identifiers.find_by_value("isbn", "978...")
catalog.items.get_metadata_bundle(item_id)
```

The repository owns the persistence logic for the domain concept. It hides the storage shape while exposing useful metadata operations.

## Repository versus matcher versus retrieval

The catalog package should not put every metadata operation into one giant object. A useful split is:

```text
repositories/
    Own one domain area.
    CRUD-ish methods, lookup methods, and local domain mutations.

matching/
    Decide whether candidate metadata refers to an existing thing.
    May consult several repositories.

retrieval/
    Build useful read models, projections, bundles, and WEMI slices.

storage/
    Catalog-side mutation helpers and policies.
    This is not physical file storage; physical storage remains top-level storage.
```

Example repository operations:

```python
catalog.works.create(candidate)
catalog.works.get(work_id)
catalog.works.add_title(work_id, title)
catalog.works.link_agent(work_id, agent_id, role="author")
```

Example matching operation:

```python
match = catalog.matching.match_work(candidate)
```

Matching belongs outside the basic repository when it is policy-heavy or crosses domain boundaries. A Work matcher may need to inspect identifiers, titles, agents, languages, dates, manifestations, expressions, and existing WEMI links before making a decision.

Example retrieval operation:

```python
bundle = catalog.items.get_metadata_bundle(item_id)
```

The `ItemRepository` may expose this as a convenience method, but internally it can delegate to a retrieval helper such as `ItemBundleRetriever`. That keeps repositories from turning into giant grab-bags.

## Suggested package shape

```text
src/LiuXin_alpha/catalog/
    __init__.py
    catalog.py

    api/
        __init__.py
        catalog.py
        common.py
        repositories.py
        matching.py
        retrieval.py
        storage.py

    repositories/
        __init__.py
        works.py
        expressions.py
        manifestations.py
        items.py
        agents.py
        identifiers.py
        titles.py
        notes.py

    matching/
        __init__.py
        work_matcher.py
        agent_matcher.py
        identifier_matcher.py

    retrieval/
        __init__.py
        bundles.py
        projections.py

    storage/
        __init__.py
        metadata_writer.py
        mutation_policy.py
```

The `api/` structure should mirror the implementation structure. The API layer defines the public protocols and value objects; the implementation layer provides concrete classes.

## Suggested facade shape

The top-level `Catalog` object should be a small facade that wires together repositories, matchers, and retrieval helpers.

```python
class Catalog:
    def __init__(self, db: DatabaseAPI) -> None:
        self.db = db

        self.works = WorkRepository(db)
        self.expressions = ExpressionRepository(db)
        self.manifestations = ManifestationRepository(db)
        self.items = ItemRepository(db)

        self.agents = AgentRepository(db)
        self.identifiers = IdentifierRepository(db)
        self.titles = TitleRepository(db)
        self.notes = NoteRepository(db)

        self.matching = MatchingFacade(
            works=WorkMatcher(db),
            agents=AgentMatcher(db),
            identifiers=IdentifierMatcher(db),
        )

        self.retrieval = RetrievalFacade(
            bundles=BundleRetriever(db),
            projections=ProjectionRetriever(db),
        )
```

Callers should then write code like:

```python
catalog = Catalog(db)

work = catalog.works.get(work_id)
agent = catalog.agents.resolve_name("Mary Shelley")
match = catalog.matching.works.match(candidate_work)
bundle = catalog.items.get_metadata_bundle(item_id)
```

They should not write:

```python
db.get_work_metadata(...)
db.resolve_agent(...)
db.match_identifier_to_work(...)
```

Those are catalog operations, not raw database operations.

## Worked examples

### Example: creating a new Work from imported metadata

Belongs in catalog:

```python
work_id = catalog.works.create(candidate)
catalog.works.add_title(work_id, candidate.title)
catalog.works.add_identifier(work_id, candidate.primary_identifier)
catalog.works.link_agent(work_id, author_id, role="author")
```

Does not belong in catalog:

- walking an import folder
- hashing files
- deciding backup policy
- rendering a UI progress bar
- creating a squashfs pack

Those are library, storage, jobs, or surfaces concerns.

### Example: resolving an Agent

Belongs in catalog:

```python
agent = catalog.agents.find_by_name("Mary Shelley")
match = catalog.matching.agents.match(candidate_agent)
agent = catalog.agents.resolve_or_create(candidate_agent)
```

Does not belong in databases:

```python
db.resolve_author_name("Mary Shelley")
```

The raw database layer may provide generic select, insert, transaction, and constraint behaviour. It should not own Agent semantics.

### Example: importing a folder of EPUB files

Belongs in library:

```python
library.import_path(path)
```

The library workflow may call:

```python
asset_id = storage.assets.register_file(path)
match = catalog.matching.works.match(candidate_work)
work_id = catalog.works.create(candidate_work)
item_id = catalog.items.create_for_manifestation(manifestation_id)
catalog.items.link_digital_asset(item_id, asset_id)
```

The workflow itself does not belong in catalog because it combines metadata decisions with file and storage operations.

### Example: fetching an Item metadata bundle

Belongs in catalog retrieval:

```python
bundle = catalog.items.get_metadata_bundle(item_id)
```

This may gather:

- Item fields
- Manifestation fields
- Expression fields
- Work fields
- titles
- identifiers
- agents
- notes
- relevant WEMI links

It should return a display-neutral object. Human formatting belongs in `surfaces`.

### Example: Calibre compatibility

Belongs partly in catalog:

- building a legacy-style metadata projection from WEMI records
- deciding how catalog concepts map to compatibility shapes
- exposing display-neutral compatibility records

Belongs partly in databases:

- SQL views that expose stable compatibility shapes
- generic query support for those views

Belongs outside catalog:

- UI-specific column labels
- interactive display formatting
- import wizard behaviour
- terminal table formatting

### Example: duplicate Agent merge

Belongs in catalog:

```python
catalog.agents.merge(primary_agent_id, duplicate_agent_id)
```

The merge may need to:

- move links from the duplicate Agent to the primary Agent
- preserve identifiers and notes
- record provenance
- mark affected metadata as dirty
- ensure constraints are not violated

Does not belong in databases:

```python
db.merge_agents(...)
```

The database layer can provide transactions and constraints. The catalog layer decides what it means to merge Agents.

### Example: physical replica verification

Belongs in storage:

```python
storage.replicas.verify(replica_id)
```

May call into databases for persistence. May update catalog or library state if metadata should be marked dirty. But the verification of files, stores, checksums, and physical replicas is not a catalog responsibility.

### Example: generated display title

A display-neutral title projection may belong in catalog:

```python
projection = catalog.retrieval.projections.item_title_projection(item_id)
```

Human display formatting belongs in surfaces:

```python
terminal.render_item_title(projection)
```

The catalog may provide the pieces. The surface decides how to show them.

## Mutation policy

Catalog mutation should be explicit.

The catalog layer may provide helpers such as:

- create Work
- attach title
- attach identifier
- link Agent to Work
- link Expression to Work
- merge duplicate Agents
- mark metadata dirty
- update provenance
- resolve or create a record

Avoid implicit, surprising writes.

For example, this is dangerous:

```python
agent = catalog.agents.find("Mary Shelley")
```

if `find()` secretly creates an Agent when none exists.

Prefer clear method names:

```python
agent = catalog.agents.find_by_name("Mary Shelley")
agent = catalog.agents.resolve_or_create(candidate_agent)
```

The method name should reveal whether mutation may occur.

## Migration plan

A safe migration path is:

1. Create the `catalog/` package and importable API skeleton.
2. Move the most obviously metadata-aware `Database` methods into matching catalog repositories.
3. Leave deprecated pass-through methods on the old database class where needed.
4. Add tests against the new `Catalog` API before deleting the old route.
5. Gradually update callers from `db.resolve_agent(...)` to `catalog.agents.resolve(...)`.
6. Keep pure SQL, driver, schema, trigger, transaction, and row logic in `databases`.
7. Delete old pass-through methods once all tests are green.

This should be a thin move first, not a grand rewrite.

## Placement rules

Use these rules when deciding where new code belongs.

### Put it in databases if...

- it is about SQL execution
- it is about connection handling
- it is about schema generation
- it is about triggers
- it is about generic rows
- it is about transactions
- it is about database integrity
- it has no bibliographic meaning

### Put it in metadata if...

- it defines meaning
- it defines a value object
- it defines a metadata field
- it validates a metadata object without persistence
- it describes WEMI concepts
- it describes identifiers, titles, agents, notes, or language semantics

### Put it in catalog if...

- it stores metadata records
- it finds metadata records
- it matches candidate metadata to existing records
- it links WEMI objects
- it mutates bibliographic records
- it retrieves metadata bundles
- it builds display-neutral catalog projections
- it manages metadata merge policy

### Put it in storage if...

- it handles files
- it handles digital assets
- it handles replicas
- it handles physical stores
- it handles backup packs
- it handles checksums for physical file validation
- it handles storage policy

### Put it in library if...

- it performs a workflow
- it coordinates catalog and storage
- it imports files
- it enriches metadata
- it plans backup work
- it repairs a library state
- it runs a user-level operation

### Put it in surfaces if...

- it formats output for people
- it renders terminal UI
- it renders GUI UI
- it handles external API presentation
- it handles report layout
- it turns display-neutral data into human-facing text

## Anti-patterns

### Anti-pattern: metadata_database

Avoid creating a module that sounds like a second database layer.

Bad names:

```text
metadata_database
database_metadata
metadata_db
db_metadata
```

These names blur the boundary between raw persistence and metadata-aware persistence.

Prefer:

```text
catalog
```

### Anti-pattern: table classes as repositories

A repository is not a class wrapper around exactly one table.

Bad shape:

```python
class WorksTable:
    def insert(...): ...
    def select(...): ...
```

Better shape:

```python
class WorkRepository:
    def create(...): ...
    def get(...): ...
    def add_title(...): ...
    def add_identifier(...): ...
    def link_agent(...): ...
```

The repository exposes the domain concept, not the storage detail.

### Anti-pattern: catalog as junk drawer

Do not let catalog absorb:

- ingest orchestration
- backup planning
- UI formatting
- job scheduling
- physical replica validation
- terminal progress bars
- Calibre import wizard flow

Those are higher-layer or neighbouring-layer concerns.

### Anti-pattern: lookup methods with hidden writes

Avoid method names that sound read-only but mutate state.

Bad:

```python
catalog.agents.find(name)
```

if it creates a missing Agent.

Better:

```python
catalog.agents.find_by_name(name)
catalog.agents.resolve_or_create(candidate)
```

## Design rules

1. Keep databases boring.

   The database layer should be durable, generic, and predictable. It should not accumulate bibliographic policy.

2. Keep metadata pure.

   Metadata classes should describe meaning. They should not execute queries.

3. Keep catalog semantic.

   Catalog APIs should be named in terms of Works, Expressions, Manifestations, Items, Agents, Titles, Identifiers, Notes, matching, retrieval, and mutation.

4. Keep library workflow-oriented.

   If an operation sounds like a task a user asked the program to perform, it probably belongs in library, not catalog.

5. Keep surfaces out of catalog.

   Catalog projections should be display-neutral. Formatting for humans, terminal output, GUI tables, and reports belongs in surfaces.

6. Prefer explicit writes.

   Avoid lookup methods that secretly create records. Use clear names such as `resolve_or_create_*` when mutation may occur.

7. Prefer repositories over table classes.

   Repositories model domain concepts. Tables model storage details.

8. Keep matching policy visible.

   Matching is complicated enough to deserve named objects, tests, and explicit confidence results.

9. Keep retrieval display-neutral.

   Catalog may build a projection. Surfaces decide how to present it.

10. Let library orchestrate.

    Catalog should do metadata-aware persistence. Library should combine catalog, storage, jobs, and surfaces into workflows.

## Pasteable module docstring

This docstring can be used in `src/LiuXin_alpha/catalog/__init__.py` or `src/LiuXin_alpha/catalog/catalog.py`.

```python
"""
LiuXin catalog layer.

The catalog layer is the metadata-aware persistence API for LiuXin.

It sits above the raw database layer and below the higher-level library
workflows. Its job is to combine database access with metadata semantics:
WEMI objects, agents, identifiers, titles, notes, matching rules, and
metadata retrieval.

Layer responsibilities
----------------------

    databases
        Raw persistence machinery.

        This layer owns connections, drivers, SQL execution, schema creation,
        migrations, transactions, triggers, generic row handling, and low-level
        database utilities.

        It should not know what a Work title means, how an Agent should be
        resolved, or how to match an imported book candidate to an existing
        bibliographic record.

    metadata
        Metadata meanings and value types.

        This layer owns dataclasses, protocols, field meanings, WEMI concepts,
        identifier types, title semantics, and other domain vocabulary.

        It should not execute SQL or know where records are physically stored.

    catalog
        Metadata-aware persistence.

        This layer owns repository APIs, metadata matching, metadata mutation,
        WEMI-aware retrieval, and catalog projections.

        It answers questions like:

            * Which Work does this identifier belong to?
            * Does this candidate match an existing Work?
            * What titles are attached to this Expression?
            * Which Agents are linked to this Work?
            * What coherent WEMI metadata bundle describes this Item?

    storage
        Physical and logical file storage.

        This layer owns digital assets, replicas, stores, storage policies,
        backup packs, and physical file locations.

        It should not decide bibliographic identity.

    library
        High-level orchestration.

        This layer combines catalog and storage operations into workflows:
        ingest, import, enrichment, backup planning, repair, indexing, and
        user-facing library operations.

Repository pattern
------------------

A repository is a catalog-facing object that provides a clean API for one
metadata concept or aggregate.

Repositories are not table classes. A repository may use several SQL tables
internally if that is what the domain concept requires.

For example, WorkRepository may touch tables for works, titles, identifiers,
agent links, notes, and WEMI links. Callers should not need to know that.

Typical repository usage::

    catalog = Catalog(db)

    work = catalog.works.get(work_id)
    work_id = catalog.works.create(candidate_work)
    catalog.works.add_title(work_id, title)
    catalog.works.add_identifier(work_id, identifier)

    agent = catalog.agents.resolve_name("Mary Shelley")
    catalog.works.link_agent(work_id, agent.id, role="author")

    item_bundle = catalog.items.get_metadata_bundle(item_id)

Matching
--------

Matching is separated from basic repositories because it is policy-heavy and
often crosses repository boundaries.

A Work matcher may need to consult identifiers, titles, agents, languages,
dates, manifestations, and existing WEMI links before deciding whether a
candidate describes an existing Work or a new Work.

Typical matching usage::

    result = catalog.matching.match_work(candidate)

    if result.is_confident:
        work_id = result.matched_id
    else:
        work_id = catalog.works.create(candidate)

Retrieval and projections
-------------------------

Retrieval helpers build useful read models from several underlying records.

Examples include:

    * an Item metadata bundle
    * a WEMI slice
    * a display-neutral title projection
    * a contributor summary
    * a legacy compatibility projection

These helpers may use repositories internally, but they should not perform
large ingest workflows or physical storage operations.

Typical retrieval usage::

    bundle = catalog.retrieval.items.bundle_for_item(item_id)
    summary = catalog.retrieval.projections.item_summary(item_id)

Mutation policy
---------------

Catalog mutation should be explicit.

The catalog layer may provide helpers such as:

    * create Work
    * attach title
    * attach identifier
    * link Agent to Work
    * link Expression to Work
    * merge duplicate Agents
    * mark metadata dirty

But it should avoid implicit, surprising writes.

For example, a lookup method should not silently create a new Agent unless its
name makes that behaviour clear, such as resolve_or_create_agent().

Design rules
------------

1. Keep databases boring.
2. Keep metadata pure.
3. Keep catalog semantic.
4. Keep library workflow-oriented.
5. Keep surfaces out of catalog.
6. Prefer explicit writes.
7. Prefer repositories over table classes.
"""
```
