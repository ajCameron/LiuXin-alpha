
"""
Catalog is intended for metadata aware database operations.

The database class is responsible for raw storage and persistence.
Catalog handles this in combination with metadata operations.
These include operations such as
 - matching agents
 - adding agents
 - removing agents
E.t.c.

Metadata-aware catalog layer for LiuXin.

`catalog` sits above the raw `databases` package and below higher-level `library`
workflows. It owns WEMI-aware storage, matching, and retrieval APIs, while leaving
SQL execution, transactions, schema generation, triggers, and driver mechanics in
`databases`.

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

Worked examples
---------------

Example: creating a new Work from imported metadata.

    Belongs in catalog:

        catalog.works.create(candidate)
        catalog.works.add_title(work_id, candidate.title)
        catalog.works.add_identifier(work_id, candidate.primary_identifier)
        catalog.works.link_agent(work_id, author_id, role="author")

    Does not belong in catalog:

        walking an import folder
        hashing files
        deciding backup policy
        rendering a UI progress bar

Example: resolving an Agent.

    Belongs in catalog:

        catalog.agents.find_by_name("Mary Shelley")
        catalog.agents.match(candidate_agent)
        catalog.agents.resolve_or_create(candidate_agent)

    Does not belong in databases:

        db.resolve_author_name("Mary Shelley")

    The raw database layer may provide generic select, insert, transaction,
    and constraint behaviour, but it should not own Agent semantics.

Example: importing a folder of EPUB files.

    Belongs in library:

        library.import_path(path)

    The library workflow may call:

        storage.assets.register_file(path)
        catalog.matching.match_work(candidate)
        catalog.works.create(candidate)
        catalog.items.link_digital_asset(item_id, asset_id)

    The workflow itself does not belong in catalog because it combines metadata
    decisions with file/storage operations.

Example: fetching an Item metadata bundle.

    Belongs in catalog retrieval:

        catalog.items.get_metadata_bundle(item_id)

    This may gather:

        * Item fields
        * Manifestation fields
        * Expression fields
        * Work fields
        * titles
        * identifiers
        * agents
        * notes
        * relevant WEMI links

    It should return a display-neutral object. Human formatting belongs in
    surfaces.

Example: Calibre compatibility.

    Belongs partly in catalog:

        building a legacy-style metadata projection from WEMI records

    Belongs partly in databases:

        SQL views that expose stable compatibility shapes

    Belongs outside catalog:

        UI-specific column labels
        interactive display formatting
        import wizard behaviour

Design rules
------------

1. Keep databases boring.

   The database layer should be durable, generic, and predictable. It should
   not accumulate bibliographic policy.

2. Keep metadata pure.

   Metadata classes should describe meaning. They should not execute queries.

3. Keep catalog semantic.

   Catalog APIs should be named in terms of Works, Expressions,
   Manifestations, Items, Agents, Titles, Identifiers, Notes, matching,
   retrieval, and mutation.

4. Keep library workflow-oriented.

   If an operation sounds like a task a user asked the program to perform,
   it probably belongs in library, not catalog.

5. Keep surfaces out of catalog.

   Catalog projections should be display-neutral. Formatting for humans,
   terminal output, GUI tables, and reports belongs in surfaces.

6. Prefer explicit writes.

   Avoid lookup methods that secretly create records. Use clear names such as
   resolve_or_create_* when mutation may occur.

7. Prefer repositories over table classes.

   Repositories model domain concepts. Tables model storage details.
"""


from .catalog import Catalog

__all__ = ["Catalog"]
