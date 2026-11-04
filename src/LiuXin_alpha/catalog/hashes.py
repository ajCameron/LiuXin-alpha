
"""
Build legacy relationship fingerprints from linked table names and row IDs.

Fingerprints are sets of table_id strings, not content hashes. They ignore row text,
link ordering and link properties, and skip many unavailable relation queries. Equal
fingerprints therefore do not establish metadata equality. The helpers retain legacy
books/titles naming for compatibility.

Being metadata aware, this must be stored in the Catalog.

Design notes — 2026-10-28
-------------------------
The following describes a proposed replacement. The functions below still implement
the legacy behaviour; moving this file has not made them WEMI-aware.

Observed limitations
    Current schemas store relationships against Works, Expressions, Manifestations
    and Items. Looking for title_* or book_* link tables misses those relationships:
    two different Works, only one tagged, both produced an empty set in review probes.
    Projected book_id values such as "1:1:1" identify rays; book_work_id identifies
    their Work. The current fallback incorrectly looks up the ray ID in titles.

    Title-family traversal expects endpoint Rows from get_intralinked_rows(), which
    currently returns link Rows. Broad exception handlers also turn failed reads
    into incomplete sets indistinguishable from successful empty results. Existing
    tests check set types, repeatability and duplicate-implementation parity without
    asserting the expected relationships. These are correctness issues to resolve
    before using the result for change detection or matching.

Give each purpose its own contract
    A catalog snapshot fingerprint should answer whether the selected metadata and
    relationships of one identified subject have changed. It belongs to a specific
    catalog identity namespace, WEMI level, entity ID and versioned selection profile.
    Equality means equality of that selected representation, subject to digest
    collisions; it says nothing about fields outside the profile.

    Matching signatures should instead select possible equivalent descriptions across
    catalogs. They must exclude local row IDs, reuse existing identifier/text policies
    and retain the evidence used. Missing data and shared names cause ambiguity;
    signature equality alone must never authorize a merge, overwrite or deletion.
    Catalog's matching policy should make the identity decision.

    File-byte checksums belong to storage's asset-verification contract. A metadata
    fingerprint can reference a verified asset checksum when its profile requires
    one, but metadata equality cannot establish that two files have identical bytes.

Select a WEMI subject and a versioned profile
    Accept an explicit entity reference such as ("manifestation", 42). A ray reference
    should explicitly name its Work, Expression and Manifestation. Resolve legacy
    projections at the compatibility boundary; never infer a Work ID from book_id.

    Each named profile must define its field allowlist, traversed edge kinds and
    directions, depth, ordering rules and normalization rules. Description-only and
    description-with-holdings profiles can make different deliberate selections.
    Exclude operational timestamps, scratch fields and storage locations unless the
    requested purpose actually depends on them. Changes to these rules need a new
    profile version, rather than silently changing existing persisted digests.

    Preserve the graph: a Manifestation can embody an introduction and a novel through
    different Expressions and Works. A whole-manifestation profile must retain both
    paths and their roles/sequence; selecting its primary ray loses that information.
    Changing the introduction should affect that profile, but need not affect a
    profile rooted at the novel Work which excludes sibling Works. A newly acquired
    Item should affect a holdings profile, while leaving a description-only profile
    unchanged. Shared nodes, repeated edges and cycles need explicit handling.

Build a canonical, explainable representation before hashing
    For an initial local snapshot profile, use typed node references and explicit
    edge records. Include selected metadata values, relationship type/role, meaningful
    ordering and selected link properties. Sort unordered collections deterministically;
    preserve meaningful sequences and multiplicity instead of flattening them to sets.
    Include the identity namespace, root reference and profile/version in the envelope.

    Specify canonical bytes, including field names, type distinctions and framing.
    Preserve NULL versus empty text and the literal string "None"; define numeric,
    timestamp and Unicode handling explicitly. Change detection should preserve display
    text unless the profile deliberately defines a normalized equivalence. Unsupported
    values should fail validation, rather than falling back to repr() or str().
    A fixed-output digest such as SHA-256 can summarize those bytes. Its algorithm and
    encoding version must accompany the value; choosing a hash cannot repair an
    incomplete or ambiguous representation.

    Proposed API shape, not implemented here:
        catalog.fingerprints.snapshot(
            subject=("manifestation", 42),
            profile="manifestation-description-v1",
        )

    Return an immutable typed result carrying the subject/namespace, profile version,
    encoding version, algorithm and digest. Offer canonical payload/diff diagnostics
    so callers can see which selected fields or edges changed. Comparisons must check
    compatible profiles and namespaces before comparing digests. Hash collisions
    remain possible; consequential equality decisions should compare the payloads.

Read consistently and report incomplete work
    Every contributing read must share one connection and a verified snapshot across
    both backend families. An ordinary multi-query transaction is not automatically a
    consistent snapshot on every backend. Existing retrieval.graph.for_work() explicitly
    lacks a shared snapshot and limits output rather than all database reads; wrapping
    that result in a digest would not solve either limitation.

    Missing required entities, query failures, unsupported schemas and exceeded
    traversal limits should fail the fingerprint operation. A diagnostic partial
    traversal may report omissions, but must not expose an ordinary complete digest.
    Bound queries and traversal as well as output, detect cycles, and retain all
    selected edges when deduplicating shared nodes. Start without a persistent cache;
    later caching requires invalidation for every selected node and relationship,
    including writes through lower-level compatibility paths.

Ownership, migration and evidence
    Keep one implementation under Catalog. Consolidate metadata_tools.fingerprints
    with this module and migrate the remaining legacy book-creation caller. If old
    entry points are retained, make them explicit legacy adapters with documented
    input/schema limits. Changing their swallowed-error behaviour is a compatibility
    change to test and document. Database table digests and generic linked-row queries
    can remain independent primitives without importing this metadata-aware service.

    Implement one narrow snapshot profile first, with inspectable canonical payloads.
    Add fixed expected vectors, rather than parity between duplicated algorithms. Test
    selected-field and edge changes, deliberate exclusions, ordered versus unordered
    relationships, NULL/text distinctions, multi-Work Manifestations, shared nodes,
    cycles, missing entities, failed reads and truncation. Verify one coherent snapshot
    under concurrent edits and identical canonical bytes across supported backends.
    For any later matching signature, also test row-ID renumbering, normalization,
    missing identifiers and conflicting evidence without accidental automatic merges.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Union

from copy import deepcopy

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.row_api import RowAPI


# Todo: There has to be better ways to do this
def _row_value(row: Union[dict, "RowAPI"], key: str, default: Optional[Any] = None) -> Any:
    """
    Read a mapping-like row value with a fallback for unsupported access.

    Dictionaries use get directly. Other rows are probed with membership before
    subscription; any exception in that probe is swallowed. A dict subclass whose get
    raises is not covered by that handler.

    Example:
        >>> _row_value({"book_id": 7}, "book_id")
        7
        >>> _row_value(object(), "book_id", "missing")
        'missing'


    :param row: Dictionary or row supporting membership and item lookup.
    :param key: Column name to look up.
    :param default: Fallback when the column is absent or non-dict access fails.
    :return: Stored value, including None, or default.
    """
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        if key in row:
            return row[key]
        return default
    except Exception:
        return default


# Todo: In general, these are not relevant anymore - as we're working on WEMI principles.
def generate_book_fingerprint(db: "DatabaseAPI", book_row: "RowAPI") -> set[str]:
    """
    Union a book’s legacy title fingerprint with its other direct links.

    Resolve the title in titles using book_title, falling back to book_id only when
    book_title is None. If no title resolves, start empty. Then inspect book links to
    main tables except books and titles. Missing link tables and relation-query
    exceptions are skipped. Title lookup, row-ID formatting and errors during lazy
    linked-row iteration can still propagate. Neither the book ID nor its text is
    included by itself.

    Example:
        fingerprint = generate_book_fingerprint(db, book_row)
        related_ids = sorted(fingerprint)


    :param db: Database exposing main_tables, driver_wrapper relation discovery and
        row/link queries.
    :param book_row: Book row with book_title or, when that value is None, a book_id
        fallback.
    :return: Set of table_id strings from the resolved title group and direct book
        relations.
    """
    title_id = _row_value(book_row, "book_title", None)
    if title_id is None:

        # In FRBR-era schemas, books can be keyed directly by book_id/title_id.
        title_id = _row_value(book_row, "book_id", None)

    title_row = db.get_row_from_id("titles", title_id) if title_id is not None else None
    if title_row is not None:
        fingerprint = generate_title_fingerprint(db=db, title_row=title_row)
    else:
        fingerprint = set()

    # Include all other main tables in the fingerprint
    main_tables = set(deepcopy(db.main_tables))
    main_tables.discard("books")
    main_tables.discard("titles")
    for table in main_tables:
        base_print = deepcopy(table) + "_{}"
        try:
            if not db.driver_wrapper.get_link_table_name("books", table):
                continue
            linked_rows = db.get_interlinked_rows(primary_row=book_row, secondary_table=table)
        except Exception:
            continue
        for row in linked_rows:
            fingerprint.add(base_print.format(row.row_id))

    return fingerprint


def generate_title_fingerprint(db: "DatabaseAPI", title_row: "RowAPI") -> set[str]:
    """
    Union direct-link fingerprints for a title and its immediate intralink neighbours.

    Include the supplied title first. If the schema exposes title intralinks, inspect
    both outgoing and incoming neighbours, without recursively walking further titles.
    Each directional neighbour loop suppresses exceptions, retaining contributions added
    before failure. The initial title fingerprint and intralink-table check can still
    fail. A neighbour title ID itself is not automatically included.

    Example:
        fingerprint = generate_title_fingerprint(db, title_row)
        unchanged_links = fingerprint == previous_fingerprint


    :param db: Database exposing title relation discovery and link queries.
    :param title_row: Title row whose direct relations and immediate neighbours are
        inspected.
    :return: Set of table_id strings accumulated across the title and its immediate
        neighbours.
    """
    fingerprint = set()

    # generate the title fingerprint and add it
    fingerprint = fingerprint.union(generate_one_title_fingerprint(db=db, title_row=title_row))

    # Only query title intralinks when the schema exposes that relation.
    if db.driver_wrapper.check_for_intralink_table("titles"):
        # Match the title as primary row
        try:
            for p_title_row in db.get_intralinked_rows(primary_row=title_row, secondary_row=None):
                fingerprint = fingerprint.union(generate_one_title_fingerprint(db=db, title_row=p_title_row))
        except Exception:
            pass

        # Match the title as secondary rows
        try:
            for s_title_row in db.get_intralinked_rows(primary_row=None, secondary_row=title_row):
                fingerprint = fingerprint.union(generate_one_title_fingerprint(db=db, title_row=s_title_row))
        except Exception:
            pass

    return fingerprint


def generate_one_title_fingerprint(db: "DatabaseAPI", title_row: "RowAPI") -> set[str]:
    """
    Collect IDs linked directly to one title across the other main tables.

    Exclude titles, but include books when that relation exists. Skip missing link
    tables and exceptions while discovering or requesting each relation. Iteration and
    row_id access occur outside that handler. The title’s own ID, metadata values,
    priorities and link types are not fingerprint components.

    Example:
        fingerprint = generate_one_title_fingerprint(db, title_row) linked_agents =
        sorted(value for value in fingerprint if value.startswith("agents_"))


    :param db: Database whose main_tables and driver_wrapper describe available
        relations.
    :param title_row: Title row passed as the primary row to get_interlinked_rows.
    :return: Set of table_id strings, deduplicated across retrieved linked rows.
    """
    fp = set()

    main_tables = set(deepcopy(db.main_tables))
    main_tables.discard("titles")

    for table in main_tables:
        base_print = deepcopy(table) + "_{}"
        try:
            if not db.driver_wrapper.get_link_table_name("titles", table):
                continue
            linked_rows = db.get_interlinked_rows(primary_row=title_row, secondary_table=table)
        except Exception:
            continue
        for row in linked_rows:
            fp.add(base_print.format(row.row_id))

    return fp
