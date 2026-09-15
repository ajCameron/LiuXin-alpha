"""
Verify Catalog repository mutation, identity, and relationship invariants.

Use the provisioned database for identifier ownership/copy/replacement, cache
synchronization, Work Unicode identity/lookup, alias rejection, and ordered-link
behavior. Narrow monkeypatch seams inject assignment failure and a missing linked
row; full-table snapshots or row counts distinguish rejection from partial writes.
Sequential reuse tests do not establish concurrent uniqueness guarantees.
"""

from __future__ import annotations

from types import SimpleNamespace
import unicodedata
import uuid

import pytest

from LiuXin_alpha.caches.write.identifiers_writer import IdentifiersWrite
from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.api import (
    CatalogAmbiguousMatchError,
    CatalogMutationError,
    CatalogNotFoundError,
    IdentifierCandidate,
    MetadataCandidate,
)


def _token(prefix: str) -> str:
    """
    Create a descriptive UUID-suffixed value to distinguish real database fixtures.

    Each call generates a fresh UUID4 string after the supplied prefix and a hyphen. The prefix is
    formatted directly; this helper performs no database lookup or absolute uniqueness check.

    Example:
        >>> value = _token("work")
        >>> value.startswith("work-")
        True
        >>> uuid.UUID(value[5:]).version
        4


    :param prefix: Readable fixture label prepended to the generated UUID text.
    :return: Prefix and fresh UUID4 joined by a hyphen.
    """
    return f"{prefix}-{uuid.uuid4()}"


def _identifier_rows(catalog: Catalog) -> tuple[dict[str, object], ...]:
    """
    Snapshot all curated identifier rows as shallow dictionaries in repository order.

    Materialize the private repository scan and copy each returned mapping again. Tests compare this
    tuple before and after rejected mutations to observe full identifier-table changes, including
    ownership and provenance fields.

    Example:
        >>> before = _identifier_rows(catalog)  # doctest: +SKIP


    :param catalog: Catalog whose curated identifier repository supplies the rows.
    :return: Tuple of shallow dictionaries retaining the repository row order.
    """
    return tuple(dict(row) for row in catalog.identifiers._all_rows())


def test_identifier_replacement_rejects_normalised_scheme_collisions_atomically(
    db,
) -> None:
    """
    Reject normalized scheme collisions and invalid ISBN replacement without changing identifiers.

    Seed one Work-owned UUID identifier and snapshot the whole identifier table. Replacement with
    DOI/doi duplicate scheme keys must raise CatalogMutationError; a later replacement containing a
    malformed ISBN must raise ValueError. Compare all identifier rows to the snapshot after each
    error. These pre-write rejection checks are separate from the injected mid-transaction rollback
    regression.

    Example:
        >>> test_identifier_replacement_rejects_normalised_scheme_collisions_atomically(db)  # doctest: +SKIP


    :param db: Provisioned schema-backed database used for real Catalog rows, links, and row-count assertions.
    :return: None after the stated invariant assertions pass.
    """

    catalog = Catalog(db)
    work_id = catalog.works.create({"title": _token("identifier-collision")})
    catalog.identifiers.replace_for_wemi(
        level="work",
        entity_id=work_id,
        identifiers={"uuid": str(uuid.uuid4())},
    )
    before = _identifier_rows(catalog)

    with pytest.raises(CatalogMutationError, match="duplicate normalized scheme"):
        catalog.identifiers.replace_for_wemi(
            level="work",
            entity_id=work_id,
            identifiers={
                "DOI": f"10.1234/{uuid.uuid4()}",
                "doi": f"10.5678/{uuid.uuid4()}",
            },
        )

    assert _identifier_rows(catalog) == before

    with pytest.raises(ValueError, match="invalid ISBN"):
        catalog.identifiers.replace_for_wemi(
            level="work",
            entity_id=work_id,
            identifiers={
                "doi": f"10.9012/{uuid.uuid4()}",
                "isbn13": "978-0-306-40615-8",
            },
        )

    assert _identifier_rows(catalog) == before


def test_identifier_match_or_create_reuses_normalised_logical_identity(db) -> None:
    """
    Reuse equivalent DOI input while retaining the first stored provenance.

    Match/create URL-prefixed and doi-prefixed case variants of one UUID-suffixed DOI, then assert
    both calls choose the same row and keep first-observation provenance. Confirm normalized find
    returns that row and an unrelated DOI returns None. This exercises logical reuse without
    assigning a WEMI owner.

    Example:
        >>> test_identifier_match_or_create_reuses_normalised_logical_identity(db)  # doctest: +SKIP


    :param db: Provisioned schema-backed database used for real Catalog rows, links, and row-count assertions.
    :return: None after the stated invariant assertions pass.
    """

    catalog = Catalog(db)
    suffix = str(uuid.uuid4())
    identifier_id = catalog.identifiers.match_or_create(
        IdentifierCandidate(
            "DOI",
            f"https://doi.org/10.1357/{suffix}",
            source="first-observation",
        )
    )
    repeated_id = catalog.identifiers.match_or_create(
        IdentifierCandidate(
            "doi",
            f"doi:10.1357/{suffix.upper()}",
            source="later-observation",
        )
    )

    assert repeated_id == identifier_id
    assert catalog.identifiers.require(identifier_id)[
        "entity_identifier_provenance"
    ] == "first-observation"
    assert catalog.identifiers.find(
        identifier_type="doi",
        value=f"10.1357/{suffix.upper()}",
    ) == catalog.identifiers.require(identifier_id)
    assert catalog.identifiers.find(
        identifier_type="doi",
        value=f"10.1357/{uuid.uuid4()}",
    ) is None


def test_identifier_replacement_rolls_back_a_mid_transaction_failure(
    db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Restore all identifier rows when the second replacement assignment fails.

    Seed a Work-owned UUID and snapshot the identifier table. Patch link_to_wemi so the first
    assignment executes normally and the second raises RuntimeError. Attempt a two-scheme
    replacement, assert exactly two assignment calls, and compare the entire table to its earlier
    snapshot. This observes rollback of the replacement workflow after work has begun, not just
    input rejection.

    Example:
        >>> test_identifier_replacement_rolls_back_a_mid_transaction_failure(db, monkeypatch)  # doctest: +SKIP


    :param db: Provisioned schema-backed database used for real Catalog rows, links, and row-count assertions.
    :param monkeypatch: Pytest fixture restoring the injected Catalog/macro failure seam after this test.
    :return: None after the stated invariant assertions pass.
    """

    catalog = Catalog(db)
    work_id = catalog.works.create({"title": _token("identifier-rollback")})
    catalog.identifiers.replace_for_wemi(
        level="work",
        entity_id=work_id,
        identifiers={"uuid": str(uuid.uuid4())},
    )
    before = _identifier_rows(catalog)
    original_link = catalog.identifiers.link_to_wemi
    call_count = 0

    def fail_second_assignment(
        *,
        identifier_id: int,
        level: str,
        entity_id: int,
        priority: int | None = None,
    ) -> int:
        """
        Count assignments and fail the second before invoking the real owner assignment.

        Increment the enclosing call_count first. Forward every supplied keyword to the captured
        original method on other calls, retaining the actual database side effects needed to
        exercise the enclosing replacement transaction.

        Example:
            >>> assigned = fail_second_assignment(identifier_id=identifier_id, level="work", entity_id=work_id)  # doctest: +SKIP


        :param identifier_id: Curated identifier row passed through to the original assignment.
        :param level: WEMI level forwarded to the original method without validation here.
        :param entity_id: Destination owner ID forwarded unchanged.
        :param priority: Optional primary/ordering hint forwarded unchanged.
        :return: The original assignment result on calls other than the second.
        :raises RuntimeError: On the second invocation, before the original method runs.
        :raises Exception: Original assignment failures propagate on forwarded calls.
        """
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("injected identifier assignment failure")
        return original_link(
            identifier_id=identifier_id,
            level=level,  # type: ignore[arg-type]
            entity_id=entity_id,
            priority=priority,
        )

    monkeypatch.setattr(
        catalog.identifiers,
        "link_to_wemi",
        fail_second_assignment,
    )

    with pytest.raises(RuntimeError, match="injected identifier assignment"):
        catalog.identifiers.replace_for_wemi(
            level="work",
            entity_id=work_id,
            identifiers={
                "doi": f"10.3456/{uuid.uuid4()}",
                "url": f"https://rollback.example/{uuid.uuid4()}",
            },
        )

    assert call_count == 2
    assert _identifier_rows(catalog) == before


def test_identifier_copy_and_replacement_preserve_other_owners(db) -> None:
    """
    Copy owned identifiers without changing another Work's later state.

    Assign one unowned UUID to two Works and verify the first reuses the original ID while the
    second gets a copy with equal scheme/value/provenance. Check primary flags from priorities one
    and zero. Replace only the first owner's identifiers with a DOI, then clear them and confirm the
    second owner's UUID row and attachment survive both operations.

    Example:
        >>> test_identifier_copy_and_replacement_preserve_other_owners(db)  # doctest: +SKIP


    :param db: Provisioned schema-backed database used for real Catalog rows, links, and row-count assertions.
    :return: None after the stated invariant assertions pass.
    """

    catalog = Catalog(db)
    first_work = catalog.works.create({"title": _token("identifier-owner-one")})
    second_work = catalog.works.create({"title": _token("identifier-owner-two")})
    shared_value = str(uuid.uuid4())
    original_id = catalog.identifiers.match_or_create(
        IdentifierCandidate("uuid", shared_value, source="owner-isolation-test")
    )
    first_id = catalog.identifiers.link_to_wemi(
        identifier_id=original_id,
        level="work",
        entity_id=first_work,
        priority=1,
    )
    second_id = catalog.identifiers.link_to_wemi(
        identifier_id=original_id,
        level="work",
        entity_id=second_work,
        priority=0,
    )

    assert first_id == original_id
    assert second_id != first_id
    first_row = catalog.identifiers.require(first_id)
    second_row = catalog.identifiers.require(second_id)
    assert second_row["entity_identifier_scheme"] == first_row[
        "entity_identifier_scheme"
    ]
    assert second_row["entity_identifier_value"] == first_row[
        "entity_identifier_value"
    ]
    assert second_row["entity_identifier_provenance"] == (
        first_row["entity_identifier_provenance"]
    ) == "owner-isolation-test"
    assert first_row["entity_identifier_is_primary"] == 0
    assert second_row["entity_identifier_is_primary"] == 1

    replacement = f"10.7890/{uuid.uuid4()}"
    assigned = catalog.identifiers.replace_for_wemi(
        level="work",
        entity_id=first_work,
        identifiers={"DOI": replacement},
    )

    assert tuple(assigned) == ("doi",)
    assert {
        (
            row["entity_identifier_scheme"],
            row["entity_identifier_value"],
        )
        for row in catalog.identifiers.list_for_wemi(
            level="work",
            entity_id=first_work,
        )
    } == {("doi", replacement)}
    assert catalog.identifiers.require(second_id)["entity_identifier_value"] == (
        shared_value
    )

    catalog.identifiers.replace_for_wemi(
        level="work",
        entity_id=first_work,
        identifiers={},
    )

    assert not catalog.identifiers.list_for_wemi(
        level="work",
        entity_id=first_work,
    )
    assert catalog.identifiers.list_for_wemi(
        level="work",
        entity_id=second_work,
    ) == (catalog.identifiers.require(second_id),)


def test_identifier_cache_writer_changes_cache_only_after_storage_succeeds(
    db,
) -> None:
    """
    Keep identifier cache maps aligned with rejected and successful storage replacement.

    Seed a DOI in real storage and build small forward/reverse cache-map stand-ins. An invalid ISBN
    update must leave both maps and stored identifiers unchanged. A successful UUID update reports
    the affected Work, replaces storage and the forward map, and retains the old DOI reverse key
    with an empty set while adding the UUID reverse membership.

    Example:
        >>> test_identifier_cache_writer_changes_cache_only_after_storage_succeeds(db)  # doctest: +SKIP


    :param db: Provisioned schema-backed database used for real Catalog rows, links, and row-count assertions.
    :return: None after the stated invariant assertions pass.
    """

    catalog = Catalog(db)
    work_id = catalog.works.create({"title": _token("identifier-cache")})
    old_value = f"10.2468/{uuid.uuid4()}"
    catalog.identifiers.replace_for_wemi(
        level="work",
        entity_id=work_id,
        identifiers={"doi": old_value},
    )
    table = SimpleNamespace(
        book_col_map={work_id: {"doi": old_value}},
        col_book_map={"doi": {work_id}},
    )
    field = SimpleNamespace(table=table)

    with pytest.raises(ValueError, match="invalid ISBN"):
        IdentifiersWrite.identifiers(
            {work_id: {"isbn13": "978-0-306-40615-8"}},
            db,
            field,
        )

    assert table.book_col_map == {work_id: {"doi": old_value}}
    assert table.col_book_map == {"doi": {work_id}}
    stored = catalog.identifiers.list_for_wemi(
        level="work",
        entity_id=work_id,
    )
    assert tuple(
        (row["entity_identifier_scheme"], row["entity_identifier_value"])
        for row in stored
    ) == (("doi", old_value),)

    new_value = str(uuid.uuid4())
    assert IdentifiersWrite.identifiers(
        {work_id: {"uuid": new_value}},
        db,
        field,
    ) == {work_id}

    assert table.book_col_map == {work_id: {"uuid": new_value}}
    assert table.col_book_map == {
        "doi": set(),
        "uuid": {work_id},
    }
    assert tuple(
        (row["entity_identifier_scheme"], row["entity_identifier_value"])
        for row in catalog.identifiers.list_for_wemi(
            level="work",
            entity_id=work_id,
        )
    ) == (("uuid", new_value),)


def test_work_match_or_create_reuses_one_unicode_equivalent_identity(db) -> None:
    """
    Create one Work across repeated Unicode-equivalent match/create requests.

    Use fullwidth text, combining accents, Japanese characters, whitespace, and case/decomposition
    variants with a unique fixture suffix. Assert both calls return one ID, the Work count grows by
    exactly one, and the originally supplied title text is preserved. The test covers sequential
    reuse, not concurrent uniqueness or an enclosing transaction guarantee.

    Example:
        >>> test_work_match_or_create_reuses_one_unicode_equivalent_identity(db)  # doctest: +SKIP


    :param db: Provisioned schema-backed database used for real Catalog rows, links, and row-count assertions.
    :return: None after the stated invariant assertions pass.
    """

    catalog = Catalog(db)
    suffix = _token("work-idempotency")
    stored = f"  Ｃａｆｅ\u0301 東京 {suffix}  "
    candidate = MetadataCandidate(
        {"title": unicodedata.normalize("NFKD", stored).swapcase()}
    )
    before = db.driver_wrapper.get_record_count("works")

    work_id = catalog.works.match_or_create(candidate)
    repeated_id = catalog.works.match_or_create(
        MetadataCandidate({"title": stored})
    )

    assert repeated_id == work_id
    assert db.driver_wrapper.get_record_count("works") == before + 1
    assert catalog.works.require(work_id)["work_title"] == candidate.data["title"]


def test_work_lookup_uses_canonical_titles_with_stable_unicode_limits(db) -> None:
    """
    Order preferred/canonical title matches consistently across Unicode spellings.

    Create three matching Works, including two with unrelated preferred titles and equivalent
    canonical titles. A limit of two must return the first two IDs; the unrestricted equivalent
    query must return all three in creation/ID order. This checks exact normalized lookup rather
    than fuzzy title policy.

    Example:
        >>> test_work_lookup_uses_canonical_titles_with_stable_unicode_limits(db)  # doctest: +SKIP


    :param db: Provisioned schema-backed database used for real Catalog rows, links, and row-count assertions.
    :return: None after the stated invariant assertions pass.
    """

    catalog = Catalog(db)
    suffix = _token("canonical-order")
    canonical = f"  Ｃａｆｅ\u0301 東京 {suffix}  "
    equivalent = unicodedata.normalize("NFKD", canonical).swapcase()
    first_id = catalog.works.create(
        {
            "title": _token("unrelated-preferred"),
            "canonical_title": canonical,
        }
    )
    second_id = catalog.works.create({"title": equivalent})
    third_id = catalog.works.create(
        {
            "title": _token("another-unrelated-preferred"),
            "canonical_title": equivalent,
        }
    )

    assert tuple(
        row["work_id"]
        for row in catalog.works.find_by_title(canonical, limit=2)
    ) == (first_id, second_id)
    assert tuple(
        row["work_id"]
        for row in catalog.works.find_by_title(equivalent)
    ) == (first_id, second_id, third_id)


def test_work_match_or_create_rejects_unicode_equivalent_ambiguity(db) -> None:
    """
    Preserve duplicate Works and reject creation when normalized identity is ambiguous.

    Create one preferred-title Work and one canonical-title Work with equivalent Unicode text.
    match_or_create must raise CatalogAmbiguousMatchError carrying both IDs in alternatives, and the
    Work row count must remain unchanged.

    Example:
        >>> test_work_match_or_create_rejects_unicode_equivalent_ambiguity(db)  # doctest: +SKIP


    :param db: Provisioned schema-backed database used for real Catalog rows, links, and row-count assertions.
    :return: None after the stated invariant assertions pass.
    """

    catalog = Catalog(db)
    suffix = _token("work-ambiguity")
    title = f"Ｃａｆｅ\u0301 — 東京 {suffix}"
    equivalent = unicodedata.normalize("NFKD", title).swapcase()
    first_id = catalog.works.create({"title": title})
    second_id = catalog.works.create({"canonical_title": equivalent})
    before = db.driver_wrapper.get_record_count("works")

    with pytest.raises(CatalogAmbiguousMatchError) as raised:
        catalog.works.match_or_create(
            MetadataCandidate({"title": equivalent})
        )

    assert raised.value.result.alternatives == (first_id, second_id)
    assert db.driver_wrapper.get_record_count("works") == before


def test_repository_alias_conflicts_are_rejected_without_writes(db) -> None:
    """
    Reject unequal public/storage values for one Work column before insertion.

    Supply distinct title and work_title values, require the conflict-specific CatalogMutationError,
    and compare the Work record count before and after. The assertion observes no new Work; equal
    duplicate aliases are not exercised by this particular regression.

    Example:
        >>> test_repository_alias_conflicts_are_rejected_without_writes(db)  # doctest: +SKIP


    :param db: Provisioned schema-backed database used for real Catalog rows, links, and row-count assertions.
    :return: None after the stated invariant assertions pass.
    """

    catalog = Catalog(db)
    before = db.driver_wrapper.get_record_count("works")

    with pytest.raises(CatalogMutationError, match="conflicting values"):
        catalog.works.create(
            {
                "title": _token("public-title"),
                "work_title": _token("storage-title"),
            }
        )

    assert db.driver_wrapper.get_record_count("works") == before


def test_repeated_ordered_link_retains_existing_priority(db) -> None:
    """
    Preserve priority when an existing Work/Agent credit is linked again.

    Create a person and Work, link the author role at priority seven, then repeat the same role
    without a priority. Assert traversal returns one credit and its _catalog_link priority remains
    seven. This distinguishes preserving an existing ordered link from allocating a new default
    priority.

    Example:
        >>> test_repeated_ordered_link_retains_existing_priority(db)  # doctest: +SKIP


    :param db: Provisioned schema-backed database used for real Catalog rows, links, and row-count assertions.
    :return: None after the stated invariant assertions pass.
    """

    catalog = Catalog(db)
    work_id = catalog.works.create({"title": _token("ordered-link")})
    agent_id = catalog.agents.create_person({"name": _token("ordered-agent")})
    catalog.agents.link_to_wemi(
        agent_id=agent_id,
        level="work",
        entity_id=work_id,
        role="aut",
        priority=7,
    )

    catalog.agents.link_to_wemi(
        agent_id=agent_id,
        level="work",
        entity_id=work_id,
        role="aut",
    )

    linked = catalog.agents.list_for_wemi(
        level="work",
        entity_id=work_id,
    )
    assert len(linked) == 1
    assert linked[0]["_catalog_link"]["priority"] == 7


def test_dangling_catalog_link_raises_a_catalog_not_found_error(
    db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Report a linked destination hidden by the read seam as a Catalog not-found error.

    Create a real Work/Agent author link, then patch get_row to hide just the linked Agent while
    forwarding all other reads. Traversal must raise an error naming the source relationship instead
    of silently dropping the destination. The row is not physically deleted and the test does not
    manufacture persistent corruption.

    Example:
        >>> test_dangling_catalog_link_raises_a_catalog_not_found_error(db, monkeypatch)  # doctest: +SKIP


    :param db: Provisioned schema-backed database used for real Catalog rows, links, and row-count assertions.
    :param monkeypatch: Pytest fixture restoring the injected Catalog/macro failure seam after this test.
    :return: None after the stated invariant assertions pass.
    """

    catalog = Catalog(db)
    work_id = catalog.works.create({"title": _token("dangling-link")})
    agent_id = catalog.agents.create_person({"name": _token("dangling-agent")})
    catalog.agents.link_to_wemi(
        agent_id=agent_id,
        level="work",
        entity_id=work_id,
        role="aut",
    )
    macros = catalog.agents._macros
    original_get_row = macros.get_row

    def hide_linked_agent(
        table: str,
        row_id: int,
        *,
        id_column: str | None = None,
    ):
        """
        Hide the test's Agent row while preserving all unrelated macro reads.

        Match both the agents table and the captured Agent ID. Return None only for that pair;
        otherwise delegate with the optional ID-column keyword unchanged. This simulates a dangling
        read without deleting database state.

        Example:
            >>> hide_linked_agent("agents", agent_id) is None  # doctest: +SKIP
            True


        :param table: Storage table requested by Catalog traversal.
        :param row_id: Requested row identifier compared with the captured Agent ID.
        :param id_column: Optional explicit ID column forwarded on non-hidden reads.
        :return: None for the hidden Agent, otherwise the original macro read result.
        """
        if table == "agents" and row_id == agent_id:
            return None
        return original_get_row(table, row_id, id_column=id_column)

    monkeypatch.setattr(macros, "get_row", hide_linked_agent)

    with pytest.raises(CatalogNotFoundError, match="linked from works"):
        catalog.agents.list_for_wemi(
            level="work",
            entity_id=work_id,
        )
