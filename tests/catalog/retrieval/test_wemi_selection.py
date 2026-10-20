"""Compare Catalog preferred paths with real generated SQLite WEMI views."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
    database_generator,
)


@pytest.fixture
def graph(tmp_path: Path, driver_spec) -> Iterator[tuple[Catalog, sqlite3.Connection]]:
    """Open an empty current schema through the configured SQLite driver."""
    path = tmp_path / "selection.sqlite"
    connection = sqlite3.connect(path)
    database_generator.create_new_database(connection)
    connection.execute("PRAGMA foreign_keys = ON")
    database = Database(
        metadata={"database_path": str(path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    )
    try:
        yield Catalog(database), connection
    finally:
        database.close()
        connection.close()


def _seed(conn: sqlite3.Connection) -> None:
    """Create one Work, two translations and three editions with competing ranks."""
    conn.execute("INSERT INTO works (work_id, work_title) VALUES (1, 'Novel')")
    conn.executemany(
        "INSERT INTO expressions (expression_id, expression_label) VALUES (?, ?)",
        ((11, "English"), (12, "French")),
    )
    conn.executemany(
        "INSERT INTO manifestations (manifestation_id, manifestation_subtitle) "
        "VALUES (?, ?)",
        ((101, "English EPUB"), (102, "English print"), (103, "French EPUB")),
    )
    conn.executemany(
        "INSERT INTO expression_work_links (expression_work_link_work_id, "
        "expression_work_link_expression_id, expression_work_link_primary, "
        "expression_work_link_priority) VALUES (1, ?, ?, ?)",
        ((11, 1, 0), (12, 0, 10)),
    )
    conn.executemany(
        "INSERT INTO expression_manifestation_links "
        "(expression_manifestation_link_expression_id, "
        "expression_manifestation_link_manifestation_id, "
        "expression_manifestation_link_primary, "
        "expression_manifestation_link_priority) VALUES (?, ?, ?, ?)",
        ((11, 101, 1, 0), (11, 102, 0, 10), (12, 103, 1, 0)),
    )
    conn.executemany(
        "INSERT INTO items (item_id, item_manifestation_id) VALUES (?, ?)",
        ((1002, 101), (1001, 101), (1003, 103)),
    )
    conn.commit()


def _ids(bundle) -> tuple[int | None, ...]:
    """Read only selected identities, keeping missing levels explicit."""
    return tuple(
        None if row is None else row[f"{level}_id"]
        for level, row in (
            ("work", bundle.work),
            ("expression", bundle.expression),
            ("manifestation", bundle.manifestation),
            ("item", bundle.item),
        )
    )


@pytest.mark.parametrize(
    ("updates", "expected"),
    (
        ("", (1, 11, 101, 1001)),
        (
            "UPDATE expression_work_links SET expression_work_link_primary = 1",
            (1, 11, 101, 1001),
        ),
        (
            "UPDATE expression_work_links SET expression_work_link_primary = NULL, "
            "expression_work_link_priority = NULL; "
            "UPDATE expression_manifestation_links "
            "SET expression_manifestation_link_primary = NULL, "
            "expression_manifestation_link_priority = NULL",
            (1, 11, 101, 1001),
        ),
        (
            "UPDATE expression_work_links SET expression_work_link_primary = 0; "
            "UPDATE expression_work_links SET expression_work_link_priority = -10 "
            "WHERE expression_work_link_expression_id = 12",
            (1, 12, 103, 1003),
        ),
        (
            # A downstream primary breaks a tied Work edge before Expression ID.
            "UPDATE expression_work_links SET expression_work_link_primary = NULL, "
            "expression_work_link_priority = NULL; "
            "UPDATE expression_manifestation_links "
            "SET expression_manifestation_link_primary = 0 "
            "WHERE expression_manifestation_link_expression_id = 11",
            (1, 12, 103, 1003),
        ),
        (
            # Preferred translation is incomplete; choose an existing complete ray.
            "DELETE FROM expression_manifestation_links "
            "WHERE expression_manifestation_link_expression_id = 11",
            (1, 12, 103, 1003),
        ),
        (
            # Item availability does not override bibliographic preference.
            "DELETE FROM items WHERE item_manifestation_id = 101",
            (1, 11, 101, None),
        ),
    ),
)
def test_work_bundle_matches_sql_primary_ray(graph, updates, expected) -> None:
    """Primary flags, nulls, priorities, ties and missing Items agree with SQL."""
    catalog, conn = graph
    _seed(conn)
    conn.executescript(updates)
    conn.commit()
    before = (conn.total_changes, conn.execute("PRAGMA data_version").fetchone())
    sql = conn.execute(
        "SELECT work_id, expression_id, manifestation_id "
        "FROM wemi_primary_rays_v WHERE work_id = 1"
    ).fetchone()
    assert sql == expected[:3]
    assert _ids(catalog.retrieval.bundles.for_work(1)) == expected
    assert (
        conn.total_changes,
        conn.execute("PRAGMA data_version").fetchone(),
    ) == before


def test_partial_bundles_keep_requested_owner_and_available_levels(graph) -> None:
    """An absent complete SQL ray does not discard the requested Catalog owner."""
    catalog, conn = graph
    _seed(conn)
    conn.execute("DELETE FROM expression_manifestation_links")
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM wemi_primary_rays_v").fetchone() == (0,)
    assert _ids(catalog.retrieval.bundles.for_work(1)) == (1, 11, None, None)
    assert _ids(catalog.retrieval.bundles.for_expression(11)) == (1, 11, None, None)
    assert _ids(catalog.retrieval.bundles.for_manifestation(101)) == (
        None,
        None,
        101,
        1001,
    )
    assert _ids(catalog.retrieval.bundles.for_item(1001)) == (None, None, 101, 1001)
    conn.execute("DELETE FROM expression_work_links")
    conn.execute("UPDATE items SET item_manifestation_id = NULL WHERE item_id = 1001")
    conn.commit()
    assert _ids(catalog.retrieval.bundles.for_work(1)) == (1, None, None, None)
    assert _ids(catalog.retrieval.bundles.for_expression(11)) == (None, 11, None, None)
    assert _ids(catalog.retrieval.bundles.for_item(1001)) == (None, None, None, 1001)


def test_introduction_and_novel_share_an_edition_without_losing_either_work(
    graph,
) -> None:
    """Default projection chooses one path; list/SQL membership retains both Works."""
    catalog, conn = graph
    _seed(conn)
    conn.execute("INSERT INTO works (work_id, work_title) VALUES (2, 'Introduction')")
    conn.execute(
        "INSERT INTO expressions (expression_id, expression_label) VALUES (13, 'Intro')"
    )
    conn.execute(
        "INSERT INTO expression_work_links (expression_work_link_work_id, "
        "expression_work_link_expression_id, expression_work_link_primary, "
        "expression_work_link_priority) VALUES (2, 13, 1, 0)"
    )
    conn.execute(
        "INSERT INTO expression_manifestation_links "
        "(expression_manifestation_link_expression_id, "
        "expression_manifestation_link_manifestation_id, "
        "expression_manifestation_link_primary, "
        "expression_manifestation_link_priority) VALUES (13, 101, 1, 5)"
    )
    conn.commit()
    for operation, owner in (
        (catalog.retrieval.bundles.for_expression, 11),
        (catalog.retrieval.bundles.for_manifestation, 101),
        (catalog.retrieval.bundles.for_item, 1001),
    ):
        assert _ids(operation(owner)) == (1, 11, 101, 1001)
    assert _ids(catalog.retrieval.bundles.for_work(2)) == (2, 13, 101, 1001)
    assert catalog.retrieval.projections.item_summary(1001)["work_id"] == 1
    assert {
        row["expression_id"]
        for row in catalog.retrieval.hierarchy.parents(
            level="manifestation", entity_id=101
        ).entities
    } == {11, 13}
    assert conn.execute(
        "SELECT work_id, expression_id, manifestation_id FROM wemi_rays_v "
        "WHERE manifestation_id = 101 ORDER BY work_id"
    ).fetchall() == [(1, 11, 101), (2, 13, 101)]
    # Repository ordering is a list contract, independent of preferred selection.
    assert [row["expression_id"] for row in catalog.expressions.list_for_work(1)] == [
        12,
        11,
    ]
    # Passive metadata retains every related Work. Its primary relation accessor
    # answers an edge-local question, not the complete-ray projection above.
    from LiuXin_alpha.catalog.retrieval.hydration.item_metadata_hydrator import (
        ItemMetadataHydrator,
    )

    conn.execute(
        "UPDATE expression_work_links SET expression_work_link_priority = 10 "
        "WHERE expression_work_link_work_id = 2"
    )
    conn.execute(
        "UPDATE expression_manifestation_links "
        "SET expression_manifestation_link_priority = -5 "
        "WHERE expression_manifestation_link_expression_id = 13"
    )
    conn.commit()
    metadata = ItemMetadataHydrator(catalog.db).from_item_id(1001)
    assert {link.target.row_id for link in metadata.get_relation_links("works")} == {
        1,
        2,
    }
    assert metadata.primary_expression_id == 13
    assert _ids(catalog.retrieval.bundles.for_item(1001)) == (1, 11, 101, 1001)


def test_shared_expression_uses_link_ranks_and_work_id_tie_breaker(graph) -> None:
    """Reverse traversal uses the same WEM ranking instead of list order."""
    catalog, conn = graph
    _seed(conn)
    conn.execute("INSERT INTO works (work_id, work_title) VALUES (2, 'Collection')")
    conn.execute(
        "INSERT INTO expression_work_links (expression_work_link_work_id, "
        "expression_work_link_expression_id, expression_work_link_primary, "
        "expression_work_link_priority) VALUES (2, 11, 1, 5)"
    )
    conn.commit()
    assert _ids(catalog.retrieval.bundles.for_expression(11)) == (1, 11, 101, 1001)
    conn.execute(
        "UPDATE expression_work_links SET expression_work_link_priority = NULL "
        "WHERE expression_work_link_expression_id = 11"
    )
    conn.commit()
    assert _ids(catalog.retrieval.bundles.for_item(1001)) == (1, 11, 101, 1001)


def test_worked_bilingual_introduction_and_omnibus_outputs(graph) -> None:
    """Execute the documented graph, primary change and unlink replacement policy."""
    catalog, conn = graph
    _seed(conn)
    conn.execute(
        "UPDATE items SET item_manifestation_id = 102 WHERE item_id IN (1002, 1003)"
    )
    conn.executemany(
        "INSERT INTO works (work_id, work_title) VALUES (?, ?)",
        (
            (2, "Collection"),
            (3, "Winter Orchard"),
            (4, "Introduction"),
            (5, "Summer Road"),
        ),
    )
    conn.executemany(
        "INSERT INTO expressions (expression_id, expression_label) VALUES (?, ?)",
        ((13, "Orchard text"), (14, "Intro text"), (15, "Road text")),
    )
    conn.executemany(
        "INSERT INTO manifestations (manifestation_id) VALUES (?)",
        ((104,), (105,), (106,)),
    )
    conn.executemany(
        "INSERT INTO items (item_id, item_manifestation_id) VALUES (?, ?)",
        ((1004, 104), (1005, 105), (1006, 106)),
    )
    conn.executemany(
        "INSERT INTO expression_work_links (expression_work_link_work_id, "
        "expression_work_link_expression_id, expression_work_link_primary, "
        "expression_work_link_priority) VALUES (?, ?, 1, 0)",
        ((2, 11), (3, 13), (4, 14), (5, 15)),
    )
    conn.executemany(
        "INSERT INTO expression_manifestation_links "
        "(expression_manifestation_link_expression_id, "
        "expression_manifestation_link_manifestation_id, "
        "expression_manifestation_link_primary, "
        "expression_manifestation_link_priority) VALUES (?, ?, ?, ?)",
        (
            (11, 104, 0, 20),
            (12, 104, 0, 10),
            (13, 105, 1, 0),
            (14, 105, 1, 0),
            (13, 106, 0, 10),
            (15, 106, 1, 0),
        ),
    )
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM wemi_rays_v").fetchone() == (12,)
    assert _ids(catalog.retrieval.bundles.for_work(1)) == (1, 11, 101, 1001)
    assert _ids(catalog.retrieval.bundles.for_item(1005)) == (3, 13, 105, 1005)
    assert _ids(catalog.retrieval.bundles.for_item(1006)) == (5, 15, 106, 1006)
    assert conn.execute(
        "SELECT work_id, expression_id, manifestation_id FROM wemi_rays_v "
        "WHERE manifestation_id IN (105, 106) ORDER BY manifestation_id, work_id"
    ).fetchall() == [(3, 13, 105), (4, 14, 105), (3, 13, 106), (5, 15, 106)]
    catalog.mutations.writer.link_wemi(
        parent_level="work",
        parent_id=1,
        child_level="expression",
        child_id=12,
        primary=True,
        priority=10,
        origin="curation",
    )
    assert _ids(catalog.retrieval.bundles.for_work(1)) == (1, 12, 103, None)
    assert conn.execute(
        "SELECT ray_id FROM wemi_primary_rays_v WHERE work_id = 1"
    ).fetchone() == ("1:12:103",)
    assert conn.execute("SELECT COUNT(*) FROM wemi_rays_v").fetchone() == (12,)
    catalog.mutations.writer.unlink_wemi(
        parent_level="work",
        parent_id=1,
        child_level="expression",
        child_id=12,
    )
    assert _ids(catalog.retrieval.bundles.for_work(1)) == (1, 11, 101, 1001)
    assert conn.execute(
        "SELECT expression_work_link_primary FROM expression_work_links "
        "WHERE expression_work_link_work_id = 1"
    ).fetchone() == (0,)
