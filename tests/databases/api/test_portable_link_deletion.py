"""Check validated link deletion on real SQLite and APSW macro transactions."""

import pytest

from LiuXin_alpha.errors import InputIntegrityError


@pytest.fixture
def links(db):
    db.driver_wrapper.executescript("""
        CREATE TABLE ownership_links (
            ownership_link_id INTEGER PRIMARY KEY,
            ownership_link_left_id INTEGER,
            ownership_link_right_id INTEGER,
            ownership_link_type TEXT
        );
        INSERT INTO ownership_links VALUES
            (1, 10, 20, 'author'), (2, 10, 20, 'editor'),
            (3, 11, 21, 'author'), (4, 12, 22, 'author');
    """)
    return db.macros


def remaining(macros):
    return [
        row["ownership_link_id"]
        for row in macros.get_rows("ownership_links", order_by=("ownership_link_id",))
    ]


@pytest.mark.parametrize("shape", ["scalars", "bindings", "generator"])
def test_delete_endpoint_batch_accepts_legacy_and_scalar_bindings(links, shape):
    values = {
        "scalars": [10, 11],
        "bindings": ((10,), (11,)),
        "generator": (value for value in [10, 11]),
    }[shape]
    links.delete_link_rows(
        "ownership_links", "ownership_link_left_id", values, "author"
    )
    assert remaining(links) == [2, 4]


def test_delete_link_pair_and_outer_rollback(links):
    with pytest.raises(RuntimeError):
        with links.transaction():
            links.delete_link_pair(
                "ownership_links",
                "ownership_link_left_id",
                "ownership_link_right_id",
                10,
                20,
            )
            links.delete_link_rows("ownership_links", "ownership_link_left_id", 11)
            assert remaining(links) == [4]
            raise RuntimeError("rollback")
    assert remaining(links) == [1, 2, 3, 4]
    links.delete_link_pair(
        "ownership_links", "ownership_link_left_id", "ownership_link_right_id", 10, 20
    )
    assert remaining(links) == [3, 4]


def test_link_deletion_validates_before_effects_and_binds_types(links):
    for invalid in ([10, "11"], [10, (11, 12)], [True], "10"):
        with pytest.raises(TypeError):
            links.delete_link_rows("ownership_links", "ownership_link_left_id", invalid)
        assert remaining(links) == [1, 2, 3, 4]
    with pytest.raises(InputIntegrityError):
        links.delete_link_rows("ownership_links", "missing_column", 10)
    with pytest.raises(InputIntegrityError):
        links.delete_link_rows(
            "ownership_links; DELETE FROM works", "ownership_link_left_id", 10
        )

    def broken_batch():
        yield 10
        raise RuntimeError("batch failed")

    with pytest.raises(RuntimeError):
        links.delete_link_rows(
            "ownership_links", "ownership_link_left_id", broken_batch()
        )
    links.delete_link_rows("ownership_links", "ownership_link_left_id", [], "author")
    links.delete_link_rows(
        "ownership_links", "ownership_link_left_id", 10, "author' OR 1=1 --"
    )
    assert remaining(links) == [1, 2, 3, 4]


def test_batch_execution_failure_rolls_back_earlier_deletions(links, db):
    db.driver_wrapper.executescript("""
        CREATE TRIGGER refuse_last_link BEFORE DELETE ON ownership_links
        WHEN OLD.ownership_link_id = 4
        BEGIN SELECT RAISE(ABORT, 'cannot remove final link'); END;
    """)
    with pytest.raises(Exception, match="cannot remove final link"):
        links.delete_link_rows("ownership_links", "ownership_link_left_id", [10, 12])
    assert remaining(links) == [1, 2, 3, 4]


def test_older_delete_macros_share_validation_and_transaction(links):
    with pytest.raises(RuntimeError):
        with links.transaction():
            links.delete_in_table("ownership_links", "ownership_link_type", "editor")
            links.bulk_delete_in_table(
                "ownership_links", "ownership_link_left_id", [(11,)]
            )
            links.bulk_delete_items_in_table_two_matching_cols(
                "ownership_links",
                "ownership_link_left_id",
                "ownership_link_type",
                [(12, "author")],
            )
            assert remaining(links) == [1]
            raise RuntimeError("rollback")
    assert remaining(links) == [1, 2, 3, 4]
    with pytest.raises(InputIntegrityError):
        links.delete_in_table("ownership_links", "bad_column", 10)
    with pytest.raises(ValueError):
        links.bulk_delete_items_in_table_two_matching_cols(
            "ownership_links",
            "ownership_link_left_id",
            "ownership_link_type",
            [(10, "author"), (12,)],
        )
    assert remaining(links) == [1, 2, 3, 4]
