"""Language seed plans preserve imported identities and reject conflicting aliases."""

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.language_upgrade import (
    plan_rows,
)
from LiuXin_alpha.databases.language_constants import language_rows


def test_seed_is_complete_deterministic_and_idempotent():
    rows = language_rows()
    assert rows == language_rows()
    assert len(rows) > 400
    assert len({row[0] for row in rows}) == len(rows)
    assert len({row[4] for row in rows}) == len(rows)
    assert plan_rows(rows, "library") == ((), ())
    statements, blockers = plan_rows((), "library")
    assert len(statements) == len(rows) + 1
    assert not blockers


def test_existing_ids_and_names_are_preserved():
    english = next(row for row in language_rows() if row[4] == "eng")
    old = (9000, "User display name", *english[2:])
    statements, blockers = plan_rows([old], "library")
    assert not blockers
    assert all("'eng'" not in sql for sql in statements)
    assert not any("UPDATE" in sql or "DELETE" in sql for sql in statements)


def test_collision_uses_fresh_id_without_renumbering():
    rows = language_rows()
    old = (rows[0][0], *rows[-1][1:])
    statements, blockers = plan_rows([old], "library")
    assert not blockers
    assert f"VALUES ({max(row[0] for row in rows) + 1}," in statements[0]


def test_conflicting_alias_requires_review():
    english = next(row for row in language_rows() if row[4] == "eng")
    wrong = (*english[:2], "wrong", *english[3:])
    _, blockers = plan_rows([wrong], "library")
    assert any("eng" in blocker for blocker in blockers)
