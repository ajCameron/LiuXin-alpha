"""Exercise scoped duplicate inspection and the retired destructive fallback."""

from contextlib import closing
from types import SimpleNamespace

import pytest

from LiuXin_alpha.catalog.duplicates import DuplicateScanLimit, find_duplicates
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.core.factory import create_core
from LiuXin_alpha.errors import DatabaseIntegrityError


@pytest.mark.parametrize("backend", ("SQLite", "SQLite_apsw"))
def test_real_core_duplicate_inspection_is_scoped_bounded_and_read_only(
    tmp_path, backend
):
    if backend == "SQLite_apsw":
        pytest.importorskip("apsw")
    with create_core(
        database_path=tmp_path / "catalog.sqlite",
        db_type=backend,
        create=True,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as core:
        db, catalog = core.database, core.catalog
        ids = [
            catalog.works.create({"title": title})
            for title in (
                " Ｓｔｒａße ",
                "strasse",
                "strasse",
                "unique",
                None,
                "",
                "  ",
                "Café",
                "Cafe\u0301",
            )
        ]
        before = db.macros.get_rows("works")
        payload = {"table": "works", "column": "work_title"}
        result = core.query("maintenance.duplicates.find", payload)
        assert result["comparison"] == "identity"
        assert result["scanned_rows"] == len(ids)
        assert result["duplicates"] == [
            {"value": "strasse", "scope": {}, "row_ids": ids[:3]},
            {"value": "café", "scope": {}, "row_ids": ids[-2:]},
        ]
        exact = core.query(
            "maintenance.duplicates.find", {**payload, "comparison": "exact"}
        )
        assert exact["duplicates"] == [
            {"value": "strasse", "scope": {}, "row_ids": ids[1:3]}
        ]
        nocase = core.query(
            "maintenance.duplicates.find", {**payload, "comparison": "nocase"}
        )
        assert [group["row_ids"] for group in nocase["duplicates"]] == [
            ids[1:3],
            ids[-2:],
        ]
        filtered = core.query(
            "maintenance.duplicates.find",
            {**payload, "where": {"work_title": "strasse"}, "max_rows": 2},
        )
        assert filtered["scanned_rows"] == 2
        assert filtered["duplicates"] == exact["duplicates"]

        for overrides, code in (
            ({"comparison": "unsupported"}, "invalid_payload"),
            ({"max_rows": True}, "invalid_payload"),
            ({"max_rows": 100001}, "invalid_payload"),
            ({"max_rows": 1}, "scan_limit_exceeded"),
            ({"column": "missing"}, "invalid_payload"),
            ({"table": "titles", "column": "title"}, "invalid_payload"),
            ({"where": []}, "invalid_payload"),
        ):
            with pytest.raises(CoreHandlerError) as error:
                core.query("maintenance.duplicates.find", {**payload, **overrides})
            assert error.value.code == code

        parents = [
            catalog.genres.create({"name": name}) for name in ("Parent A", "Parent B")
        ]
        children = [
            catalog.genres.create({"name": "Shared", "parent_id": parent})
            for parent in parents
        ]
        for column in ("genre", "genre_phash"):
            for comparison in ("identity", "nocase", "exact"):
                scoped = find_duplicates(db, "genres", column, comparison=comparison)
                assert scoped["scope_columns"] == ["genre_parent_id"]
                assert scoped["duplicates"] == []
        assert all(catalog.genres.get(row_id) is not None for row_id in children)
        assert db.macros.get_rows("works") == before
        assert core.query("maintenance.status")["installed"] is False
        assert core.durable_jobs._thread is None
        assert core.durable_jobs._repository is None
        assert not list(tmp_path.glob("*.jobs.sqlite"))
        with closing(db.driver.get_connection()) as connection:
            with pytest.raises(Exception, match="no such function: TA_UPDATE"):
                connection.execute("SELECT TA_UPDATE(1)")


def test_persisted_identity_policy_and_null_scopes_control_groups():
    """Use stored declarations, including their removal, rather than defaults."""
    from LiuXin_alpha.databases.normalized_identities import NormalizedIdentitySpec
    from LiuXin_alpha.databases.schema_specs import (
        RelationKind,
        StorageColumnSpec,
        StorageTableSpec,
    )
    from LiuXin_alpha.metadata.value_policies import ColumnNormalizationProfile

    rows = (
        {"genre_id": 1, "genre": " A ", "genre_parent_id": None},
        {"genre_id": 2, "genre": "a", "genre_parent_id": None},
        {"genre_id": 3, "genre": "a", "genre_parent_id": 99},
    )
    declarations = [
        NormalizedIdentitySpec(
            "genres",
            "genre",
            "genre_phash",
            ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD,
            ("genre_parent_id",),
        )
    ]
    wrapper = SimpleNamespace(
        get_table_spec=lambda table: StorageTableSpec(
            table,
            RelationKind.TABLE,
            tuple(StorageColumnSpec(name, index) for index, name in enumerate(rows[0])),
            id_column="genre_id",
        ),
        iter_normalized_identity_specs=lambda **kwargs: iter(declarations),
    )
    db = SimpleNamespace(
        driver_wrapper=wrapper,
        macros=SimpleNamespace(get_rows_page=lambda *args, **kwargs: (rows, True)),
    )
    result = find_duplicates(db, "genres", "genre")
    assert result["duplicates"] == [
        {"value": "a", "scope": {"genre_parent_id": None}, "row_ids": [1, 2]}
    ]
    assert (
        result["normalization_profile"] == declarations[0].normalization_profile.value
    )
    declarations.clear()
    result = find_duplicates(db, "genres", "genre")
    assert result["scope_columns"] == []
    assert result["duplicates"][0]["row_ids"] == [1, 2, 3]
    db.macros.get_rows_page = lambda *args, **kwargs: (rows[:1], False)
    with pytest.raises(DuplicateScanLimit):
        find_duplicates(db, "genres", "genre", max_rows=1)


def test_ambiguous_legacy_genre_lookup_does_not_attempt_repair(monkeypatch):
    from LiuXin_alpha.catalog.metadata_tools import ensure

    candidates = [
        {"genre_id": 1, "genre": "Shared"},
        {"genre_id": 2, "genre": "Shared"},
    ]
    host = SimpleNamespace(db=SimpleNamespace(search=lambda **kwargs: candidates))
    monkeypatch.setattr(ensure, "ensure_value", lambda *args: None)
    with pytest.raises(
        DatabaseIntegrityError, match="automatic duplicate repair has been retired"
    ):
        ensure.Ensure.genre(host, "Shared", standardize=False)
    assert candidates == [
        {"genre_id": 1, "genre": "Shared"},
        {"genre_id": 2, "genre": "Shared"},
    ]
