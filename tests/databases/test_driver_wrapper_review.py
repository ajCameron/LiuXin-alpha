"""Regressions for wrapper transaction ownership and physical-schema discovery."""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

from LiuXin_alpha.databases.driver_wrapper import DriverWrapper
from LiuXin_alpha.databases.schema_specs import RelationKind
from LiuXin_alpha.databases.transactions import transaction_active
from LiuXin_alpha.errors import InputIntegrityError
from tests.databases import test_row_write_safety as fixtures

# Share the existing disposable schema setup and independent durability reader.
db = fixtures.db
template = fixtures.template
stored = fixtures.stored


@pytest.mark.parametrize(
    "lookup", ["relation", "table_spec", "tables", "columns", "views"]
)
def test_schema_inspection_preserves_pending_writes(db, lookup):
    """Schema reads cannot publish or discard the primary connection's writes."""
    connection = db.driver.conn
    connection.execute("BEGIN")
    connection.execute("INSERT INTO works(work_title) VALUES ('pending')")
    wrapper = db.driver_wrapper
    calls = {
        "relation": lambda: wrapper.get_relation_type("works"),
        "table_spec": lambda: wrapper.get_table_spec("works", force_refresh=True),
        "tables": lambda: wrapper.get_tables(force_refresh=True),
        "columns": lambda: db.driver.direct_get_tables_and_columns(force_refresh=True),
        "views": lambda: wrapper.get_views(force_refresh=True),
    }
    calls[lookup]()
    assert db.driver.conn is connection
    assert transaction_active(connection)
    assert connection.execute("SELECT work_title FROM works").fetchall() == [
        ("pending",)
    ]
    assert stored(db, "SELECT work_title FROM works") == []
    connection.rollback()
    assert stored(db, "SELECT work_title FROM works") == []


def test_refresh_sees_pending_ddl_without_replacing_connection(db):
    """Table and column discovery must use the same uncommitted schema snapshot."""
    connection = db.driver.conn
    connection.execute("CREATE TEMP TABLE local_probe(value TEXT)")
    connection.execute("BEGIN")
    connection.execute(
        "CREATE TABLE review_pending(review_pending_id INTEGER PRIMARY KEY)"
    )
    try:
        assert db.driver_wrapper.get_relation_type("review_pending") == "table"
        columns = db.driver.direct_get_tables_and_columns(force_refresh=True)
        assert columns["review_pending"] == ["review_pending_id"]
        connection.execute("INSERT INTO local_probe VALUES ('alive')")
        assert db.driver.conn is connection
        assert transaction_active(connection)
    finally:
        connection.rollback()
    assert db.driver_wrapper.get_relation_type("review_pending") is None
    assert "review_pending" not in db.driver_wrapper.get_tables(force_refresh=True)
    assert connection.execute("SELECT * FROM local_probe").fetchall() == []


def test_relation_lookup_reports_errors_instead_of_absence(db, monkeypatch):
    """An unavailable backend must not look like a missing relation."""

    def fail(name):
        raise RuntimeError("backend unavailable")

    monkeypatch.setattr(db.driver, "direct_get_relation_type", fail)
    with pytest.raises(RuntimeError, match="backend unavailable"):
        db.driver_wrapper.get_relation_type("works")


def test_column_lookup_detects_schema_changes_without_committing_reads(db):
    """Removing the old committing lookup must not leave column caches stale."""
    wrapper = db.driver_wrapper
    assert "work_review_extra" not in wrapper.get_column_headings("works")
    with db.driver.get_connection() as connection:
        connection.execute("ALTER TABLE works ADD COLUMN work_review_extra TEXT")
    connection.close()
    assert "work_review_extra" in wrapper.get_column_headings("works")
    assert "work_review_extra" in {
        column.name
        for column in wrapper.get_table_spec("works", force_refresh=True).columns
    }


def test_failed_schema_probe_preserves_the_callers_transaction(db, monkeypatch):
    """Inspection failure must not close a live connection as recovery."""
    wrapper = db.driver_wrapper
    wrapper.get_tables()
    connection = db.driver.conn
    connection.execute("BEGIN")
    connection.execute("INSERT INTO works(work_title) VALUES ('pending')")

    class FailedProbe:
        def execute(self, sql, *args):
            if "schema_version" in sql.lower():
                raise RuntimeError("schema probe failed")
            return connection.execute(sql, *args)

        def __getattr__(self, name):
            return getattr(connection, name)

    with monkeypatch.context() as patch:
        proxy = FailedProbe()
        patch.setattr(db.driver, "conn", proxy)
        with pytest.raises(RuntimeError, match="schema probe failed"):
            wrapper.get_tables()
        assert db.driver.conn is proxy
        assert transaction_active(connection)
        assert connection.execute("SELECT work_title FROM works").fetchall() == [
            ("pending",)
        ]
    connection.rollback()
    assert stored(db, "SELECT work_title FROM works") == []


def test_single_column_write_preserves_an_interleaved_edit(db, monkeypatch):
    """A write by another connection after lookup must survive the target update."""
    wrapper = db.driver_wrapper
    row_id = wrapper.add_row({"work_title": "before", "work_scratch": "before"})
    original = wrapper.get_row_from_id

    def interleaved(*args, **kwargs):
        stale = original(*args, **kwargs)
        with db.driver.get_connection() as other:
            other.execute(
                "UPDATE works SET work_scratch=? WHERE work_id=?",
                ("other edit", row_id),
            )
        other.close()
        return stale

    monkeypatch.setattr(wrapper, "get_row_from_id", interleaved)
    assert wrapper.update_column("works", row_id, "work_title", "requested edit")
    assert stored(db, "SELECT work_title, work_scratch FROM works") == [
        ("requested edit", "other edit")
    ]


def test_single_column_write_preserves_normalized_keys_and_missing_row_errors(db):
    wrapper = db.driver_wrapper
    row_id = wrapper.add_row({"tag": "Before"})
    wrapper.update_column("tags", row_id, "tag", "  Ｓｃｉ   Fi! ")
    assert stored(db, "SELECT tag_phash FROM tags WHERE tag_id=?", (row_id,)) == [
        ("sci fi!",)
    ]
    wrapper.update_column("tags", row_id, "tag_phash", "incorrect")
    assert stored(db, "SELECT tag_phash FROM tags WHERE tag_id=?", (row_id,)) == [
        ("sci fi!",)
    ]
    with pytest.raises(InputIntegrityError, match="No row"):
        wrapper.update_column("works", 999999, "work_title", "missing")


def schema_objects(db):
    """Compare committed schema state including tables, indexes, views and triggers."""
    return stored(db, "SELECT type, name, sql FROM sqlite_master ORDER BY type, name")


@pytest.mark.parametrize("after_ddl", [False, True])
@pytest.mark.parametrize("datatype", ["text", "int"])
def test_custom_column_failure_rolls_back_definition_and_schema(
    db, monkeypatch, after_ddl, datatype
):
    wrapper = db.driver_wrapper
    before = schema_objects(db)
    known = set(wrapper.custom_tables)
    original = db.macros.create_cc_table

    def fail(**kwargs):
        if after_ddl:
            original(**kwargs)
        raise RuntimeError("injected DDL failure")

    with monkeypatch.context() as patch:
        patch.setattr(db.macros, "create_cc_table", fail)
        with pytest.raises(RuntimeError, match="injected DDL failure"):
            wrapper.create_custom_column(
                "Failure probe",
                label="failure_probe",
                datatype=datatype,
                in_table="works",
            )
    assert stored(db, "SELECT * FROM custom_columns") == []
    assert schema_objects(db) == before
    assert wrapper.custom_tables == known
    number = wrapper.create_custom_column(
        "Failure probe", label="failure_probe", datatype=datatype, in_table="works"
    )
    assert stored(
        db,
        "SELECT custom_column_label FROM custom_columns WHERE custom_column_id=?",
        (number,),
    ) == [("failure_probe",)]
    assert wrapper.get_relation_type(f"custom_column_{number}") == "table"


@pytest.mark.parametrize("datatype", ["text", "int"])
def test_midway_ddl_collision_rolls_back_prior_statements(db, datatype):
    """A real CREATE INDEX error must undo the preceding CREATE TABLE and insert."""
    connection = db.driver.conn
    connection.execute("CREATE TABLE custom_column_1_idx(value TEXT)")
    connection.commit()
    before = schema_objects(db)
    with pytest.raises(Exception, match="already"):
        db.driver_wrapper.create_custom_column(
            "Collision", label="collision", datatype=datatype, in_table="works"
        )
    assert stored(db, "SELECT * FROM custom_columns") == []
    assert schema_objects(db) == before


def test_custom_column_creation_refuses_nested_schema_publication(db):
    """A returned definition must have committed before facade maps can publish it."""
    with db.macros.transaction(serialized=True) as connection:
        connection.execute("INSERT INTO works(work_title) VALUES ('outer')")
        with pytest.raises(RuntimeError, match="outermost"):
            db.driver_wrapper.create_custom_column(
                "Nested", label="nested", in_table="works"
            )
        assert connection.execute("SELECT * FROM custom_columns").fetchall() == []
    assert stored(db, "SELECT work_title FROM works") == [("outer",)]


def test_current_folder_hierarchy_is_discovered_and_walked(db):
    wrapper = db.driver_wrapper
    root = wrapper.add_row({"folder_name": "root"})
    child = wrapper.add_row({"folder_name": "child", "folder_parent_id": root})
    assert wrapper.get_parent_column("folders") == "folder_parent_id"
    assert wrapper.get_table_spec("folders").parent_column == "folder_parent_id"
    assert {
        row["folder_id"]
        for row in wrapper.walk(wrapper.get_row_from_id("folders", root))
    } == {root, child}
    assert [
        row["folder_id"]
        for row in wrapper.get_linear_row_list(
            wrapper.get_row_from_id("folders", child)
        )
    ] == [root, child]


def test_table_specs_exclude_views_and_include_them_once_when_requested(db):
    wrapper = db.driver_wrapper
    physical = list(wrapper.iter_table_specs(include_views=False))
    all_specs = list(wrapper.iter_table_specs(include_views=True))
    assert physical
    assert all(spec.relation_kind == RelationKind.TABLE for spec in physical)
    views = {spec.name for spec in all_specs if spec.relation_kind == RelationKind.VIEW}
    assert {"books", "titles"} <= views
    assert len({spec.name for spec in all_specs}) == len(all_specs)
    assert {spec.name for spec in physical} == {spec.name for spec in all_specs} - views


def test_wrapper_api_contains_only_abstract_declarations():
    """Guard the whole wrapper API package against executable method bodies."""
    root = (
        Path(__file__).resolve().parents[2]
        / "src/LiuXin_alpha/databases/api/driver_wrapper_api"
    )
    count = 0
    for path in root.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            count += 1
            assert any(
                ast.unparse(decorator).endswith("abstractmethod")
                for decorator in node.decorator_list
            ), (path, node.name)
            body = node.body[1:] if ast.get_docstring(node) else node.body
            assert all(
                isinstance(statement, ast.Pass)
                or (
                    isinstance(statement, ast.Expr)
                    and isinstance(statement.value, ast.Constant)
                    and statement.value.value is Ellipsis
                )
                for statement in body
            ), (path, node.name)
    assert count > 50
    assert not inspect.isabstract(DriverWrapper)


def test_concrete_case_sensitivity_aliases_still_work(db):
    wrapper = db.driver_wrapper
    wrapper.set_column_case_sensitive("works", "work_title", True)
    assert wrapper.is_column_case_sensitive("works", "work_title") is True
    wrapper.set_column_case_sensitive("works", "work_title", False)
    assert wrapper.is_column_case_sensitive("works", "work_title") is False
