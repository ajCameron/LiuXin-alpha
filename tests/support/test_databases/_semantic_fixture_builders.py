"""
Build deterministic test-database semantic fixture builders data and relationships.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise  semantic fixture builders through a consuming regression::

        python -m pytest -q tests/databases/test_test_resources_manager.py
"""
from __future__ import annotations

import re
import sqlite3

from pathlib import Path

from LiuXin_alpha.databases.custom_columns import CustomColumns
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.catalog.field_metadata import FieldMetadata
from tests.support._surface_storage_tables import ensure_surface_asset_tables_sqlite
from tests.support import test_resources_manager as trm


_BUNDLE_ROOT_TOKEN = trm.TEST_DB_BUNDLE_ROOT_TOKEN
PNG_1X1_BYTES = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00"
    b"\x90wS\xde"
    b"\x00\x00\x00\x0cIDATx\x9cc``\xf8\xcf\xc0\x00\x00\x03\x01\x01\x00"
    b"\xc9\xfe\x92\xef"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)
JPEG_FAKE_BYTES = b"\xff\xd8\xff\xe0FAKEJPEG\xff\xd9"


def build_base_profiled_db(*, bundle_dir: Path, db_name: str, books: int) -> Path:
    """
    Build base profiled db for deterministic fixture consumers.

    Example:
        Exercise build base profiled db through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param bundle_dir: Value supplied for bundle dir under the deterministic fixture
        contract.
    :param db_name: Registered test-database profile name.
    :param books: Value supplied for books under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    db_path = Path(bundle_dir) / f"{db_name}.test_db"
    trm.build_profiled_test_database(
        db_path=db_path,
        db_name=db_name,
        books=books,
        folders=0,
        files=0,
    )
    with sqlite3.connect(str(db_path)) as conn:
        ensure_surface_asset_tables_sqlite(
            conn,
            include_images=True,
            include_file_folder_links=True,
        )
        conn.commit()
    return db_path


def open_fixture_db(db_path: Path) -> sqlite3.Connection:
    """
    Perform the open fixture db step with deterministic fixture inputs.

    Example:
        Exercise open fixture db through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param db_path: Value supplied for db path under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    trm._register_sqlite_test_functions(conn)
    return conn


def open_fixture_database(db_path: Path, *, storage_startup_on_add: bool = False) -> Database:
    """
    Perform the open fixture database step with deterministic fixture inputs.

    Example:
        Exercise open fixture database through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param db_path: Value supplied for db path under the deterministic fixture contract.
    :param storage_startup_on_add: Value supplied for storage startup on add under the
        deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    db = Database(
        metadata={"database_path": str(db_path)},
        storage_startup_on_add=storage_startup_on_add,
    )
    db.field_metadata = FieldMetadata()
    return db


def ordered_ids(conn: sqlite3.Connection, table: str, pk_col: str) -> list[int]:
    """
    Perform the ordered ids step with deterministic fixture inputs.

    Example:
        Exercise ordered ids through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param conn: SQLite connection used to create or inspect fixture state.
    :param table: Database table addressed by the fixture operation.
    :param pk_col: Value supplied for pk col under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return [int(row[0]) for row in conn.execute(f"SELECT {pk_col} FROM {table} WHERE {pk_col} > 0 ORDER BY {pk_col};")]


def lookup_language_id(conn: sqlite3.Connection, code: str) -> int:
    """
    Perform the lookup language id step with deterministic fixture inputs.

    Example:
        Exercise lookup language id through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param conn: SQLite connection used to create or inspect fixture state.
    :param code: Value supplied for code under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    row = conn.execute(
        "SELECT language_id FROM languages "
        "WHERE language_code = ? OR language_iso639_2_b = ? OR language_iso639_2_t = ? OR language_iso639_1 = ? "
        "LIMIT 1;",
        (str(code), str(code), str(code), str(code)),
    ).fetchone()
    if row is None:
        raise LookupError(f"Missing seeded language code: {code}")
    return int(row[0])


def norm_text(value: str) -> str:
    """
    Perform the norm text step with deterministic fixture inputs.

    Example:
        Exercise norm text through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param value: Fixture value normalized, encoded, stored or returned.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower()).strip("-")


def bundle_token_path(*parts: str) -> str:
    """
    Perform the bundle token path step with deterministic fixture inputs.

    Example:
        Exercise bundle token path through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param parts: Message fragments joined for the test logger.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return str(Path(_BUNDLE_ROOT_TOKEN, *parts))


def create_custom_column(
    db: Database,
    *,
    label: str,
    name: str,
    datatype: str,
    is_multiple: bool,
    table: str = "works",
    display: dict | None = None,
    make_category: bool | None = None,
) -> tuple[int, str, str | None]:
    """
    Create custom column for deterministic fixture consumers.

    Example:
        Exercise create custom column through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param db: Database connection, wrapper or fixture addressed by the operation.
    :param label: Value supplied for label under the deterministic fixture contract.
    :param name: Stable fixture, profile, member or field name.
    :param datatype: Value supplied for datatype under the deterministic fixture
        contract.
    :param is_multiple: Value supplied for is multiple under the deterministic fixture
        contract.
    :param table: Database table addressed by the fixture operation.
    :param display: Value supplied for display under the deterministic fixture contract.
    :param make_category: Value supplied for make category under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    cc = CustomColumns(db=db, field_metadata=db.field_metadata, table=table)
    num = int(
        cc.create_custom_column(
            label=label,
            name=name,
            datatype=datatype,
            is_multiple=is_multiple,
            display=display or {},
            table=table,
            make_category=make_category,
        )
    )
    db.refresh_db_metadata()

    cc_table, link_table = db.driver_wrapper.custom_table_names(num, in_table=table)
    tables = set(db.get_tables(force_refresh=True))
    if link_table not in tables:
        link_table = None

    return num, str(cc_table), str(link_table) if link_table else None


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    """
    Perform the table columns step with deterministic fixture inputs.

    Example:
        Exercise table columns through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param conn: SQLite connection used to create or inspect fixture state.
    :param table: Database table addressed by the fixture operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]


def insert_row(conn: sqlite3.Connection, table: str, values: dict[str, object]) -> int:
    """
    Insert row for deterministic fixture consumers.

    Example:
        Exercise insert row through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param conn: SQLite connection used to create or inspect fixture state.
    :param table: Database table addressed by the fixture operation.
    :param values: Value supplied for values under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    columns = list(values)
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders});"
    cur = conn.execute(sql, tuple(values[column] for column in columns))
    return int(cur.lastrowid)


def insert_custom_normalized_value(
    conn: sqlite3.Connection,
    *,
    cc_table: str,
    link_table: str,
    target_id: int,
    value: object,
    extra: object = None,
) -> int:
    """
    Insert custom normalized value for deterministic fixture consumers.

    Example:
        Exercise insert custom normalized value through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param conn: SQLite connection used to create or inspect fixture state.
    :param cc_table: Value supplied for cc table under the deterministic fixture
        contract.
    :param link_table: Value supplied for link table under the deterministic fixture
        contract.
    :param target_id: Value supplied for target id under the deterministic fixture
        contract.
    :param value: Fixture value normalized, encoded, stored or returned.
    :param extra: Value supplied for extra under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    value_column = next(column for column in table_columns(conn, cc_table) if column.endswith("_value"))
    id_column = next(column for column in table_columns(conn, cc_table) if column.endswith("_id"))
    existing = conn.execute(
        f"SELECT {id_column} FROM {cc_table} WHERE {value_column} = ? LIMIT 1;",
        (value,),
    ).fetchone()
    if existing is None:
        value_id = insert_row(conn, cc_table, {value_column: value})
    else:
        value_id = int(existing[0])

    link_columns = table_columns(conn, link_table)
    link_value_column = next(column for column in link_columns if column.endswith("_value"))
    target_column = next(
        column
        for column in link_columns
        if column not in {link_value_column}
        and not column.endswith("_id")
        and not column.endswith("_extra")
    )
    payload: dict[str, object] = {
        target_column: int(target_id),
        link_value_column: int(value_id),
    }
    extra_column = next((column for column in link_columns if column.endswith("_extra")), None)
    if extra_column is not None and extra is not None:
        payload[extra_column] = extra
    insert_row(conn, link_table, payload)
    return int(value_id)


def insert_custom_scalar_value(
    conn: sqlite3.Connection,
    *,
    cc_table: str,
    target_id: int,
    value: object,
) -> int:
    """
    Insert custom scalar value for deterministic fixture consumers.

    Example:
        Exercise insert custom scalar value through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param conn: SQLite connection used to create or inspect fixture state.
    :param cc_table: Value supplied for cc table under the deterministic fixture
        contract.
    :param target_id: Value supplied for target id under the deterministic fixture
        contract.
    :param value: Fixture value normalized, encoded, stored or returned.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    columns = table_columns(conn, cc_table)
    target_column = next(
        column
        for column in columns
        if column not in {f"{cc_table}_id"}
        and not column.endswith("_id")
        and not column.endswith("_value")
    )
    value_column = next(column for column in columns if column.endswith("_value"))
    return insert_row(conn, cc_table, {target_column: int(target_id), value_column: value})


def finalize_fixture(conn: sqlite3.Connection, *, db_name: str) -> None:
    """
    Perform the finalize fixture step with deterministic fixture inputs.

    Example:
        Exercise finalize fixture through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param conn: SQLite connection used to create or inspect fixture state.
    :param db_name: Registered test-database profile name.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    trm._normalize_test_db_for_determinism(conn, db_name=db_name)
    conn.commit()

    integrity = conn.execute("PRAGMA integrity_check;").fetchone()
    if integrity is None or str(integrity[0]) != "ok":
        raise AssertionError(f"integrity_check failed for {db_name}: {integrity}")

    violations = conn.execute("PRAGMA foreign_key_check;").fetchall()
    if violations:
        raise AssertionError(f"foreign_key_check failed for {db_name}: {violations[:10]}")
