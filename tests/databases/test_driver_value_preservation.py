"""Compare actual SQLite storage after SQL export and file rebuild."""

import math
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from tests.databases.test_driver_plugin_sqlite_safety import driver as driver

TEXT_VALUES = [
    "\0first",
    "before\0after",
    "last\0",
    "\0a\0\0b\0",
    "名📚e\u0301 'quoted';\nsecond line",
    "",
    None,
    "(liuxin-nul)\0(liuxin-nul-)\0tail",
    "prefix" + "\0" * 10000 + "suffix",
]
TEXT_IDS = [
    "first",
    "middle",
    "last",
    "multiple",
    "unicode",
    "empty",
    "null",
    "markers",
    "large",
]


def stored(connection):
    return connection.execute(
        "SELECT id,typeof(value),hex(CAST(value AS BLOB)),typeof(binary),hex(binary) "
        "FROM payloads ORDER BY id"
    ).fetchall()


@pytest.mark.parametrize("value", TEXT_VALUES, ids=TEXT_IDS)
@pytest.mark.parametrize("operation", ["export", "rebuild"])
def test_text_and_blob_values_survive_public_round_trip(driver, value, operation):
    driver.conn.execute(
        "CREATE TABLE payloads(id INTEGER PRIMARY KEY,value TEXT,binary BLOB)"
    ).close()
    driver.conn.execute(
        "INSERT INTO payloads VALUES (1,?,?)",
        (value, value.encode() if value is not None else None),
    ).close()
    driver.conn.commit()
    with closing(sqlite3.connect(driver.database_path)) as reader:
        expected = stored(reader)
    if operation == "export":
        with closing(sqlite3.connect(":memory:")) as reader:
            reader.executescript("\n".join(driver.sql_dump()))
            assert stored(reader) == expected
    else:
        driver.dump_and_restore(callback=None)
        with closing(sqlite3.connect(driver.database_path)) as reader:
            assert stored(reader) == expected
            assert reader.execute("PRAGMA integrity_check").fetchall() == [("ok",)]


@pytest.mark.parametrize("source_encoding", ["UTF-8", "UTF-16le", "UTF-16be"])
@pytest.mark.parametrize("target_encoding", ["UTF-8", "UTF-16le"])
def test_dump_preserves_text_across_database_encodings_and_factories(
    driver, tmp_path, source_encoding, target_encoding
):
    path = tmp_path / "encoded.sqlite"
    with closing(sqlite3.connect(path)) as source:
        source.execute(f"PRAGMA encoding='{source_encoding}'")
        source.execute('CREATE TABLE "名"(id INTEGER PRIMARY KEY,value TIMESTAMP)')
        source.execute('INSERT INTO "名" VALUES (1,?)', ("名\0📚'tail",))
        source.commit()
    owner = type(driver)({"database_path": str(path)})
    try:
        owner.conn.row_factory = sqlite3.Row
        owner.conn.text_factory = bytes
        dump = "\n".join(owner.sql_dump())
        assert owner.conn.row_factory is sqlite3.Row
        assert owner.conn.text_factory is bytes
        with closing(sqlite3.connect(":memory:")) as reader:
            reader.execute(f"PRAGMA encoding='{target_encoding}'")
            reader.executescript(dump)
            assert reader.execute(
                'SELECT value,typeof(value) FROM "名"'
            ).fetchall() == [("名\0📚'tail", "text")]
    finally:
        owner.close()


@pytest.mark.parametrize("operation", ["export", "rebuild"])
def test_unserializable_text_refuses_without_replacing_or_completing(driver, operation):
    driver.conn.execute("CREATE TABLE invalid_text(value TEXT)").close()
    driver.conn.execute(
        "INSERT INTO invalid_text VALUES(CAST(X'FF0061' AS TEXT))"
    ).close()
    driver.conn.commit()
    inode = Path(driver.database_path).stat().st_ino
    if operation == "export":
        driver.conn.execute("INSERT INTO review_items VALUES(1,'pending')").close()
    with pytest.raises(UnicodeError):
        if operation == "export":
            list(driver.sql_dump())
        else:
            driver.dump_and_restore(callback=None)
    assert Path(driver.database_path).stat().st_ino == inode
    assert driver.conn.execute("SELECT hex(value) FROM invalid_text").fetchone() == (
        "FF0061",
    )
    if operation == "export":
        assert driver.conn.in_transaction
        assert driver.conn.execute("SELECT count(*) FROM review_items").fetchone() == (
            1,
        )
        driver.conn.rollback()
    with closing(sqlite3.connect(driver.database_path)) as reader:
        assert reader.execute("SELECT hex(value) FROM invalid_text").fetchone() == (
            "FF0061",
        )


@pytest.mark.parametrize("operation", ["export", "rebuild"])
def test_virtual_table_shadow_contents_survive(driver, operation, tmp_path):
    driver.conn.execute("CREATE VIRTUAL TABLE documents USING fts5(body)").close()
    driver.conn.execute(
        "INSERT INTO documents(rowid,body) VALUES(19,?)", ("before\0after",)
    ).close()
    driver.conn.commit()
    if operation == "export":
        target = tmp_path / "virtual-restored.db"
        with closing(sqlite3.connect(target)) as reader:
            reader.executescript("\n".join(driver.sql_dump()))
            assert reader.execute("SELECT rowid,body FROM documents").fetchall() == [
                (19, "before\0after")
            ]
            assert reader.execute(
                "SELECT rowid FROM documents WHERE documents MATCH 'before'"
            ).fetchall() == [(19,)]
            assert reader.execute("PRAGMA writable_schema").fetchone() == (0,)
    else:
        driver.dump_and_restore(callback=None)
        with closing(sqlite3.connect(driver.database_path)) as reader:
            assert reader.execute("SELECT rowid,body FROM documents").fetchall() == [
                (19, "before\0after")
            ]
            assert reader.execute(
                "SELECT rowid FROM documents WHERE documents MATCH 'before'"
            ).fetchall() == [(19,)]


def test_dump_refuses_transforming_text_factory_without_completing(driver):
    driver.conn.execute(
        "INSERT INTO review_items VALUES(1,?)", ("Upper\0Case",)
    ).close()

    def factory(value):
        return value.decode("utf-8").lower()

    driver.conn.text_factory = factory
    with pytest.raises(TypeError, match="str or bytes text_factory"):
        next(driver.sql_dump())
    assert driver.conn.text_factory is factory
    assert driver.conn.in_transaction
    driver.conn.text_factory = str
    assert driver.conn.execute(
        "SELECT review_item_value FROM review_items"
    ).fetchone() == ("Upper\0Case",)
    driver.conn.rollback()


def test_dump_preserves_schema_sequences_and_numeric_storage(driver):
    driver.conn.executescript(
        """CREATE TABLE "odd'name"(id INTEGER PRIMARY KEY AUTOINCREMENT,value TEXT,
           calculated TEXT GENERATED ALWAYS AS (value || '!') STORED);
        CREATE TABLE effects(value TEXT);
        CREATE TRIGGER record_value AFTER INSERT ON "odd'name"
          BEGIN INSERT INTO effects VALUES(new.value); END;
        CREATE INDEX value_index ON "odd'name"(value);
        CREATE VIEW projected AS SELECT value FROM "odd'name";
        CREATE TABLE cells(value);
        """
    ).close()
    driver.conn.execute(
        'INSERT INTO "odd\'name"(id,value) VALUES(100,?)', ("a\0b",)
    ).close()
    driver.conn.execute('DELETE FROM "odd\'name"').close()
    driver.conn.execute(
        'INSERT INTO "odd\'name"(id,value) VALUES(1,?)', ("c\0d",)
    ).close()
    values = [
        None,
        0,
        -(2**63),
        2**63 - 1,
        1.25,
        math.nextafter(1.0, 2.0),
        math.inf,
        -math.inf,
        b"\0\xff",
        "a\0b",
    ]
    driver.conn.executemany(
        "INSERT INTO cells VALUES (?)", ((v,) for v in values)
    ).close()
    driver.conn.commit()
    with closing(sqlite3.connect(":memory:")) as reader:
        reader.executescript("\n".join(driver.sql_dump()))
        assert reader.execute("SELECT value FROM cells").fetchall() == [
            (v,) for v in values
        ]
        assert reader.execute(
            'SELECT id,value,calculated FROM "odd\'name"'
        ).fetchall() == [(1, "c\0d", "c\0d!")]
        assert reader.execute("SELECT * FROM effects").fetchall() == [
            ("a\0b",),
            ("c\0d",),
        ]
        assert reader.execute("SELECT * FROM projected").fetchall() == [("c\0d",)]
        assert reader.execute(
            'INSERT INTO "odd\'name"(value) VALUES(?) RETURNING id', ("new",)
        ).fetchone() == (101,)
        assert reader.execute("PRAGMA integrity_check").fetchall() == [("ok",)]


def round_trip(driver, operation, target):
    """Exercise public file/export operations and return the independent-read path."""
    if operation == "export":
        with closing(sqlite3.connect(target)) as reader:
            reader.executescript("\n".join(driver.sql_dump()))
    elif operation == "backup":
        driver.direct_backup(str(target))
    else:
        driver.dump_and_restore(callback=None)
        target = driver.database_path
    return target


@pytest.mark.parametrize("operation", ["export", "rebuild", "backup"])
@pytest.mark.parametrize(
    "extra,key",
    [
        ("", "rowid"),
        (", RoWiD TEXT DEFAULT 'shadow'", "_rowid_"),
        (", rowid TEXT DEFAULT 'one', _rowid_ TEXT DEFAULT 'two'", "oid"),
        (", rowid TEXT GENERATED ALWAYS AS (body || '!') STORED", "_rowid_"),
    ],
    ids=["hidden", "shadowed", "two_shadowed", "generated_shadow"],
)
def test_external_fts_keeps_hidden_document_ids(
    driver, tmp_path, operation, extra, key
):
    driver.conn.execute(f"CREATE TABLE documents(body TEXT{extra})").close()
    driver.conn.executemany(
        f"INSERT INTO documents({key},body) VALUES(?,?)",
        [(-5, "alpha"), (42, "alpha beta"), (1000, "beta")],
    ).close()
    driver.conn.execute(
        f"CREATE VIRTUAL TABLE search USING fts5(body,content=documents,content_rowid={key})"
    ).close()
    driver.conn.execute("INSERT INTO search(search) VALUES('rebuild')").close()
    driver.conn.commit()
    with closing(sqlite3.connect(driver.database_path)) as reader:
        expected = reader.execute(
            f"SELECT {key},* FROM documents ORDER BY {key}"
        ).fetchall()
    target = round_trip(driver, operation, tmp_path / "replayed.sqlite")
    with closing(sqlite3.connect(target)) as reader:
        assert (
            reader.execute(f"SELECT {key},* FROM documents ORDER BY {key}").fetchall()
            == expected
        )
        assert reader.execute(
            "SELECT rowid,body FROM search WHERE search MATCH 'alpha' ORDER BY rowid"
        ).fetchall() == [(-5, "alpha"), (42, "alpha beta")]
        reader.execute("INSERT INTO search(search,rank) VALUES('integrity-check',1)")
        assert reader.execute("PRAGMA integrity_check").fetchall() == [("ok",)]


@pytest.mark.parametrize("operation", ["export", "rebuild"])
@pytest.mark.parametrize(
    "definition,key,insert,values",
    [
        ("(id INTEGER PRIMARY KEY,body TEXT)", "id", "id,body", [(42, "alpha")]),
        (
            "(id INTEGER PRIMARY KEY DESC,body TEXT)",
            "rowid",
            "rowid,id,body",
            [(42, 7, "alpha")],
        ),
        (
            "(id INT PRIMARY KEY,body TEXT)",
            "rowid",
            "rowid,id,body",
            [(42, 7, "alpha")],
        ),
        (
            "(id INTEGER,body TEXT,PRIMARY KEY(id DESC))",
            "id",
            "id,body",
            [(42, "alpha")],
        ),
        (
            "(id INTEGER PRIMARY KEY,rowid TEXT,_rowid_ TEXT,oid TEXT)",
            "id",
            "id,rowid,_rowid_,oid",
            [(42, "a", "b", "c")],
        ),
        (
            "(id TEXT PRIMARY KEY,body TEXT) WITHOUT ROWID",
            "id",
            "id,body",
            [("key", "alpha")],
        ),
        (
            "(rowid TEXT,_rowid_ TEXT,oid TEXT,PRIMARY KEY(rowid,_rowid_)) WITHOUT ROWID",
            "rowid",
            "rowid,_rowid_,oid",
            [("a", "b", "c")],
        ),
    ],
    ids=[
        "integer_alias",
        "descending_nonalias",
        "int_nonalias",
        "table_primary_alias",
        "alias_all_shadowed",
        "without_rowid",
        "without_all_shadowed",
    ],
)
def test_dump_distinguishes_rowid_aliases_and_without_rowid_tables(
    driver, tmp_path, operation, definition, key, insert, values
):
    driver.conn.execute(f"CREATE TABLE documents{definition}").close()
    placeholders = ",".join("?" for _ in values[0])
    driver.conn.executemany(
        f"INSERT INTO documents({insert}) VALUES({placeholders})", values
    ).close()
    driver.conn.commit()
    with closing(sqlite3.connect(driver.database_path)) as reader:
        expected = reader.execute(f"SELECT {key},* FROM documents").fetchall()
    target = round_trip(driver, operation, tmp_path / "replayed.sqlite")
    with closing(sqlite3.connect(target)) as reader:
        assert reader.execute(f"SELECT {key},* FROM documents").fetchall() == expected


@pytest.mark.parametrize("operation", ["export", "rebuild"])
def test_unaddressable_rowids_refuse_before_replacement_or_completion(
    driver, operation
):
    driver.conn.execute(
        "CREATE TABLE documents(rowid TEXT,_rowid_ TEXT,oid TEXT)"
    ).close()
    driver.conn.execute("INSERT INTO documents VALUES('a','b','c')").close()
    driver.conn.commit()
    before = Path(driver.database_path).read_bytes()
    if operation == "export":
        driver.conn.execute("INSERT INTO review_items VALUES(1,'pending')").close()
    with pytest.raises(ValueError, match="hidden rowid"):
        if operation == "export":
            list(driver.sql_dump())
        else:
            driver.dump_and_restore(callback=None)
    assert Path(driver.database_path).read_bytes() == before
    assert driver.conn.in_transaction is (operation == "export")
    if operation == "export":
        assert driver.conn.execute("SELECT * FROM review_items").fetchall() == [
            (1, "pending")
        ]
        driver.conn.rollback()
    with closing(sqlite3.connect(driver.database_path)) as reader:
        assert reader.execute("SELECT * FROM documents").fetchall() == [("a", "b", "c")]
