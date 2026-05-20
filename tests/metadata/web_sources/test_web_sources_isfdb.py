from __future__ import annotations

import queue
import sqlite3
from pathlib import Path
from threading import Event


class _Log:
    def __init__(self) -> None:
        self.events = []

    def __call__(self, *parts):
        self.events.append(("call", parts))

    def info(self, *parts):
        self.events.append(("info", parts))

    def warning(self, *parts):
        self.events.append(("warning", parts))

    def error(self, *parts):
        self.events.append(("error", parts))

    def exception(self, *parts):
        self.events.append(("exception", parts))


def _drain(out: queue.Queue) -> list:
    results = []
    while True:
        try:
            results.append(out.get_nowait())
        except queue.Empty:
            return results


def _create_isfdb_fixture(path: Path) -> Path:
    conn = sqlite3.connect(path)
    with conn:
        conn.executescript(
            """
            CREATE TABLE works (
                work_id INTEGER PRIMARY KEY,
                work_type TEXT,
                work_title TEXT,
                work_original_date TEXT,
                work_original_year INTEGER,
                work_scratch TEXT
            );
            CREATE TABLE agents (
                agent_id INTEGER PRIMARY KEY,
                agent_type TEXT,
                agent_canonical_name TEXT,
                agent_sort_name TEXT
            );
            CREATE TABLE agent_work_links (
                agent_work_link_agent_id INTEGER,
                agent_work_link_work_id INTEGER,
                agent_work_link_priority INTEGER,
                agent_work_link_type TEXT
            );
            CREATE TABLE manifestations (
                manifestation_id INTEGER PRIMARY KEY,
                manifestation_pub_year INTEGER,
                manifestation_pub_date TEXT,
                manifestation_note TEXT,
                manifestation_scratch TEXT
            );
            CREATE TABLE items (
                item_id INTEGER PRIMARY KEY,
                item_manifestation_id INTEGER,
                item_source_detail TEXT,
                item_source_name TEXT,
                item_scratch TEXT
            );
            CREATE TABLE expression_work_links (
                expression_work_link_expression_id INTEGER,
                expression_work_link_work_id INTEGER,
                expression_work_link_priority INTEGER
            );
            CREATE TABLE expression_manifestation_links (
                expression_manifestation_link_expression_id INTEGER,
                expression_manifestation_link_manifestation_id INTEGER,
                expression_manifestation_link_priority INTEGER
            );
            CREATE TABLE item_identifiers (
                item_identifier_item_id INTEGER,
                item_identifier_scheme TEXT,
                item_identifier_value TEXT
            );
            CREATE TABLE entity_identifiers (
                entity_identifier_entity_type TEXT,
                entity_identifier_entity_id INTEGER,
                entity_identifier_scheme TEXT,
                entity_identifier_value TEXT
            );
            CREATE TABLE agent_manifestation_links (
                agent_manifestation_link_agent_id INTEGER,
                agent_manifestation_link_manifestation_id INTEGER,
                agent_manifestation_link_priority INTEGER,
                agent_manifestation_link_type TEXT
            );
            CREATE TABLE languages (
                language_id INTEGER PRIMARY KEY,
                language TEXT,
                language_code TEXT,
                language_iso639_1 TEXT,
                language_iso639_2_b TEXT,
                language_iso639_2_t TEXT
            );
            CREATE TABLE language_work_links (
                language_work_link_language_id INTEGER,
                language_work_link_work_id INTEGER,
                language_work_link_priority INTEGER
            );
            CREATE TABLE series (
                series_id INTEGER PRIMARY KEY,
                series TEXT
            );
            CREATE TABLE series_work_links (
                series_work_link_series_id INTEGER,
                series_work_link_work_id INTEGER,
                series_work_link_priority INTEGER
            );
            CREATE TABLE genres (
                genre_id INTEGER PRIMARY KEY,
                genre TEXT
            );
            CREATE TABLE genre_work_links (
                genre_work_link_genre_id INTEGER,
                genre_work_link_work_id INTEGER,
                genre_work_link_priority INTEGER
            );
            CREATE TABLE labels (
                label_id INTEGER PRIMARY KEY,
                label_text TEXT
            );
            CREATE TABLE label_work_links (
                label_work_link_label_id INTEGER,
                label_work_link_work_id INTEGER,
                label_work_link_priority INTEGER,
                label_work_link_source TEXT
            );
            CREATE TABLE synopses (
                synopsis_id INTEGER PRIMARY KEY,
                synopsis TEXT
            );
            CREATE TABLE synopsis_work_links (
                synopsis_work_link_synopsis_id INTEGER,
                synopsis_work_link_work_id INTEGER,
                synopsis_work_link_priority INTEGER
            );
            CREATE TABLE comments (
                comment_id INTEGER PRIMARY KEY,
                comment TEXT
            );
            CREATE TABLE comment_work_links (
                comment_work_link_comment_id INTEGER,
                comment_work_link_work_id INTEGER,
                comment_work_link_priority INTEGER
            );
            CREATE TABLE notes (
                note_id INTEGER PRIMARY KEY,
                note TEXT
            );
            CREATE TABLE note_work_links (
                note_work_link_note_id INTEGER,
                note_work_link_work_id INTEGER,
                note_work_link_priority INTEGER
            );
            CREATE TABLE ratings (
                rating_id INTEGER PRIMARY KEY,
                rating FLOAT
            );
            CREATE TABLE rating_work_links (
                rating_work_link_rating_id INTEGER,
                rating_work_link_work_id INTEGER,
                rating_work_link_priority INTEGER
            );
            """
        )
        conn.executemany(
            "INSERT INTO works VALUES (?, ?, ?, ?, ?, ?)",
            [
                (1, "novel", "Dune", "1965-08-00", 1965, "isfdb:title:100"),
                (2, "shortfiction", "Appendix I: Ecology of Dune", "1965-08-00", 1965, "isfdb:title:101"),
                (3, "novel", "The Left Hand of Darkness", "1969-03-00", 1969, "isfdb:title:102"),
            ],
        )
        conn.executemany(
            "INSERT INTO agents VALUES (?, ?, ?, ?)",
            [
                (1, "person", "Frank Herbert", "Herbert"),
                (2, "person", "Ursula K. Le Guin", "Le Guin"),
                (10, "organisation", "Chilton Books", "Chilton Books"),
                (11, "organisation", "Ace Books", "Ace Books"),
            ],
        )
        conn.executemany(
            "INSERT INTO agent_work_links VALUES (?, ?, ?, ?)",
            [(1, 1, 1, "aut"), (1, 2, 2, "aut"), (2, 3, 1, "aut")],
        )
        conn.executemany(
            "INSERT INTO manifestations VALUES (?, ?, ?, ?, ?)",
            [
                (10, 1965, "1965-08-00", "First edition note.", "isfdb:pub:200"),
                (11, 1969, "1969-03-00", "Ace paperback note.", "isfdb:pub:201"),
            ],
        )
        conn.executemany(
            "INSERT INTO items VALUES (?, ?, ?, ?, ?)",
            [
                (1000, 10, "200", "Dune", "isfdb:pub:200"),
                (1001, 11, "201", "The Left Hand of Darkness", "isfdb:pub:201"),
            ],
        )
        conn.executemany(
            "INSERT INTO expression_work_links VALUES (?, ?, ?)",
            [(100, 1, 1), (101, 2, 2), (102, 3, 1)],
        )
        conn.executemany(
            "INSERT INTO expression_manifestation_links VALUES (?, ?, ?)",
            [(100, 10, 1), (101, 10, 2), (102, 11, 1)],
        )
        conn.executemany(
            "INSERT INTO item_identifiers VALUES (?, ?, ?)",
            [
                (1000, "isbn_13", "9780441172719"),
                (1000, "isbn_10", "0441172717"),
                (1001, "isbn_13", "9780441478125"),
            ],
        )
        conn.executemany(
            "INSERT INTO entity_identifiers VALUES (?, ?, ?, ?)",
            [("manifestation", 10, "asin", "B000TEST01")],
        )
        conn.executemany(
            "INSERT INTO agent_manifestation_links VALUES (?, ?, ?, ?)",
            [(10, 10, 1, "pbl"), (11, 11, 1, "pbl")],
        )
        conn.executemany("INSERT INTO languages VALUES (?, ?, ?, ?, ?, ?)", [(1, "English", "eng", "en", "eng", None)])
        conn.executemany("INSERT INTO language_work_links VALUES (?, ?, ?)", [(1, 1, 1), (1, 2, 1), (1, 3, 1)])
        conn.executemany("INSERT INTO series VALUES (?, ?)", [(1, "Dune"), (2, "Hainish Cycle")])
        conn.executemany("INSERT INTO series_work_links VALUES (?, ?, ?)", [(1, 1, 1), (2, 3, 1)])
        conn.executemany("INSERT INTO genres VALUES (?, ?)", [(1, "Science Fiction"), (2, "Planetary Romance")])
        conn.executemany("INSERT INTO genre_work_links VALUES (?, ?, ?)", [(1, 1, 1), (2, 1, 2), (1, 3, 1)])
        conn.executemany("INSERT INTO labels VALUES (?, ?)", [(1, "desert planet"), (2, "generated noise")])
        conn.executemany(
            "INSERT INTO label_work_links VALUES (?, ?, ?, ?)",
            [(1, 1, 1, "isfdb:tag"), (2, 1, 2, "isfdb:generated")],
        )
        conn.executemany("INSERT INTO synopses VALUES (?, ?)", [(1, "A desert planet changes imperial politics.")])
        conn.executemany("INSERT INTO synopsis_work_links VALUES (?, ?, ?)", [(1, 1, 1)])
        conn.executemany("INSERT INTO comments VALUES (?, ?)", [(1, "Generated ISFDB comment for Dune.")])
        conn.executemany("INSERT INTO comment_work_links VALUES (?, ?, ?)", [(1, 1, 1)])
        conn.executemany("INSERT INTO notes VALUES (?, ?)", [(1, "Title note.")])
        conn.executemany("INSERT INTO note_work_links VALUES (?, ?, ?)", [(1, 1, 1)])
        conn.executemany("INSERT INTO ratings VALUES (?, ?)", [(1, 4.5)])
        conn.executemany("INSERT INTO rating_work_links VALUES (?, ?, ?)", [(1, 1, 1)])
    conn.close()
    return path


def test_web_sources_isfdb_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.isfdb as isfdb

    assert isfdb is not None


def test_isfdb_helper_edges(tmp_path: Path, monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.isfdb as isfdb

    db_path = _create_isfdb_fixture(tmp_path / "isfdb.test_db")
    monkeypatch.setenv("LIUXIN_ISFDB_TEST_DB", str(db_path))

    assert isfdb._normalize_source_identifier("isfdb:title:100") == ("title", "100")
    assert isfdb._normalize_source_identifier("publication:200") == ("pub", "200")
    assert isfdb._normalize_source_identifier("100", default_kind="title") == ("title", "100")
    assert isfdb._normalize_source_identifier("not an id") is None
    assert isfdb._isfdb_id_from_identifiers({"isfdb_pub": "200"}) == ("pub", "200")
    assert isfdb._isfdb_id_from_identifiers({"isfdb_title": "100"}) == ("title", "100")
    assert isfdb._safe_isbn({"isbn": "978-0-441-17271-9"}) == "9780441172719"
    assert isfdb._safe_asin({"asin": "b000test01"}) == "B000TEST01"
    assert isfdb._isbn_query_values("9780441172719") == ["9780441172719", "0441172717"]
    assert isfdb._isbn_query_values("0441172717") == ["0441172717", "9780441172719"]
    assert isfdb._normalize_date_text("1965-08-00") == "1965-08"
    assert isfdb._parse_isfdb_date("1965-08-00").year == 1965
    assert isfdb.resolve_isfdb_database_path() == db_path


def test_isfdb_url_and_query_builders(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.web_sources.isfdb import ISFDB

    plugin = ISFDB(database_path=str(_create_isfdb_fixture(tmp_path / "isfdb.test_db")))
    assert plugin.is_configured()
    assert plugin.get_book_url({"isfdb_title": "100"}) == (
        "isfdb_title",
        "100",
        "https://www.isfdb.org/cgi-bin/title.cgi?100",
    )
    assert plugin.get_book_url({"isfdb_pub": "200"}) == (
        "isfdb_pub",
        "200",
        "https://www.isfdb.org/cgi-bin/pl.cgi?200",
    )
    assert plugin.id_from_url("https://www.isfdb.org/cgi-bin/title.cgi?100") == ("isfdb_title", "100")
    assert plugin.id_from_url("https://www.isfdb.org/cgi-bin/pl.cgi?200") == ("isfdb_pub", "200")
    assert plugin.id_from_url("https://example.invalid/cgi-bin/title.cgi?100") is None
    assert plugin.create_query(identifiers={"isfdb": "title:100"}) == [("id", ("title", "100"))]
    assert plugin.create_query(identifiers={"isbn": "9780441172719"}) == [("isbn", "9780441172719")]
    assert plugin.create_query(identifiers={"asin": "B000TEST01"}) == [("asin", "B000TEST01")]
    assert plugin.create_query(title="Dune", authors=["Frank Herbert"], identifiers={}) == [
        ("text", ("Dune", ["Frank Herbert"]))
    ]
    assert plugin.create_query(title=None, authors=None, identifiers={}) == []


def test_isfdb_identify_direct_title_maps_metadata(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.web_sources.isfdb import ISFDB

    plugin = ISFDB(database_path=str(_create_isfdb_fixture(tmp_path / "isfdb.test_db")))
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), identifiers={"isfdb_title": "100"})

    results = _drain(out)
    assert len(results) == 1
    mi = results[0]
    assert mi.title == "Dune"
    assert mi.authors == ["Frank Herbert"]
    assert mi.publisher == "Chilton Books"
    assert mi.pubdate.year == 1965
    assert mi.language == "en"
    assert mi.series == "Dune"
    assert mi.rating == 4.5
    assert "Science Fiction" in mi.tags
    assert "generated noise" not in mi.tags
    assert "desert planet" in mi.tags
    assert "A desert planet" in mi.comments
    assert "First edition note" in mi.comments
    identifiers = mi.get_identifiers()
    assert identifiers["isfdb"] == "title:100"
    assert identifiers["isfdb_title"] == "100"
    assert identifiers["isfdb_pub"] == "200"
    assert identifiers["isbn"] == "9780441172719"
    assert identifiers["asin"] == "B000TEST01"
    assert mi.all_isbns == ["0441172717", "9780441172719"]
    assert plugin.cached_isbn_to_identifier("9780441172719") == "title:100"


def test_isfdb_identify_by_isbn_prefers_publication_title_work(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.web_sources.isfdb import ISFDB

    db_path = _create_isfdb_fixture(tmp_path / "isfdb.test_db")
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM item_identifiers WHERE item_identifier_scheme = 'isbn_13'")

    plugin = ISFDB(database_path=str(db_path))
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), identifiers={"isbn": "9780441172719"})

    results = _drain(out)
    assert [mi.title for mi in results[:2]] == ["Dune", "Appendix I: Ecology of Dune"]
    assert results[0].get_identifiers()["isfdb_pub"] == "200"


def test_isfdb_identify_by_text_author_and_abort(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.web_sources.isfdb import ISFDB

    plugin = ISFDB(database_path=str(_create_isfdb_fixture(tmp_path / "isfdb.test_db")))
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), title="Left Hand Darkness", authors=["Le Guin"])
    results = _drain(out)
    assert len(results) == 1
    assert results[0].title == "The Left Hand of Darkness"
    assert results[0].publisher == "Ace Books"

    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), title="Dune", authors=["Wrong Author"])
    assert _drain(out) == []

    abort = Event()
    abort.set()
    plugin.identify(log=_Log(), result_queue=queue.Queue(), abort=abort, title="Dune")


def test_isfdb_identify_configuration_schema_and_parse_failures(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.isfdb import ISFDB

    plugin = ISFDB(database_path=str(tmp_path / "missing.test_db"))
    out = queue.Queue()
    log = _Log()
    plugin.identify(log=log, result_queue=out, abort=Event(), title="Dune")
    assert _drain(out) == []
    assert any(level == "warning" and "not configured" in parts[0] for level, parts in log.events)

    bad_db = tmp_path / "bad.test_db"
    sqlite3.connect(bad_db).execute("CREATE TABLE works (work_id INTEGER)").connection.close()
    plugin = ISFDB(database_path=str(bad_db))
    log = _Log()
    plugin.identify(log=log, result_queue=out, abort=Event(), title="Dune")
    assert any(level == "warning" and "missing required" in parts[0] for level, parts in log.events)

    good = ISFDB(database_path=str(_create_isfdb_fixture(tmp_path / "good.test_db")))
    monkeypatch.setattr(good, "_metadata_for_candidate", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError()))
    log = _Log()
    good.identify(log=log, result_queue=queue.Queue(), abort=Event(), title="Dune")
    assert any(level == "exception" and "Failed to parse" in parts[0] for level, parts in log.events)


def test_isfdb_import_web_source_module() -> None:
    from LiuXin_alpha.metadata.web_sources import import_web_source_module, iter_known_web_source_modules

    assert "isfdb" in iter_known_web_source_modules()
    mod = import_web_source_module("isfdb")
    assert hasattr(mod, "ISFDB")
