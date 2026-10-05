"""
Provide test site ebook mapper script utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test site ebook mapper script through a consuming regression::

        python -m pytest -q tests/utils/test_site_ebook_mapper_script.py
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_mapper():
    """
    Perform the load mapper utility operation under explicit compatibility rules.

    Example:
        Exercise  load mapper through a consuming regression::

            python -m pytest -q tests/utils/test_site_ebook_mapper_script.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "site_ebook_mapper.py"
    spec = importlib.util.spec_from_file_location("site_ebook_mapper", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _fake_fetcher(module, mapping: dict[str, dict[str, object]]):
    """
    Perform the fake fetcher utility operation under explicit compatibility rules.

    Example:
        Exercise  fake fetcher through a consuming regression::

            python -m pytest -q tests/utils/test_site_ebook_mapper_script.py


    :param module: Value supplied for module under the utility contract.
    :param mapping: Value supplied for mapping under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def _fetch(url: str, *, timeout_s: float, user_agent: str):
        """
        Perform the fetch utility operation under explicit compatibility rules.

        Example:
            Exercise  fake fetcher. fetch through a consuming regression::

                python -m pytest -q tests/utils/test_site_ebook_mapper_script.py


        :param url: Value supplied for url under the utility contract.
        :param timeout_s: Value supplied for timeout s under the utility contract.
        :param user_agent: Value supplied for user agent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        del timeout_s
        assert user_agent
        payload = mapping[url]
        return module.FetchResult(
            url=str(payload.get("url", url)),
            status=int(payload.get("status", 200)),
            content_type=str(payload.get("content_type", "text/html; charset=utf-8")),
            body=bytes(payload.get("body", b"")),
        )

    return _fetch


def test_mapper_discovers_full_urls_and_exports_jsonl(tmp_path: Path) -> None:
    """
    Perform the test mapper discovers full urls and exports jsonl utility operation under explicit compatibility rules.

    Example:
        Exercise test mapper discovers full urls and exports jsonl through a consuming regression::

            python -m pytest -q tests/utils/test_site_ebook_mapper_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mapper = _load_mapper()
    state_db = tmp_path / "map.sqlite3"
    export_path = tmp_path / "ebooks.jsonl"
    root_url = "https://example.test/start/"

    pages = {
        "https://example.test/start/": {
            "body": b"""
                <html><body>
                <a href="/library/">Library</a>
                <a href="/download?file=novel-one.epub">EPUB</a>
                <a href="https://elsewhere.test/outside.pdf">Outside</a>
                </body></html>
            """,
        },
        "https://example.test/library/": {
            "body": b"""
                <html><body>
                <a href="novel-two.pdf#frag">PDF</a>
                <a href="/book-pages/story.html">HTML Book</a>
                </body></html>
            """,
        },
    }

    summary = mapper.crawl_site(
        root_url=root_url,
        state_db_path=state_db,
        fetcher=_fake_fetcher(mapper, pages),
        max_depth=4,
        rate_limit_s=0.0,
        respect_robots=False,
        print_every=0,
    )

    assert summary["processed_this_run"] == 2
    assert summary["ebooks"] == 3

    written = mapper.export_ebooks(
        state_db_path=state_db,
        root_url=root_url,
        output_path=export_path,
        output_format="jsonl",
    )
    assert written == 3

    exported = [json.loads(line) for line in export_path.read_text(encoding="utf-8").splitlines()]
    urls = {entry["url"] for entry in exported}
    assert urls == {
        "https://example.test/book-pages/story.html",
        "https://example.test/download?file=novel-one.epub",
        "https://example.test/library/novel-two.pdf",
    }


def test_mapper_resumes_across_multiple_runs(tmp_path: Path) -> None:
    """
    Perform the test mapper resumes across multiple runs utility operation under explicit compatibility rules.

    Example:
        Exercise test mapper resumes across multiple runs through a consuming regression::

            python -m pytest -q tests/utils/test_site_ebook_mapper_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mapper = _load_mapper()
    state_db = tmp_path / "resume.sqlite3"
    root_url = "https://example.test/root/"

    pages = {
        "https://example.test/root/": {
            "body": b'<html><body><a href="/catalog/">Catalog</a></body></html>',
        },
        "https://example.test/catalog/": {
            "body": b'<html><body><a href="/ebooks/final-book.mobi">Book</a></body></html>',
        },
    }

    first = mapper.crawl_site(
        root_url=root_url,
        state_db_path=state_db,
        fetcher=_fake_fetcher(mapper, pages),
        max_pages=1,
        max_depth=4,
        rate_limit_s=0.0,
        respect_robots=False,
        print_every=0,
    )
    assert first["processed_this_run"] == 1
    assert first["ebooks"] == 0
    assert first["pending"] == 1

    second = mapper.crawl_site(
        root_url=root_url,
        state_db_path=state_db,
        fetcher=_fake_fetcher(mapper, pages),
        max_depth=4,
        rate_limit_s=0.0,
        respect_robots=False,
        print_every=0,
    )
    assert second["processed_this_run"] == 1
    assert second["ebooks"] == 1
    assert second["pending"] == 0


def test_mapper_requeues_in_progress_pages_after_crash(tmp_path: Path) -> None:
    """
    Perform the test mapper requeues in progress pages after crash utility operation under explicit compatibility rules.

    Example:
        Exercise test mapper requeues in progress pages after crash through a consuming regression::

            python -m pytest -q tests/utils/test_site_ebook_mapper_script.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mapper = _load_mapper()
    state_db = tmp_path / "crash.sqlite3"
    root_url = "https://example.test/root/"

    db = mapper.CrawlStateDB(state_db, mapper.canonicalize_url(root_url))
    claimed = db.claim_next_page()
    assert claimed is not None
    assert claimed.url == mapper.canonicalize_url(root_url)
    counts = db.counts()
    assert counts["in_progress"] == 1
    db.close()

    db = mapper.CrawlStateDB(state_db, mapper.canonicalize_url(root_url))
    counts = db.counts()
    assert counts["in_progress"] == 0
    assert counts["pending"] == 1
    db.close()
