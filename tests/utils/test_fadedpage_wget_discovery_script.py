from __future__ import annotations

import importlib.util
import io
import json
import os
import sys
from pathlib import Path

import pytest


def _load_script():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "fadedpage_wget_discovery.py"
    spec = importlib.util.spec_from_file_location("fadedpage_wget_discovery", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _fake_runner(module, lines: list[str], *, raise_after: int | None = None):
    def _run(args, **kwargs):
        del args
        line_callback = kwargs.get("line_callback")
        emitted: list[str] = []
        for index, line in enumerate(lines, start=1):
            emitted.append(line)
            if callable(line_callback):
                line_callback(line)
            if raise_after is not None and index >= raise_after:
                raise RuntimeError("simulated wget failure")
        return module.WgetResult(args=["wget"], returncode=0, stdout="\n".join(emitted), stderr="")

    return _run


class _FakeTty(io.StringIO):
    def isatty(self) -> bool:
        return True


def test_classify_fadedpage_candidate_supports_query_file_and_path() -> None:
    script = _load_script()

    html_candidate = script.classify_fadedpage_candidate("https://www.fadedpage.com/link.php?file=20240507.html")
    assert html_candidate is not None
    assert html_candidate.url == "https://www.fadedpage.com/link.php?file=20240507.html"
    assert html_candidate.filename == "20240507.html"
    assert html_candidate.extension == "html"
    assert html_candidate.object_kind == "ebook_html"
    assert html_candidate.source_kind == "query_file"

    epub_candidate = script.classify_fadedpage_candidate("https://www.fadedpage.com/books/novel.epub")
    assert epub_candidate is not None
    assert epub_candidate.filename == "novel.epub"
    assert epub_candidate.extension == "epub"
    assert epub_candidate.object_kind == "ebook_file"
    assert epub_candidate.source_kind == "path"
    assert script.classify_fadedpage_candidate("https://www.fadedpage.com/robots.txt") is None


def test_build_wget_args_is_verbose_by_default() -> None:
    script = _load_script()

    args = script.build_wget_args(
        root_url="https://www.fadedpage.com/",
        requests_per_hour=1800.0,
        recurse=True,
        max_depth=None,
        no_parent=True,
        span_hosts=False,
        respect_robots=True,
        user_agent=script.DEFAULT_USER_AGENT,
        no_verbose=False,
    )

    assert "--no-verbose" not in args
    assert "--output-file=-" in args


def test_wget_discovery_exports_full_urls_to_json(tmp_path: Path) -> None:
    script = _load_script()
    state_db = tmp_path / "fadedpage.sqlite3"
    output_path = tmp_path / "fadedpage.json"
    root_url = "https://www.fadedpage.com/"
    lines = [
        "URL:https://www.fadedpage.com/index.html [200]",
        "URL:https://www.fadedpage.com/link.php?file=novel-one.html [200]",
        "URL:https://www.fadedpage.com/books/novel-one.epub [200]",
        "URL:https://www.fadedpage.com/books/example.pdf [200]",
        "URL:https://elsewhere.test/outside.epub [200]",
    ]

    summary = script.crawl_with_wget(
        root_url=root_url,
        state_db_path=state_db,
        output_path=output_path,
        runner=_fake_runner(script, lines),
        requests_per_hour=None,
        print_every=0,
        export_every=1,
        export_interval_s=0.0,
    )

    assert summary["observed_this_run"] == 5
    assert summary["candidates_total"] == 3

    exported = json.loads(output_path.read_text(encoding="utf-8"))
    urls = {entry["url"] for entry in exported["objects"]}
    assert urls == {
        "https://www.fadedpage.com/link.php?file=novel-one.html",
        "https://www.fadedpage.com/books/novel-one.epub",
        "https://www.fadedpage.com/books/example.pdf",
    }
    assert exported["stats"]["candidate_count"] == 3
    assert exported["stats"]["group_count"] == 2
    assert exported["stats"]["book_count"] == 2
    assert exported["stats"]["accepted_count"] == 3
    assert exported["stats"]["rejected_count"] == 2
    assert exported["stats"]["reason_counts"] == {
        "accepted": 3,
        "not_ebook_shaped": 1,
        "out_of_scope": 1,
    }
    assert exported["stats"]["rejection_reason_counts"] == {
        "not_ebook_shaped": 1,
        "out_of_scope": 1,
    }

    grouped = {entry["stem"]: entry for entry in exported["groups"]}
    assert grouped["novel-one"]["variant_count"] == 2
    assert grouped["novel-one"]["primary_url"] == "https://www.fadedpage.com/books/novel-one.epub"
    assert {entry["url"] for entry in grouped["novel-one"]["variants"]} == {
        "https://www.fadedpage.com/link.php?file=novel-one.html",
        "https://www.fadedpage.com/books/novel-one.epub",
    }

    books = {entry["stem"]: entry for entry in exported["books"]}
    assert books["novel-one"]["likely_book"] is True
    assert books["novel-one"]["confidence"] == "high"
    assert books["novel-one"]["reader_page_count"] == 1
    assert books["novel-one"]["download_format_count"] == 1
    assert books["novel-one"]["reader_pages"][0]["url"] == "https://www.fadedpage.com/link.php?file=novel-one.html"
    assert books["novel-one"]["download_formats"][0]["url"] == "https://www.fadedpage.com/books/novel-one.epub"


def test_fadedpage_grouping_collapses_known_variant_suffixes(tmp_path: Path) -> None:
    script = _load_script()
    state_db = tmp_path / "variants.sqlite3"
    output_path = tmp_path / "variants.json"

    script.crawl_with_wget(
        root_url="https://www.fadedpage.com/",
        state_db_path=state_db,
        output_path=output_path,
        runner=_fake_runner(
            script,
            [
                "URL:https://www.fadedpage.com/link.php?file=20240507.html [200]",
                "URL:https://www.fadedpage.com/link.php?file=20240507.epub [200]",
                "URL:https://www.fadedpage.com/link.php?file=20240507-a5.pdf [200]",
                "URL:https://www.fadedpage.com/link.php?file=20240507-h.zip [200]",
                "URL:https://www.fadedpage.com/link.php?file=20240507-k.epub [200]",
            ],
        ),
        requests_per_hour=None,
        print_every=0,
        export_every=1,
        export_interval_s=0.0,
    )

    exported = json.loads(output_path.read_text(encoding="utf-8"))
    assert exported["stats"]["group_count"] == 1
    assert exported["stats"]["book_count"] == 1

    group = exported["groups"][0]
    assert group["stem"] == "20240507"
    assert group["source_stems"] == ["20240507", "20240507-a5", "20240507-h", "20240507-k"]
    assert group["variant_suffixes"] == ["-a5", "-h", "-k"]
    assert group["extensions"] == ["epub", "html", "pdf", "zip"]

    book = exported["books"][0]
    assert book["stem"] == "20240507"
    assert book["suspicious"] is False
    assert book["source_stems"] == ["20240507", "20240507-a5", "20240507-h", "20240507-k"]


def test_wget_discovery_echoes_raw_wget_lines_by_default(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    script = _load_script()
    state_db = tmp_path / "chatty.sqlite3"
    output_path = tmp_path / "chatty.json"

    script.crawl_with_wget(
        root_url="https://www.fadedpage.com/",
        state_db_path=state_db,
        output_path=output_path,
        runner=_fake_runner(script, ["URL:https://www.fadedpage.com/link.php?file=novel-one.html [200]"]),
        requests_per_hour=None,
        print_every=0,
        export_every=1,
        export_interval_s=0.0,
    )

    captured = capsys.readouterr()
    assert "[wget] URL:https://www.fadedpage.com/link.php?file=novel-one.html [200]" in captured.out
    assert "[candidate] extension=html" in captured.out


def test_wget_discovery_renders_live_progress_footer_on_tty(monkeypatch, tmp_path: Path) -> None:
    script = _load_script()
    state_db = tmp_path / "progress.sqlite3"
    output_path = tmp_path / "progress.json"
    progress_stream = _FakeTty()
    monkeypatch.setattr(
        script.shutil,
        "get_terminal_size",
        lambda fallback=None: os.terminal_size((240, 20)),
    )

    summary = script.crawl_with_wget(
        root_url="https://www.fadedpage.com/",
        state_db_path=state_db,
        output_path=output_path,
        runner=_fake_runner(
            script,
            [
                "URL:https://www.fadedpage.com/link.php?file=novel-one.html [200]",
                "URL:https://www.fadedpage.com/books/novel-one.epub [200]",
            ],
        ),
        requests_per_hour=None,
        echo_wget_lines=False,
        live_progress=True,
        progress_stream=progress_stream,
        print_every=0,
        export_every=1,
        export_interval_s=0.0,
    )

    assert summary["candidates_total"] == 2
    progress_output = progress_stream.getvalue()
    assert "[status] elapsed=" in progress_output
    assert "observed=2 run / 2 total" in progress_output
    assert "candidates=2 run / 2 total" in progress_output


def test_fadedpage_text_report_surfaces_reason_counts_and_suspicious_books(tmp_path: Path) -> None:
    script = _load_script()
    state_db = tmp_path / "report.sqlite3"
    output_path = tmp_path / "report.json"

    script.crawl_with_wget(
        root_url="https://www.fadedpage.com/",
        state_db_path=state_db,
        output_path=output_path,
        runner=_fake_runner(
            script,
            [
                "URL:https://www.fadedpage.com/link.php?file=20240507.html [200]",
                "URL:https://www.fadedpage.com/link.php?file=20240507.epub [200]",
                "URL:https://www.fadedpage.com/link.php?file=20240508.epub [200]",
                "URL:https://www.fadedpage.com/robots.txt [200]",
                "URL:https://www.fadedpage.com/index.html [200]",
                "URL:https://elsewhere.test/outside.epub [200]",
            ],
        ),
        requests_per_hour=None,
        print_every=0,
        export_every=1,
        export_interval_s=0.0,
    )

    payload = script.build_export_payload(state_db_path=state_db, root_url="https://www.fadedpage.com/")
    report = script.render_text_report(payload, report_limit=10)

    assert "Faded Page Discovery" in report
    assert "Reasons: accepted=3, not_ebook_shaped=2, out_of_scope=1" in report
    assert "Suspicious / Incomplete Books" in report
    assert "20240508 | warnings=missing_reader_page,single_download_format,single_variant,incomplete_core_formats" in report
    assert "Likely Books" in report


def test_build_export_payload_refilters_stale_candidates_from_state_db(tmp_path: Path) -> None:
    script = _load_script()
    db = script.DiscoveryStateDB(tmp_path / "stale.sqlite3", root_url="https://www.fadedpage.com/")
    try:
        db.record_observation(
            url="https://www.fadedpage.com/robots.txt",
            within_scope=True,
            file_like=True,
            accepted=True,
            reason="accepted",
        )
        db.record_candidate(
            script.CandidateRecord(
                url="https://www.fadedpage.com/robots.txt",
                host="www.fadedpage.com",
                path="/robots.txt",
                filename="robots.txt",
                stem="robots",
                extension="txt",
                object_kind="ebook_file",
                source_kind="path",
                query_filename=None,
            )
        )
    finally:
        db.close()

    payload = script.build_export_payload(
        state_db_path=tmp_path / "stale.sqlite3",
        root_url="https://www.fadedpage.com/",
    )

    assert payload["objects"] == []
    assert payload["books"] == []
    assert payload["stats"]["accepted_count"] == 0
    assert payload["stats"]["rejected_count"] == 1
    assert payload["stats"]["reason_counts"] == {
        "accepted": 0,
        "filtered_after_classification": 1,
    }


def test_main_export_only_text_report_prints_summary(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    script = _load_script()
    state_db = tmp_path / "main.sqlite3"
    output_path = tmp_path / "main.json"
    script.crawl_with_wget(
        root_url="https://www.fadedpage.com/",
        state_db_path=state_db,
        output_path=output_path,
        runner=_fake_runner(script, ["URL:https://www.fadedpage.com/link.php?file=novel-one.html [200]"]),
        requests_per_hour=None,
        print_every=0,
        export_every=1,
        export_interval_s=0.0,
    )

    exit_code = script.main(
        [
            "--state-db",
            str(state_db),
            "--output",
            str(output_path),
            "--export-only",
            "--report",
            "text",
            "https://www.fadedpage.com/",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "Faded Page Discovery" in captured.out
    assert "exported 1 candidate objects across 1 grouped books" in captured.out


def test_wget_discovery_resumes_after_failure_without_duplicate_candidates(tmp_path: Path) -> None:
    script = _load_script()
    state_db = tmp_path / "resume.sqlite3"
    output_path = tmp_path / "resume.json"
    root_url = "https://www.fadedpage.com/"
    first_line = "URL:https://www.fadedpage.com/link.php?file=20240507.html [200]"
    second_line = "URL:https://www.fadedpage.com/books/example.epub [200]"

    with pytest.raises(RuntimeError):
        script.crawl_with_wget(
            root_url=root_url,
            state_db_path=state_db,
            output_path=output_path,
            runner=_fake_runner(script, [first_line], raise_after=1),
            requests_per_hour=None,
            print_every=0,
            export_every=1,
            export_interval_s=0.0,
        )

    exported = json.loads(output_path.read_text(encoding="utf-8"))
    assert exported["stats"]["candidate_count"] == 1

    summary = script.crawl_with_wget(
        root_url=root_url,
        state_db_path=state_db,
        output_path=output_path,
        runner=_fake_runner(script, [first_line, second_line]),
        requests_per_hour=None,
        print_every=0,
        export_every=1,
        export_interval_s=0.0,
    )

    assert summary["candidates_total"] == 2
    exported = json.loads(output_path.read_text(encoding="utf-8"))
    assert exported["stats"]["candidate_count"] == 2
    assert {entry["url"] for entry in exported["objects"]} == {
        "https://www.fadedpage.com/link.php?file=20240507.html",
        "https://www.fadedpage.com/books/example.epub",
    }
