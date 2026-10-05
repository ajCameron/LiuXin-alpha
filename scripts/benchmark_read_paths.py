#!/usr/bin/env python3
"""
Benchmark library read paths.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise benchmark read paths through a consuming regression::

        python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
"""

from __future__ import annotations

import argparse
import sys

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Callable, Optional

from _benchmark_common import (
    DEFAULT_CACHE_DIR,
    environment_payload,
    first_alnum_query_term,
    print_report_summary,
    resolved_benchmark_database,
    run_benchmark,
    stderr_progress,
    utc_now_iso,
    write_json_report,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

for candidate in (str(REPO_ROOT), str(SRC_ROOT)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from LiuXin_alpha.surfaces.web_readonly.app import ReadOnlyWebApplication, _open_database  # noqa: E402
from LiuXin_alpha.surfaces.web_readonly.app import _row_value  # noqa: E402


def parse_args() -> argparse.Namespace:
    """
    Parse args under the format's safety and compatibility rules.

    Example:
        Exercise parse args through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    parser = argparse.ArgumentParser(description="Benchmark backend read paths on a LiuXin database.")
    parser.add_argument("--db-name", default="benchmark_db_medium", help="Named test DB to provision.")
    parser.add_argument("--database", default="", help="Existing database path to benchmark instead of provisioning.")
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR), help="Cache directory for named DB provisioning.")
    parser.add_argument("--regenerate", action="store_true", help="Force regeneration of named DB templates.")
    parser.add_argument("--keep-provisioned", action="store_true", help="Do not delete the temporary provisioned DB copy.")
    parser.add_argument("--iterations", type=int, default=7, help="Measured iterations per scenario.")
    parser.add_argument("--warmups", type=int, default=2, help="Warmup iterations per scenario.")
    parser.add_argument("--query", default="", help="Explicit search query. Defaults to a token from the first work title.")
    parser.add_argument(
        "--scenarios",
        default="open_database,work_list_title,work_search_global,work_detail,file_download,image_bytes",
        help="Comma-separated scenario list.",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress progress logging and only emit the final summary line.")
    parser.add_argument("--output", default="", help="Write JSON report to this path instead of stdout.")
    return parser.parse_args()


def _load_app(database_path: Path) -> ReadOnlyWebApplication:
    """
    Perform the load app operation under explicit file-format and conversion rules.

    Example:
        Exercise  load app through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param database_path: Value supplied for database path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    db = _open_database(database_path=str(database_path), db_type="sqlite")
    return ReadOnlyWebApplication(db)


def _drain_response(response) -> tuple[str, int]:
    """
    Perform the drain response operation under explicit file-format and conversion rules.

    Example:
        Exercise  drain response through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param response: Value supplied for response under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    body_bytes = 0
    try:
        for chunk in response.body:
            body_bytes += len(bytes(chunk))
    finally:
        if response.close is not None:
            response.close()
    return str(response.status), body_bytes


def _choose_query(app: ReadOnlyWebApplication, explicit_query: str) -> str:
    """
    Perform the choose query operation under explicit file-format and conversion rules.

    Example:
        Exercise  choose query through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param app: Value supplied for app under the utility contract.
    :param explicit_query: Value supplied for explicit query under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    text = str(explicit_query or "").strip()
    if text:
        return text
    rows = app.read_model.work_rows(sorted_by="title")
    if not rows:
        return "work"
    title = app._row_primary_text("works", rows[0])
    return first_alnum_query_term(title)


def _first_work_row(app: ReadOnlyWebApplication):
    """
    Perform the first work row operation under explicit file-format and conversion rules.

    Example:
        Exercise  first work row through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param app: Value supplied for app under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    rows = app.read_model.work_rows(sorted_by="title")
    if not rows:
        raise RuntimeError("No works rows are available for benchmarking.")
    return rows[0]


def _first_file_row(app: ReadOnlyWebApplication):
    """
    Perform the first file row operation under explicit file-format and conversion rules.

    Example:
        Exercise  first file row through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param app: Value supplied for app under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not app._table_exists("files"):
        return None
    rows = list(app.db.get_all_rows("files", iterator_return=False))
    return rows[0] if rows else None


def _first_image_row(app: ReadOnlyWebApplication):
    """
    Perform the first image row operation under explicit file-format and conversion rules.

    Example:
        Exercise  first image row through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param app: Value supplied for app under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not app._table_exists("images"):
        return None
    work_rows = app.read_model.work_rows(sorted_by="title")
    for work_row in work_rows:
        image_row = app.images.work_image_row(work_row)
        if image_row is not None:
            return image_row
    rows = list(app.db.get_all_rows("images", iterator_return=False))
    return rows[0] if rows else None


def run_read_path_benchmarks(
    *,
    db_name: str,
    database: str,
    cache_dir: str,
    regenerate: bool,
    keep_provisioned: bool,
    iterations: int,
    warmups: int,
    query: str,
    scenario_names: list[str],
    progress: Optional[Callable[[str], None]] = None,
) -> dict[str, object]:
    """
    Perform the run read path benchmarks operation under explicit file-format and conversion rules.

    Example:
        Exercise run read path benchmarks through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param db_name: Value supplied for db name under the utility contract.
    :param database: Value supplied for database under the utility contract.
    :param cache_dir: Value supplied for cache dir under the utility contract.
    :param regenerate: Value supplied for regenerate under the utility contract.
    :param keep_provisioned: Value supplied for keep provisioned under the utility
        contract.
    :param iterations: Value supplied for iterations under the utility contract.
    :param warmups: Value supplied for warmups under the utility contract.
    :param query: Search expression parsed or evaluated by the utility.
    :param scenario_names: Value supplied for scenario names under the utility contract.
    :param progress: Value supplied for progress under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if progress is not None:
        progress("preparing read-path benchmark target={}".format(db_name or database))
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        with resolved_benchmark_database(
            db_name=db_name,
            db_path=database,
            cache_dir=Path(cache_dir),
            regenerate=regenerate,
            keep_provisioned=keep_provisioned,
        ) as handle:
            if progress is not None:
                progress("resolved database source={} path={}".format(handle.source, handle.db_path))
            app = _load_app(handle.db_path)
            try:
                requested = set(scenario_names)
                needs_work_row = bool(requested.intersection({"work_list_title", "work_search_global", "work_detail"}))
                needs_query = bool(requested.intersection({"work_search_global"}))
                needs_file_row = "file_download" in requested
                needs_image_row = "image_bytes" in requested

                work_row = _first_work_row(app) if needs_work_row or needs_query else None
                work_id = None
                if work_row is not None:
                    raw_work_id = _row_value(work_row, app._id_column("works") or "work_id")
                    work_id = int(raw_work_id) if raw_work_id not in (None, "") else None

                file_row = _first_file_row(app) if needs_file_row else None
                image_row = _first_image_row(app) if needs_image_row else None
                query_text = _choose_query(app, query) if needs_query else str(query or "").strip()
                if progress is not None:
                    progress(
                        "prepared source={} work_id={} query={} file_row={} image_row={}".format(
                            handle.source,
                            work_id,
                            query_text,
                            "yes" if file_row is not None else "no",
                            "yes" if image_row is not None else "no",
                        )
                    )

                scenarios: dict[str, Callable[[], dict[str, object]]] = {
                    "open_database": lambda: _scenario_open_database(handle.db_path),
                    "work_list_title": lambda: _scenario_work_list_title(app),
                    "work_search_global": lambda: _scenario_work_search_global(app, query_text=query_text),
                    "work_detail": lambda: _scenario_work_detail(app, work_row=work_row),
                }
                if file_row is not None:
                    scenarios["file_download"] = lambda: _scenario_file_download(app, file_row=file_row)
                if image_row is not None:
                    scenarios["image_bytes"] = lambda: _scenario_image_bytes(app, image_row=image_row)

                results: list[dict[str, object]] = []
                for scenario_name in scenario_names:
                    scenario = scenarios.get(scenario_name)
                    if scenario is None:
                        if progress is not None:
                            progress("skipping {} reason=unsupported_or_missing_fixture_data".format(scenario_name))
                        results.append({"name": scenario_name, "skipped": True, "reason": "unsupported_or_missing_fixture_data"})
                        continue
                    results.append(
                        run_benchmark(
                            name=scenario_name,
                            func=scenario,
                            iterations=iterations,
                            warmups=warmups,
                            progress=(
                                (lambda message, source=handle.source, scenario_name=scenario_name: progress("{} {} {}".format(source, scenario_name, message)))
                                if progress is not None
                                else None
                            ),
                        )
                    )
            finally:
                app.db.close()

    return {
        "script": "benchmark_read_paths",
        "created_utc": utc_now_iso(),
        "environment": environment_payload(),
        "database": {
            "source": handle.source,
            "db_path": str(handle.db_path),
            "provision_root": str(handle.provision_root) if handle.provision_root is not None else "",
        },
        "inputs": {
            "db_name": db_name,
            "database": database,
            "iterations": iterations,
            "warmups": warmups,
            "query": query,
            "resolved_query": query_text,
            "work_id": work_id,
            "scenario_names": scenario_names,
        },
        "results": results,
    }


def _scenario_open_database(database_path: Path) -> dict[str, object]:
    """
    Perform the scenario open database operation under explicit file-format and conversion rules.

    Example:
        Exercise  scenario open database through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param database_path: Value supplied for database path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    with _open_database(database_path=str(database_path), db_type="sqlite") as db:
        works = int(db.get_record_count("works")) if "works" in set(db.get_tables()) else 0
        return {"works": works}


def _scenario_work_list_title(app: ReadOnlyWebApplication) -> dict[str, object]:
    """
    Perform the scenario work list title operation under explicit file-format and conversion rules.

    Example:
        Exercise  scenario work list title through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param app: Value supplied for app under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    rows = app.read_model.work_rows(sorted_by="title")
    first_title = app._row_primary_text("works", rows[0]) if rows else ""
    return {"count": len(rows), "first_title": first_title}


def _scenario_work_search_global(app: ReadOnlyWebApplication, *, query_text: str) -> dict[str, object]:
    """
    Perform the scenario work search global operation under explicit file-format and conversion rules.

    Example:
        Exercise  scenario work search global through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param app: Value supplied for app under the utility contract.
    :param query_text: Value supplied for query text under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    payload = app.read_model.search_results_payload(query_text=query_text, table_filter="works", limit=25, offset=0)
    return {
        "query": query_text,
        "total": int(payload["total"]),
        "first_table": str(payload["results"][0]["table"]) if payload["results"] else "",
    }


def _scenario_work_detail(app: ReadOnlyWebApplication, *, work_row) -> dict[str, object]:
    """
    Perform the scenario work detail operation under explicit file-format and conversion rules.

    Example:
        Exercise  scenario work detail through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param app: Value supplied for app under the utility contract.
    :param work_row: Value supplied for work row under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    payload = app.read_model.work_detail_payload(work_row)
    return {
        "title": str(payload["work"]["title"]),
        "credits": len(payload["credits"]),
        "files": len(payload["files"]),
        "related_tables": len(payload["related"]),
    }


def _scenario_file_download(app: ReadOnlyWebApplication, *, file_row) -> dict[str, object]:
    """
    Perform the scenario file download operation under explicit file-format and conversion rules.

    Example:
        Exercise  scenario file download through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param app: Value supplied for app under the utility contract.
    :param file_row: Value supplied for file row under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    file_id = int(file_row["file_id"])
    response = app._serve_file_download(str(file_id), {"wsgi.file_wrapper": None})
    status, body_bytes = _drain_response(response)
    return {
        "file_id": file_id,
        "status": status,
        "body_bytes": body_bytes,
    }


def _scenario_image_bytes(app: ReadOnlyWebApplication, *, image_row) -> dict[str, object]:
    """
    Perform the scenario image bytes operation under explicit file-format and conversion rules.

    Example:
        Exercise  scenario image bytes through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param app: Value supplied for app under the utility contract.
    :param image_row: Value supplied for image row under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    image_id = int(image_row["image_id"])
    stored = app.images.resolve_storage_image(image_row)
    if stored is not None:
        payload = stored.as_bytes()
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        elif not isinstance(payload, bytes):
            payload = bytes(payload)
        return {"image_id": image_id, "source": "storage", "body_bytes": len(payload)}
    target = app.images.resolve_image_target(image_row)
    if target is None:
        return {"image_id": image_id, "source": "missing", "body_bytes": 0}
    if target.mode == "local":
        return {"image_id": image_id, "source": "local", "body_bytes": len(Path(target.location).read_bytes())}
    return {"image_id": image_id, "source": "redirect", "body_bytes": 0}


def main(argv: Optional[list[str]] = None) -> int:
    """
    Perform the main operation under explicit file-format and conversion rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param argv: Value supplied for argv under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    args = parse_args()
    scenario_names = [one.strip() for one in str(args.scenarios).split(",") if one.strip()]
    progress = None if bool(args.quiet) else stderr_progress
    payload = run_read_path_benchmarks(
        db_name=str(args.db_name),
        database=str(args.database),
        cache_dir=str(args.cache_dir),
        regenerate=bool(args.regenerate),
        keep_provisioned=bool(args.keep_provisioned),
        iterations=max(1, int(args.iterations)),
        warmups=max(0, int(args.warmups)),
        query=str(args.query),
        scenario_names=scenario_names,
        progress=progress,
    )
    print_report_summary(payload)
    write_json_report(payload, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
