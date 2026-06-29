#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Callable, Optional

from _benchmark_common import (
    DEFAULT_CACHE_DIR,
    consume_wsgi_response,
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

from LiuXin_alpha.surfaces.api_readonly.app import ApiReadOnlyApplication  # noqa: E402
from LiuXin_alpha.surfaces.opds.api import encode_compat_token, opds_nav_token  # noqa: E402
from LiuXin_alpha.surfaces.opds_readonly.app import OpdsReadOnlyApplication  # noqa: E402
from LiuXin_alpha.surfaces.web_readonly.app import ReadOnlyWebApplication, _open_database  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark WSGI surface paths on a LiuXin database.")
    parser.add_argument("--db-name", default="metadata_rich_db_1", help="Named test DB to provision.")
    parser.add_argument("--database", default="", help="Existing database path to benchmark instead of provisioning.")
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR), help="Cache directory for named DB provisioning.")
    parser.add_argument("--regenerate", action="store_true", help="Force regeneration of named DB templates.")
    parser.add_argument("--keep-provisioned", action="store_true", help="Do not delete the temporary provisioned DB copy.")
    parser.add_argument("--iterations", type=int, default=7, help="Measured iterations per route.")
    parser.add_argument("--warmups", type=int, default=2, help="Warmup iterations per route.")
    parser.add_argument("--query", default="", help="Explicit search query. Defaults to a token from the first work title.")
    parser.add_argument(
        "--apps",
        default="web,api,opds",
        help="Comma-separated app list from: web, api, opds.",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress progress logging and only emit the final summary line.")
    parser.add_argument("--output", default="", help="Write JSON report to this path instead of stdout.")
    return parser.parse_args()


def _choose_query(web_app: ReadOnlyWebApplication, explicit_query: str) -> str:
    text = str(explicit_query or "").strip()
    if text:
        return text
    rows = web_app.read_model.work_rows(sorted_by="title")
    if not rows:
        return "work"
    return first_alnum_query_term(web_app._row_primary_text("works", rows[0]))


def run_surface_path_benchmarks(
    *,
    db_name: str,
    database: str,
    cache_dir: str,
    regenerate: bool,
    keep_provisioned: bool,
    iterations: int,
    warmups: int,
    query: str,
    app_names: list[str],
    progress: Optional[Callable[[str], None]] = None,
) -> dict[str, object]:
    if progress is not None:
        progress("preparing surface benchmark target={}".format(db_name or database))
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
            with _open_database(database_path=str(handle.db_path), db_type="sqlite") as db:
                web_app = ReadOnlyWebApplication(db)
                api_app = ApiReadOnlyApplication(db)
                opds_app = OpdsReadOnlyApplication(db)

                work_rows = web_app.read_model.work_rows(sorted_by="title")
                if not work_rows:
                    raise RuntimeError("No works rows are available for surface benchmarking.")
                work_row = work_rows[0]
                work_id = int(web_app.read_model.work_metadata_payload(work_row)["id"])
                query_text = _choose_query(web_app, query)
                if progress is not None:
                    progress("prepared source={} work_id={} query={} apps={}".format(handle.source, work_id, query_text, ",".join(app_names)))

                first_format = ""
                work_metadata = web_app.read_model.work_metadata_payload(work_row)
                if work_metadata.get("formats_detail"):
                    first_format = str(work_metadata["formats_detail"][0]["format"]).lower()

                route_sets: dict[str, tuple[object, dict[str, str]]] = {
                    "web": (
                        web_app,
                        {
                            "home": "/",
                            "search": "/search?global_q={}".format(query_text),
                            "work_detail": "/tables/works/{}".format(work_id),
                        },
                    ),
                    "api": (
                        api_app,
                        {
                            "index": "/api",
                            "works": "/api/works?sort=title&limit=25",
                            "work_detail": "/api/works/{}".format(work_id),
                            "search": "/api/search?q={}".format(query_text),
                        },
                    ),
                    "opds": (
                        opds_app,
                        {
                            "root": "/opds",
                            "titles": "/opds/navcatalog/{}".format(opds_nav_token("titles")),
                            "search": "/opds/search/{}".format(encode_compat_token(query_text)),
                        },
                    ),
                }
                if first_format:
                    route_sets["opds"][1]["download"] = "/get/{}/{}/main".format(first_format, work_id)

                results: list[dict[str, object]] = []
                for app_name in app_names:
                    route_set = route_sets.get(app_name)
                    if route_set is None:
                        if progress is not None:
                            progress("skipping app={} reason=unknown_app".format(app_name))
                        results.append({"name": app_name, "skipped": True, "reason": "unknown_app"})
                        continue
                    app, routes = route_set
                    if progress is not None:
                        progress("running app={} routes={}".format(app_name, ",".join(routes.keys())))
                    for route_name, path in routes.items():
                        scenario_name = "{}:{}".format(app_name, route_name)
                        results.append(
                            run_benchmark(
                                name=scenario_name,
                                func=_route_scenario(app, path),
                                iterations=iterations,
                                warmups=warmups,
                                progress=(
                                    (lambda message, source=handle.source, scenario_name=scenario_name: progress("{} {} {}".format(source, scenario_name, message)))
                                    if progress is not None
                                    else None
                                ),
                            )
                        )

    return {
        "script": "benchmark_surface_paths",
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
            "apps": app_names,
        },
        "results": results,
    }


def _route_scenario(app, path: str) -> Callable[[], dict[str, object]]:
    def _inner() -> dict[str, object]:
        response = consume_wsgi_response(app, path)
        return {
            "path": path,
            "status": str(response["status"]),
            "body_bytes": int(response["body_bytes"]),
        }

    return _inner


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args()
    app_names = [one.strip() for one in str(args.apps).split(",") if one.strip()]
    progress = None if bool(args.quiet) else stderr_progress
    payload = run_surface_path_benchmarks(
        db_name=str(args.db_name),
        database=str(args.database),
        cache_dir=str(args.cache_dir),
        regenerate=bool(args.regenerate),
        keep_provisioned=bool(args.keep_provisioned),
        iterations=max(1, int(args.iterations)),
        warmups=max(0, int(args.warmups)),
        query=str(args.query),
        app_names=app_names,
        progress=progress,
    )
    print_report_summary(payload)
    write_json_report(payload, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
