#!/usr/bin/env python3
"""
Provide shared benchmark setup and reporting helpers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise  benchmark common through a consuming regression::

        python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import sys
import tempfile
import time

from contextlib import contextmanager
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from statistics import mean, median
from typing import Callable, Iterator, Optional
from wsgiref.util import setup_testing_defaults


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

for candidate in (str(REPO_ROOT), str(SRC_ROOT)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from tests.support.test_resources_manager import TestResourcesManager  # noqa: E402
from LiuXin_alpha.constants.paths import LiuXin_data_folder  # noqa: E402


def _resolve_liuxin_data_dir() -> Path:
    """
    Perform the resolve liuxin data dir operation under explicit file-format and conversion rules.

    Example:
        Exercise  resolve liuxin data dir through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    raw = str(os.environ.get("LIUXIN_DATA_DIR", "")).strip()
    if raw:
        return Path(raw).expanduser()
    return Path(LiuXin_data_folder).expanduser()


DEFAULT_BENCHMARK_ROOT = _resolve_liuxin_data_dir() / "benchmarks"
DEFAULT_CACHE_DIR = DEFAULT_BENCHMARK_ROOT / "cache"
DEFAULT_PROVISIONED_DIR = DEFAULT_BENCHMARK_ROOT / "provisioned"
DEFAULT_RESULTS_DIR = DEFAULT_BENCHMARK_ROOT / "results"
DEFAULT_DATABASES_DIR = DEFAULT_BENCHMARK_ROOT / "databases"
ProgressCallback = Callable[[str], None]


@dataclass(frozen=True)
class BenchmarkDatabaseHandle:
    """
    Resolved benchmark database plus any temporary provision root.

    Example:
        Exercise BenchmarkDatabaseHandle through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
    """

    source: str
    db_path: Path
    provision_root: Optional[Path] = None


def utc_now_iso() -> str:
    """
    Perform the utc now iso operation under explicit file-format and conversion rules.

    Example:
        Exercise utc now iso through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def stderr_progress(message: str) -> None:
    """
    Perform the stderr progress operation under explicit file-format and conversion rules.

    Example:
        Exercise stderr progress through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param message: Value supplied for message under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    sys.__stderr__.write("[benchmark] {}\n".format(str(message)))
    sys.__stderr__.flush()


def environment_payload() -> dict[str, object]:
    """
    Perform the environment payload operation under explicit file-format and conversion rules.

    Example:
        Exercise environment payload through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return {
        "timestamp_utc": utc_now_iso(),
        "hostname": socket.gethostname(),
        "python": sys.version.split()[0],
        "cwd": str(REPO_ROOT),
    }


@contextmanager
def resolved_benchmark_database(
    *,
    db_name: str,
    db_path: str,
    cache_dir: Path,
    regenerate: bool,
    keep_provisioned: bool,
) -> Iterator[BenchmarkDatabaseHandle]:
    """
    Perform the resolved benchmark database operation under explicit file-format and conversion rules.

    Example:
        Exercise resolved benchmark database through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param db_name: Value supplied for db name under the utility contract.
    :param db_path: Value supplied for db path under the utility contract.
    :param cache_dir: Value supplied for cache dir under the utility contract.
    :param regenerate: Value supplied for regenerate under the utility contract.
    :param keep_provisioned: Value supplied for keep provisioned under the utility
        contract.
    :return: An iterator yielding the normalized values described above.
    """
    explicit_path = str(db_path or "").strip()
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Benchmark database path does not exist: {path}")
        yield BenchmarkDatabaseHandle(source="path", db_path=path, provision_root=None)
        return

    fixture_name = str(db_name or "").strip()
    if not fixture_name:
        raise ValueError("Either --db-name or --database must be provided.")

    cache_root = Path(cache_dir).expanduser()
    mgr = TestResourcesManager(cache_dir=cache_root, regenerate=regenerate)
    provision_parent = cache_root.parent / "provisioned"
    provision_parent.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="liuxin-benchmark-", dir=str(provision_parent)))
    try:
        provisioned = mgr.provision_named_test_database(name=fixture_name, dst_dir=root)
        yield BenchmarkDatabaseHandle(
            source=f"named:{fixture_name}",
            db_path=provisioned.db_path,
            provision_root=provisioned.root,
        )
    finally:
        if not keep_provisioned:
            shutil.rmtree(root, ignore_errors=True)


def summarize_durations_ms(durations_ms: list[float]) -> dict[str, object]:
    """
    Perform the summarize durations ms operation under explicit file-format and conversion rules.

    Example:
        Exercise summarize durations ms through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param durations_ms: Value supplied for durations ms under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not durations_ms:
        return {
            "iterations": 0,
            "min_ms": None,
            "mean_ms": None,
            "median_ms": None,
            "max_ms": None,
            "durations_ms": [],
        }
    return {
        "iterations": len(durations_ms),
        "min_ms": round(min(durations_ms), 3),
        "mean_ms": round(mean(durations_ms), 3),
        "median_ms": round(median(durations_ms), 3),
        "max_ms": round(max(durations_ms), 3),
        "durations_ms": [round(value, 3) for value in durations_ms],
    }


def run_benchmark(
    *,
    name: str,
    func: Callable[[], dict[str, object]],
    iterations: int,
    warmups: int,
    progress: Optional[ProgressCallback] = None,
) -> dict[str, object]:
    """
    Perform the run benchmark operation under explicit file-format and conversion rules.

    Example:
        Exercise run benchmark through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param func: Value supplied for func under the utility contract.
    :param iterations: Value supplied for iterations under the utility contract.
    :param warmups: Value supplied for warmups under the utility contract.
    :param progress: Value supplied for progress under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if progress is not None:
        progress("starting {} warmups={} iterations={}".format(name, max(0, warmups), max(1, iterations)))
    sample: dict[str, object] = {}
    for _ in range(max(0, warmups)):
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            sample = dict(func())

    durations_ms: list[float] = []
    for _ in range(max(1, iterations)):
        started = time.perf_counter()
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            sample = dict(func())
        durations_ms.append((time.perf_counter() - started) * 1000.0)

    payload = {
        "name": name,
        "warmups": max(0, warmups),
        **summarize_durations_ms(durations_ms),
        "sample": sample,
    }
    if progress is not None:
        progress(
            "finished {} mean={} median={} max={}".format(
                name,
                _format_duration_ms(payload.get("mean_ms")),
                _format_duration_ms(payload.get("median_ms")),
                _format_duration_ms(payload.get("max_ms")),
            )
        )
    return payload


def write_json_report(payload: dict[str, object], output_path: Optional[str]) -> None:
    """
    Write json report under the format's safety and compatibility rules.

    Example:
        Exercise write json report through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param payload: Value supplied for payload under the utility contract.
    :param output_path: Value supplied for output path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if not output_path:
        sys.stdout.write(text + "\n")
        sys.stdout.flush()
        return
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text + "\n", encoding="utf-8")


def build_wsgi_environ(*, path: str, method: str = "GET") -> dict[str, object]:
    """
    Perform the build wsgi environ operation under explicit file-format and conversion rules.

    Example:
        Exercise build wsgi environ through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param method: Value supplied for method under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    raw_path = str(path or "/")
    if "?" in raw_path:
        path_info, query_string = raw_path.split("?", 1)
    else:
        path_info, query_string = raw_path, ""
    environ: dict[str, object] = {}
    setup_testing_defaults(environ)
    environ["REQUEST_METHOD"] = str(method).upper()
    environ["PATH_INFO"] = path_info or "/"
    environ["QUERY_STRING"] = query_string
    environ["SERVER_NAME"] = "benchmark.local"
    environ["SERVER_PORT"] = "80"
    environ["HTTP_HOST"] = "benchmark.local"
    return environ


def consume_wsgi_response(app, path: str, *, method: str = "GET") -> dict[str, object]:
    """
    Perform the consume wsgi response operation under explicit file-format and conversion rules.

    Example:
        Exercise consume wsgi response through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param app: Value supplied for app under the utility contract.
    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param method: Value supplied for method under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    environ = build_wsgi_environ(path=path, method=method)
    captured: dict[str, object] = {"status": None, "headers": []}

    def start_response(status, headers, exc_info=None):
        """
        Perform the start response operation under explicit file-format and conversion rules.

        Example:
            Exercise consume wsgi response.start response through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param status: Value supplied for status under the utility contract.
        :param headers: Value supplied for headers under the utility contract.
        :param exc_info: Value supplied for exc info under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        del exc_info
        captured["status"] = str(status)
        captured["headers"] = [(str(key), str(value)) for key, value in headers]

    body_iterable = app(environ, start_response)
    body = b""
    try:
        for chunk in body_iterable:
            body += bytes(chunk)
    finally:
        close = getattr(body_iterable, "close", None)
        if callable(close):
            close()

    return {
        "status": captured["status"] or "",
        "headers": captured["headers"],
        "body_bytes": len(body),
    }


def first_alnum_query_term(text: str) -> str:
    """
    Perform the first alnum query term operation under explicit file-format and conversion rules.

    Example:
        Exercise first alnum query term through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for token in str(text or "").replace("_", " ").replace("-", " ").split():
        cleaned = "".join(char for char in token if char.isalnum())
        if len(cleaned) >= 3:
            return cleaned
    cleaned = "".join(char for char in str(text or "") if char.isalnum())
    return cleaned[:12] or "work"


def print_report_summary(payload: dict[str, object]) -> None:
    """
    Perform the print report summary operation under explicit file-format and conversion rules.

    Example:
        Exercise print report summary through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param payload: Value supplied for payload under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    sys.stderr.write(
        "[benchmark] script={script} target={target} scenarios={count}\n".format(
            script=payload.get("script", ""),
            target=payload.get("database", {}).get("source", ""),
            count=len(list(payload.get("results") or [])),
        )
    )
    sys.stderr.flush()


def _format_duration_ms(value: object) -> str:
    """
    Perform the format duration ms operation under explicit file-format and conversion rules.

    Example:
        Exercise  format duration ms through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param value: Value normalized, stored, formatted or returned.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if value is None:
        return "-"
    return "{:.3f}ms".format(float(value))
