"""Opt-in mixed clients, real ledger cancellation and versioned metadata outcomes.

The background handler performs controlled local byte reads and hashing. It uses
the real jobs executor but does not measure an optional converter or network tool.
Timing excludes fixture construction and initial cache loading.
"""

import hashlib
import json
import math
import os
import platform
import threading
import time
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from contextlib import nullcontext
from pathlib import Path
from shutil import copyfile
from types import SimpleNamespace
from uuid import uuid4

import pytest

from LiuXin_alpha.core import CoreRuntime
from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.durable_jobs import DurableJobService
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.jobs.handler_api import JobHandlerAPI, JobHandlerRegistry
from LiuXin_alpha.jobs.models import JobDefinition
from LiuXin_alpha.jobs.worker import JobWorker
from tests.core.test_metadata_workflow_boundary import public_client
from tests.core.test_native_metadata_postgres_live import pg_database as pg_database
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)

pytestmark = [
    pytest.mark.slow,
    pytest.mark.core_long,
    pytest.mark.skipif(
        os.environ.get("LIUXIN_RUN_CORE_LONG") != "1",
        reason="use scripts/run_core_long_tests.py",
    ),
]
SIZES = {
    "smoke": (128, 2),
    "warm": (10_000, 60),
    "large": (100_000, 120),
    "million": (1_000_000, 300),
    "soak": (10_000, 8 * 60 * 60),
}


class Samples:
    """Retain bounded 2%-wide histograms even during an eight-hour soak."""

    def __init__(self):
        self.lock = threading.Lock()
        self.values = {}

    def add(self, name, seconds):
        bucket = math.ceil(math.log(max(seconds, 1e-9), 1.02))
        with self.lock:
            counts = self.values.setdefault(name, {})
            counts[bucket] = counts.get(bucket, 0) + 1

    def report(self):
        result = {}
        for name, counts in self.values.items():
            count = sum(counts.values())
            cumulative = 0
            for bucket, number in sorted(counts.items()):
                cumulative += number
                if cumulative >= math.ceil(count * 0.95):
                    result[name] = {"count": count, "p95_upper_seconds": 1.02**bucket}
                    break
        return result


class BackgroundRead(JobHandlerAPI):
    job_kind = "core.acceptance.background-read"

    def __init__(self, path):
        self.path = path
        self.entered = threading.Event()

    def validate_payload(self, payload_json):
        assert payload_json == "{}"

    def run(self, *, payload_json, run_context):
        with self.path.open("rb") as source:
            self.entered.set()
            while True:
                run_context.raise_if_cancel_requested()
                hashlib.sha256(source.read(1024 * 1024)).digest()
                source.seek(0)
                time.sleep(0.01)


def selected(name, defaults):
    value = os.environ.get(name, "all")
    return defaults if value == "all" else (value,)


@pytest.mark.parametrize(
    "backend",
    selected("LIUXIN_CORE_LONG_BACKEND", ("SQLite", "SQLite_apsw", "PostgreSQL")),
)
@pytest.mark.parametrize(
    "mode",
    selected(
        "LIUXIN_CORE_LONG_CACHE",
        ("none", "database_backed", "schema_backed", "numpy_vectorized"),
    ),
)
@pytest.mark.parametrize(
    "transport", selected("LIUXIN_CORE_LONG_TRANSPORT", ("local", "http"))
)
def test_mixed_clients_and_control_latency(
    backend, mode, transport, metadata_template, request, tmp_path
):
    size = os.environ.get("LIUXIN_CORE_LONG_SIZE", "warm")
    records, duration = SIZES[size]
    if backend == "PostgreSQL":
        if not os.environ.get("LIUXIN_TEST_POSTGRES_URL"):
            pytest.fail("Required PostgreSQL acceptance backend is not configured")
        database = request.getfixturevalue("pg_database")[0]
        owned = nullcontext(database)
    else:
        path = tmp_path / "library.sqlite"
        copyfile(metadata_template, path)
        owned = Database(
            metadata={"database_path": str(path)},
            db_type=backend,
            create=False,
            backup=False,
            repair_bootstrap_rows=False,
            enable_storage_manager=False,
            enable_maintenance=False,
        )
    measurements = Samples()
    report = {
        "size": size,
        "records": records,
        "duration_requested_seconds": duration,
        "backend": backend,
        "cache": mode,
        "transport": transport,
        "clients": 4,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "load_average_start": os.getloadavg() if hasattr(os, "getloadavg") else None,
    }
    artifact = Path(os.environ.get("LIUXIN_CORE_LONG_ARTIFACTS", str(tmp_path)))
    artifact.mkdir(parents=True, exist_ok=True)
    try:
        with owned as database:
            for start in range(0, records, 1000):
                database.macros.insert_rows(
                    "works",
                    (
                        {"work_id": i + 1, "work_title": f"Load {i:08d}"}
                        for i in range(start, min(start + 1000, records))
                    ),
                )
            service = DurableJobService(
                path=tmp_path / "jobs.sqlite",
                library_uuid=str(database.uuid),
                open_library=lambda: nullcontext(
                    SimpleNamespace(library=SimpleNamespace(db=database))
                ),
            )
            with CoreRuntime(
                library=SimpleNamespace(database=database),
                durable_jobs=service,
                cache_type=None if mode == "none" else mode,
            ) as runtime:
                with public_client(
                    runtime, transport == "http", timeout_seconds=120
                ) as (client, _):
                    client.query("metadata.read_for_edit", {"entity_id": 1})
                    data = tmp_path / "background.bin"
                    data.write_bytes(b"x" * (1024 * 1024))
                    handler = BackgroundRead(data)
                    registry = JobHandlerRegistry()
                    registry.register(handler)
                    background = service.repository.submit_job(
                        JobDefinition(
                            job_kind=handler.job_kind,
                            job_name="Acceptance byte reads",
                            payload_json="{}",
                        ),
                        request_key="background",
                    )
                    worker = JobWorker(repository=service.repository, handlers=registry)
                    thread = threading.Thread(target=worker.run_once)
                    thread.start()
                    assert handler.entered.wait(10)
                    stop = threading.Event()
                    deadline = time.monotonic() + duration

                    def timed(name, function, *args):
                        begin = time.perf_counter()
                        try:
                            return function(*args)
                        finally:
                            measurements.add(name, time.perf_counter() - begin)

                    def edits(index):
                        rounds = 0
                        title = f"Load {index:08d}"
                        while not stop.is_set() and time.monotonic() < deadline:
                            for _ in range(4):
                                value = timed(
                                    "read",
                                    client.query,
                                    "metadata.read_for_edit",
                                    {"entity_id": index + 1},
                                )
                                assert value["metadata"]["work"]["work_id"] == index + 1
                            title = f"Client {index} edit {rounds}"
                            command = CoreCommand(
                                "metadata.update",
                                {"entity_id": index + 1, "data": {"title": title}},
                            )
                            first = timed(
                                "write", client.execute_command, command
                            ).result
                            assert first["persistence"] == "committed"
                            if rounds % 3 == 0:
                                replay = timed(
                                    "replay", client.execute_command, command
                                ).result
                                assert replay["result"] == first["result"]
                            rounds += 1
                        return index, title, rounds

                    def controls():
                        while not stop.is_set() and time.monotonic() < deadline:
                            assert timed("health", client.query, "health.live", {})[
                                "live"
                            ]
                            pending = service.repository.submit_job(
                                JobDefinition(
                                    job_kind=handler.job_kind,
                                    job_name="Cancel queued acceptance work",
                                    payload_json="{}",
                                ),
                                request_key=str(uuid4()),
                            )
                            timed(
                                "cancel",
                                client.command,
                                "jobs.cancel",
                                {"job_id": service._job_id(pending.job_run_id)},
                            )
                            stop.wait(0.1)

                    started = time.monotonic()
                    try:
                        with ThreadPoolExecutor(max_workers=5) as pool:
                            control = pool.submit(controls)
                            futures = [pool.submit(edits, index) for index in range(4)]
                            done, _ = wait(
                                [control, *futures], return_when=FIRST_EXCEPTION
                            )
                            if any(future.exception() is not None for future in done):
                                stop.set()
                            outcomes = [future.result() for future in futures]
                            control.result()
                        for index, title, rounds in outcomes:
                            assert rounds > 0
                            saved = client.query(
                                "metadata.read_many", {"entity_ids": [index + 1]}
                            )
                            assert saved["entities"][0]["work"]["work_title"] == title
                        report["elapsed_seconds"] = time.monotonic() - started
                        report["samples"] = measurements.report()
                        # The reference SLO is defined at 10,000 Works. Larger
                        # datasets retain measurements without inventing a new SLO.
                        if size in {"warm", "soak"}:
                            for name, budget in {
                                "read": 1,
                                "write": 5,
                                "health": 0.25,
                                "cancel": 0.25,
                            }.items():
                                assert (
                                    report["samples"][name]["p95_upper_seconds"]
                                    <= budget
                                ), report["samples"]
                        report["passed"] = True
                    finally:
                        stop.set()
                        service.repository.cancel_action(background.job_run_id)
                        thread.join(10)
                        assert not thread.is_alive()
    finally:
        report["load_average_end"] = (
            os.getloadavg() if hasattr(os, "getloadavg") else None
        )
        if os.name == "posix":
            import resource

            peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            report["process_peak_rss_bytes_through_case"] = int(
                peak if platform.system() == "Darwin" else peak * 1024
            )
        report["samples"] = measurements.report()
        (artifact / f"{size}-{backend}-{mode}-{transport}.json").write_text(
            json.dumps(report, indent=2) + "\n"
        )
