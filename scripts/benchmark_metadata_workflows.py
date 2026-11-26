"""Measure real SQLite metadata picker/select/edit/save workflows and reservations.

Run serially on an otherwise idle machine for latency comparisons. Assertions
verify every result but remain outside operation timings. Capture and refresh
samples are nested in their parent operations; never add them to parent timings.
The contention experiment deliberately holds a writer for at least 100 ms and
reports this separately from ordinary latency. It imposes no timing threshold.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from contextlib import contextmanager
from pathlib import Path
from threading import Event
from time import perf_counter
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from LiuXin_alpha.caches.cache import create_cache  # noqa: E402
from LiuXin_alpha.caches.metadata import CatalogCache, MetadataAPI  # noqa: E402
from LiuXin_alpha.catalog.catalog import Catalog  # noqa: E402
from LiuXin_alpha.databases.database import Database  # noqa: E402
from scripts.run_cache_long_tests import source_hashes  # noqa: E402
from tests.support.cache_long_runs import BACKENDS, Measurements  # noqa: E402


@contextmanager
def observe(cache, measurements):
    """Record actual live captures and full refreshes without replacing their work."""
    from contextlib import ExitStack

    with ExitStack() as stack:
        if cache is not None:
            for owner, name, phase in (
                (cache.storage, "query_view", "live_capture"),
                (cache, "reload_data", "full_data_refresh"),
                (cache, "reload", "full_schema_refresh"),
            ):
                original = getattr(owner, name, None)
                if callable(original):

                    def measured(*args, _original=original, _phase=phase, **kwargs):
                        if measurements.depth == 0:
                            return _original(*args, **kwargs)
                        return measurements.measure(_phase, _original, *args, **kwargs)

                    stack.enter_context(patch.object(owner, name, measured))
        yield


def workflows(database, works, tags, mode, repeats, profile_path=None):
    """Exercise each operation against a real native catalogue and check readback."""
    measurements = Measurements(profile=profile_path is not None)
    cache = (
        None
        if mode is None
        else measurements.measure("load", create_cache, database, mode)
    )
    catalog = Catalog(database)
    api = MetadataAPI(catalog) if cache is None else CatalogCache(catalog, cache=cache)
    mode_offset = (None, *BACKENDS).index(mode)
    try:
        with observe(cache, measurements):
            for index in range(repeats):
                page = measurements.measure(
                    "picker", api.tags.suggest, "Topic", limit=17
                )
                assert page.total_count == len(tags)
                assert tuple(record.entity_id for record in page.records) == tags[:17]
                selected = measurements.measure(
                    "select", api.tags.require, tags[index % len(tags)]
                )
                assert selected.entity_id == tags[index % len(tags)]
                results = measurements.measure(
                    "hydrated_search", api.works.search, "work.title:Document", limit=7
                )
                assert results.total_count == len(works)
                assert len(results.records) == min(7, len(works))
                work = works[index % len(works)]
                snapshot = measurements.measure(
                    "read_for_edit", api.works.read_for_edit, work
                )
                title = f"Document edited {mode} {index}"
                receipt = measurements.measure(
                    "save",
                    api.works.update,
                    work,
                    {"title": title},
                    expected_revision=snapshot.revision,
                )
                assert receipt.persistence == "committed"
                assert api.works.require(work).work.work_title == title
                membership = tuple(
                    tags[(index + mode_offset + offset) % len(tags)]
                    for offset in range(7)
                )
                receipt = measurements.measure(
                    "save_relationship", api.works.set_tags, work, membership
                )
                assert receipt.persistence == "committed"
                assert {
                    value.member.entity_id for value in api.works.list_tags(work)
                } == set(membership)
    finally:
        if cache is not None:
            cache.close()
        if measurements.profiler is not None:
            measurements.profiler.dump_stats(profile_path)
    return dict(measurements.samples)


def contention(first_db, other_db, tags):
    """Demonstrate that a reservation delays a disjoint cooperating writer too."""
    first = MetadataAPI(Catalog(first_db))
    other = MetadataAPI(Catalog(other_db))
    entered, started, release = Event(), Event(), Event()

    def hold(current):
        current.tags.update(tags[0], {"name": "Topic held"})
        entered.set()
        assert release.wait(30)

    def compete():
        started.set()
        start = perf_counter()
        other.tags.update(tags[-1], {"name": "Topic independent"})
        return perf_counter() - start

    with ThreadPoolExecutor(max_workers=2) as pool:
        owner = pool.submit(first.batch, hold)
        try:
            assert entered.wait(30)
            waiting = pool.submit(compete)
            assert started.wait(30)
            try:
                waiting.result(timeout=0.1)
            except TimeoutError:
                blocked = True
            else:
                blocked = False
        finally:
            release.set()
        owner.result(timeout=30)
        duration = waiting.result(timeout=30)
    assert blocked, "disjoint writer bypassed the metadata writer reservation"
    assert other.tags.require(tags[-1]).fields["name"] == "Topic independent"
    return {
        "deliberate_hold_seconds": 0.1,
        "competing_save_seconds": duration,
        "disjoint_writer_blocked": blocked,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tags", type=int, default=1000)
    parser.add_argument("--works", type=int, default=100)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Save operation profiles; timings include profiler overhead",
    )
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.tags < 8 or min(args.works, args.repeats) < 2:
        parser.error("tags must be at least eight; works and repeats at least two")
    artifacts = args.artifacts.resolve()
    if artifacts == ROOT or ROOT in artifacts.parents:
        parser.error("Keep benchmark artifacts outside the checkout")
    artifacts.mkdir(parents=True, exist_ok=False)
    hashes = source_hashes()
    hashes["scripts/benchmark_metadata_workflows.py"] = hashlib.sha256(
        Path(__file__).read_bytes()
    ).hexdigest()
    report = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "tags": args.tags,
        "works": args.works,
        "repeats": args.repeats,
        "profiled": args.profile,
        "source_sha256": hashes,
        "modes": {},
    }
    (artifacts / "run.json").write_text(json.dumps(report, indent=2) + "\n")
    options = dict(
        backup=False,
        repair_bootstrap_rows=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    )
    with tempfile.TemporaryDirectory(prefix="liuxin-metadata-workflow-") as scratch:
        metadata = {"database_path": str(Path(scratch) / "catalog.sqlite")}
        with Database(metadata=metadata, create=True, **options) as database:
            api = MetadataAPI(Catalog(database))

            def seed(current):
                tags = tuple(
                    current.tags.create({"name": f"Topic {i:05d}"}).result
                    for i in range(args.tags)
                )
                works = tuple(
                    current.works.create({"title": f"Document {i:05d}"}).result
                    for i in range(args.works)
                )
                return tags, works

            tags, works = api.batch(seed).result
            for mode in (None, *BACKENDS):
                report["modes"][str(mode)] = workflows(
                    database,
                    works,
                    tags,
                    mode,
                    args.repeats,
                    artifacts / f"{mode or 'uncached'}.prof" if args.profile else None,
                )
                (artifacts / "partial.json").write_text(
                    json.dumps(report, indent=2) + "\n"
                )
                print(f"Completed {mode or 'uncached'}", flush=True)
            with Database(metadata=metadata, **options) as independent:
                report["contention"] = contention(database, independent, tags)
    report["completed"] = True
    (artifacts / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
