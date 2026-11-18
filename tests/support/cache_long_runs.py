"""Opt-in cache workloads and per-operation evidence shared by the long lane."""

from __future__ import annotations

import cProfile
import hashlib
import json
import os
import platform
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from time import perf_counter

import pytest

BACKENDS = ("schema_backed", "database_backed", "numpy_vectorized")


def long_marks():
    """Evaluate opt-in at test-module import, after the runner sets its environment."""
    return [
        pytest.mark.slow,
        pytest.mark.cache_long,
        pytest.mark.skipif(
            os.environ.get("LIUXIN_RUN_CACHE_LONG_TESTS") != "1",
            reason="opt in with scripts/run_cache_long_tests.py",
        ),
    ]


@dataclass(frozen=True)
class Workload:
    """Fixed sizes keep smoke validation distinct from the longer acceptance run."""

    records: int
    steps: int
    database_records: int
    cycles: int
    seed: int = 20261117


WORKLOADS = {
    "smoke": Workload(records=128, steps=12, database_records=12, cycles=2),
    "full": Workload(records=10_000, steps=1_000, database_records=1_000, cycles=12),
}


class Measurements:
    """Time only operations; oracle scans and assertions stay outside samples."""

    def __init__(self, profile: bool):
        self.samples = defaultdict(list)
        self.profiler = cProfile.Profile() if profile else None
        self.depth = 0

    def measure(self, phase, operation, *args, **kwargs):
        if self.profiler is not None and self.depth == 0:
            self.profiler.enable()
        self.depth += 1
        start = perf_counter()
        try:
            return operation(*args, **kwargs)
        finally:
            elapsed = perf_counter() - start
            self.depth -= 1
            if self.profiler is not None and self.depth == 0:
                self.profiler.disable()
            self.samples[phase].append(elapsed)


@pytest.fixture
def workload():
    name = os.environ.get("LIUXIN_CACHE_LONG_SIZE", "full")
    if name not in WORKLOADS:
        pytest.fail(f"Unknown cache long workload: {name}")
    return WORKLOADS[name]


@pytest.fixture
def measurements(request, workload, tmp_path):
    """Retain partial timings even on failure; the runner owns the pass/fail audit."""
    result = Measurements(os.environ.get("LIUXIN_CACHE_LONG_CPROFILE") == "1")
    yield result
    root = Path(os.environ.get("LIUXIN_CACHE_LONG_ARTIFACTS", str(tmp_path)))
    root.mkdir(parents=True, exist_ok=True)
    nodeid = request.node.nodeid
    suffix = hashlib.sha256(nodeid.encode()).hexdigest()[:12]
    name = re.sub(r"[^a-zA-Z0-9_-]", "_", request.node.name) + "-" + suffix
    (root / (name + ".json")).write_text(
        json.dumps(
            {
                "nodeid": nodeid,
                "workload": asdict(workload),
                "python": platform.python_version(),
                "platform": platform.platform(),
                "profiled": result.profiler is not None,
                "samples_seconds": result.samples,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    if result.profiler is not None:
        result.profiler.dump_stats(root / (name + ".prof"))
