"""Opt-in durable pytest activity logs with an independent Linux resource sampler."""

from __future__ import annotations

import math
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

from scripts import test_resource_sampler as sampler


def pytest_addoption(parser: pytest.Parser) -> None:
    """Keep ordinary runs unchanged; diagnostics require an explicit directory."""
    group = parser.getgroup("test telemetry")
    group.addoption(
        "--telemetry-dir",
        help="Write fsynced test activity and Linux resource JSONL logs here",
    )
    group.addoption(
        "--telemetry-interval",
        type=float,
        default=1.0,
        help="Resource sampling interval in seconds (minimum 0.1; default 1)",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Resolve paths before tests change cwd and before xdist copies options."""
    directory = config.getoption("telemetry_dir")
    if directory is None:
        return
    interval = config.getoption("telemetry_interval")
    if not math.isfinite(interval) or interval < 0.1:
        raise pytest.UsageError("--telemetry-interval must be finite and >= 0.1")
    directory = Path(directory).expanduser().resolve()
    config.option.telemetry_dir = str(directory)
    try:
        directory.mkdir(parents=True, exist_ok=True)
        recorder = Telemetry(config, directory, interval)
    except OSError as error:
        raise pytest.UsageError(f"Cannot start test telemetry: {error}") from error
    config.pluginmanager.register(recorder, "durable-test-telemetry")


class Telemetry:
    """Own one process's event stream and the controller's sampler lifetime."""

    def __init__(self, config: pytest.Config, directory: Path, interval: float):
        self.worker = getattr(config, "workerinput", {}).get("workerid", "controller")
        self.distributed_controller = self.worker == "controller" and bool(
            config.getoption("numprocesses", default=0)
        )
        token = f"{self.worker}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.directory = directory
        self.stream = (directory / f"events-{token}.jsonl").open("x", encoding="utf-8")
        self.monitor = None
        self.record(
            "session_start",
            worker=self.worker,
            pid=os.getpid(),
            python=sys.version,
            platform=sys.platform,
            workers=config.getoption("numprocesses", default=0),
        )
        if self.worker == "controller" and sys.platform.startswith("linux"):
            info = sampler.process_info(Path("/proc") / str(os.getpid()))
            if info is None:
                self.record("sampler_unavailable", reason="cannot read /proc/self/stat")
                return
            try:
                with (directory / f"sampler-{token}.stderr").open("wb") as errors:
                    self.monitor = subprocess.Popen(
                        [
                            sys.executable,
                            str(Path(sampler.__file__).resolve()),
                            "_watch",
                            str(os.getpid()),
                            str(info["start_ticks"]),
                            str(directory / f"resources-{token}.jsonl"),
                            str(interval),
                            tempfile.gettempdir(),
                        ],
                        stdin=subprocess.PIPE,
                        stdout=subprocess.DEVNULL,
                        stderr=errors,
                        start_new_session=True,
                    )
            except OSError:
                self.stream.close()
                raise
        elif self.worker == "controller":
            self.record(
                "sampler_unavailable", reason="resource sampling requires Linux"
            )

    def record(self, event: str, **fields: object) -> None:
        """Write immediately so a missing finish record is meaningful after a crash."""
        sampler.write_record(self.stream, event, **fields)

    def pytest_report_header(self) -> str:
        return f"Durable test telemetry: {self.directory}"

    @pytest.hookimpl(hookwrapper=True, tryfirst=True)
    def pytest_make_collect_report(self, collector: pytest.Collector):
        # Explicit file/node selections do not emit collectreport for every parent.
        # Wrapping the actual collection operation gives us a balanced pair.
        self.record("collector_start", nodeid=collector.nodeid)
        outcome = yield
        report = outcome.get_result()
        self.record("collector_finish", nodeid=collector.nodeid, outcome=report.outcome)

    def pytest_runtest_logstart(self, nodeid: str) -> None:
        if not self.distributed_controller:
            self.record("test_start", nodeid=nodeid)

    def pytest_runtest_logfinish(self, nodeid: str) -> None:
        if not self.distributed_controller:
            self.record("test_finish", nodeid=nodeid)

    @pytest.hookimpl(hookwrapper=True, tryfirst=True)
    def pytest_runtest_setup(self, item: pytest.Item):
        self.record("phase_start", nodeid=item.nodeid, phase="setup")
        yield
        self.record("phase_finish", nodeid=item.nodeid, phase="setup")

    @pytest.hookimpl(hookwrapper=True, tryfirst=True)
    def pytest_runtest_call(self, item: pytest.Item):
        self.record("phase_start", nodeid=item.nodeid, phase="call")
        yield
        self.record("phase_finish", nodeid=item.nodeid, phase="call")

    @pytest.hookimpl(hookwrapper=True, tryfirst=True)
    def pytest_runtest_teardown(self, item: pytest.Item):
        self.record("phase_start", nodeid=item.nodeid, phase="teardown")
        yield
        self.record("phase_finish", nodeid=item.nodeid, phase="teardown")

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if self.distributed_controller:
            return
        self.record(
            "test_report",
            nodeid=report.nodeid,
            phase=report.when,
            outcome=report.outcome,
            duration=report.duration,
        )

    def pytest_sessionfinish(self, exitstatus: int) -> None:
        self.record("session_finish", exitstatus=int(exitstatus))

    def pytest_unconfigure(self) -> None:
        """Close the sampler control pipe, with bounded cleanup if sampling hangs."""
        if self.monitor is not None:
            self.monitor.stdin.close()
            try:
                self.monitor.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.monitor.kill()
                self.monitor.wait(timeout=3)
            self.record("sampler_exit", returncode=self.monitor.returncode)
            if self.monitor.returncode != 0:
                print(
                    f"Test resource sampler failed; inspect {self.directory}",
                    file=sys.stderr,
                )
        self.stream.close()
