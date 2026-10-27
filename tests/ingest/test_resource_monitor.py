"""Check capacity admission, host discovery and telemetry during blocked ingest."""

from __future__ import annotations

import io
import json
import subprocess
import threading
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.ingest import resource_monitor as module
from LiuXin_alpha.ingest import resource_probes as probes
from LiuXin_alpha.ingest.resource_monitor import ResourceMonitor, ResourcePolicy
from LiuXin_alpha.storage.durable_manager import StorageManager
from LiuXin_alpha.storage.ingest import MixedFormatIngestCoordinator


@pytest.fixture(autouse=True)
def ordinary_host(monkeypatch):
    """Unit fixtures do not depend on the developer's Windows registry or available space."""
    monkeypatch.setattr(probes, "is_wsl", lambda: False)
    monkeypatch.setattr(
        module.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(free=10_000, total=20_000),
    )


@pytest.mark.parametrize(
    "role", ["catalogue", "cache", "temporary", "logs", "report", "lock", "host_0"]
)
def test_each_output_volume_can_stop_ingest(tmp_path, monkeypatch, role):
    """A low independent output filesystem stops even when other volumes have room."""
    output = tmp_path / role
    output.mkdir()
    monkeypatch.setattr(
        module.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(
            free=99 if path == output else 10_000, total=20_000
        ),
    )
    records = []
    with ResourceMonitor(
        ResourcePolicy(100), {role: output}, lambda *record: records.append(record)
    ) as monitor:
        assert role in monitor.check()
        assert monitor.latest["disks"][0]["free_bytes"] == 99
    assert records[0][0] == "resource_stop"


def test_shared_volume_admission_and_latched_reason(tmp_path, monkeypatch):
    """Cache publication copies and member spools add on a shared device, then remain stopped."""
    monkeypatch.setattr(
        module.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(free=699, total=20_000),
    )
    with ResourceMonitor(
        ResourcePolicy(100),
        {"cache": tmp_path, "temporary": tmp_path},
        lambda *args: None,
    ) as monitor:
        assert monitor.check() is None
        reason = monitor.check(200, 200)
        assert "700 required" in reason
        monkeypatch.setattr(
            module.shutil,
            "disk_usage",
            lambda path: SimpleNamespace(free=20_000, total=20_000),
        )
        assert monitor.check(0, 0) == reason


def test_exact_reserve_passes_and_future_directory_is_not_created(
    tmp_path, monkeypatch
):
    """Measure the nearest existing parent without creating the lazily owned cache."""
    future = tmp_path / "new" / "cache"
    monkeypatch.setattr(
        module.shutil, "disk_usage", lambda path: SimpleNamespace(free=100, total=1000)
    )
    with ResourceMonitor(
        ResourcePolicy(100), {"cache": future}, lambda *args: None
    ) as monitor:
        assert monitor.check() is None
        assert monitor.latest["disks"][0]["probed_path"] == str(tmp_path)
    assert not future.exists()


def test_unknown_capacity_is_not_healthy(tmp_path, monkeypatch):
    """An inaccessible output filesystem requests a stop, with evidence retained."""

    def unavailable(path):
        raise OSError("disconnected")

    monkeypatch.setattr(module.shutil, "disk_usage", unavailable)
    with ResourceMonitor(
        ResourcePolicy(100), {"cache": tmp_path}, lambda *args: None
    ) as monitor:
        assert "cannot measure cache" in monitor.check()
        assert monitor.latest["disks"][0]["error"] == "disconnected"


def test_background_sampling_keeps_member_context_and_stops(tmp_path, monkeypatch):
    """The sampler runs while the main thread is blocked and notices external disk consumption."""
    sampled = threading.Event()
    low = threading.Event()
    records = []

    def emit(event, details):
        records.append((event, details))
        if details["activity"].get("member_path") == "novel.epub":
            sampled.set()

    monkeypatch.setattr(
        module.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(free=1 if low.is_set() else 10_000, total=20_000),
    )
    with ResourceMonitor(
        ResourcePolicy(100, 0.1), {"temporary": tmp_path}, emit
    ) as monitor:
        monitor.progress(
            "member_started",
            {"container_path": "pack.squashfs", "member_path": "novel.epub"},
        )
        low.set()
        assert sampled.wait(3)
        assert "low space" in monitor.check()
    assert not monitor._thread.is_alive()
    assert records[-1][1]["activity"]["container_path"] == "pack.squashfs"
    assert "process_rss_bytes" in records[-1][1]["memory"]


def test_cache_growth_distinguishes_reserved_and_confirmed_bytes(tmp_path):
    """Do not mistake reservations or preexisting reused copies for new cache growth."""
    with ResourceMonitor(
        ResourcePolicy(100), {"cache": tmp_path}, lambda *args: None
    ) as monitor:
        monitor.progress("materialization_reserved", {"run_materialized_bytes": 500})
        monitor.progress("container_complete", {"materialized_bytes": 200})
        monitor.progress("container_complete", {"materialized_bytes": 0})
        monitor.sample(force_emit=True)
        assert monitor.latest["cache_materialized_bytes"] == 200
        assert monitor.latest["cache_reserved_bytes"] == 500


def test_telemetry_write_failure_stops_work(tmp_path):
    """A dead sampler output must not silently allow an unattended import to proceed."""

    def broken(*args):
        raise OSError("log full")

    with ResourceMonitor(ResourcePolicy(100), {"logs": tmp_path}, broken) as monitor:
        assert "cannot persist resource telemetry" in monitor.check()


def test_changed_host_mount_is_not_mistaken_for_healthy_parent(tmp_path, monkeypatch):
    """An unmounted drive can leave a directory with plenty of unrelated Linux free space."""
    with ResourceMonitor(
        ResourcePolicy(100, host_paths=(tmp_path,)), {}, lambda *args: None
    ) as monitor:
        original_stat = Path.stat

        def stat(path, *args, **kwargs):
            result = original_stat(path, *args, **kwargs)
            if path == tmp_path:
                return SimpleNamespace(st_mode=result.st_mode, st_dev=result.st_dev + 1)
            return result

        monkeypatch.setattr(Path, "stat", stat)
        monitor.sample()
        assert "host filesystem changed" in monitor.check()


def test_distinct_filesystems_do_not_sum_unrelated_demands(tmp_path, monkeypatch):
    """Separate cache and spool volumes need their own demands, not the combined total."""
    cache, temporary = tmp_path / "cache", tmp_path / "temporary"
    cache.mkdir()
    temporary.mkdir()
    original_stat = Path.stat

    def stat(path, *args, **kwargs):
        result = original_stat(path, *args, **kwargs)
        if path == temporary:
            return SimpleNamespace(st_mode=result.st_mode, st_dev=result.st_dev + 1)
        return result

    monkeypatch.setattr(Path, "stat", stat)
    monkeypatch.setattr(
        module.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(free=500 if path == cache else 300, total=1000),
    )
    with ResourceMonitor(
        ResourcePolicy(100),
        {"cache": cache, "temporary": temporary},
        lambda *args: None,
    ) as monitor:
        assert monitor.check(200, 200) is None


@pytest.mark.parametrize(
    "reserve,interval", [(0, 5), (-1, 5), (1, 0), (1, float("nan")), (1, float("inf"))]
)
def test_invalid_resource_controls(reserve, interval):
    with pytest.raises(ValueError):
        ResourcePolicy(reserve, interval)


def test_wsl_unknown_host_fails_closed_but_explicit_path_works(tmp_path, monkeypatch):
    monkeypatch.setattr(probes, "is_wsl", lambda: True)
    monkeypatch.setattr(probes.shutil, "which", lambda name: None)
    probes.wsl_backing_path.cache_clear()
    with ResourceMonitor(
        ResourcePolicy(100), {"catalogue": tmp_path}, lambda *args: None
    ) as monitor:
        assert "--host-space-path" in monitor.check()
    with ResourceMonitor(
        ResourcePolicy(100, host_paths=(tmp_path,)), {}, lambda *args: None
    ) as monitor:
        assert monitor.check() is None
        assert monitor.latest["disks"][0]["role"] == "host_0"


def test_wsl_detection_uses_registered_drive_and_treats_name_as_data(
    tmp_path, monkeypatch
):
    """A moved distribution on D: must not silently monitor C: instead."""
    monkeypatch.setattr(probes.shutil, "which", lambda name: name)
    monkeypatch.setattr(Path, "is_mount", lambda path: path == tmp_path)
    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        output = (
            json.dumps("\\\\?\\D:\\WSL\\books")
            if argv[0] == "powershell.exe"
            else str(tmp_path)
        )
        return subprocess.CompletedProcess(argv, 0, stdout=output)

    monkeypatch.setattr(probes.subprocess, "run", run)
    probes.wsl_backing_path.cache_clear()
    name = "Books'; throw 'bad"
    assert probes.wsl_backing_path(name) == tmp_path
    assert calls[0][1]["input"] == json.dumps(name)
    assert name not in calls[0][0][-1]
    assert calls[1][0] == ["wslpath", "-u", "D:\\"]
    assert all(call[1]["timeout"] == 5 for call in calls)
    probes.wsl_backing_path.cache_clear()


def test_automatic_wsl_probe_rejects_unmounted_drive_directory(tmp_path, monkeypatch):
    """A leftover directory must not substitute Linux free space for Windows capacity."""
    monkeypatch.setattr(probes.shutil, "which", lambda name: name)
    monkeypatch.setattr(Path, "is_mount", lambda path: False)

    def run(argv, **kwargs):
        output = (
            json.dumps("C:\\WSL\\books")
            if argv[0] == "powershell.exe"
            else str(tmp_path)
        )
        return subprocess.CompletedProcess(argv, 0, stdout=output)

    monkeypatch.setattr(probes.subprocess, "run", run)
    probes.wsl_backing_path.cache_clear()
    with pytest.raises(OSError, match="--host-space-path"):
        probes.wsl_backing_path("unmounted-drive-test")
    probes.wsl_backing_path.cache_clear()


def test_nested_cache_copy_is_refused_before_allocation_and_can_resume(
    tmp_path, monkeypatch
):
    """Actual nested ingest stops on combined copy demand and later reuses prior adoption."""
    source = tmp_path / "source"
    source.mkdir()
    inner_stream = io.BytesIO()
    with zipfile.ZipFile(inner_stream, "w") as archive:
        archive.writestr("novel.epub", b"book")
    inner = inner_stream.getvalue()
    with zipfile.ZipFile(source / "outer.zip", "w") as archive:
        archive.writestr("inner.zip", inner)
    free = [100 + len(inner) + 1]
    monkeypatch.setattr(
        module.shutil,
        "disk_usage",
        lambda path: SimpleNamespace(free=free[0], total=10000),
    )
    paths = {"cache": tmp_path / "cache", "temporary": tmp_path}
    with StorageManager() as manager:
        with ResourceMonitor(ResourcePolicy(100), paths, lambda *args: None) as monitor:
            report = MixedFormatIngestCoordinator(
                manager,
                materialization_root=paths["cache"],
                continue_on_error=False,
                resource_check_callback=monitor.check,
                progress_callback=monitor.progress,
            ).ingest(source)
            assert report.halt_reason.startswith("low space")
            assert report.materialized_bytes == 0
            assert report.members_adopted == 1
            assert monitor.cache_reserved_bytes == 0
        free[0] = 10000
        with ResourceMonitor(ResourcePolicy(100), paths, lambda *args: None) as monitor:
            report = MixedFormatIngestCoordinator(
                manager,
                materialization_root=paths["cache"],
                resource_check_callback=monitor.check,
                progress_callback=monitor.progress,
            ).ingest(source)
            assert report.ok
            assert report.materialized_bytes == len(inner)
            assert report.assets_created == 1
