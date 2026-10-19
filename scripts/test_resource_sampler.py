#!/usr/bin/env python3
"""Sample Linux test resources independently of pytest; summarize durable logs.

The pytest plugin starts the sampler automatically with ``--telemetry-dir``.
After a run (including an interrupted run), use::

    python scripts/test_resource_sampler.py summary PATH_TO_TELEMETRY
"""

from __future__ import annotations

import argparse
import json
import os
import select
import shutil
import sys
import time
from datetime import UTC, datetime
from os import close as _close
from os import fsync as _fsync
from os import open as _open
from pathlib import Path
from typing import TextIO


def write_record(stream: TextIO, event: str, **fields: object) -> None:
    """Persist one complete record before returning to the test or sampler."""
    first_record = stream.tell() == 0
    record = {
        "event": event,
        "time": datetime.now(UTC).isoformat(),
        "monotonic": time.monotonic(),
        **fields,
    }
    stream.write(json.dumps(record, separators=(",", ":")) + "\n")
    stream.flush()
    # Tests can patch the shared os module until fixture teardown. Keep telemetry
    # on the original primitives so those doubles cannot break phase reporting.
    _fsync(stream.fileno())
    if first_record and sys.platform.startswith("linux"):
        # Persist the new filename as well as its contents on Linux filesystems.
        descriptor = _open(Path(stream.name).parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            _fsync(descriptor)
        finally:
            _close(descriptor)


def read_counters(path: Path, *, kilobytes: bool = False) -> dict[str, int]:
    """Read available procfs counters; disappearing processes are normal."""
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return {}
    counters = {}
    for line in lines:
        fields = line.replace(":", "", 1).split()
        if len(fields) >= 2 and fields[1].isdigit():
            multiplier = 1024 if kilobytes and fields[2:] == ["kB"] else 1
            counters[fields[0]] = int(fields[1]) * multiplier
    return counters


def process_info(path: Path) -> dict[str, object] | None:
    """Read process identity without exposing command lines or environment."""
    try:
        stat = (path / "stat").read_text()
        name, tail = stat.split("(", 1)[1].rsplit(")", 1)
        fields = tail.split()
        status = read_counters(path / "status", kilobytes=True)
        return {
            "pid": int(path.name),
            "ppid": int(fields[1]),
            "name": name,
            "state": fields[0],
            "start_ticks": int(fields[19]),
            "cpu_ticks": int(fields[11]) + int(fields[12]),
            "rss_bytes": status.get("VmRSS"),
            "peak_rss_bytes": status.get("VmHWM"),
            "swap_bytes": status.get("VmSwap"),
            "threads": status.get("Threads"),
        }
    except (OSError, ValueError, IndexError):
        return None


def resource_snapshot(pid: int, paths: list[str]) -> dict[str, object]:
    """Capture system pressure and the pytest process tree, excluding this sampler."""
    processes = {}
    for path in Path("/proc").iterdir():
        if path.name.isdigit() and (
            int(path.name) != os.getpid() or pid == os.getpid()
        ):
            info = process_info(path)
            if info is not None:
                processes[info["pid"]] = info
    selected = {pid} if pid in processes else set()
    while True:
        children = {
            child for child, info in processes.items() if info["ppid"] in selected
        }
        expanded = selected | children
        if expanded == selected:
            break
        selected = expanded
    tree = [processes[child] for child in sorted(selected)]
    disks = {}
    for path in paths:
        try:
            usage = shutil.disk_usage(path)
            disks[path] = dict(zip(("total", "used", "free"), usage, strict=True))
        except OSError:
            disks[path] = None
    vmstat = read_counters(Path("/proc/vmstat"))
    try:
        pressure = Path("/proc/pressure/memory").read_text().strip()
    except OSError:
        pressure = None
    return {
        "memory_bytes": {
            key: value
            for key, value in read_counters(
                Path("/proc/meminfo"), kilobytes=True
            ).items()
            if key
            in {
                "MemTotal",
                "MemAvailable",
                "MemFree",
                "Buffers",
                "Cached",
                "SwapTotal",
                "SwapFree",
                "Shmem",
                "Dirty",
                "Writeback",
                "Slab",
            }
        },
        "vm_counters": {
            key: vmstat[key]
            for key in ("oom_kill", "pswpin", "pswpout", "pgmajfault")
            if key in vmstat
        },
        "memory_pressure": pressure,
        "disks_bytes": disks,
        "processes": tree,
        "process_count": len(tree),
        "tree_rss_bytes": sum(item["rss_bytes"] or 0 for item in tree),
    }


def watch(pid: int, start_ticks: int, output: Path, interval: float, temp: str) -> int:
    """Sample until pytest exits or closes its control pipe; never signal pytest."""
    paths = list(dict.fromkeys(["/dev/shm", temp, str(output.parent)]))
    with output.open("x", encoding="utf-8") as stream:
        write_record(
            stream,
            "sampler_start",
            pid=pid,
            interval=interval,
            cpu_ticks_per_second=os.sysconf("SC_CLK_TCK"),
        )
        while True:
            target = process_info(Path("/proc") / str(pid))
            if (
                target is None
                or target["start_ticks"] != start_ticks
                or target["state"] == "Z"
            ):
                write_record(stream, "sampler_stop", reason="target_exited")
                return 0
            write_record(stream, "sample", **resource_snapshot(pid, paths))
            if select.select([sys.stdin], [], [], interval)[0]:
                write_record(stream, "sampler_stop", reason="control_pipe_closed")
                return 0


def summarize(directory: Path) -> dict[str, object]:
    """Stream logs, retaining last activity and resource extremes after a crash."""
    sessions = []
    resources = []
    for path in sorted(directory.glob("*.jsonl")):
        state = {
            "file": path.name,
            "session_finished": False,
            "active_test": None,
            "phase": None,
            "collectors": [],
            "invalid_records": 0,
        }
        last_sample = None
        peak_rss = 0
        minimum_available = None
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line in stream:
                try:
                    record = json.loads(line)
                    if not isinstance(record, dict) or "event" not in record:
                        raise ValueError("not a telemetry record")
                except ValueError:
                    state["invalid_records"] += 1
                    continue
                event = record["event"]
                state["last_event"] = record
                if event == "session_start":
                    state["worker"] = record["worker"]
                elif event == "test_start":
                    state["active_test"] = record["nodeid"]
                elif event == "phase_start":
                    state["phase"] = record["phase"]
                elif event == "phase_finish":
                    state["phase"] = None
                elif event == "test_finish":
                    state["last_completed_test"] = record["nodeid"]
                    state["active_test"] = None
                elif event == "collector_start":
                    state["collectors"].append(record["nodeid"])
                elif event == "collector_finish":
                    if record["nodeid"] in state["collectors"]:
                        state["collectors"].remove(record["nodeid"])
                elif event == "session_finish":
                    state["session_finished"] = True
                    state["exitstatus"] = record["exitstatus"]
                elif event == "sample":
                    last_sample = record
                    peak_rss = max(peak_rss, record["tree_rss_bytes"])
                    available = record["memory_bytes"].get("MemAvailable")
                    if available is not None:
                        minimum_available = (
                            available
                            if minimum_available is None
                            else min(minimum_available, available)
                        )
        if path.name.startswith("events-"):
            sessions.append(state)
        else:
            resources.append(
                {
                    "file": path.name,
                    "last_sample": last_sample,
                    "peak_tree_rss_bytes": peak_rss,
                    "minimum_available_bytes": minimum_available,
                    "invalid_records": state["invalid_records"],
                    "last_event": state.get("last_event"),
                }
            )
    return {"sessions": sessions, "resources": resources}


def main() -> int:
    """Expose the summary command and the plugin's internal sampler entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    summary = commands.add_parser("summary", help="Read completed or interrupted logs")
    summary.add_argument("directory", type=Path)
    sampler = commands.add_parser("_watch", help=argparse.SUPPRESS)
    sampler.add_argument("pid", type=int)
    sampler.add_argument("start_ticks", type=int)
    sampler.add_argument("output", type=Path)
    sampler.add_argument("interval", type=float)
    sampler.add_argument("temp")
    args = parser.parse_args()
    if args.command == "summary":
        if not args.directory.is_dir() or not any(args.directory.glob("*.jsonl")):
            parser.error("telemetry directory contains no JSONL logs")
        print(json.dumps(summarize(args.directory), indent=2))
        return 0
    return watch(args.pid, args.start_ticks, args.output, args.interval, args.temp)


if __name__ == "__main__":
    raise SystemExit(main())
