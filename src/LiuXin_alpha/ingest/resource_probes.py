"""Read resource counters without scanning source trees or allocating probe files."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from functools import lru_cache
from pathlib import Path, PureWindowsPath


def memory_snapshot() -> dict[str, int | None]:
    """Return Linux process/system byte counters; unsupported counters remain null."""
    counters: dict[str, int] = {}
    for path in (Path("/proc/self/status"), Path("/proc/meminfo")):
        try:
            lines = path.read_text().splitlines()
        except OSError:
            continue
        for line in lines:
            fields = line.replace(":", "", 1).split()
            if len(fields) == 3 and fields[1].isdigit() and fields[2] == "kB":
                counters[fields[0]] = int(fields[1]) * 1024
    return {
        "process_rss_bytes": counters.get("VmRSS"),
        "process_peak_rss_bytes": counters.get("VmHWM"),
        "process_swap_bytes": counters.get("VmSwap"),
        "system_available_bytes": counters.get("MemAvailable"),
        "system_swap_free_bytes": counters.get("SwapFree"),
    }


def is_wsl() -> bool:
    """Identify WSL from the kernel, including sudo environments without WSL variables."""
    try:
        return "microsoft" in Path("/proc/sys/kernel/osrelease").read_text().lower()
    except OSError:
        return False


@lru_cache(maxsize=8)
def wsl_backing_path(distribution: str) -> Path:
    """Resolve this distribution's registered Windows volume, never assume C:.

    Read HKCU's WSL registry through a fixed PowerShell program. Distribution names
    travel as JSON data on stdin, not command text. Imported distributions and
    relocated VHDs use their registered BasePath. Unknown identity, inaccessible
    registry/interop, UNC volumes, or unavailable drive mounts fail visibly; callers
    can supply an explicit host path instead. Each subprocess has a five-second bound.
    """
    executable = shutil.which("powershell.exe")
    converter = shutil.which("wslpath")
    if not executable or not converter or not distribution:
        raise OSError(
            "cannot locate WSL backing volume; supply --host-space-path for its Windows drive"
        )
    script = r"""$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$name = [Console]::In.ReadToEnd() | ConvertFrom-Json
$rows = @(Get-ItemProperty 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss\*' |
    Where-Object { $_.DistributionName -eq $name })
if ($rows.Count -ne 1) { throw 'WSL distribution registry entry is not unique' }
[Console]::Out.Write(($rows[0].BasePath | ConvertTo-Json -Compress))
"""
    try:
        result = subprocess.run(
            [executable, "-NoProfile", "-NonInteractive", "-Command", script],
            input=json.dumps(distribution),
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        base = json.loads(result.stdout.lstrip("\ufeff"))
        if not isinstance(base, str):
            raise ValueError("invalid WSL registry path")
        drive = PureWindowsPath(base.removeprefix("\\\\?\\")).drive
        if len(drive) != 2 or drive[1] != ":" or not drive[0].isalpha():
            raise ValueError("WSL backing volume is not a local drive")
        result = subprocess.run(
            [converter, "-u", drive + "\\"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        path = Path(result.stdout.strip())
        if not path.is_absolute() or not path.is_dir() or not path.is_mount():
            raise ValueError("WSL backing drive is not mounted")
        return path
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        raise OSError(
            "cannot identify WSL backing volume; supply --host-space-path for its Windows drive"
        ) from error


def host_space_paths(explicit: tuple[Path, ...]) -> tuple[Path, ...]:
    """Use operator-selected volumes, or discover the WSL backing volume automatically."""
    if explicit:
        for path in explicit:
            if not path.is_dir():
                raise OSError(
                    f"host space path must be an existing mounted directory: {path}"
                )
        return explicit
    if is_wsl():
        return (wsl_backing_path(os.environ.get("WSL_DISTRO_NAME", "")),)
    return ()
