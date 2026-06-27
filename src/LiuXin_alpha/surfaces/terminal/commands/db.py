"""Database maintenance commands for the terminal interface."""

from __future__ import annotations

import errno
import os
import shutil
import signal
import sqlite3
import subprocess
import time

from dataclasses import dataclass
from pathlib import Path

from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI


@dataclass(frozen=True)
class _DbUnlockOptions:
    kill: bool
    kill9: bool
    sudo: bool
    wait_s: float
    clear_sidecars: bool
    run_integrity_check: bool
    dry_run: bool


@dataclass(frozen=True)
class _FileHolder:
    pid: int
    command: str
    path: str


def _parse_unlock_options(args: list[str]) -> _DbUnlockOptions:
    kill = False
    kill9 = False
    sudo = False
    wait_s = 3.0
    clear_sidecars = False
    run_integrity_check = True
    dry_run = False

    idx = 0
    while idx < len(args):
        token = str(args[idx]).strip()
        if token == "--kill":
            kill = True
            idx += 1
            continue
        if token in {"--kill9", "--kill-9", "--sigkill"}:
            kill = True
            kill9 = True
            idx += 1
            continue
        if token in {"--sudo", "--with-sudo"}:
            sudo = True
            idx += 1
            continue
        if token == "--clear-sidecars":
            clear_sidecars = True
            idx += 1
            continue
        if token == "--no-check":
            run_integrity_check = False
            idx += 1
            continue
        if token == "--dry-run":
            dry_run = True
            idx += 1
            continue
        if token == "--wait-s" or token.startswith("--wait-s="):
            if "=" in token:
                value = token.split("=", 1)[1].strip()
                idx += 1
            else:
                if idx + 1 >= len(args):
                    raise ValueError("Option --wait-s requires a numeric value.")
                value = str(args[idx + 1]).strip()
                idx += 2
            try:
                wait_s = float(value)
            except Exception as exc:
                raise ValueError("Option --wait-s requires a numeric value.") from exc
            if wait_s <= 0:
                raise ValueError("Option --wait-s must be > 0.")
            continue
        raise ValueError("Unknown option: {!r}".format(token))

    return _DbUnlockOptions(
        kill=bool(kill),
        kill9=bool(kill9),
        sudo=bool(sudo),
        wait_s=float(wait_s),
        clear_sidecars=bool(clear_sidecars),
        run_integrity_check=bool(run_integrity_check),
        dry_run=bool(dry_run),
    )


def _candidate_lock_paths(db_path: Path) -> tuple[Path, Path, Path]:
    return (
        db_path,
        Path(str(db_path) + "-wal"),
        Path(str(db_path) + "-shm"),
    )


def _run_sudo_command(
    args: list[str],
    *,
    timeout_s: float = 15.0,
    allowed_returncodes: set[int] | None = None,
) -> subprocess.CompletedProcess:
    sudo_path = shutil.which("sudo")
    if not sudo_path:
        raise ValueError("sudo is not available on this system.")
    command = [sudo_path, "-n"] + [str(part) for part in args]
    proc = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        timeout=float(timeout_s),
    )
    allowed = set(allowed_returncodes or {0})
    if proc.returncode in allowed:
        return proc

    stderr = str(proc.stderr or "").strip()
    lowered = stderr.lower()
    if "password is required" in lowered or "a password is required" in lowered:
        raise PermissionError("sudo credentials are required. Run `sudo -v` in shell, then retry.")
    if "not in the sudoers" in lowered:
        raise PermissionError("Current user is not allowed to run sudo commands.")
    raise RuntimeError("sudo command failed ({}): {}".format(proc.returncode, stderr or proc.stdout))


def _parse_lsof_output(stdout: str, *, exclude_pids: set[int]) -> list[_FileHolder]:
    holders: list[_FileHolder] = []
    seen: set[tuple[int, str]] = set()
    pid: int | None = None
    command = ""
    for raw_line in str(stdout or "").splitlines():
        line = str(raw_line).strip()
        if not line:
            continue
        key = line[0]
        value = line[1:]
        if key == "p":
            try:
                pid = int(value)
            except Exception:
                pid = None
            command = ""
            continue
        if key == "c":
            command = value
            continue
        if key == "n" and pid is not None:
            if pid in exclude_pids:
                continue
            token = (pid, value)
            if token in seen:
                continue
            seen.add(token)
            holders.append(
                _FileHolder(
                    pid=int(pid),
                    command=str(command),
                    path=str(value),
                )
            )
    holders.sort(key=lambda one: (one.pid, one.path))
    return holders


def _list_file_holders(
    paths: tuple[Path, ...],
    *,
    exclude_pids: set[int] | None = None,
    use_sudo: bool = False,
) -> list[_FileHolder]:
    lsof_path = shutil.which("lsof")
    if not lsof_path:
        return []
    exclude = set(exclude_pids or set())

    cmd = [lsof_path, "-w", "-n", "-Fpcn", "--"] + [str(path) for path in paths]
    if use_sudo:
        proc = _run_sudo_command(cmd, timeout_s=15.0, allowed_returncodes={0, 1})
    else:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
    if proc.returncode not in {0, 1}:
        return []

    return _parse_lsof_output(str(proc.stdout or ""), exclude_pids=exclude)


def _probe_database_write_lock(db_path: Path, *, timeout_s: float = 1.0) -> tuple[bool, str]:
    conn = sqlite3.connect(str(db_path), timeout=float(timeout_s), isolation_level=None)
    try:
        conn.execute("PRAGMA busy_timeout = {};".format(max(1, int(timeout_s * 1000.0))))
        conn.execute("BEGIN IMMEDIATE;")
        conn.execute("ROLLBACK;")
        return True, ""
    except sqlite3.OperationalError as exc:
        message = str(exc)
        lowered = message.lower()
        if "locked" in lowered or "busy" in lowered:
            return False, message
        raise
    finally:
        conn.close()


def _send_signal_to_pids(pids: set[int], sig: int) -> list[int]:
    signalled: list[int] = []
    for pid in sorted(pids):
        try:
            os.kill(int(pid), int(sig))
            signalled.append(int(pid))
        except OSError as exc:
            if exc.errno in {errno.ESRCH, errno.EPERM}:
                continue
            raise
    return signalled


def _send_signal_to_pids_via_sudo(pids: set[int], sig: int) -> list[int]:
    signalled: list[int] = []
    signal_num = int(sig)
    for pid in sorted(int(v) for v in pids if int(v) > 0):
        cmd = ["kill", "-{}".format(signal_num), str(pid)]
        try:
            _run_sudo_command(cmd, timeout_s=10.0)
            signalled.append(pid)
        except RuntimeError as exc:
            text = str(exc).lower()
            if "no such process" in text:
                continue
            if "operation not permitted" in text:
                continue
            raise
    return signalled


def _wait_for_holders_to_clear(
    paths: tuple[Path, ...],
    *,
    exclude_pids: set[int],
    timeout_s: float,
    use_sudo: bool = False,
    poll_s: float = 0.2,
) -> list[_FileHolder]:
    deadline = time.monotonic() + max(0.1, float(timeout_s))
    while True:
        holders = _list_file_holders(paths, exclude_pids=exclude_pids, use_sudo=bool(use_sudo))
        if not holders:
            return []
        if time.monotonic() >= deadline:
            return holders
        time.sleep(max(0.05, float(poll_s)))


def _run_recovery_pragmas(db_path: Path, *, run_integrity_check: bool) -> dict[str, object]:
    conn = sqlite3.connect(str(db_path), timeout=5.0, isolation_level=None)
    try:
        conn.execute("PRAGMA busy_timeout = 5000;")
        checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE);").fetchone()
        busy = int(checkpoint[0]) if checkpoint and len(checkpoint) > 0 else -1
        log_frames = int(checkpoint[1]) if checkpoint and len(checkpoint) > 1 else -1
        ckpt_frames = int(checkpoint[2]) if checkpoint and len(checkpoint) > 2 else -1
        integrity = None
        if run_integrity_check:
            row = conn.execute("PRAGMA integrity_check;").fetchone()
            integrity = "" if not row else str(row[0])
        return {
            "checkpoint_busy": busy,
            "checkpoint_log_frames": log_frames,
            "checkpoint_frames_checkpointed": ckpt_frames,
            "integrity_check": integrity,
        }
    finally:
        conn.close()


def _rename_sidecars_if_present(paths: tuple[Path, ...]) -> list[tuple[Path, Path]]:
    renamed: list[tuple[Path, Path]] = []
    suffix = str(int(time.time()))
    for sidecar in paths[1:]:
        if not sidecar.exists():
            continue
        candidate = sidecar.with_name(sidecar.name + ".stale." + suffix)
        serial = 1
        while candidate.exists():
            serial += 1
            candidate = sidecar.with_name(sidecar.name + ".stale.{}.{}".format(suffix, serial))
        sidecar.rename(candidate)
        renamed.append((sidecar, candidate))
    return renamed


class DbUnlockCommand(TerminalCommandAPI):
    """Recover a locked SQLite database by clearing external lock holders."""

    group = "db"
    group_aliases = ("database",)
    expose_direct = False
    name = "unlock"
    aliases = ("recover-lock", "unlock-db")
    summary = "Probe/repair DB lock state (optionally stop external holders)."
    usage = (
        "db unlock [--kill] [--kill9] [--sudo] [--wait-s <sec>] [--clear-sidecars] [--no-check] [--dry-run]"
    )

    def execute(self, browser, args: list[str]) -> bool:
        options = _parse_unlock_options(args)

        db_path_raw = str(browser.database_path or "").strip()
        if not db_path_raw:
            raise ValueError("Cannot resolve database path for this session.")
        db_path = Path(db_path_raw).expanduser()
        if not db_path.exists():
            raise ValueError("Database file does not exist: {!r}".format(str(db_path)))

        lock_paths = _candidate_lock_paths(db_path)
        self_pid = int(os.getpid())
        exclude_pids = {self_pid}

        browser.emit("DB unlock check: {}".format(str(db_path)))
        browser.emit("Self PID excluded from holder scan: {}".format(self_pid))

        writable, lock_error = _probe_database_write_lock(db_path)
        holders = _list_file_holders(lock_paths, exclude_pids=exclude_pids, use_sudo=False)
        if (not holders) and options.sudo:
            try:
                holders = _list_file_holders(lock_paths, exclude_pids=exclude_pids, use_sudo=True)
                if holders:
                    browser.emit("Holder scan via sudo found additional processes.")
            except Exception as exc:
                browser.emit("WARNING: sudo holder scan failed: {}".format(exc))

        if writable:
            browser.emit("Write lock probe: OK")
        else:
            browser.emit("Write lock probe: LOCKED ({})".format(lock_error))

        if holders:
            browser.emit("External holders:")
            for holder in holders:
                cmd = holder.command or "<unknown>"
                browser.emit("  pid={} cmd={} path={}".format(holder.pid, cmd, holder.path))
        else:
            browser.emit("External holders: none detected")

        if (not writable) and holders and options.kill:
            pids = {int(holder.pid) for holder in holders if int(holder.pid) > 0}
            if pids:
                if options.dry_run:
                    browser.emit("DRY RUN: would send SIGTERM to pids {}".format(sorted(pids)))
                else:
                    sent = _send_signal_to_pids(pids, signal.SIGTERM)
                    browser.emit("Sent SIGTERM to pids {}".format(sent))
                    remaining_pids = {int(pid) for pid in pids if int(pid) not in set(sent)}
                    if remaining_pids and options.sudo:
                        sent_sudo = _send_signal_to_pids_via_sudo(remaining_pids, signal.SIGTERM)
                        browser.emit("Sent sudo SIGTERM to pids {}".format(sent_sudo))
                    remaining = _wait_for_holders_to_clear(
                        lock_paths,
                        exclude_pids=exclude_pids,
                        timeout_s=options.wait_s,
                        use_sudo=options.sudo,
                    )
                    if remaining and options.kill9:
                        browser.emit("Holders remain after SIGTERM; sending SIGKILL.")
                        remaining_pid_set = {int(holder.pid) for holder in remaining if int(holder.pid) > 0}
                        sent_kill = _send_signal_to_pids(remaining_pid_set, signal.SIGKILL)
                        browser.emit("Sent SIGKILL to pids {}".format(sent_kill))
                        remaining_kill9 = {int(pid) for pid in remaining_pid_set if int(pid) not in set(sent_kill)}
                        if remaining_kill9 and options.sudo:
                            sent_sudo_kill = _send_signal_to_pids_via_sudo(remaining_kill9, signal.SIGKILL)
                            browser.emit("Sent sudo SIGKILL to pids {}".format(sent_sudo_kill))
                        remaining = _wait_for_holders_to_clear(
                            lock_paths,
                            exclude_pids=exclude_pids,
                            timeout_s=max(1.0, options.wait_s),
                            use_sudo=options.sudo,
                        )
                    holders = remaining
            writable, lock_error = _probe_database_write_lock(db_path)

        if (not writable) and (not options.kill):
            raise ValueError(
                "Database is still locked. Retry with `db unlock --kill` (and optionally `--kill9`)."
            )
        if (not writable) and holders:
            raise ValueError(
                "Database is still locked after kill attempt: {} (holders={})".format(
                    lock_error,
                    len(holders),
                )
            )
        if not writable:
            raise ValueError("Database is still locked: {}".format(lock_error))

        if options.clear_sidecars:
            if holders:
                raise ValueError("Refusing to rename sidecars while external holders are present.")
            if options.dry_run:
                browser.emit("DRY RUN: would rename sidecar files if present.")
            else:
                renamed = _rename_sidecars_if_present(lock_paths)
                if renamed:
                    browser.emit("Renamed sidecars:")
                    for old, new in renamed:
                        browser.emit("  {} -> {}".format(str(old), str(new)))
                else:
                    browser.emit("No sidecar files to rename.")

        if options.dry_run:
            browser.emit("DRY RUN complete.")
            return True

        report = _run_recovery_pragmas(
            db_path,
            run_integrity_check=options.run_integrity_check,
        )
        browser.emit(
            "WAL checkpoint(TRUNCATE): busy={} log_frames={} checkpointed={}".format(
                report["checkpoint_busy"],
                report["checkpoint_log_frames"],
                report["checkpoint_frames_checkpointed"],
            )
        )
        integrity = report.get("integrity_check")
        if integrity is not None:
            browser.emit("integrity_check: {}".format(str(integrity)))
            if str(integrity).strip().lower() != "ok":
                raise ValueError("Integrity check failed: {}".format(integrity))
        browser.emit("DB unlock completed.")
        return True


__all__ = [
    "DbUnlockCommand",
]
