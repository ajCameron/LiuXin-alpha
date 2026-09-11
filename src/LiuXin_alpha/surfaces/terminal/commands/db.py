"""
Inspect SQLite write-lock availability and optionally signal holders or run recovery operations.

The grouped ``db unlock`` command uses local filesystem paths, SQLite connections,
and optional ``lsof``/noninteractive ``sudo`` subprocesses. It is not a generic
remote-database unlock facility. Holder detection is best effort: an empty list
can mean the inspection tool is unavailable, not that exclusive access is proven.

Opt-in process signals and sidecar renames can disrupt other sessions. The command
does not coordinate all writers or provide transactional rollback of those effects.
Dry-run mode suppresses those operations and recovery pragmas, but still opens the
database for a write-lock probe and performs holder inspection.
"""

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
    """
    Hold parsed process-control, wait, sidecar, integrity, and dry-run choices.

    ``kill`` permits SIGTERM and ``kill9`` permits escalation; ``sudo`` enables
    privileged fallbacks. ``wait_s`` is the requested holder-polling timeout in
    seconds. The remaining flags control sidecar renaming, integrity reporting,
    and suppression of recovery effects. Direct construction does not validate
    combinations or values; the parser establishes its own defaults.

    Example:
        >>> options = _parse_unlock_options([])
        >>> options.kill, options.wait_s, options.run_integrity_check
        (False, 3.0, True)
    """

    kill: bool
    kill9: bool
    sudo: bool
    wait_s: float
    clear_sidecars: bool
    run_integrity_check: bool
    dry_run: bool


@dataclass(frozen=True)
class _FileHolder:
    """
    Record a process and one advertised open-file path from an lsof scan.

    ``pid`` identifies the process, ``command`` is its reported command name, and
    ``path`` is the unverified path text. A holder is not proof that the process
    owns the SQLite write lock; one process may have several records.

    Example:
        >>> holder = _FileHolder(pid=123, command="reader", path="library.sqlite")
        >>> holder.pid, holder.path
        (123, 'library.sqlite')
    """

    pid: int
    command: str
    path: str


def _parse_unlock_options(args: list[str]) -> _DbUnlockOptions:
    """
    Parse unlock flags without performing any database, filesystem, or process operation.

    SIGKILL aliases also enable SIGTERM. Repeated switches are accepted; the last
    wait value wins. Wait values are rejected when conversion fails or they compare
    less than or equal to zero; non-finite floats are not explicitly rejected.

    Example:
        >>> options = _parse_unlock_options(["--kill9", "--wait-s=2", "--dry-run"])
        >>> options.kill, options.kill9, options.wait_s, options.dry_run
        (True, True, 2.0, True)


    :param args: Option tokens, with a separate value or equals form for ``--wait-s``.
    :return: Frozen options with defaults filled and requested aliases normalized.
    :raises ValueError: If a token is unknown or the wait argument is missing or rejected.
    """
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
    """
    Derive the database, WAL, and shared-memory paths in the order used by holder scans.

    Paths are constructed by suffixing the supplied spelling, without resolving
    symlinks, expanding home directories, or checking existence.

    Example:
        >>> [path.name for path in _candidate_lock_paths(Path("library.sqlite"))]
        ['library.sqlite', 'library.sqlite-wal', 'library.sqlite-shm']


    :param db_path: Database path whose string form receives the sidecar suffixes.
    :return: Three paths, with the database first so sidecar operations can skip it.
    """
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
    """
    Run one command through noninteractive sudo and classify unsuccessful exit results.

    Arguments are passed as an argv list rather than a shell expression. Captured
    stderr identifies password-required and sudoers-denial messages; other rejected
    exit statuses raise ``RuntimeError``. An empty allowed-code set uses the same
    default as ``None`` and therefore still accepts zero.

    Example:
        >>> result = _run_sudo_command(["lsof", "-v"])  # doctest: +SKIP


    :param args: Executable and argument tokens appended after the discovered sudo path and ``-n``.
    :param timeout_s: Subprocess timeout in seconds, converted to float.
    :param allowed_returncodes: Accepted exit statuses; falsey values select ``{0}``.
    :return: Completed process with captured text stdout/stderr and an accepted exit status.
    :raises ValueError: If sudo cannot be found on PATH.
    :raises PermissionError: If recognized stderr indicates missing credentials or sudoers denial.
    :raises RuntimeError: If the exit status is rejected without a recognized permission message.
    :raises subprocess.TimeoutExpired: If the subprocess exceeds the requested timeout.
    """
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
        raise PermissionError(
            "sudo credentials are required. Run `sudo -v` in shell, then retry."
        )
    if "not in the sudoers" in lowered:
        raise PermissionError("Current user is not allowed to run sudo commands.")
    raise RuntimeError(
        "sudo command failed ({}): {}".format(proc.returncode, stderr or proc.stdout)
    )


def _parse_lsof_output(stdout: str, *, exclude_pids: set[int]) -> list[_FileHolder]:
    """
    Decode lsof's process/command/name fields into sorted, deduplicated holder records.

    A process field resets the remembered command; an invalid PID disables name
    records until another valid process field. Blank and unrelated lines are
    ignored. Paths and command names remain unvalidated text, with outer whitespace
    stripped as part of line parsing.

    Example:
        >>> output = chr(10).join(["p12", "cpython", "nlibrary.sqlite", "nlibrary.sqlite"])
        >>> _parse_lsof_output(output, exclude_pids=set())
        [_FileHolder(pid=12, command='python', path='library.sqlite')]


    :param stdout: Field-format lsof output using ``p``, ``c``, and ``n`` line prefixes.
    :param exclude_pids: Process IDs whose file records must be omitted.
    :return: One record per retained PID/path pair, sorted by PID then path.
    """
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
    """
    Inspect the requested paths with lsof, optionally through the sudo wrapper.

    Missing lsof or an unexpected unprivileged exit status returns an empty list;
    this is not proof that no holders exist. Exit statuses zero and one are parsed.
    The direct subprocess has no timeout; the sudo path uses a fifteen-second
    timeout and propagates wrapper failures. Subprocess launch errors also escape.

    Example:
        >>> holders = _list_file_holders(paths, exclude_pids={os.getpid()})  # doctest: +SKIP


    :param paths: Paths appended after lsof's option terminator, without existence filtering.
    :param exclude_pids: Optional process IDs omitted from the decoded results.
    :param use_sudo: Whether to invoke lsof through noninteractive sudo.
    :return: Decoded holder records, or an empty list for no records or the described tool failures.
    """
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


def _probe_database_write_lock(
    db_path: Path, *, timeout_s: float = 1.0
) -> tuple[bool, str]:
    """
    Open SQLite and attempt an immediate transaction followed by rollback.

    The connection is writable and can create a missing database; this is not a
    read-only probe. Once connected, it is always closed. Only operational errors
    whose messages contain ``locked`` or ``busy`` become a negative result; other
    errors propagate. A successful probe does not reserve access for later recovery.

    Example:
        >>> writable, reason = _probe_database_write_lock(database_path)  # doctest: +SKIP


    :param db_path: Local SQLite path to open with autocommit transaction control.
    :param timeout_s: Connection wait in seconds; the busy pragma uses at least one millisecond.
    :return: ``(True, "")`` after rollback, or ``(False, message)`` for a recognized busy/lock error.
    """
    conn = sqlite3.connect(str(db_path), timeout=float(timeout_s), isolation_level=None)
    try:
        conn.execute(
            "PRAGMA busy_timeout = {};".format(max(1, int(timeout_s * 1000.0)))
        )
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
    """
    Send a signal directly to sorted process IDs, skipping missing or permission-denied targets.

    Targets are converted to integers but are not filtered for positivity or for
    the current process. Callers must establish safe targets before invoking this
    helper. A successful send is not confirmation that the process exited.

    Example:
        >>> _send_signal_to_pids(set(), signal.SIGTERM)
        []


    :param pids: Process IDs to signal; nonpositive values are forwarded rather than rejected.
    :param sig: Signal number passed to ``os.kill`` after integer conversion.
    :return: Successfully signalled IDs in processing order; ESRCH and EPERM targets are omitted.
    :raises OSError: If signalling fails for a reason other than a missing process or permission denial.
    """
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
    """
    Signal positive process IDs with separate noninteractive sudo kill commands.

    Runtime-error messages for missing processes or denied operations are skipped.
    Other wrapper errors, including credential failures and timeouts, propagate.
    Each target has a ten-second subprocess timeout; there is no aggregate timeout.

    Example:
        >>> _send_signal_to_pids_via_sudo(set(), signal.SIGTERM)
        []


    :param pids: IDs converted to integers, filtered to positive values, then processed in sorted order.
    :param sig: Signal number included in each kill command's numeric signal option.
    :return: IDs whose sudo commands succeeded, without waiting for those processes to exit.
    """
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
    """
    Repeatedly inspect holder records until none are reported or the polling deadline is reached.

    The first scan is immediate. Timeout and sleep requests clamp to at least
    0.1 and 0.05 seconds respectively. The deadline is checked after each scan,
    so scans and sleeps can overshoot it; an unbounded direct lsof call can delay
    it indefinitely. Empty results inherit the scanner's tool-unavailable ambiguity.

    Example:
        >>> remaining = _wait_for_holders_to_clear(  # doctest: +SKIP
        ...     paths, exclude_pids={os.getpid()}, timeout_s=3.0
        ... )


    :param paths: Paths to inspect on every scan.
    :param exclude_pids: Process IDs omitted from all scans.
    :param timeout_s: Requested elapsed polling duration in seconds, not a hard subprocess deadline.
    :param use_sudo: Whether each scan uses the privileged lsof wrapper.
    :param poll_s: Requested sleep between nonempty scans, in seconds.
    :return: Empty list when a scan reports no holders, otherwise the last records at timeout.
    """
    deadline = time.monotonic() + max(0.1, float(timeout_s))
    while True:
        holders = _list_file_holders(
            paths, exclude_pids=exclude_pids, use_sudo=bool(use_sudo)
        )
        if not holders:
            return []
        if time.monotonic() >= deadline:
            return holders
        time.sleep(max(0.05, float(poll_s)))


def _run_recovery_pragmas(
    db_path: Path, *, run_integrity_check: bool
) -> dict[str, object]:
    """
    Attempt a truncating WAL checkpoint and optionally read the first integrity-check result.

    Opens a writable SQLite connection with a five-second busy timeout and closes
    it afterward. A busy checkpoint is reported numerically, not raised as failure.
    Only the first integrity row is retained; interpretation belongs to the caller.
    This helper does not guarantee repair or exclusive access.

    Example:
        >>> report = _run_recovery_pragmas(database_path, run_integrity_check=True)  # doctest: +SKIP


    :param db_path: Local SQLite path to open; a missing file can be created by the connection.
    :param run_integrity_check: Whether to execute the integrity pragma after checkpointing.
    :return: Mapping of three checkpoint counters and the first integrity text, or ``None`` if disabled.
    """
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
    """
    Move existing paths after the first entry to timestamped stale-file names.

    The first path is assumed to be the database and is skipped. Destination
    collisions detected before renaming add serial suffixes starting at two; the
    existence check is not an atomic reservation. No holder or SQLite-state check
    is performed here, and a later failure does not undo earlier renames. Renaming
    active sidecars can disrupt a live database; the caller must assess safety.

    Example:
        >>> _rename_sidecars_if_present((Path("database-entry-is-skipped"),))
        []


    :param paths: Database path followed by sidecar paths; names and file types are not validated.
    :return: Original/destination path pairs for completed renames, in input order.
    """
    renamed: list[tuple[Path, Path]] = []
    suffix = str(int(time.time()))
    for sidecar in paths[1:]:
        if not sidecar.exists():
            continue
        candidate = sidecar.with_name(sidecar.name + ".stale." + suffix)
        serial = 1
        while candidate.exists():
            serial += 1
            candidate = sidecar.with_name(
                sidecar.name + ".stale.{}.{}".format(suffix, serial)
            )
        sidecar.rename(candidate)
        renamed.append((sidecar, candidate))
    return renamed


class DbUnlockCommand(TerminalCommandAPI):
    """
    Expose local SQLite lock inspection and explicitly requested recovery operations.

    Default execution probes, scans holders, and, if writable, runs a WAL checkpoint
    and integrity check. Process signalling and sidecar renaming require flags.
    Detection excludes this process and can miss external holders, so success is
    not an exclusive-access guarantee or proof of a fully repaired database.

    Example:
        >>> command = DbUnlockCommand()
        >>> command.group, command.name, command.expose_direct
        ('db', 'unlock', False)
    """

    group = "db"
    group_aliases = ("database",)
    expose_direct = False
    name = "unlock"
    aliases = ("recover-lock", "unlock-db")
    summary = "Probe/repair DB lock state (optionally stop external holders)."
    usage = "db unlock [--kill] [--kill9] [--sudo] [--wait-s <sec>] [--clear-sidecars] [--no-check] [--dry-run]"

    def execute(self, browser, args: list[str]) -> bool:
        """
        Inspect the host's database path, optionally stop detected holders, and report recovery results.

        The path must exist but its backend/file type is not validated before
        opening it as SQLite. Only a failed write probe plus detected holders and
        ``--kill`` permits signalling. SIGTERM precedes optional SIGKILL escalation;
        sudo may supplement inspection or retry unsuccessful direct signal sends.
        The initial privileged-scan failure is printed as a warning, while later
        signalling/wait failures propagate.

        ``--clear-sidecars`` requires a successful probe and no detected external
        holders, then renames sidecars before checkpointing. These observations
        are neither exclusive nor race-free, and earlier effects are not rolled
        back on failure. ``--dry-run`` still probes/scans and may raise for a
        remaining lock, but suppresses signals, renames, and recovery pragmas.
        A busy checkpoint alone does not fail the command; a returned integrity
        message other than ``ok`` does.

        Example:
            >>> command = DbUnlockCommand()
            >>> command.execute(browser, ["--dry-run"])  # doctest: +SKIP


        :param browser: Host supplying a local ``database_path`` and an ``emit`` output method.
        :param args: Unlock option tokens accepted by ``_parse_unlock_options``.
        :return: ``True`` after dry-run or recovery reporting; this keeps the browser loop running.
        :raises ValueError: For invalid options/path, a remaining lock, unsafe detected holders, or failed integrity text.
        """
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
        holders = _list_file_holders(
            lock_paths, exclude_pids=exclude_pids, use_sudo=False
        )
        if (not holders) and options.sudo:
            try:
                holders = _list_file_holders(
                    lock_paths, exclude_pids=exclude_pids, use_sudo=True
                )
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
                browser.emit(
                    "  pid={} cmd={} path={}".format(holder.pid, cmd, holder.path)
                )
        else:
            browser.emit("External holders: none detected")

        if (not writable) and holders and options.kill:
            pids = {int(holder.pid) for holder in holders if int(holder.pid) > 0}
            if pids:
                if options.dry_run:
                    browser.emit(
                        "DRY RUN: would send SIGTERM to pids {}".format(sorted(pids))
                    )
                else:
                    sent = _send_signal_to_pids(pids, signal.SIGTERM)
                    browser.emit("Sent SIGTERM to pids {}".format(sent))
                    remaining_pids = {
                        int(pid) for pid in pids if int(pid) not in set(sent)
                    }
                    if remaining_pids and options.sudo:
                        sent_sudo = _send_signal_to_pids_via_sudo(
                            remaining_pids, signal.SIGTERM
                        )
                        browser.emit("Sent sudo SIGTERM to pids {}".format(sent_sudo))
                    remaining = _wait_for_holders_to_clear(
                        lock_paths,
                        exclude_pids=exclude_pids,
                        timeout_s=options.wait_s,
                        use_sudo=options.sudo,
                    )
                    if remaining and options.kill9:
                        browser.emit("Holders remain after SIGTERM; sending SIGKILL.")
                        remaining_pid_set = {
                            int(holder.pid)
                            for holder in remaining
                            if int(holder.pid) > 0
                        }
                        sent_kill = _send_signal_to_pids(
                            remaining_pid_set, signal.SIGKILL
                        )
                        browser.emit("Sent SIGKILL to pids {}".format(sent_kill))
                        remaining_kill9 = {
                            int(pid)
                            for pid in remaining_pid_set
                            if int(pid) not in set(sent_kill)
                        }
                        if remaining_kill9 and options.sudo:
                            sent_sudo_kill = _send_signal_to_pids_via_sudo(
                                remaining_kill9, signal.SIGKILL
                            )
                            browser.emit(
                                "Sent sudo SIGKILL to pids {}".format(sent_sudo_kill)
                            )
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
                raise ValueError(
                    "Refusing to rename sidecars while external holders are present."
                )
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
