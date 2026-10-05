"""
Resolve rclone executables and capture subprocess text or decoded JSON results.

These low-level helpers inherit and optionally overlay the process environment,
run argument vectors without a shell, and expose complete captured output. Error
messages can contain command arguments and remote output; storage-level translation
and selective diagnostic filtering belong to callers.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


class RcloneNotInstalledError(RuntimeError):
    """
    Signal that executable lookup could not resolve the configured rclone program.

    Example:
        >>> error = RcloneNotInstalledError("rclone is unavailable")
        >>> isinstance(error, RuntimeError)
        True
    """
    pass


@dataclass(frozen=True)
class RcloneResult:
    """
    Retain the resolved argument vector, return code, and captured text streams of one command.

    The record is frozen, but its args list remains mutable. Fields are retained without status
    validation, sanitization, or output truncation.

    Example:
        >>> result = RcloneResult(["rclone", "version"], 0, "version output", "")
        >>> result.returncode
        0


    :ivar args: Executable and arguments as a mutable list of strings.
    :ivar returncode: Subprocess completion code.
    :ivar stdout: Complete captured standard output as text.
    :ivar stderr: Complete captured diagnostic output as text.
    """
    args: list[str]
    returncode: int
    stdout: str
    stderr: str


def which_rclone(exe: str = "rclone") -> str:
    """
    Resolve the configured executable with shutil.which or raise a dedicated installation error.

    Example:
        >>> executable = which_rclone()  # doctest: +SKIP


    :param exe: Executable name or path passed to shutil.which, defaulting to rclone.
    :return: Resolved executable path; lookup does not execute it or verify its version.
    """
    path = shutil.which(exe)
    if not path:
        raise RcloneNotInstalledError(
            f"rclone executable not found (looked for {exe!r}). Install rclone or set rclone_exe."
        )
    return path


def run_rclone(
    args: Sequence[str],
    *,
    rclone_exe: str = "rclone",
    extra_args: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
    timeout_s: float | None = None,
    check: bool = True,
) -> RcloneResult:
    """
    Run rclone directly with captured text output and optional inherited-environment overrides.

    Extra arguments precede command arguments; no shell is used. The subprocess always runs with
    check=False, then an optional manual status check raises RuntimeError containing the command and
    stripped stderr. This helper does not redact those values, bound captured output, or apply Store
    pacing. Spawn, timeout, and text-decoding failures propagate from subprocess.run.

    Example:
        >>> result = run_rclone(["version"], timeout_s=10)  # doctest: +SKIP


    :param args: Command and arguments appended after the resolved executable and extra options.
    :param rclone_exe: Executable name/path resolved before invocation.
    :param extra_args: Optional global rclone arguments prepended to command arguments.
    :param env: Optional mapping overlaid on the current process environment; it does not replace the full environment.
    :param timeout_s: Timeout passed to subprocess.run, or None for no explicit timeout.
    :param check: Whether a nonzero completion code raises RuntimeError after output capture.
    :return: RcloneResult with the complete command and captured output when the configured status policy allows it.
    """
    exe = which_rclone(rclone_exe)
    cmd = [exe]
    if extra_args:
        cmd.extend(list(extra_args))
    cmd.extend(list(args))

    merged_env = os.environ.copy()
    if env:
        merged_env.update(dict(env))

    p = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=merged_env,
        timeout=timeout_s,
        check=False,
        text=True,
    )
    res = RcloneResult(args=cmd, returncode=p.returncode, stdout=p.stdout, stderr=p.stderr)
    if check and p.returncode != 0:
        raise RuntimeError(f"rclone failed ({p.returncode}): {' '.join(cmd)}\n{p.stderr.strip()}")
    return res


def run_rclone_json(
    args: Sequence[str],
    *,
    rclone_exe: str = "rclone",
    extra_args: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
    timeout_s: float | None = None,
    check: bool = True,
) -> Any:
    """
    Run a captured command and decode nonblank stdout as JSON.

    Blank output becomes None. JSONDecodeError becomes RuntimeError containing the command and
    complete stdout/stderr, without redaction or truncation here. With check=False, nonzero-exit
    output is still eligible for decoding. Other invocation or decoder exceptions propagate.

    Example:
        >>> listing = run_rclone_json(["lsjson", "archive:"])  # doctest: +SKIP


    :param args: Command and arguments forwarded to the captured-output runner.
    :param rclone_exe: Executable name/path forwarded for lookup.
    :param extra_args: Optional global arguments prepended before the command.
    :param env: Optional overlay on the inherited process environment.
    :param timeout_s: Subprocess timeout in seconds, or None to omit it.
    :param check: Whether the underlying runner rejects a nonzero exit before JSON decoding.
    :return: Decoded JSON value of any shape, or None for whitespace-only stdout; no listing/stat schema is enforced.
    """
    res = run_rclone(
        args,
        rclone_exe=rclone_exe,
        extra_args=extra_args,
        env=env,
        timeout_s=timeout_s,
        check=check,
    )
    if not res.stdout.strip():
        return None
    try:
        return json.loads(res.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Invalid JSON from rclone. Command: {' '.join(res.args)}\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
        ) from e
