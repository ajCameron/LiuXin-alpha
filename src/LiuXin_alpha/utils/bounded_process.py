"""
Run an external tool on untrusted input with a time limit, resource limits and capped output.

Tools such as poppler's ``pdfinfo`` and ``pdftoppm`` parse whatever file they are
given. A crafted file can make them run for minutes, allocate gigabytes, or print
without end. :func:`run_tool` runs them with a wall-clock timeout and, on POSIX, CPU,
address-space, file-size and core-dump limits. It sets those limits through a
``/bin/sh`` wrapper that execs the tool, so no Python runs in the child after the
fork and the call is safe from threaded programs. The tool leads its own process group,
so a timeout stops anything it started as well. Output goes to temporary files
the file-size limit bounds, so a runaway tool cannot fill memory or disk.

Every failure is a :class:`subprocess.CalledProcessError`: a non-zero exit, death by
a signal (including the limits' signals), a timeout (return code -9), output over
the cap, or a missing tool (127). Callers written for ``subprocess.check_output``
keep working.

Example:
    >>> import sys
    >>> run_tool([sys.executable, "-c", "print('hi')"]).strip()
    b'hi'
"""

from __future__ import annotations

import os
import signal
import subprocess
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass

__all__ = ["ToolLimits", "run_tool"]

_LIMIT_SCRIPT = (
    "ulimit -t {cpu} 2>/dev/null; ulimit -v {kilobytes} 2>/dev/null; "
    "ulimit -f {blocks} 2>/dev/null; ulimit -c 0 2>/dev/null; exec \"$@\""
)


@dataclass(frozen=True)
class ToolLimits:
    """
    Bound one run of an external tool.

    Example:
        >>> ToolLimits().timeout
        60.0
    """

    #: Seconds of wall-clock time before the tool is killed.
    timeout: float = 60.0
    #: Seconds of CPU time (POSIX).
    cpu_seconds: int = 60
    #: Bytes of address space (POSIX, where the system enforces it).
    memory_bytes: int = 1 << 30
    #: Bytes of output captured, and the size of any file the tool writes (POSIX).
    #: Shells count the file-size limit in 512- or 1024-byte blocks, so a file the
    #: tool writes may reach up to twice this; captured output never passes it.
    output_bytes: int = 64 << 20


def _wrapped(args: Sequence[str], limits: ToolLimits) -> list[str]:
    """
    Prefix a command with the shell wrapper that sets the limits and execs it.

    Example:
        >>> _wrapped(["pdfinfo", "x.pdf"], ToolLimits())[-2:] if os.name == "posix" else ["pdfinfo", "x.pdf"]
        ['pdfinfo', 'x.pdf']


    :param args: The command.
    :param limits: The limits.
    :return: The command to run.
    """
    if os.name != "posix":
        return list(args)
    script = _LIMIT_SCRIPT.format(
        cpu=max(1, int(limits.cpu_seconds)),
        kilobytes=max(1, limits.memory_bytes // 1024),
        # One block more than the cap, so a tool that stops at the limit has
        # still written more than the cap, and is refused rather than truncated.
        blocks=limits.output_bytes // 512 + 1,
    )
    return ["/bin/sh", "-c", script, "bounded-tool", *args]


def run_tool(
    args: Sequence[str],
    *,
    cwd: str | os.PathLike[str] | None = None,
    limits: ToolLimits | None = None,
    creationflags: int = 0,
) -> bytes:
    """
    Run a tool within limits and return what it printed.

    Example:
        >>> import sys
        >>> run_tool([sys.executable, "-c", "import sys; sys.exit(3)"])  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        subprocess.CalledProcessError: Command '[...]' returned non-zero exit status 3.


    :param args: The command and its arguments; never passed through a shell.
    :param cwd: The directory to run it in; this process's directory does not change.
    :param limits: The limits; the defaults suit a document tool.
    :param creationflags: Windows process-creation flags.
    :return: Its standard output, at most :attr:`ToolLimits.output_bytes`.
    :raises subprocess.CalledProcessError: The tool failed, was killed, timed out,
        printed too much, or was not found.
    """
    limits = limits or ToolLimits()
    command = list(args)
    posix = os.name == "posix"
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        try:
            process = subprocess.Popen(
                _wrapped(command, limits),
                stdin=subprocess.DEVNULL,
                stdout=out,
                stderr=err,
                cwd=cwd,
                creationflags=creationflags,
                start_new_session=posix,
            )
        except FileNotFoundError as error:
            raise subprocess.CalledProcessError(127, command, stderr=str(error).encode()) from None
        try:
            returncode = process.wait(timeout=limits.timeout)
        except subprocess.TimeoutExpired:
            # The tool leads its own process group: stop anything it started too.
            try:
                if posix:
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except ProcessLookupError:
                pass
            process.wait()
            raise subprocess.CalledProcessError(
                -9, command, stderr=f"timed out after {limits.timeout:g} s".encode()
            ) from None
        out.seek(0)
        output = out.read(limits.output_bytes + 1)
        err.seek(0)
        errors = err.read(4096)
    if len(output) > limits.output_bytes:
        raise subprocess.CalledProcessError(
            returncode or 1,
            command,
            output=output[: limits.output_bytes],
            stderr=f"printed more than {limits.output_bytes} bytes".encode(),
        )
    if returncode:
        raise subprocess.CalledProcessError(returncode, command, output=output, stderr=errors)
    return output
