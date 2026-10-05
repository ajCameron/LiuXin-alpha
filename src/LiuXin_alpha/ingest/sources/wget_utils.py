"""
Invoke wget with optional streamed diagnostics and extract normalized HTTP(S) candidates.

Arguments/environment are trusted caller inputs, not sanitized command policy.
No shell is used. Streamed output is limited while retained; non-streamed output
is captured fully before its size check. Diagnostics and failure messages can
include raw command/output text. URL extraction applies filtering afterward.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import threading

from dataclasses import dataclass
from typing import Callable, Mapping, Sequence
from LiuXin_alpha.ingest.sources.html_common import normalize_http_url


class WgetNotInstalledError(RuntimeError):
    """
    Report failure to locate the requested wget executable through shutil.which.

    This exception does not cover every process-launch failure after lookup.

    Example:
        >>> str(WgetNotInstalledError('wget unavailable'))
        'wget unavailable'
    """


@dataclass(frozen=True)
class WgetResult:
    """
    Retain command arguments, exit status, and captured text from a completed invocation.

    Frozen fields do not freeze the args list or validate returncode/output. In
    callback mode stdout holds merged stdout/stderr and stderr is empty; otherwise
    streams remain separate. This result is not proof of complete discovery.

    Example:
        >>> WgetResult(['wget', '--version'], 0, 'version text', '').returncode
        0


    :ivar args: Executable, extra arguments, then invocation arguments in execution order.
    :ivar returncode: Integer process exit status.
    :ivar stdout: Captured stdout or merged callback-mode output, retaining line endings.
    :ivar stderr: Separate stderr in non-streamed mode, otherwise empty text.
    """

    args: list[str]
    returncode: int
    stdout: str
    stderr: str


def which_wget(exe: str = "wget") -> str:
    """
    Resolve the requested executable through shutil.which or raise a focused lookup error.

    This does not run the executable, verify its identity, or reserve its path.

    Example:
        >>> executable = which_wget()  # doctest: +SKIP


    :param exe: Executable name or path passed to shutil.which.
    :return: Located executable path string.
    :raises WgetNotInstalledError: Lookup returns no usable executable.
    """
    path = shutil.which(exe)
    if not path:
        raise WgetNotInstalledError(
            "wget executable not found (looked for {!r}). Install wget or set wget_exe.".format(exe)
        )
    return path


def run_wget(
    args: Sequence[str],
    *,
    wget_exe: str = "wget",
    extra_args: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
    timeout_s: float | None = None,
    check: bool = True,
    line_callback: Callable[[str], None] | None = None,
    max_output_chars: int | None = 8 * 1024 * 1024,
) -> WgetResult:
    """
    Execute wget, capture decoded output, and enforce the selected reporting policy.

    Prepend extra_args to args and overlay env on a copy of the process environment.
    Decode text as UTF-8 with surrogateescape. Without a callback, subprocess.run
    captures both streams fully before checking their combined character length;
    that branch does not bound peak capture memory. With a callback, merge stderr
    into stdout, read at most 64 Ki characters per readline where supported, and
    reject an over-limit chunk before retaining or delivering it.

    Streamed callbacks receive complete lines without CR/LF and any final partial
    line. Their errors propagate; a timer kills the child on timeout, and finally
    cancels the timer, closes output, and attempts child kill/wait cleanup. Cleanup
    errors are ignored; descendant-process cleanup is not promised. Final partial
    delivery precedes the timeout check, so its exception can mask TimeoutExpired.
    Earlier callbacks may have caused effects even when execution later fails.

    Nonzero exit status raises only with check=True, including raw command and
    captured text in the message. No credential scrubbing is performed.

    Example:
        >>> result = run_wget(['--version'], timeout_s=15)  # doctest: +SKIP


    :param args: Invocation tokens appended without shell interpretation.
    :param wget_exe: Executable selector resolved before process creation.
    :param extra_args: Optional tokens inserted immediately after the executable.
    :param env: Optional environment overlay; unspecified inherited entries remain.
    :param timeout_s: Process timeout in seconds, or None without a deadline.
    :param check: Raise on nonzero exit instead of returning its result.
    :param line_callback: Optional synchronous consumer selecting merged streamed capture.
    :param max_output_chars: Positive captured-character ceiling, or None to disable the check.
    :return: Completed command/exit/output facts after applicable policy checks.
    :raises ValueError: A supplied output ceiling is less than one.
    :raises WgetNotInstalledError: Executable lookup fails.
    :raises subprocess.TimeoutExpired: The selected subprocess/timer deadline expires.
    :raises RuntimeError: Output exceeds its ceiling or checked execution exits nonzero.
    """
    if max_output_chars is not None and max_output_chars < 1:
        raise ValueError("max_output_chars must be positive or None.")
    exe = which_wget(wget_exe)
    cmd = [exe]
    if extra_args:
        cmd.extend(list(extra_args))
    cmd.extend(list(args))

    merged_env = os.environ.copy()
    if env:
        merged_env.update(dict(env))

    if line_callback is None:
        p = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=merged_env,
            timeout=timeout_s,
            check=False,
            text=True,
            encoding="utf-8",
            errors="surrogateescape",
        )
        result = WgetResult(
            args=cmd,
            returncode=int(p.returncode),
            stdout=p.stdout,
            stderr=p.stderr,
        )
        if (
            max_output_chars is not None
            and len(result.stdout or "") + len(result.stderr or "")
            > max_output_chars
        ):
            raise RuntimeError("wget output exceeded its configured size limit")
    else:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=merged_env,
            text=True,
            encoding="utf-8",
            errors="surrogateescape",
            bufsize=1,
        )
        merged_lines: list[str] = []
        pending_line = ""
        output_chars = 0
        timed_out = threading.Event()

        def _kill_timed_out_process() -> None:
            """
            Mark timeout and kill the child only if its current poll reports running.

            Executed by a daemon timer; poll/kill errors are not caught in this
            callback. It targets the child, not an entire process group.

            Example:
                >>> _kill_timed_out_process()  # doctest: +SKIP


            :return: None after any timeout marking and child kill attempt.
            """
            if proc.poll() is None:
                timed_out.set()
                proc.kill()

        timer = (
            None
            if timeout_s is None
            else threading.Timer(timeout_s, _kill_timed_out_process)
        )
        if timer is not None:
            timer.daemon = True
            timer.start()
        try:
            assert proc.stdout is not None
            while True:
                try:
                    chunk = proc.stdout.readline(64 * 1024)
                except TypeError:
                    # Minimal injected test streams may expose only readline().
                    chunk = proc.stdout.readline()
                if chunk:
                    output_chars += len(chunk)
                    if (
                        max_output_chars is not None
                        and output_chars > max_output_chars
                    ):
                        try:
                            proc.kill()
                        except Exception:
                            pass
                        raise RuntimeError(
                            "wget output exceeded its configured size limit"
                        )
                    merged_lines.append(chunk)
                    if line_callback is not None:
                        pending_line += chunk
                        while "\n" in pending_line:
                            line, pending_line = pending_line.split("\n", 1)
                            try:
                                line_callback(line.rstrip("\r"))
                            except Exception:
                                try:
                                    proc.kill()
                                except Exception:
                                    pass
                                raise
                if chunk == "" and proc.poll() is not None:
                    break
            returncode = int(proc.wait())
            if line_callback is not None and pending_line:
                line_callback(pending_line.rstrip("\r"))
            if timed_out.is_set():
                raise subprocess.TimeoutExpired(cmd, timeout_s)
        finally:
            if timer is not None:
                timer.cancel()
            if proc.stdout is not None:
                try:
                    proc.stdout.close()
                except Exception:
                    pass
            try:
                running = proc.poll() is None
            except Exception:
                running = True
            if running:
                try:
                    proc.kill()
                except Exception:
                    pass
            try:
                proc.wait(timeout=1)
            except Exception:
                pass

        result = WgetResult(
            args=cmd,
            returncode=returncode,
            stdout="".join(merged_lines),
            stderr="",
        )
    if check and result.returncode != 0:
        message = str(result.stderr or "").strip() or str(result.stdout or "").strip()
        raise RuntimeError("wget failed ({}): {}\n{}".format(result.returncode, " ".join(cmd), message))
    return result


_URL_TOKEN_PATTERN = re.compile(r"https?://[^\s\"'<>]+", flags=re.IGNORECASE)


def _normalize_url(url: str) -> str | None:
    """
    Delegate extracted-token normalization to the shared HTTP(S) URL policy.

    Example:
        >>> _normalize_url('HTTPS://example.test/book.epub#part')
        'https://example.test/book.epub'


    :param url: Raw token extracted from diagnostics, without punctuation trimming.
    :return: Normalized accepted address or None.
    """
    return normalize_http_url(url)


def extract_http_urls_from_wget_output(output: str) -> list[str]:
    """
    Extract unique HTTP URLs from wget diagnostic output.

    Scan case-insensitively until whitespace, quotes, or angle brackets, then
    normalize/reject tokens and deduplicate accepted text in first-seen order.
    Other trailing punctuation may remain part of a token. Output itself is not
    bounded or scrubbed by this helper, and no candidate is fetched here.

    Example:
        >>> extract_http_urls_from_wget_output('URL: https://example.test/book.epub https://example.test/book.epub')
        ['https://example.test/book.epub']


    :param output: Diagnostic text coerced with str, treating falsey input as empty.
    :return: Ordered distinct normalized HTTP(S) URL candidates.
    """
    urls: list[str] = []
    seen: set[str] = set()
    for raw in _URL_TOKEN_PATTERN.findall(str(output or "")):
        normalized = _normalize_url(raw)
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return urls


__all__ = [
    "WgetNotInstalledError",
    "WgetResult",
    "extract_http_urls_from_wget_output",
    "run_wget",
    "which_wget",
]
