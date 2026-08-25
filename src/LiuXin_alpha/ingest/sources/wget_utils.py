"""Helpers for invoking `wget` and extracting crawled URLs."""

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
    """Raised when the configured `wget` executable cannot be found."""


@dataclass(frozen=True)
class WgetResult:
    """Normalized result payload from a `wget` invocation."""

    args: list[str]
    returncode: int
    stdout: str
    stderr: str


def which_wget(exe: str = "wget") -> str:
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
    return normalize_http_url(url)


def extract_http_urls_from_wget_output(output: str) -> list[str]:
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
