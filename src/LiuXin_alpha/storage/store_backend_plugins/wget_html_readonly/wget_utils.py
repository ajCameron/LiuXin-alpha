"""Helpers for invoking ``wget`` and extracting crawled URLs."""

from __future__ import annotations

import os
import re
import shutil
import subprocess

from dataclasses import dataclass
from typing import Callable, Mapping, Sequence
from urllib.parse import urlparse, urlunparse


class WgetNotInstalledError(RuntimeError):
    """Raised when the configured ``wget`` executable cannot be found."""


@dataclass(frozen=True)
class WgetResult:
    """Normalized result payload from a ``wget`` invocation."""

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
) -> WgetResult:
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
        )
        result = WgetResult(
            args=cmd,
            returncode=int(p.returncode),
            stdout=p.stdout,
            stderr=p.stderr,
        )
    else:
        # Stream wget output so callers can surface crawl progress in real-time.
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=merged_env,
            text=True,
            bufsize=1,
        )
        merged_lines: list[str] = []
        try:
            assert proc.stdout is not None
            while True:
                line = proc.stdout.readline()
                if line:
                    merged_lines.append(line)
                    try:
                        line_callback(line.rstrip("\r\n"))
                    except Exception:
                        pass
                if line == "" and proc.poll() is not None:
                    break
            returncode = int(proc.wait(timeout=timeout_s))
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            raise
        finally:
            if proc.stdout is not None:
                proc.stdout.close()

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
    text = str(url or "").strip()
    if not text:
        return None
    parsed = urlparse(text)
    if parsed.scheme.lower() not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    normalized = parsed._replace(fragment="")
    return urlunparse(normalized)


def extract_http_urls_from_wget_output(output: str) -> list[str]:
    """
    Extract HTTP(S) URLs from wget text output.

    The parser is intentionally permissive because wget output format varies by
    version and verbosity flags.
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
