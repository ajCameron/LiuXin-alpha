from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


class RcloneNotInstalledError(RuntimeError):
    pass


@dataclass(frozen=True)
class RcloneResult:
    args: list[str]
    returncode: int
    stdout: str
    stderr: str


def which_rclone(exe: str = "rclone") -> str:
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
