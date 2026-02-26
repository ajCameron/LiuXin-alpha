from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.utils.ipc.simple_worker import WorkerError, fork_job


def test_fork_job_serial_success() -> None:
    result = fork_job("math", "pow", args=(2, 6), no_output=True, backend="serial")

    assert result["result"] == 64.0
    assert result["stdout_stderr"] is None


def test_fork_job_process_success_with_source_module() -> None:
    source = """
def add(a, b):
    print('running add')
    return a + b
"""

    result = fork_job(source, "add", args=(3, 4), module_is_source_code=True, no_output=False, backend="process")

    assert result["result"] == 7
    assert result["stdout_stderr"] is not None
    log_text = Path(result["stdout_stderr"]).read_text(encoding="utf-8", errors="replace")
    assert "running add" in log_text


def test_fork_job_raises_worker_error_with_traceback() -> None:
    source = """
def fail_now():
    raise ValueError('boom')
"""

    with pytest.raises(WorkerError) as exc:
        fork_job(source, "fail_now", module_is_source_code=True, no_output=True, backend="serial")

    assert "ValueError" in exc.value.orig_tb
    assert "boom" in exc.value.orig_tb


def test_fork_job_timeout_raises_worker_error() -> None:
    source = """
import time

def slow():
    time.sleep(1.0)
"""

    with pytest.raises(WorkerError, match="hung"):
        fork_job(source, "slow", module_is_source_code=True, no_output=True, timeout=0.1, backend="process")
