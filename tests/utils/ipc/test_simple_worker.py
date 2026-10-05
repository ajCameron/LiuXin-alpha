"""
Provide test simple worker utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test simple worker through a consuming regression::

        python -m pytest -q tests/utils/ipc/test_simple_worker.py
"""
from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.utils.ipc.simple_worker import WorkerError, fork_job


def test_fork_job_serial_success() -> None:
    """
    Perform the test fork job serial success utility operation under explicit compatibility rules.

    Example:
        Exercise test fork job serial success through a consuming regression::

            python -m pytest -q tests/utils/ipc/test_simple_worker.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    result = fork_job("math", "pow", args=(2, 6), no_output=True, backend="serial")

    assert result["result"] == 64.0
    assert result["stdout_stderr"] is None


def test_fork_job_process_success_with_source_module() -> None:
    """
    Perform the test fork job process success with source module utility operation under explicit compatibility rules.

    Example:
        Exercise test fork job process success with source module through a consuming regression::

            python -m pytest -q tests/utils/ipc/test_simple_worker.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test fork job raises worker error with traceback utility operation under explicit compatibility rules.

    Example:
        Exercise test fork job raises worker error with traceback through a consuming regression::

            python -m pytest -q tests/utils/ipc/test_simple_worker.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    source = """
def fail_now():
    raise ValueError('boom')
"""

    with pytest.raises(WorkerError) as exc:
        fork_job(source, "fail_now", module_is_source_code=True, no_output=True, backend="serial")

    assert "ValueError" in exc.value.orig_tb
    assert "boom" in exc.value.orig_tb


def test_fork_job_timeout_raises_worker_error() -> None:
    """
    Perform the test fork job timeout raises worker error utility operation under explicit compatibility rules.

    Example:
        Exercise test fork job timeout raises worker error through a consuming regression::

            python -m pytest -q tests/utils/ipc/test_simple_worker.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    source = """
import time

def slow():
    time.sleep(1.0)
"""

    with pytest.raises(WorkerError, match="hung"):
        fork_job(source, "slow", module_is_source_code=True, no_output=True, timeout=0.1, backend="process")
