"""
Provide test jobs api utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test jobs api through a consuming regression::

        python -m pytest -q tests/utils/jobs/test_jobs_api.py
"""
from __future__ import annotations

from pathlib import Path

from LiuXin_alpha.utils.jobs import JobRequest, available_backends, execute_job


def test_available_backends_exposes_serial_and_process() -> None:
    """
    Perform the test available backends exposes serial and process utility operation under explicit compatibility rules.

    Example:
        Exercise test available backends exposes serial and process through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_api.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    names = set(available_backends())
    assert {"serial", "process"}.issubset(names)


def test_execute_job_serial_success() -> None:
    """
    Perform the test execute job serial success utility operation under explicit compatibility rules.

    Example:
        Exercise test execute job serial success through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_api.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    req = JobRequest(module_name="math", function_name="sqrt", args=(81,))
    result = execute_job(req, backend="serial", no_output=True)

    assert result.ok is True
    assert result.result == 9.0
    assert result.log_path is None


def test_execute_job_process_success_and_output_capture() -> None:
    """
    Perform the test execute job process success and output capture utility operation under explicit compatibility rules.

    Example:
        Exercise test execute job process success and output capture through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_api.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    source = """
def run(x):
    print('hello from worker', x)
    return x + 1
"""
    req = JobRequest(module_name=source, function_name="run", args=(41,), module_is_source_code=True)
    result = execute_job(req, backend="process", no_output=False)

    assert result.ok is True
    assert result.result == 42
    assert result.log_path is not None
    text = Path(result.log_path).read_text(encoding="utf-8", errors="replace")
    assert "hello from worker 41" in text


def test_execute_job_process_timeout_sets_timed_out_flag() -> None:
    """
    Perform the test execute job process timeout sets timed out flag utility operation under explicit compatibility rules.

    Example:
        Exercise test execute job process timeout sets timed out flag through a consuming regression::

            python -m pytest -q tests/utils/jobs/test_jobs_api.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    source = """
import time

def wait_for_a_bit():
    time.sleep(1.0)
    return 1
"""
    req = JobRequest(module_name=source, function_name="wait_for_a_bit", module_is_source_code=True)
    result = execute_job(req, backend="process", no_output=True, timeout=0.1)

    assert result.ok is False
    assert result.timed_out is True
    assert result.result is None
