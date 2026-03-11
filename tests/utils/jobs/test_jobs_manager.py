from __future__ import annotations

import time

from pathlib import Path

from LiuXin_alpha.utils.jobs import JobRequest, submit_job
from LiuXin_alpha.utils.jobs.manager import InMemoryJobManager


def test_in_memory_job_manager_submit_wait_and_result() -> None:
    manager = InMemoryJobManager(max_workers=2, default_backend="serial")
    try:
        req = JobRequest(module_name="math", function_name="sqrt", args=(49,))
        job_id = manager.submit(req, no_output=True, label="sqrt")
        info = manager.wait(job_id, timeout=2.0)

        assert info.state == "succeeded"
        assert info.execution is not None
        assert info.execution.ok is True
        assert info.execution.result == 7.0
        assert info.label == "sqrt"
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_in_memory_job_manager_cancel_pending_job() -> None:
    manager = InMemoryJobManager(max_workers=1, default_backend="serial")
    try:
        sleep_source = """
import time

def run(seconds):
    time.sleep(seconds)
    return seconds
"""
        first = manager.submit(
            JobRequest(module_name=sleep_source, function_name="run", args=(0.5,), module_is_source_code=True),
            no_output=True,
        )
        second = manager.submit(
            JobRequest(module_name=sleep_source, function_name="run", args=(0.01,), module_is_source_code=True),
            no_output=True,
        )

        assert manager.cancel(second) is True
        manager.wait(first, timeout=3.0)
        info_second = manager.wait(second, timeout=1.0)
        assert info_second.state == "cancelled"
        assert info_second.execution is not None
        assert info_second.execution.aborted is True
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_in_memory_job_manager_cancel_running_job_process_backend() -> None:
    manager = InMemoryJobManager(max_workers=1, default_backend="process")
    try:
        sleep_source = """
import time

def run(seconds):
    time.sleep(seconds)
    return seconds
"""
        job_id = manager.submit(
            JobRequest(module_name=sleep_source, function_name="run", args=(5.0,), module_is_source_code=True),
            no_output=True,
            timeout=30.0,
        )

        deadline = time.time() + 2.0
        while time.time() < deadline:
            state = manager.get(job_id).state
            if state == "running":
                break
            time.sleep(0.02)

        assert manager.cancel(job_id) is True
        info = manager.wait(job_id, timeout=5.0)
        assert info.state in {"aborted", "cancelled"}
        assert info.execution is not None
        assert info.execution.aborted is True
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_in_memory_job_manager_marks_timeout() -> None:
    manager = InMemoryJobManager(max_workers=1, default_backend="process")
    try:
        source = """
import time

def wait_for_a_bit():
    time.sleep(1.0)
    return 1
"""
        job_id = manager.submit(
            JobRequest(module_name=source, function_name="wait_for_a_bit", module_is_source_code=True),
            no_output=True,
            timeout=0.1,
        )
        info = manager.wait(job_id, timeout=5.0)
        assert info.state == "timed_out"
        assert info.execution is not None
        assert info.execution.timed_out is True
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_in_memory_job_manager_preallocates_log_path_for_running_jobs() -> None:
    manager = InMemoryJobManager(max_workers=1, default_backend="process")
    try:
        source = """
import time

def run():
    print("manager panel hello")
    time.sleep(0.2)
    print("manager panel done")
    return 1
"""
        job_id = manager.submit(
            JobRequest(module_name=source, function_name="run", module_is_source_code=True),
            no_output=False,
            timeout=5.0,
        )

        info_initial = manager.get(job_id)
        assert info_initial.log_path
        assert Path(info_initial.log_path).exists()

        info_done = manager.wait(job_id, timeout=5.0)
        assert info_done.state == "succeeded"
        assert info_done.execution is not None
        assert info_done.execution.log_path == info_done.log_path
    finally:
        manager.shutdown(wait=True, cancel_pending=True)


def test_submit_job_uses_default_manager() -> None:
    job_id = submit_job(JobRequest(module_name="math", function_name="sqrt", args=(64,)), backend="serial", no_output=True)
    assert isinstance(job_id, str)
    assert job_id
