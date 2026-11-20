"""
Check that tools run by ``run_tool`` are stopped by each of their limits, and fail as ``check_output`` does.

Example:
    Run the tests::

        python -m pytest -q tests/utils/test_bounded_process.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from LiuXin_alpha.utils.bounded_process import ToolLimits, run_tool

needs_posix = pytest.mark.skipif(os.name != "posix", reason="the limits are set through /bin/sh")


def python(code: str) -> list[str]:
    """
    Make a command that runs Python code.

    Example:
        >>> python("pass")[1:]
        ['-c', 'pass']


    :param code: The code.
    :return: The command.
    """
    return [sys.executable, "-c", code]


def test_output_errors_and_missing_tools(tmp_path: Path) -> None:
    """
    Return the output, raise for a failure with its stderr, and give 127 for a missing tool.

    Example:
        >>> test_output_errors_and_missing_tools(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A directory to run in.
    :return: None; the assertions are the check.
    """
    before = os.getcwd()
    assert run_tool(python("import os; print(os.getcwd())"), cwd=tmp_path).decode().strip() == str(tmp_path)
    assert os.getcwd() == before
    with pytest.raises(subprocess.CalledProcessError) as failed:
        run_tool(python("import sys; sys.stderr.write('bad input'); sys.exit(2)"))
    assert failed.value.returncode == 2 and b"bad input" in failed.value.stderr
    with pytest.raises(subprocess.CalledProcessError) as missing:
        run_tool([str(tmp_path / "no-such-tool")])
    assert missing.value.returncode == 127


def test_timeouts_stop_the_tool_and_what_it_started(tmp_path: Path) -> None:
    """
    Stop a tool at its timeout, with any process it started.

    Example:
        >>> test_timeouts_stop_the_tool_and_what_it_started(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: A directory for the child's process id.
    :return: None; the assertions are the check.
    """
    code = (
        "import subprocess, sys, time; "
        f"child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
        f"open({str(tmp_path / 'child')!r}, 'w').write(str(child.pid)); time.sleep(60)"
    )
    started = time.monotonic()
    with pytest.raises(subprocess.CalledProcessError) as timed_out:
        run_tool(python(code), limits=ToolLimits(timeout=2.0))
    assert timed_out.value.returncode == -9 and b"timed out" in timed_out.value.stderr
    assert time.monotonic() - started < 15
    if os.name == "posix":
        child = int((tmp_path / "child").read_text())
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                os.kill(child, 0)
            except ProcessLookupError:
                break
            time.sleep(0.1)
        else:
            pytest.fail("the tool's child process outlived the timeout")


def test_output_is_capped() -> None:
    """
    Refuse a tool that prints more than the cap, without keeping more than the cap.

    Example:
        >>> test_output_is_capped()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    for size in (3 << 20, (1 << 20) + 1):
        code = f"import sys; sys.stdout.write('x' * {size})"
        with pytest.raises(subprocess.CalledProcessError) as failed:
            run_tool(python(code), limits=ToolLimits(output_bytes=1 << 20))
        assert len(failed.value.output or b"") <= 1 << 20
    assert len(run_tool(python("import sys; sys.stdout.write('x' * (1 << 20))"), limits=ToolLimits(output_bytes=1 << 20))) == 1 << 20


@needs_posix
def test_memory_cpu_and_file_size_limits_apply() -> None:
    """
    Stop a tool that allocates too much, computes too long, or writes a file that is too large.

    Example:
        >>> test_memory_cpu_and_file_size_limits_apply()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    with pytest.raises(subprocess.CalledProcessError):
        run_tool(python("x = bytearray(1 << 30)"), limits=ToolLimits(memory_bytes=512 << 20))
    started = time.monotonic()
    with pytest.raises(subprocess.CalledProcessError) as spun:
        run_tool(python("while True: pass"), limits=ToolLimits(timeout=60.0, cpu_seconds=1))
    assert b"timed out" not in (spun.value.stderr or b"")  # the CPU limit, well before the timeout
    assert time.monotonic() - started < 20
    code = "open('big', 'wb').write(bytes(4 << 20))"
    with pytest.raises(subprocess.CalledProcessError):
        run_tool(python(code), cwd=os.environ.get("TMPDIR", "/tmp"), limits=ToolLimits(output_bytes=1 << 20))
