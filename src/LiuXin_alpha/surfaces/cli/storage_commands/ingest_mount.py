"""Launch opted-in ingest in a private Linux namespace and relay cancellation.

Only the worker runs ingest and opens its catalogue. Namespace or mount failures
stop execution without a writable fallback. Mount evidence is attached to normal
run reports; resume replays the public option and establishes fresh protection.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
from types import FrameType

from LiuXin_alpha.ingest.source_mount import (
    SourceMountError,
    mount_namespace_id,
)


def launch_mounted_ingest(args: argparse.Namespace) -> int:
    """Run the mount worker with inherited output and explicitly forwarded stop signals."""
    try:
        direct = bool(
            getattr(args, "source_image", None) or getattr(args, "source_device", None)
        )
        if direct != bool(getattr(args, "source_filesystem", None)):
            raise SourceMountError(
                "source-image/source-device requires --source-filesystem, and vice versa"
            )
        if getattr(args, "source_image", None) and getattr(args, "source_device", None):
            raise SourceMountError("select only one of source-image and source-device")
        namespace = mount_namespace_id()
        executable = shutil.which("unshare")
        if executable is None:
            raise SourceMountError(
                "--mount-source-read-only requires util-linux unshare"
            )
        values = {
            key: value
            for key, value in vars(args).items()
            if not key.startswith("_") and not callable(value)
        }
        payload = json.dumps(values, default=str).encode()
        # Block filesystems need privileges in the original user namespace.
        # Root also stays there for directory binds: a user namespace would
        # discard DAC override on private directories owned by other host users.
        # Ordinary users can establish directory binds via a new user namespace.
        command = [
            executable,
            *([] if direct or os.geteuid() == 0 else ["--user", "--map-root-user"]),
            "--mount",
            "--propagation",
            "private",
            "--",
            sys.executable,
            "-m",
            "LiuXin_alpha.surfaces.cli.storage_commands.ingest_mount_worker",
            str(namespace),
        ]
        with subprocess.Popen(
            command, stdin=subprocess.PIPE, start_new_session=True
        ) as process:
            return _wait_worker(process, payload)
    except (OSError, SourceMountError) as error:
        print(f"ERROR: read-only source mounting failed: {error}", file=sys.stderr)
        return 2


def _wait_worker(process: subprocess.Popen[bytes], payload: bytes) -> int:
    """Forward SIGINT/SIGTERM and reap the worker; exceptional exits also stop its process group."""

    def forward(number: int, _frame: FrameType | None) -> None:
        """Deliver cooperative cancellation to the ingest worker once per received signal."""
        if process.poll() is None:
            process.send_signal(number)

    previous = {
        number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        for number in previous:
            signal.signal(number, forward)
        process.communicate(payload)
        code = process.returncode
        assert code is not None
        if code != 0:
            print(
                "Read-only ingest exited unsuccessfully; no writable fallback was run.",
                file=sys.stderr,
            )
        return code if code >= 0 else 128 - code
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
