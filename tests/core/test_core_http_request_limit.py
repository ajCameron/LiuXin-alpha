"""
Provide test core http request limit utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test core http request limit through a consuming regression::

        python -m pytest -q tests/core/test_core_http_request_limit.py
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.core.transport.http import CoreHttpDaemon


class _Runtime:
    """
    Provide the runtime contract for validated ebook processing.

    Example:
        Exercise  Runtime through a consuming regression::

            python -m pytest -q tests/core/test_core_http_request_limit.py
    """
    def subscribe(self, _callback):
        """
        Perform the subscribe operation under explicit file-format and conversion rules.

        Example:
            Exercise  Runtime.subscribe through a consuming regression::

                python -m pytest -q tests/core/test_core_http_request_limit.py


        :param _callback: Value supplied for callback under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return lambda: None


def test_daemon_rejects_request_larger_than_configured_limit() -> None:
    """
    Perform the test daemon rejects request larger than configured limit operation under explicit file-format and conversion rules.

    Example:
        Exercise test daemon rejects request larger than configured limit through a consuming regression::

            python -m pytest -q tests/core/test_core_http_request_limit.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    daemon = CoreHttpDaemon(_Runtime(), max_request_bytes=16)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="configured 16 byte limit"):
        daemon.validate_request_body_length(34)
    assert daemon.validate_request_body_length(16) == 16


def test_daemon_rejects_empty_request_body() -> None:
    """
    Perform the test daemon rejects empty request body operation under explicit file-format and conversion rules.

    Example:
        Exercise test daemon rejects empty request body through a consuming regression::

            python -m pytest -q tests/core/test_core_http_request_limit.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    daemon = CoreHttpDaemon(_Runtime(), max_request_bytes=16)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="cannot be empty"):
        daemon.validate_request_body_length(0)


def test_daemon_rejects_non_positive_request_limit() -> None:
    """
    Perform the test daemon rejects non positive request limit operation under explicit file-format and conversion rules.

    Example:
        Exercise test daemon rejects non positive request limit through a consuming regression::

            python -m pytest -q tests/core/test_core_http_request_limit.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(ValueError, match="max_request_bytes"):
        CoreHttpDaemon(_Runtime(), max_request_bytes=0)  # type: ignore[arg-type]
