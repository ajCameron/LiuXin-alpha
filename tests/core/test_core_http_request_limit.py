"""Safety contract for bounded Core HTTP request bodies."""

from __future__ import annotations

import pytest

from LiuXin_alpha.core.transport.http import CoreHttpDaemon


class _Runtime:
    def subscribe(self, _callback):
        return lambda: None


def test_daemon_rejects_request_larger_than_configured_limit() -> None:
    daemon = CoreHttpDaemon(_Runtime(), max_request_bytes=16)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="configured 16 byte limit"):
        daemon.validate_request_body_length(34)
    assert daemon.validate_request_body_length(16) == 16


def test_daemon_rejects_empty_request_body() -> None:
    daemon = CoreHttpDaemon(_Runtime(), max_request_bytes=16)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="cannot be empty"):
        daemon.validate_request_body_length(0)


def test_daemon_rejects_non_positive_request_limit() -> None:
    with pytest.raises(ValueError, match="max_request_bytes"):
        CoreHttpDaemon(_Runtime(), max_request_bytes=0)  # type: ignore[arg-type]
