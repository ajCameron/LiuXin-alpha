from __future__ import annotations

import logging

import pytest


def test_coerce_level_accepts_int_and_strings() -> None:
    from LiuXin_alpha.utils.logging import _coerce_level

    assert _coerce_level(10) == 10
    assert _coerce_level("DEBUG") == logging.DEBUG
    assert _coerce_level("warn") == logging.WARNING
    assert _coerce_level("fatal") == logging.CRITICAL


def test_coerce_pairs_accepts_tuples_and_mappings() -> None:
    from LiuXin_alpha.utils.logging import _coerce_pairs

    out = _coerce_pairs(("a", 1), {"b": 2}, ("c", None))
    assert out == {"a": 1, "b": 2, "c": None}

    with pytest.raises(TypeError):
        _coerce_pairs(["not", "a", "pair"])  # type: ignore[arg-type]


def test_safe_repr_truncates_and_samples_containers() -> None:
    from LiuXin_alpha.utils.logging import _safe_repr

    big = list(range(10_000))
    s = _safe_repr(big, max_len=100, max_items=3)
    assert s.startswith("[") and s.endswith("]")
    assert "..." in s or "…" in s


def test_compat_logger_log_variables_emits_and_returns_string(caplog) -> None:
    from LiuXin_alpha.utils.logging import LogVariablesFormat, get_compat_logger, install_compat_logger_class

    install_compat_logger_class()
    logger = get_compat_logger("test-log-vars")
    logger.setLevel(logging.DEBUG)
    logger.propagate = True

    with caplog.at_level(logging.INFO):
        msg = logger.log_variables(
            "base",
            "INFO",
            ("z", 1),
            ("a", {"k": "v"}),
        )

    assert "base" in msg
    # keys sorted by default
    assert msg.splitlines()[1].startswith("a")
    # emitted to logging
    assert any("base" in r.message for r in caplog.records)

    # Per-call formatting override
    fmt = LogVariablesFormat(prefix="  ", kv_sep=": ")
    msg2 = logger.log_variables("", 20, ("x", 1), emit=False, fmt=fmt)
    assert msg2.startswith("")
    assert "  x: 1" in msg2


def test_get_compat_logger_requires_install_order(monkeypatch) -> None:
    from LiuXin_alpha.utils.logging import get_compat_logger, install_compat_logger_class

    name = "test-standard-preinstall"

    # Simulate: some other code created a standard logger before install.
    logging.setLoggerClass(logging.Logger)
    std = logging.getLogger(name)
    assert type(std) is logging.Logger

    # Now install compat and verify the existing logger triggers the guard.
    install_compat_logger_class()
    with pytest.raises(TypeError):
        get_compat_logger(name)