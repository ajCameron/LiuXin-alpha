"""
Provide test compat logger utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test compat logger through a consuming regression::

        python -m pytest -q tests/utils/logging/test_compat_logger.py
"""
from __future__ import annotations

import logging

import pytest


def test_coerce_level_accepts_int_and_strings() -> None:
    """
    Perform the test coerce level accepts int and strings utility operation under explicit compatibility rules.

    Example:
        Exercise test coerce level accepts int and strings through a consuming regression::

            python -m pytest -q tests/utils/logging/test_compat_logger.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.logging import _coerce_level

    assert _coerce_level(10) == 10
    assert _coerce_level("DEBUG") == logging.DEBUG
    assert _coerce_level("warn") == logging.WARNING
    assert _coerce_level("fatal") == logging.CRITICAL


def test_coerce_pairs_accepts_tuples_and_mappings() -> None:
    """
    Perform the test coerce pairs accepts tuples and mappings utility operation under explicit compatibility rules.

    Example:
        Exercise test coerce pairs accepts tuples and mappings through a consuming regression::

            python -m pytest -q tests/utils/logging/test_compat_logger.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.logging import _coerce_pairs

    out = _coerce_pairs(("a", 1), {"b": 2}, ("c", None))
    assert out == {"a": 1, "b": 2, "c": None}

    with pytest.raises(TypeError):
        _coerce_pairs(["not", "a", "pair"])  # type: ignore[arg-type]


def test_safe_repr_truncates_and_samples_containers() -> None:
    """
    Perform the test safe repr truncates and samples containers utility operation under explicit compatibility rules.

    Example:
        Exercise test safe repr truncates and samples containers through a consuming regression::

            python -m pytest -q tests/utils/logging/test_compat_logger.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.logging import _safe_repr

    big = list(range(10_000))
    s = _safe_repr(big, max_len=100, max_items=3)
    assert s.startswith("[") and s.endswith("]")
    assert "..." in s or "…" in s


def test_compat_logger_log_variables_emits_and_returns_string(caplog) -> None:
    """
    Perform the test compat logger log variables emits and returns string utility operation under explicit compatibility rules.

    Example:
        Exercise test compat logger log variables emits and returns string through a consuming regression::

            python -m pytest -q tests/utils/logging/test_compat_logger.py


    :param caplog: Value supplied for caplog under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test get compat logger requires install order utility operation under explicit compatibility rules.

    Example:
        Exercise test get compat logger requires install order through a consuming regression::

            python -m pytest -q tests/utils/logging/test_compat_logger.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
