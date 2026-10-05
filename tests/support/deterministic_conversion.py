"""
Freeze volatile conversion inputs so generated format fixtures remain reproducible.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise deterministic conversion through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_ownership.py
"""
from __future__ import annotations

import hashlib
import uuid

from datetime import date as _date
from typing import Callable, Iterable


def sha256_hex(data: bytes) -> str:
    """
    Perform the sha256 hex step with deterministic fixture inputs.

    Example:
        Exercise sha256 hex through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param data: Bytes or structured data consumed by the operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return hashlib.sha256(data).hexdigest()


def freeze_uuid4(monkeypatch, value: str = "11111111-2222-3333-4444-555555555555") -> None:
    """
    Freeze uuid4 under the fixture contract.

    Example:
        Exercise freeze uuid4 through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param monkeypatch: Pytest fixture used to isolate dependencies and mutable process
        state.
    :param value: Fixture value normalized, encoded, stored or returned.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    fixed = uuid.UUID(value)
    monkeypatch.setattr(uuid, "uuid4", lambda: fixed)


def freeze_module_date_today(monkeypatch, module, *, year: int, month: int, day: int, attr_name: str = "date") -> None:
    """
    Freeze module date today under the fixture contract.

    Example:
        Exercise freeze module date today through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param monkeypatch: Pytest fixture used to isolate dependencies and mutable process
        state.
    :param module: Value supplied for module under the deterministic fixture contract.
    :param year: Value supplied for year under the deterministic fixture contract.
    :param month: Value supplied for month under the deterministic fixture contract.
    :param day: Value supplied for day under the deterministic fixture contract.
    :param attr_name: Value supplied for attr name under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    class _FixedDate:
        """
        Represent the FixedDate state used by deterministic test-support operations.

        Example:
            Exercise freeze module date today. FixedDate through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_ownership.py
        """
        @classmethod
        def today(cls):
            """
            Perform the today step with deterministic fixture inputs.

            Example:
                Exercise freeze module date today. FixedDate.today through a consuming regression::

                    python -m pytest -q tests/scripts/test_docstring_ownership.py


            :return: The deterministic fixture value, path, bytes, record or collection
                described above.
            """
            return _date(year, month, day)

    monkeypatch.setattr(module, attr_name, _FixedDate)


def assert_bytes_deterministic(
    render_once: Callable[[str], bytes],
    *,
    run_names: Iterable[str] = ("deterministic_1", "deterministic_2"),
) -> bytes:
    """
    Assert bytes deterministic under the fixture contract.

    Example:
        Exercise assert bytes deterministic through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param render_once: Value supplied for render once under the deterministic fixture
        contract.
    :param run_names: Value supplied for run names under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    names = list(run_names)
    if len(names) < 2:
        raise ValueError("need at least two run names to check determinism")

    first = render_once(names[0])
    for name in names[1:]:
        candidate = render_once(name)
        assert candidate == first
    return first
