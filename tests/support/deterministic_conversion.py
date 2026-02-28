from __future__ import annotations

import hashlib
import uuid

from datetime import date as _date
from typing import Callable, Iterable


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def freeze_uuid4(monkeypatch, value: str = "11111111-2222-3333-4444-555555555555") -> None:
    fixed = uuid.UUID(value)
    monkeypatch.setattr(uuid, "uuid4", lambda: fixed)


def freeze_module_date_today(monkeypatch, module, *, year: int, month: int, day: int, attr_name: str = "date") -> None:
    class _FixedDate:
        @classmethod
        def today(cls):
            return _date(year, month, day)

    monkeypatch.setattr(module, attr_name, _FixedDate)


def assert_bytes_deterministic(
    render_once: Callable[[str], bytes],
    *,
    run_names: Iterable[str] = ("deterministic_1", "deterministic_2"),
) -> bytes:
    names = list(run_names)
    if len(names) < 2:
        raise ValueError("need at least two run names to check determinism")

    first = render_once(names[0])
    for name in names[1:]:
        candidate = render_once(name)
        assert candidate == first
    return first
