"""Preserve the historical instance-method entry points during extraction."""

import inspect

import pytest

from LiuXin_alpha.core.program_api import CoreProgramAPI
from LiuXin_alpha.core.program_services import preferences, stores


@pytest.mark.parametrize(
    ("name", "owner", "envelope_name"),
    [
        ("preferences_list", preferences, "query"),
        ("preferences_get", preferences, "query"),
        ("preferences_set", preferences, "command"),
        ("preferences_delete", preferences, "command"),
        ("storage_store_get", stores, "query"),
        ("storage_store_probe", stores, "command"),
        ("storage_store_delete", stores, "command"),
        ("storage_default_set", stores, "command"),
    ],
)
def test_instance_delegates_preserve_bound_and_unbound_calls(
    monkeypatch,
    name,
    owner,
    envelope_name,
):
    runtime, envelope = object(), object()
    calls = []

    def handler(actual_runtime, actual_envelope):
        calls.append((actual_runtime, actual_envelope))
        return {"delegated": True}

    monkeypatch.setattr(owner, name, handler)
    instance = CoreProgramAPI()
    bound = getattr(instance, name)
    unbound = getattr(CoreProgramAPI, name)
    assert list(inspect.signature(bound).parameters) == ["runtime", envelope_name]
    assert list(inspect.signature(unbound).parameters) == [
        "self",
        "runtime",
        envelope_name,
    ]
    assert bound(runtime, envelope) == {"delegated": True}
    assert unbound(instance, runtime, envelope) == {"delegated": True}
    assert calls == [(runtime, envelope), (runtime, envelope)]
