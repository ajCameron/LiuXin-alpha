"""
Check historical bound/unbound program-facade call shapes and delegation to current owner attributes.

The eight parametrized instance delegates are invoked against monkeypatched
handlers, not real subsystem workflows. Static aliases and endpoint installation
are outside this module's assertions.
"""

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
    """
    Require the selected instance delegate to preserve its signature and forward both calls unchanged.

    Replacing the owner attribute before invocation also checks that these eight
    wrappers resolve the handler at call time rather than retaining a static alias.

    Example:
        >>> with pytest.MonkeyPatch.context() as patch:
        ...     test_instance_delegates_preserve_bound_and_unbound_calls(
        ...         patch, "preferences_list", preferences, "query",
        ...     )


    :param monkeypatch: Pytest fixture temporarily replacing the selected service-owner handler.
    :param name: Parametrized facade method and corresponding owner attribute to exercise.
    :param owner: Preferences or stores service module whose handler is replaced.
    :param envelope_name: Expected query or command parameter name in the public call signature.
    :return: None if bound/unbound signatures, receipts, and the two forwarded argument pairs match.
    """
    runtime, envelope = object(), object()
    calls = []

    def handler(actual_runtime, actual_envelope):
        """
        Record forwarded runtime/envelope identities and return a fixed delegation marker.

        Example:
            >>> handler(runtime, envelope)  # doctest: +SKIP
            {'delegated': True}


        :param actual_runtime: Runtime argument received from the facade without transformation.
        :param actual_envelope: Query or command object received from the facade without transformation.
        :return: New dictionary containing delegated=True.
        """
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
