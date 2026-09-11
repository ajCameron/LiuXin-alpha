"""
Check generated proxy documentation and the dispatch/result-shape boundaries it describes.

Mocks replace runtime and HTTP calls, so these tests do not open databases,
start threads, or contact a server. Static declaration coverage is checked by
the project docstring audit; this suite additionally examines live closures.
"""

from __future__ import annotations

import doctest
import inspect
import re
from unittest.mock import Mock

import pytest

from LiuXin_alpha.core.proxies.local import LocalDatabaseProxy, LocalJobsProxy
from LiuXin_alpha.core.proxies.remote import RemoteDatabaseProxy, RemoteJobsProxy
from LiuXin_alpha.core.queries import CoreQueryResult


def test_generated_proxy_docstrings_describe_live_callable_arguments() -> None:
    """
    Require descriptive reST fields and explicitly skipped integration examples on generated dispatchers.

    Inspect actual closures so a runtime docstring replacement cannot silently
    discard documentation that exists only on the nested source declaration.

    Example:
        >>> test_generated_proxy_docstrings_describe_live_callable_arguments()


    :return: ``None`` if both local and remote generated functions document their real arguments and return value.
    """
    proxies = (
        LocalDatabaseProxy(Mock()),
        RemoteDatabaseProxy(endpoint="http://127.0.0.1:8080"),
    )
    for proxy in proxies:
        dispatcher = proxy.get_row
        description = inspect.getdoc(dispatcher)
        assert description is not None
        assert description.splitlines()[0].startswith("Dispatch")
        assert re.findall(r"^:param ([^:]+): .+", description, re.MULTILINE) == list(
            inspect.signature(dispatcher).parameters
        )
        assert re.search(r"^:return: .+", description, re.MULTILINE)
        assert "Example:" in description
        examples = doctest.DocTestParser().get_examples(description)
        assert examples
        assert all(example.options.get(doctest.SKIP) for example in examples)
    assert "database.get_row" in inspect.getdoc(proxies[0].get_row)


def test_local_dynamic_dispatch_consumes_the_write_override() -> None:
    """
    Preserve name-inferred query routing and explicit write overrides on the local generated callable.

    Example:
        >>> test_local_dynamic_dispatch_consumes_the_write_override()


    :return: ``None`` if target arguments are preserved and the write control never leaks into target kwargs.
    """
    runtime = Mock()
    runtime.invoke_query.return_value = "read"
    runtime.invoke_command.return_value = "write"
    proxy = LocalDatabaseProxy(runtime)

    assert proxy.get_row(7) == "read"
    runtime.invoke_query.assert_called_once_with(
        target="database", method="get_row", args=(7,), kwargs={}
    )
    assert proxy.get_row(7, write=True, include_links=True) == "write"
    runtime.invoke_command.assert_called_once_with(
        target="database",
        method="get_row",
        args=(7,),
        kwargs={"include_links": True},
    )
    assert proxy.get_row.__name__ == "proxy_database_get_row"


def test_remote_dynamic_dispatch_keeps_write_as_a_target_keyword() -> None:
    """
    Preserve remote name-based routing while forwarding a write keyword as target data.

    Unlike the local generic call helper, the remote dynamic closure consumes no
    write override. Mocked RPC methods expose the exact invoke payloads.

    Example:
        >>> test_remote_dynamic_dispatch_keeps_write_as_a_target_keyword()


    :return: ``None`` if read/write method names choose the documented routes and keyword arguments survive.
    """
    proxy = RemoteDatabaseProxy(endpoint="http://127.0.0.1:8080")
    proxy._rpc_query = Mock(return_value="read")
    proxy._rpc_command = Mock(return_value="write")

    assert proxy.get_row(7, write=True) == "read"
    proxy._rpc_query.assert_called_once_with(
        "invoke",
        payload={
            "target": "database",
            "method": "get_row",
            "args": [7],
            "kwargs": {"write": True},
        },
    )
    assert proxy.set_pref("view", "list") == "write"
    proxy._rpc_command.assert_called_once_with(
        "invoke",
        payload={
            "target": "database",
            "method": "set_pref",
            "args": ["view", "list"],
            "kwargs": {},
        },
    )
    assert proxy.get_row.__name__ == "remote_proxy_get_row"


def test_job_proxies_retain_their_distinct_unexpected_result_policies() -> None:
    """
    Characterize the documented local rejection and remote empty-dictionary fallback for malformed job results.

    This records the existing compatibility difference rather than changing the
    remote behavior as part of a documentation pass.

    Example:
        >>> test_job_proxies_retain_their_distinct_unexpected_result_policies()


    :return: ``None`` if a nonmapping successful result raises locally but becomes an empty remote dictionary.
    """
    runtime = Mock()
    runtime.execute_query.return_value = CoreQueryResult(True, "request-1", result=[])
    with pytest.raises(TypeError, match="jobs.get result must be a mapping"):
        LocalJobsProxy(runtime).get("job-1")

    remote = RemoteJobsProxy(endpoint="http://127.0.0.1:8080")
    remote._rpc_query = Mock(return_value=[])
    assert remote.get("job-1") == {}
