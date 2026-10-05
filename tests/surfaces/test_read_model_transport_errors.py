"""
Verify that real direct and loopback-HTTP Core failures survive shared surface projection.

Temporary runtimes use deterministic handlers or failing cache doubles rather
than a database. Every started daemon/runtime is stopped in finally. These tests
require local socket permission; they do not contact an external service.
"""

from types import SimpleNamespace

import pytest

from LiuXin_alpha.caches.api import (
    UnknownCacheFieldError,
    UnknownCacheTableError,
    UnsupportedCacheQueryError,
)
from LiuXin_alpha.core import CoreHttpDaemon, CoreRuntime, RemoteCoreClient
from LiuXin_alpha.core.errors import CoreError, CoreHandlerError
from LiuXin_alpha.core.proxies.remote import RemoteProxyError
from LiuXin_alpha.surfaces.core import CoreRow
from LiuXin_alpha.surfaces.web_readonly.app import ReadOnlyWebApplication


def test_read_model_preserves_direct_and_rpc_error_codes_and_details() -> None:
    """
    Preserve structured failure codes/details across table, count, lookup, search, and relationship reads on both transports.

    A deliberately absent row remains None, distinguishing normal missing data
    from the same handlers' explicit CoreError failures. The HTTP client uses an
    ephemeral loopback daemon and all runtime resources are cleaned up in finally.

    Example:
        >>> test_read_model_preserves_direct_and_rpc_error_codes_and_details()  # doctest: +SKIP


    :return: None after direct/RPC error translation and missing-row assertions.
    """
    runtime = CoreRuntime(library=SimpleNamespace(database=SimpleNamespace()))
    schema = {
        "tables": [
            {
                "name": "works",
                "columns": ["work_id", "work_title"],
                "id_column": "work_id",
                "related_tables": ["tags"],
                "relations_included": True,
            },
            {
                "name": "tags",
                "columns": ["tag_id", "tag"],
                "id_column": "tag_id",
                "related_tables": ["works"],
                "relations_included": True,
            },
        ]
    }
    runtime.register_query_handler("schema.tables", lambda _runtime, _query: schema)

    def fail_read(_runtime, query):
        """
        Return an absent-record receipt only for row 404, otherwise raise an operation-labelled CoreError.

        Example:
            >>> receipt = fail_read(runtime, query)  # doctest: +SKIP


        :param _runtime: Dispatcher-supplied runtime, unused by this deterministic handler.
        :param query: Read request whose name and row_id select the missing-record exception to failure.
        :return: Mapping with record None only for rows.get at ID 404.
        :raises CoreError: For every other request, with test_read_failed and operation details.
        """
        if query.name == "rows.get" and query.payload["row_id"] == 404:
            return {"record": None}
        raise CoreError(
            "read failed", code="test_read_failed", details={"operation": query.name}
        )

    for name in ("rows.get", "rows.query", "relations.list"):
        runtime.register_query_handler(name, fail_read)
    daemon = CoreHttpDaemon(runtime, endpoint_namespace="read-errors")
    try:
        daemon.start()
        for client, error_type in (
            (runtime, CoreHandlerError),
            (RemoteCoreClient(endpoint=daemon.base_url), RemoteProxyError),
        ):
            application = ReadOnlyWebApplication(client)
            read_model = application.read_model
            for name, args, operation in (
                ("rows_for_table", ("works",), "rows.query"),
                ("table_record_count", ("works",), "rows.query"),
                ("search_rows", ("works", "work_title", "snow"), "rows.query"),
                ("row_by_id", ("works", 7), "rows.get"),
                (
                    "interlinked_rows",
                    (CoreRow("works", 7, {"work_id": 7}), "tags"),
                    "relations.list",
                ),
            ):
                with pytest.raises(error_type) as raised:
                    getattr(read_model, name)(*args)
                assert raised.value.code == "test_read_failed"
                assert raised.value.details == {"operation": operation}
            assert read_model.row_by_id("works", 404) is None
    finally:
        daemon.stop()
        runtime.shutdown()


@pytest.mark.parametrize(
    ("failure", "code"),
    [
        (UnknownCacheTableError, "read_query_unavailable"),
        (UnsupportedCacheQueryError, "read_query_unavailable"),
        (UnknownCacheFieldError, "handler_error"),
        (RuntimeError, "handler_error"),
    ],
)
def test_only_known_cache_capability_failures_receive_the_unavailable_code(
    failure, code
) -> None:
    """
    Restrict read_query_unavailable translation to known table/query capability errors on direct and HTTP clients.

    Unknown fields and arbitrary runtime failures remain handler_error. Both
    clients make exactly one cache query, proving these failures do not trigger
    alternate-shape retries or silent database fallback in the tested path.

    Example:
        >>> test_only_known_cache_capability_failures_receive_the_unavailable_code(UnknownCacheTableError, "read_query_unavailable")  # doctest: +SKIP


    :param failure: Parametrized cache exception class raised by the query double.
    :param code: Exact expected public error code for that exception class.
    :return: None after both transport receipts, details, and combined call count are verified.
    """
    queries = []

    def query_cache(query):
        """
        Record a cache request and raise the enclosing test's selected capability or handler error.

        Example:
            >>> query_cache(cache_query)  # doctest: +SKIP


        :param query: Original cache query appended to the shared per-test call log.
        :return: No normal receipt; the selected failure is always raised.
        """
        queries.append(query)
        raise failure("unsupported_view")

    runtime = CoreRuntime(
        library=SimpleNamespace(database=SimpleNamespace()),
        read_source=SimpleNamespace(query_cache=query_cache),
    )
    daemon = CoreHttpDaemon(runtime, endpoint_namespace="cache-read-errors")
    try:
        daemon.start()
        for client, error_type in (
            (runtime, CoreHandlerError),
            (RemoteCoreClient(endpoint=daemon.base_url), RemoteProxyError),
        ):
            with pytest.raises(error_type) as raised:
                client.query("rows.query", {"table": "unsupported_view", "limit": 0})
            assert raised.value.code == code
            if code == "read_query_unavailable":
                assert raised.value.details == {
                    "table": "unsupported_view",
                    "reason": failure.__name__,
                }
            else:
                assert raised.value.details == {"exception_type": failure.__name__}
        # Each client makes exactly one query; unsupported cache reads never
        # silently consult the database or retry another query shape.
        assert len(queries) == 2
    finally:
        daemon.stop()
        runtime.shutdown()
