"""Real direct and HTTP Core failures must survive the surface read model."""

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
    queries = []

    def query_cache(query):
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
