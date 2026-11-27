"""Legacy scans and small content reads fail before unbounded materialization."""

import io
from types import SimpleNamespace

import pytest

from LiuXin_alpha.core.acquisition_ranges import CHUNK_BYTES, read_small_location
from LiuXin_alpha.core.application_api import CoreApplicationAPI
from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.execution_bounds import bounded_table_rows, bounded_values
from LiuXin_alpha.core.program_services.catalog import search_global


def test_scan_lookahead_is_bounded_and_no_partial_result_escapes():
    consumed = []

    def values():
        for value in range(100):
            consumed.append(value)
            yield value

    with pytest.raises(CoreDispatchError) as error:
        bounded_values(values(), maximum=3)
    assert error.value.code == "scan_limit_exceeded"
    assert consumed == [0, 1, 2, 3]
    assert bounded_values(range(3), maximum=3) == [0, 1, 2]


def test_count_guard_does_not_open_oversized_table_reader():
    def forbidden(*args, **kwargs):
        raise AssertionError("oversized source must not materialize")

    source = SimpleNamespace(
        get_record_count=lambda table: 10001, get_all_rows=forbidden
    )
    with pytest.raises(CoreDispatchError, match="row budget"):
        bounded_table_rows(source, "works")


def test_global_scan_shares_budget_across_tables(monkeypatch):
    import LiuXin_alpha.core.program_services.catalog as module

    monkeypatch.setattr(module, "SCAN_ROWS", 3)
    source = SimpleNamespace(
        get_all_rows=lambda table, **kwargs: iter([{"title": "match"}] * 2)
    )
    with pytest.raises(CoreDispatchError) as error:
        search_global(
            SimpleNamespace(read_source=source),
            SimpleNamespace(payload={"text": "match", "tables": ["works", "agents"]}),
        )
    assert error.value.code == "scan_limit_exceeded"


def test_legacy_rows_query_caps_all_page_spellings():
    api = CoreApplicationAPI()
    for value in (None, 1001, 100000):
        assert api._cache_query({"table": "works", "limit": value}).limit == 1000
    assert api._cache_query({"table": "works"}).limit == 1000
    assert api._cache_query({"table": "works", "limit": 0}).limit == 0


def test_small_content_checks_size_before_opening_and_detects_growth():
    def forbidden(*args, **kwargs):
        raise AssertionError("large content must not be opened")

    manager = SimpleNamespace(
        stat=lambda location: SimpleNamespace(size=CHUNK_BYTES + 1), get=forbidden
    )
    with pytest.raises(CoreDispatchError) as error:
        read_small_location(manager, object())
    assert error.value.code == "content_too_large"

    reads = []

    class Source(io.BytesIO):
        def read(self, size=-1):
            reads.append(size)
            assert 0 < size <= CHUNK_BYTES + 1
            return super().read(size)

    manager.stat = lambda location: SimpleNamespace(
        size=3, version="one", modified_at=None
    )
    manager.get = lambda *args, **kwargs: Source(b"growing")
    with pytest.raises(CoreDispatchError) as error:
        read_small_location(manager, object())
    assert error.value.code == "acquisition_changed"
    assert reads == [CHUNK_BYTES + 1]
    manager.get = lambda *args, **kwargs: Source(b"abc")
    assert read_small_location(manager, object()) == b"abc"


def test_readiness_reports_unavailable_catalogue_without_driver_secrets():
    from LiuXin_alpha.core.runtime import CoreRuntime

    def unavailable():
        raise RuntimeError("password=secret database connection failed")

    runtime = SimpleNamespace(
        catalog=SimpleNamespace(
            db=SimpleNamespace(macros=SimpleNamespace(read_transaction=unavailable))
        ),
        cache=None,
        core_uuid="test",
        is_shutdown=False,
        _events=SimpleNamespace(statistics=lambda: {}),
    )
    status = CoreRuntime._handle_readiness_query(None, runtime, None)
    assert status["state"] == "degraded" and status["ready"] is False
    assert status["catalogue"] == "unavailable"
    assert "secret" not in str(status)
