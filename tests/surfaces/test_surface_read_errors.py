"""
Provide test surface read errors utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test surface read errors through a consuming regression::

        python -m pytest -q tests/surfaces/test_surface_read_errors.py
"""

from io import BytesIO, StringIO
from unittest.mock import Mock, create_autospec
from wsgiref.handlers import SimpleHandler
from wsgiref.util import setup_testing_defaults

import pytest

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.core.errors import CoreHandlerError
from LiuXin_alpha.core.proxies.remote import RemoteProxyError
from LiuXin_alpha.surfaces.api_readonly.app import ApiReadOnlyApplication
from LiuXin_alpha.surfaces.core import CoreRow, CoreRowPage, CoreSurfaceModel
from LiuXin_alpha.surfaces.opds_readonly.app import OpdsReadOnlyApplication
from LiuXin_alpha.surfaces.web_calibre_readonly.app import CalibreReadOnlyWebApplication
from LiuXin_alpha.surfaces.web_readonly.app import ReadOnlyWebApplication


def _app(application_type=ApiReadOnlyApplication):
    """
    Perform the app operation under explicit file-format and conversion rules.

    Example:
        Exercise  app through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :param application_type: Value supplied for application type under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    client = create_autospec(CoreClientAPI, instance=True, spec_set=True)
    application = application_type(client)
    model = create_autospec(CoreSurfaceModel, instance=True, spec_set=True)
    model.table_names.return_value = ("works", "tags", "series", "agents", "files")
    model.table_exists.return_value = True
    model.columns.side_effect = lambda table: (
        ("work_id", "work_title") if table == "works" else ("id", "name")
    )
    model.id_column.side_effect = lambda table: "work_id" if table == "works" else "id"
    model.row.return_value = None
    model.rows.return_value = ()
    model.related.return_value = ((), ())
    model.related_tables.return_value = ()
    model.record_count.return_value = 0
    model.query_rows.return_value = CoreRowPage((), 0, 0, None, True, "test")
    application.model = model
    application.read_model.model = model
    return application


def _environ(path: str):
    """
    Perform the environ operation under explicit file-format and conversion rules.

    Example:
        Exercise  environ through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    environ = {}
    setup_testing_defaults(environ)
    environ["PATH_INFO"], _, environ["QUERY_STRING"] = path.partition("?")
    return environ


@pytest.mark.parametrize(
    "failure",
    (KeyError, ValueError, TypeError, OSError, CoreHandlerError, RemoteProxyError),
)
@pytest.mark.parametrize(
    "path",
    (
        "/api/works/7",
        "/api/files/7",
        "/api/tags/7",
        "/api/series/7",
        "/api/authors/agents/7",
    ),
)
def test_detail_routes_do_not_translate_failed_lookups_into_404(path, failure) -> None:
    """
    Perform the test detail routes do not translate failed lookups into 404 operation under explicit file-format and conversion rules.

    Example:
        Exercise test detail routes do not translate failed lookups into 404 through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param failure: Value supplied for failure under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    application = _app()
    error = failure("read failed")
    application.model.row.side_effect = error
    with pytest.raises(failure) as raised:
        application.handle_request(_environ(path))
    assert raised.value is error


@pytest.mark.parametrize(
    "path",
    (
        "/api/works/7",
        "/api/files/7",
        "/api/tags/7",
        "/api/series/7",
        "/api/authors/agents/7",
    ),
)
def test_detail_routes_keep_404_for_missing_records(path) -> None:
    """
    Perform the test detail routes keep 404 for missing records operation under explicit file-format and conversion rules.

    Example:
        Exercise test detail routes keep 404 for missing records through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    response = _app().handle_request(_environ(path))
    assert response.status == "404 Not Found"


@pytest.mark.parametrize(
    ("path", "status"),
    [
        ("/api/works/bad", "400 Bad Request"),
        ("/api/files/bad", "400 Bad Request"),
        ("/api/tags/bad", "404 Not Found"),
        ("/api/series/bad", "404 Not Found"),
        ("/api/authors/agents/bad", "404 Not Found"),
    ],
)
def test_malformed_ids_keep_the_existing_response_contract(path, status) -> None:
    """
    Perform the test malformed ids keep the existing response contract operation under explicit file-format and conversion rules.

    Example:
        Exercise test malformed ids keep the existing response contract through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param status: Value supplied for status under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    application = _app()
    response = application.handle_request(_environ(path))
    assert response.status == status
    application.model.row.assert_not_called()


@pytest.mark.parametrize(
    "application_type", (OpdsReadOnlyApplication, CalibreReadOnlyWebApplication)
)
def test_opds_relationship_failures_are_not_silently_omitted(application_type) -> None:
    """
    Perform the test opds relationship failures are not silently omitted operation under explicit file-format and conversion rules.

    Example:
        Exercise test opds relationship failures are not silently omitted through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :param application_type: Value supplied for application type under the utility
        contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    application = _app(application_type)
    error = CoreHandlerError("relations failed")
    application.model.related.side_effect = error
    with pytest.raises(CoreHandlerError) as raised:
        application._opds_related_rows_by_table(CoreRow("works", 7, {"work_id": 7}))
    assert raised.value is error


def test_author_route_selection_does_not_hide_lookup_failures() -> None:
    """
    Perform the test author route selection does not hide lookup failures operation under explicit file-format and conversion rules.

    Example:
        Exercise test author route selection does not hide lookup failures through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    application = _app()
    error = ValueError("corrupt row")
    application.model.row.side_effect = error
    with pytest.raises(ValueError) as raised:
        application.catalog.category_route_target("authors", 7)
    assert raised.value is error
    assert (
        application.catalog.category_route_target("authors", "bad") == "/browse/authors"
    )


def test_relationship_schema_failures_reach_the_caller() -> None:
    """
    Perform the test relationship schema failures reach the caller operation under explicit file-format and conversion rules.

    Example:
        Exercise test relationship schema failures reach the caller through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    application = _app(ReadOnlyWebApplication)
    error = OSError("schema unavailable")
    application.model.related_tables.side_effect = error
    with pytest.raises(OSError) as raised:
        application._ordered_related_tables(CoreRow("works", 7, {"work_id": 7}))
    assert raised.value is error
    assert application._ordered_related_tables({}) == []


@pytest.mark.parametrize("method", ("_resolve_storage_file", "_resolve_file_target"))
def test_file_capability_queries_do_not_turn_failures_into_unavailability(
    method,
) -> None:
    """
    Perform the test file capability queries do not turn failures into unavailability operation under explicit file-format and conversion rules.

    Example:
        Exercise test file capability queries do not turn failures into unavailability through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :param method: Value supplied for method under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    application = _app(ReadOnlyWebApplication)
    error = RemoteProxyError("resolution failed")
    application.model.acquisition_resolve.side_effect = error
    with pytest.raises(RemoteProxyError) as raised:
        getattr(application, method)({"file_id": 7, "file_name": "book.epub"})
    assert raised.value is error


@pytest.mark.parametrize("method", ("_resolve_storage_file", "_resolve_file_target"))
def test_explicitly_unavailable_files_are_not_query_failures(method) -> None:
    """
    Perform the test explicitly unavailable files are not query failures operation under explicit file-format and conversion rules.

    Example:
        Exercise test explicitly unavailable files are not query failures through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :param method: Value supplied for method under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    application = _app(ReadOnlyWebApplication)
    application.model.acquisition_resolve.return_value = {
        "readable": False,
        "delivery": "unavailable",
    }
    assert getattr(application, method)({"file_id": 7}) is None
    assert getattr(application, method)({"file_id": "bad"}) is None
    assert application.model.acquisition_resolve.call_count == 1


@pytest.mark.parametrize(
    ("application_type", "path", "failing_method"),
    [
        (ReadOnlyWebApplication, "/tables/works", "record_count"),
        (ReadOnlyWebApplication, "/", "record_count"),
        (ApiReadOnlyApplication, "/api/works", "query_rows"),
        (ApiReadOnlyApplication, "/api/search?q=snow", "query_rows"),
        (ApiReadOnlyApplication, "/api/tags/7", "row"),
        (OpdsReadOnlyApplication, "/opds/search/snow", "query_rows"),
        (CalibreReadOnlyWebApplication, "/ajax/search/main", "query_rows"),
    ],
)
def test_wsgi_server_reports_failure_without_exposing_private_error_text(
    application_type, path, failing_method
) -> None:
    """
    Perform the test wsgi server reports failure without exposing private error text operation under explicit file-format and conversion rules.

    Example:
        Exercise test wsgi server reports failure without exposing private error text through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :param application_type: Value supplied for application type under the utility
        contract.
    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param failing_method: Value supplied for failing method under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    application = _app(application_type)
    detail = "private-database-detail-for-test"
    getattr(application.model, failing_method).side_effect = OSError(detail)
    output, errors = BytesIO(), StringIO()
    handler = SimpleHandler(
        BytesIO(), output, errors, _environ(path), multithread=False
    )
    handler.run(application)
    response = output.getvalue()
    assert b"500 Internal Server Error" in response
    assert detail.encode() not in response
    assert "Traceback" in errors.getvalue()
    assert detail in errors.getvalue()


def test_complete_empty_collection_is_still_a_success() -> None:
    """
    Perform the test complete empty collection is still a success operation under explicit file-format and conversion rules.

    Example:
        Exercise test complete empty collection is still a success through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    application = _app()
    response = application.handle_request(_environ("/api/works"))
    assert response.status == "200 OK"
    assert b'"items": []' in b"".join(response.body)


@pytest.mark.parametrize("failure", (CoreHandlerError, RemoteProxyError))
def test_home_counts_show_explicit_query_unavailability_without_claiming_zero(
    failure,
) -> None:
    """
    Perform the test home counts show explicit query unavailability without claiming zero operation under explicit file-format and conversion rules.

    Example:
        Exercise test home counts show explicit query unavailability without claiming zero through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :param failure: Value supplied for failure under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    application = _app(ReadOnlyWebApplication)
    application.model.record_count.side_effect = failure(
        "cache cannot query this table",
        code="read_query_unavailable",
    )
    response = application.handle_request(_environ("/"))
    body = b"".join(response.body)
    assert response.status == "200 OK"
    assert b"count unavailable" in body
    assert b"0 rows" not in body
    application.model.rows.assert_not_called()


def test_required_collection_does_not_turn_known_unavailability_into_empty_success() -> (
    None
):
    """
    Perform the test required collection does not turn known unavailability into empty success operation under explicit file-format and conversion rules.

    Example:
        Exercise test required collection does not turn known unavailability into empty success through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    application = _app()
    error = CoreHandlerError("unsupported query", code="read_query_unavailable")
    application.model.query_rows.side_effect = error
    with pytest.raises(CoreHandlerError) as raised:
        application.handle_request(_environ("/api/works"))
    assert raised.value is error


def test_category_presentation_errors_are_not_reported_as_missing_rows() -> None:
    """
    Perform the test category presentation errors are not reported as missing rows operation under explicit file-format and conversion rules.

    Example:
        Exercise test category presentation errors are not reported as missing rows through a consuming regression::

            python -m pytest -q tests/surfaces/test_surface_read_errors.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    application = _app()
    application.model.row.return_value = CoreRow("tags", 7, {"id": 7, "name": "snow"})
    application._entity_summary_payload = Mock(side_effect=KeyError("broken presenter"))
    with pytest.raises(KeyError, match="broken presenter"):
        application.handle_request(_environ("/api/tags/7"))
