"""
Exercise compatibility acquisition with in-memory Core receipts and response factories.

Retained legacy host methods are fixture compatibility helpers, not evidence that
the current adapter invokes them. Local targets are simulated with fixed bytes;
no file is opened and example redirect URLs are never contacted. Some historical
test names predate the Core-backed path; their docstrings describe current coverage.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from LiuXin_alpha.surfaces.acquisition.api import (
    AcquisitionCompatApi,
    _coerce_payload_bytes,
    _cover_dimensions,
)
from LiuXin_alpha.surfaces.web_readonly.app import _ResolvedFileTarget, _Response


@dataclass(frozen=True)
class _WorkRow:
    """
    Carry the fixed work identity used by the in-memory browse receipt.

    Example:
        >>> _WorkRow().work_id
        1
    """

    work_id: int = 1


@dataclass(frozen=True)
class _ImageRow:
    """
    Carry the image ID inserted into the fake Core cover-discovery receipt.

    Example:
        >>> _ImageRow().image_id
        1
    """

    image_id: int = 1


@dataclass(frozen=True)
class _FileRow:
    """
    Pair an intentionally unvalidated file ID with a filename used to derive fixture format metadata.

    Example:
        >>> _FileRow(file_id=None).name
        'dummy.epub'
    """

    file_id: object = 7
    name: str = "dummy.epub"


class _DummyCore:
    """
    Answer browse, cover, format, and read queries from mutable host fixture state without external I/O.

    Example:
        >>> host = _DummyHost()
        >>> host.core.query("browse.work", {"work_id": 2})
        {'work': None}
    """

    def __init__(self, host: "_DummyHost") -> None:
        """
        Retain the fixture host by reference so later test mutations affect query receipts.

        Example:
            >>> host = _DummyHost()
            >>> _DummyCore(host).host is host
            True


        :param host: Mutable fixture owner supplying work, image, file, payload, and target state.
        :return: None after assigning the host reference.
        """
        self.host = host

    def query(self, name: str, payload=None):
        """
        Return deterministic acquisition receipts, raise injected image-read errors, or reject unknown query names.

        Only work ID one exists. Format discovery drops None IDs, derives suffixes,
        and marks every remaining file readable. Image reads return fixed file bytes
        for a None payload or the configured value; non-image reads return download.

        Example:
            >>> _DummyHost().core.query("acquisition.read", {"kind": "legacy-file"})["content"]
            b'download'


        :param name: Expected browse.work, acquisition.cover, acquisition.formats, or acquisition.read query.
        :param payload: Optional request mapping shallow-copied before operation-specific access.
        :return: Fresh mapping receipt whose row/payload values may remain shared with the host.
        :raises AssertionError: For any unsupported query name.
        """
        payload = dict(payload or {})
        if name == "browse.work":
            return {
                "work": self.host.work_row
                if int(payload["work_id"]) == 1
                else None
            }
        if name == "acquisition.cover":
            if self.host.image_row is None:
                return {"covers": []}
            resolution = self._image_resolution()
            return {
                "covers": [
                    {
                        "kind": "image",
                        "id": self.host.image_row.image_id,
                        "name": "cover.png",
                        "mime_type": "image/png",
                        "resolution": resolution,
                    }
                ]
            }
        if name == "acquisition.formats":
            formats = []
            for row in self.host.file_rows:
                if row.file_id is None:
                    continue
                formats.append(
                    {
                        "kind": "legacy-file",
                        "id": row.file_id,
                        "name": row.name,
                        "extension": Path(row.name).suffix.lstrip("."),
                        "mime_type": "application/epub+zip",
                        "resolution": {
                            "delivery": "core",
                            "readable": True,
                        },
                    }
                )
            return {"formats": formats}
        if name == "acquisition.read":
            if payload["kind"] == "image":
                stored = self.host.stored_payload
                if isinstance(stored, Exception):
                    raise stored
                return {
                    "resource": self._image_resolution(),
                    "content": b"file" if stored is None else stored,
                }
            return {
                "resource": {"delivery": "core", "readable": True},
                "content": b"download",
            }
        raise AssertionError("Unexpected Core query: {}".format(name))

    def _image_resolution(self) -> dict[str, object]:
        """
        Prefer an unreadable redirect target, otherwise advertise Core readability for any target or non-None payload.

        A redirect wins even when stored_payload is an exception; that fixture
        state therefore does not itself exercise a failed read before redirecting.

        Example:
            >>> _DummyHost().core._image_resolution()
            {'delivery': 'core', 'readable': True}


        :return: Redirect, readable Core, or unavailable resolution derived from current host state.
        """
        target = self.host.image_target
        if target is not None and target.mode == "redirect":
            return {
                "delivery": "redirect",
                "readable": False,
                "location": target.location,
            }
        if self.host.stored_payload is not None or target is not None:
            return {"delivery": "core", "readable": True}
        return {"delivery": "unavailable", "readable": False}


class _DummyHost:
    """
    Supply mutable acquisition fixtures, current Core-backed responses, and retained legacy adapter hooks.

    Example:
        >>> _DummyHost().placeholder_dimensions
        []
    """

    def __init__(self) -> None:
        """
        Initialize one work, cover, EPUB, image bytes, a simulated local target, and an empty placeholder-call log.

        Example:
            >>> _DummyHost().stored_payload
            b'image-bytes'


        :return: None after attaching fixture values and a Core double borrowing this host.
        """
        self.work_row = _WorkRow()
        self.image_row = _ImageRow()
        self.file_row = _FileRow()
        self.file_rows = [self.file_row]
        self.stored_payload: object | None = b"image-bytes"
        self.image_target: _ResolvedFileTarget | None = _ResolvedFileTarget(
            mode="local",
            location="/tmp/cover.png",
            download_name="cover.png",
        )
        self.placeholder_dimensions: list[tuple[int, int]] = []
        self.core = _DummyCore(self)

    def acquisition_text_response(self, status: str, text: str, *, content_type: str) -> _Response:
        """
        Encode fixture text into a one-chunk response with the selected status and media type.

        Example:
            >>> _DummyHost().acquisition_text_response("404 Not Found", "missing", content_type="text/plain").status
            '404 Not Found'


        :param status: HTTP status text retained unchanged.
        :param text: Response text encoded as UTF-8.
        :param content_type: Content-Type header value supplied by the adapter.
        :return: In-memory response containing one byte chunk.
        """
        return _Response(status=status, headers=[("Content-Type", content_type)], body=[text.encode("utf-8")])

    def acquisition_bytes_response(
        self,
        payload: bytes,
        *,
        download_name: str,
        disposition: str = "attachment",
        content_type_override: str | None = None,
    ) -> _Response:
        """
        Wrap a supplied payload without copying it and expose filename/disposition through assertion-only X headers.

        Example:
            >>> _DummyHost().acquisition_bytes_response(b"image", download_name="cover.png").body
            [b'image']


        :param payload: Byte payload retained as the sole body element without validation.
        :param download_name: Suggested name recorded in X-Name rather than a real Content-Disposition header.
        :param disposition: Disposition token recorded in X-Disposition, defaulting to attachment.
        :param content_type_override: Truthy media type, otherwise application/octet-stream.
        :return: 200 response with fixture headers and the original payload object.
        """
        return _Response(
            status="200 OK",
            headers=[("Content-Type", content_type_override or "application/octet-stream"), ("X-Name", download_name), ("X-Disposition", disposition)],
            body=[payload],
        )

    def acquisition_redirect_response(self, location: str) -> _Response:
        """
        Return a simulated 302 Location response without validating or following its URL.

        Example:
            >>> _DummyHost().acquisition_redirect_response("/cover").status
            '302 Found'


        :param location: Location header value retained unchanged.
        :return: Redirect response with a single empty byte chunk.
        """
        return _Response(status="302 Found", headers=[("Location", location)], body=[b""])

    def acquisition_file_response(self, path: Path, *, download_name: str, environ, disposition: str = "attachment", content_type_override: str | None = None) -> _Response:
        """
        Retain a legacy file-response fixture that records path metadata but never opens the file.

        Example:
            >>> response = host.acquisition_file_response(Path("cover.png"), download_name="cover.png", environ={})  # doctest: +SKIP


        :param path: Path stringified only for the X-Path assertion header.
        :param download_name: Name recorded in X-Name.
        :param environ: Request context discarded without inspection.
        :param disposition: Token recorded in X-Disposition.
        :param content_type_override: Truthy Content-Type value, otherwise application/octet-stream.
        :return: 200 response containing fixed file bytes, not bytes read from path.
        """
        del environ
        return _Response(
            status="200 OK",
            headers=[("Content-Type", content_type_override or "application/octet-stream"), ("X-Path", str(path)), ("X-Name", download_name), ("X-Disposition", disposition)],
            body=[b"file"],
        )

    def acquisition_split_book_token(self, raw_book_id: str) -> tuple[int | None, str]:
        """
        Special-case bad as invalid, otherwise int-convert the first underscore component and retain the suffix.

        Unlike the production catalogue parser, other malformed integer text
        raises instead of becoming None.

        Example:
            >>> _DummyHost().acquisition_split_book_token("1_90_120")
            (1, '90_120')


        :param raw_book_id: Fixture token with an integer prefix, or exact bad failure selector.
        :return: Parsed integer/suffix pair, or (None, '') for bad.
        """
        if raw_book_id == "bad":
            return None, ""
        base, _sep, suffix = str(raw_book_id).partition("_")
        return int(base), suffix

    def acquisition_work_row(self, row_id: int):
        """
        Retain the legacy direct-work hook, resolving only integer-converted ID one.

        Example:
            >>> _DummyHost().acquisition_work_row(2) is None
            True


        :param row_id: Identifier converted to int before fixture comparison.
        :return: Shared work row for one, otherwise None.
        """
        return self.work_row if int(row_id) == 1 else None

    def acquisition_work_image_row(self, work_row):
        """
        Retain the legacy image-selection hook for a work equal to the configured fixture.

        Example:
            >>> host = _DummyHost()
            >>> host.acquisition_work_image_row(host.work_row) is host.image_row
            True


        :param work_row: Work value compared by equality with the configured work.
        :return: Configured image row on a match, otherwise None.
        """
        return self.image_row if work_row == self.work_row else None

    def acquisition_resolve_storage_image(self, image_row):
        """
        Retain a legacy stored-image double capturing the current payload for a matching image row.

        Example:
            >>> host = _DummyHost()
            >>> host.acquisition_resolve_storage_image(host.image_row).as_bytes()
            b'image-bytes'


        :param image_row: Image compared by equality with the configured fixture.
        :return: Captured-payload reader, or None for a mismatch or None payload.
        """
        payload = self.stored_payload

        class _Stored:
            """
            Hold access to the enclosing call's captured payload rather than later host mutations.

            Example:
                >>> stored = _Stored()  # doctest: +SKIP
            """

            def as_bytes(self_nonlocal):
                """
                Return the captured payload unchanged or raise it when it is an Exception instance.

                Example:
                    >>> content = stored.as_bytes()  # doctest: +SKIP


                :return: Captured value without bytes coercion, unless it represents an injected exception.
                """
                if isinstance(payload, Exception):
                    raise payload
                return payload

        if image_row != self.image_row or payload is None:
            return None
        return _Stored()

    def acquisition_resolve_image_target(self, image_row):
        """
        Retain legacy target selection by fixture image equality without checking the target path or URL.

        Example:
            >>> host = _DummyHost()
            >>> host.acquisition_resolve_image_target(host.image_row) is host.image_target
            True


        :param image_row: Image compared with the configured fixture.
        :return: Shared configured target for a match, otherwise None.
        """
        return self.image_target if image_row == self.image_row else None

    def acquisition_image_download_name(self, image_row) -> str:
        """
        Return the fixed legacy fixture cover filename regardless of the supplied row.

        Example:
            >>> _DummyHost().acquisition_image_download_name(None)
            'cover.png'


        :param image_row: Compatibility row argument, intentionally ignored.
        :return: Fixed cover.png filename.
        """
        return "cover.png"

    def acquisition_image_content_type(self, image_row) -> str:
        """
        Return the fixture's fixed PNG MIME declaration without inspecting a row or payload.

        Example:
            >>> _DummyHost().acquisition_image_content_type(None)
            'image/png'


        :param image_row: Compatibility image context, intentionally ignored.
        :return: Fixed image/png MIME text.
        """
        return "image/png"

    def acquisition_placeholder_cover_svg(self, work_row, *, width: int, height: int) -> bytes:
        """
        Record placeholder dimensions and encode a minimal SVG-shaped size marker without rendering artwork.

        Example:
            >>> _DummyHost().acquisition_placeholder_cover_svg(None, width=4, height=5)
            b'<svg>4x5</svg>'


        :param work_row: Work context accepted but not inspected by this fixture renderer.
        :param width: Width appended unchanged to the call log and formatted into the marker.
        :param height: Height appended unchanged to the call log and formatted into the marker.
        :return: UTF-8 bytes containing the requested dimensions inside an svg element.
        """
        self.placeholder_dimensions.append((width, height))
        return f"<svg>{width}x{height}</svg>".encode("utf-8")

    def acquisition_related_rows_by_table(self, work_row) -> dict[str, list[object]]:
        """
        Retain a legacy relationship hook returning a copied list of configured file rows for any work.

        Example:
            >>> len(_DummyHost().acquisition_related_rows_by_table(None)["files"])
            1


        :param work_row: Work context ignored by this fixed-data provider.
        :return: New files mapping/list retaining the original fixture row objects.
        """
        return {"files": list(self.file_rows)}

    def acquisition_work_file_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]:
        """
        Copy a legacy files group without additional relationship traversal or filtering.

        Example:
            >>> _DummyHost().acquisition_work_file_rows({})
            []


        :param related_rows_by_table: Mapping supplying an optional files iterable.
        :return: New list of the supplied file objects, empty when the key is absent.
        """
        return list(related_rows_by_table.get("files", []))

    def acquisition_download_name_for_file_row(self, file_row) -> str:
        """
        Return the legacy fixture file's name attribute unchanged.

        Example:
            >>> _DummyHost().acquisition_download_name_for_file_row(_FileRow())
            'dummy.epub'


        :param file_row: Fixture object exposing name.
        :return: Original name value without sanitization or fallback.
        """
        return file_row.name

    def acquisition_file_id(self, file_row) -> object:
        """
        Return the fixture file ID unchanged, including intentionally invalid or None IDs.

        Example:
            >>> _DummyHost().acquisition_file_id(_FileRow())
            7


        :param file_row: Fixture object exposing file_id.
        :return: Raw file_id attribute without numeric conversion.
        """
        return file_row.file_id

    def acquisition_serve_file_download(self, raw_file_id: str, environ) -> _Response:
        """
        Retain a legacy fixed-download response that records the requested ID without resolving content.

        Example:
            >>> _DummyHost().acquisition_serve_file_download("7", {}).body
            [b'download']


        :param raw_file_id: Identifier stringified into the X-File-Id assertion header.
        :param environ: Compatibility request context discarded without inspection.
        :return: 200 response with fixed download bytes.
        """
        del environ
        return _Response(status="200 OK", headers=[("X-File-Id", str(raw_file_id))], body=[b"download"])


def _decode_response(response: _Response) -> tuple[str, dict[str, str], bytes]:
    """
    Consume in-memory response chunks and collapse duplicate header names for test assertions.

    Example:
        >>> _decode_response(_DummyHost().acquisition_redirect_response("/cover"))[2]
        b''


    :param response: Fixture response whose body is joined without calling close.
    :return: Status text, last-value header dict, and joined raw bytes.
    """
    return response.status, dict(response.headers), b"".join(response.body)


def test_acquisition_api_serves_cover_bytes() -> None:
    """
    Serve Core-provided cover bytes inline with the receipt's PNG media type.

    Example:
        >>> test_acquisition_api_serves_cover_bytes()


    :return: None after status, MIME, disposition, and exact payload assertions.
    """
    api = AcquisitionCompatApi(_DummyHost())
    status, headers, body = _decode_response(api.serve_compat_get("cover", "1", {}, {}))
    assert status == "200 OK"
    assert headers["Content-Type"] == "image/png"
    assert headers["X-Disposition"] == "inline"
    assert body == b"image-bytes"


def test_acquisition_api_serves_format_download() -> None:
    """
    Match EPUB metadata and serve the fixed Core download payload under its declared filename.

    Example:
        >>> test_acquisition_api_serves_format_download()


    :return: None after success, filename, and payload assertions.
    """
    api = AcquisitionCompatApi(_DummyHost())
    status, headers, body = _decode_response(api.serve_compat_get("epub", "1", {}, {}))
    assert status == "200 OK"
    assert headers["X-Name"] == "dummy.epub"
    assert body == b"download"


def test_acquisition_api_rejects_invalid_book_id() -> None:
    """
    Turn the host parser's invalid-ID sentinel into a plain-text 400 response for a format request.

    Example:
        >>> test_acquisition_api_rejects_invalid_book_id()


    :return: None after bad-request status, media type, and message assertions.
    """
    api = AcquisitionCompatApi(_DummyHost())
    status, headers, body = _decode_response(api.serve_compat_get("epub", "bad", {}, {}))
    assert status == "400 Bad Request"
    assert headers["Content-Type"] == "text/plain"
    assert b"Invalid book id" in body


@pytest.mark.parametrize(
    ("payload", "expected"),
    (
        ("cover", b"cover"),
        (b"cover", b"cover"),
        (bytearray(b"cover"), b"cover"),
    ),
)
def test_payloads_are_coerced_to_bytes(payload: object, expected: bytes) -> None:
    """
    Exercise the retained standalone payload coercer for text, bytes, and mutable byte arrays.

    Example:
        >>> test_payloads_are_coerced_to_bytes("cover", b"cover")


    :param payload: Parametrized supported compatibility value passed to the helper.
    :param expected: Exact bytes expected from normalization.
    :return: None after byte equality; this does not prove endpoints call the helper.
    """
    assert _coerce_payload_bytes(payload) == expected


@pytest.mark.parametrize(
    ("suffix", "query", "thumb", "expected"),
    (
        ("90_120", {}, True, (90, 120)),
        ("single", {}, True, (60, 80)),
        ("bad_suffix", {}, True, (60, 80)),
        ("90_120", {"sz": ["4x5"]}, True, (4, 5)),
        ("", {"sz": ["0x-2"]}, True, (1, 1)),
        ("90_120", {"sz": ["badxsize"]}, True, (90, 120)),
        ("", {"sz": ["full"]}, True, (240, 320)),
        ("", {}, False, (240, 320)),
        ("", {"sz": ["7"]}, True, (7, 7)),
        ("", {"sz": ["invalid"]}, True, (60, 80)),
    ),
)
def test_cover_dimensions_accept_compatibility_size_forms(
    suffix: str,
    query: dict[str, list[str]],
    thumb: bool,
    expected: tuple[int, int],
) -> None:
    """
    Verify suffix dimensions, query precedence, minimum clamping, full-cover defaults, and malformed-size fallback.

    Example:
        >>> test_cover_dimensions_accept_compatibility_size_forms("90_120", {}, True, (90, 120))


    :param suffix: Parametrized underscore size hint.
    :param query: Parametrized parsed sz values or empty mapping.
    :param thumb: Thumbnail/full-cover selection for default behavior.
    :param expected: Exact width/height pair required for this combination.
    :return: None after helper output matches the selected compatibility rule.
    """
    assert _cover_dimensions(suffix=suffix, query=query, thumb=thumb) == expected


def test_cover_requests_reject_invalid_or_missing_books() -> None:
    """
    Distinguish an invalid host-parsed cover ID from a valid ID with no work receipt.

    Example:
        >>> test_cover_requests_reject_invalid_or_missing_books()


    :return: None after separate 400 invalid-ID and 404 missing-work response assertions.
    """
    api = AcquisitionCompatApi(_DummyHost())

    invalid = _decode_response(api.serve_cover_or_thumb("bad", query={}, environ={}, thumb=True))
    missing = _decode_response(api.serve_cover_or_thumb("2", query={}, environ={}, thumb=True))

    assert invalid[0] == "400 Bad Request"
    assert b"Invalid book id" in invalid[2]
    assert missing[0] == "404 Not Found"
    assert b"Book row not found" in missing[2]


def test_cover_request_uses_placeholder_when_no_image_exists() -> None:
    """
    Generate inline SVG fixture bytes using the thumbnail suffix when cover discovery returns no rows.

    Example:
        >>> test_cover_request_uses_placeholder_when_no_image_exists()


    :return: None after placeholder MIME/name, recorded dimensions, and exact marker bytes are checked.
    """
    host = _DummyHost()
    host.image_row = None
    api = AcquisitionCompatApi(host)

    status, headers, body = _decode_response(
        api.serve_compat_get("thumb", "1_90_120", {}, {})
    )

    assert status == "200 OK"
    assert headers["Content-Type"] == "image/svg+xml"
    assert headers["X-Name"] == "cover.svg"
    assert host.placeholder_dimensions == [(90, 120)]
    assert body == b"<svg>90x120</svg>"


def test_failed_stored_cover_falls_back_to_redirect_target() -> None:
    """
    Serve the fixture's unreadable redirect resolution despite its independently configured stored-payload exception.

    The fake Core marks redirects unreadable, so the current test does not attempt
    a stored read and does not itself prove recovery after a read failure.

    Example:
        >>> test_failed_stored_cover_falls_back_to_redirect_target()


    :return: None after redirect status, exact Location, and empty-body assertions.
    """
    host = _DummyHost()
    host.stored_payload = RuntimeError("unreadable")
    host.image_target = _ResolvedFileTarget(
        mode="redirect",
        location="https://covers.example/cover.png",
        download_name="cover.png",
    )
    api = AcquisitionCompatApi(host)

    status, headers, body = _decode_response(
        api.serve_cover_or_thumb("1", query={}, environ={}, thumb=False)
    )

    assert status == "302 Found"
    assert headers["Location"] == "https://covers.example/cover.png"
    assert body == b""


def test_missing_stored_cover_uses_local_file_target() -> None:
    """
    Serve the Core double's fixed file bytes when a simulated local target exists and stored_payload is None.

    No physical file is opened and the legacy host file-response hook is not the
    current adapter path; the Core double advertises readability for the target.

    Example:
        >>> test_missing_stored_cover_uses_local_file_target()


    :return: None after inline cover name, status, and fixed payload assertions.
    """
    host = _DummyHost()
    host.stored_payload = None
    api = AcquisitionCompatApi(host)

    status, headers, body = _decode_response(
        api.serve_cover_or_thumb("1", query={}, environ={"key": "value"}, thumb=False)
    )

    assert status == "200 OK"
    assert headers["X-Name"] == "cover.png"
    assert headers["X-Disposition"] == "inline"
    assert body == b"file"


def test_missing_cover_targets_fall_back_to_full_size_placeholder() -> None:
    """
    Generate a 240x320 SVG marker for a cover request with neither readable bytes nor a redirect target.

    Example:
        >>> test_missing_cover_targets_fall_back_to_full_size_placeholder()


    :return: None after placeholder media type, dimensions, and byte-content checks.
    """
    host = _DummyHost()
    host.stored_payload = None
    host.image_target = None
    api = AcquisitionCompatApi(host)

    status, headers, body = _decode_response(
        api.serve_cover_or_thumb("1", query={}, environ={}, thumb=False)
    )

    assert status == "200 OK"
    assert headers["Content-Type"] == "image/svg+xml"
    assert host.placeholder_dimensions == [(240, 320)]
    assert body == b"<svg>240x320</svg>"


def test_format_requests_report_missing_books_and_formats() -> None:
    """
    Return distinct not-found messages for an absent work and an existing work without the requested extension.

    Example:
        >>> test_format_requests_report_missing_books_and_formats()


    :return: None after both 404 response bodies are checked.
    """
    api = AcquisitionCompatApi(_DummyHost())

    missing_book = _decode_response(api.serve_compat_get("epub", "2", {}, {}))
    missing_format = _decode_response(api.serve_compat_get("pdf", "1", {}, {}))

    assert missing_book[0] == "404 Not Found"
    assert b"Book row not found" in missing_book[2]
    assert missing_format[0] == "404 Not Found"
    assert b"No such format" in missing_format[2]


def test_format_matching_skips_rows_without_file_ids() -> None:
    """
    Accept a dotted uppercase extension after the Core double removes a None-ID file from its discovery receipt.

    Missing-ID filtering occurs in this fixture's Core producer, so this case
    alone does not verify the adapter's malformed-record filtering branch.

    Example:
        >>> test_format_matching_skips_rows_without_file_ids()


    :return: None after the remaining matching filename and download payload are selected.
    """
    host = _DummyHost()
    host.file_rows = [
        _FileRow(file_id=None, name="missing.epub"),
        _FileRow(file_id=8, name="available.EPUB"),
    ]
    api = AcquisitionCompatApi(host)

    status, headers, body = _decode_response(
        api.serve_compat_get(".EPUB", "1", {}, {"request": "environment"})
    )

    assert status == "200 OK"
    assert headers["X-Name"] == "available.EPUB"
    assert body == b"download"
