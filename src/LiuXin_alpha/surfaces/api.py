from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable, Optional, Protocol, TypeAlias


SurfaceCategoryItem: TypeAlias = dict[str, object]
SurfaceEntitySummary: TypeAlias = dict[str, object]
SurfaceFilePayload: TypeAlias = dict[str, object]
SurfaceRelatedPayload: TypeAlias = dict[str, list[SurfaceEntitySummary]]
SurfaceSearchEntry: TypeAlias = dict[str, object]
SurfaceWorkMetadataPayload: TypeAlias = dict[str, object]


class SurfaceResponseAPI(Protocol):
    status: str
    headers: list[tuple[str, str]]
    body: Iterable[bytes]
    close: Optional[Callable[[], None]]


class ResolvedFileTargetAPI(Protocol):
    mode: str
    location: str
    download_name: str


class ImageHostApi(Protocol):
    @property
    def db(self) -> object: ...

    def _related_rows_by_table(self, row: object) -> dict[str, list[object]]: ...

    def _row_dict(self, table: str, row: object) -> dict[str, object]: ...

    def _row_primary_text(self, table: str, row: object) -> str: ...

    def _refresh_storage_manager(self) -> bool: ...


class ReadModelHostApi(Protocol):
    @property
    def db(self) -> object: ...

    @property
    def config(self) -> object: ...

    def _table_exists(self, table: str) -> bool: ...

    def _id_column(self, table: str) -> Optional[str]: ...

    def _row_primary_text(self, table: str, row: object) -> str: ...

    def _row_label(self, table: str, row: object) -> str: ...

    def _row_dict(self, table: str, row: object) -> dict[str, object]: ...

    def _row_href(self, table: str, row: object) -> Optional[str]: ...

    def _related_rows_by_table(self, row: object) -> dict[str, list[object]]: ...

    def _download_name_for_file_row(self, file_row: object) -> str: ...

    def _refresh_storage_manager(self) -> bool: ...

    def _work_credit_entries(self, row: object) -> list[dict[str, object]]: ...

    def _file_capabilities(self, file_row: object) -> dict[str, object]: ...

    def _stringify_detail_value(self, value: object) -> str: ...


class CalibreCatalogHostApi(ReadModelHostApi, Protocol):
    def _global_search_entries(self, query_text: str, *, table_filter: str = "") -> list[SurfaceSearchEntry]: ...


class AcquisitionHostApi(Protocol):
    def acquisition_text_response(self, status: str, text: str, *, content_type: str) -> SurfaceResponseAPI: ...

    def acquisition_bytes_response(
        self,
        payload: bytes,
        *,
        download_name: str,
        disposition: str = "attachment",
        content_type_override: Optional[str] = None,
    ) -> SurfaceResponseAPI: ...

    def acquisition_redirect_response(self, location: str) -> SurfaceResponseAPI: ...

    def acquisition_file_response(
        self,
        path: Path,
        *,
        download_name: str,
        environ: object,
        disposition: str = "attachment",
        content_type_override: Optional[str] = None,
    ) -> SurfaceResponseAPI: ...

    def acquisition_split_book_token(self, raw_book_id: str) -> tuple[Optional[int], str]: ...

    def acquisition_work_row(self, row_id: int) -> object: ...

    def acquisition_work_image_row(self, work_row: object) -> object: ...

    def acquisition_resolve_storage_image(self, image_row: object) -> object: ...

    def acquisition_resolve_image_target(self, image_row: object) -> Optional[ResolvedFileTargetAPI]: ...

    def acquisition_image_download_name(self, image_row: object) -> str: ...

    def acquisition_image_content_type(self, image_row: object) -> str: ...

    def acquisition_placeholder_cover_svg(self, work_row: object, *, width: int, height: int) -> bytes: ...

    def acquisition_related_rows_by_table(self, work_row: object) -> dict[str, list[object]]: ...

    def acquisition_work_file_rows(self, related_rows_by_table: dict[str, list[object]]) -> list[object]: ...

    def acquisition_download_name_for_file_row(self, file_row: object) -> str: ...

    def acquisition_file_id(self, file_row: object) -> object: ...

    def acquisition_serve_file_download(self, raw_file_id: str, environ: object) -> SurfaceResponseAPI: ...


class OpdsHostApi(Protocol):
    @property
    def config(self) -> object: ...

    def opds_xml_response(self, xml_text: str, *, status: str = "200 OK") -> SurfaceResponseAPI: ...

    def opds_text_response(self, status: str, text: str, *, content_type: str) -> SurfaceResponseAPI: ...

    def opds_search_work_rows(self, query_text: str) -> list[object]: ...

    def opds_work_rows(self, *, sorted_by: str) -> list[object]: ...

    def opds_category_rows(self, category: str) -> list[SurfaceCategoryItem]: ...

    def opds_category_display_name(self, category: str) -> str: ...

    def opds_rows_for_category_item(self, category: str, item_token: str) -> list[object]: ...

    def opds_work_metadata_payload(self, row: object) -> SurfaceWorkMetadataPayload: ...


__all__ = [
    "AcquisitionHostApi",
    "CalibreCatalogHostApi",
    "ImageHostApi",
    "OpdsHostApi",
    "ReadModelHostApi",
    "ResolvedFileTargetAPI",
    "SurfaceCategoryItem",
    "SurfaceEntitySummary",
    "SurfaceFilePayload",
    "SurfaceRelatedPayload",
    "SurfaceResponseAPI",
    "SurfaceSearchEntry",
    "SurfaceWorkMetadataPayload",
]
