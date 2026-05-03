from __future__ import annotations

from LiuXin_alpha.metadata.api.from_database_api import MetadataObjectGetterAPI


class _LiuXinMetadata:
    pass


class _CalibreMetadata:
    pass


class _LiuXinWEMIMetadata:
    def __init__(self) -> None:
        self.liuxin = _LiuXinMetadata()
        self.calibre = _CalibreMetadata()

    def as_liuxin_metadata(self) -> _LiuXinMetadata:
        return self.liuxin

    def as_calibre_metadata(self) -> _CalibreMetadata:
        return self.calibre


class _MetadataObjectGetter(MetadataObjectGetterAPI):
    def __init__(self) -> None:
        super().__init__(db=None)
        self.requested_item_id: int | None = None
        self.requested_source_row: dict[str, int] | None = None
        self.metadata = _LiuXinWEMIMetadata()

    def get_liuxin_wemi_metadata(
        self,
        item_id: int | None = None,
        source_row: dict[str, int] | None = None,
    ) -> _LiuXinWEMIMetadata:
        self.requested_item_id = item_id
        self.requested_source_row = source_row
        return self.metadata


def test_metadata_object_getter_derives_liuxin_metadata_from_wemi_slice() -> None:
    getter = _MetadataObjectGetter()
    source_row = {"item_id": 10}

    metadata = getter.get_liuxin_metadata(source_row=source_row)

    assert metadata is getter.metadata.liuxin
    assert getter.requested_item_id is None
    assert getter.requested_source_row == source_row


def test_metadata_object_getter_derives_calibre_metadata_from_wemi_slice() -> None:
    getter = _MetadataObjectGetter()

    metadata = getter.get_calibre_metadata(item_id=10)

    assert metadata is getter.metadata.calibre
    assert getter.requested_item_id == 10
    assert getter.requested_source_row is None
