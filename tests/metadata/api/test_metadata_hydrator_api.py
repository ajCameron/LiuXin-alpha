"""
Verify hydrator protocols and lazy/eager adapter exports.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test metadata hydrator api through its owning regression module::

        python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
"""
from __future__ import annotations

from LiuXin_alpha.metadata.api.from_database_api import MetadataObjectGetterAPI


class _LiuXinMetadata:
    """
    Provide the LiuXinMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise LiuXinMetadata through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
    """
    pass


class _CalibreMetadata:
    """
    Provide the CalibreMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise CalibreMetadata through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
    """
    pass


class _LiuXinWEMIMetadata:
    """
    Provide the LiuXinWEMIMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise LiuXinWEMIMetadata through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
    """
    def __init__(self) -> None:
        """
        Initialize the LiuXinWEMIMetadata test double.

        Example:
            Exercise LiuXinWEMIMetadata.init through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :return: None; the function records state or raises through its assertions.
        """
        self.liuxin = _LiuXinMetadata()
        self.calibre = _CalibreMetadata()

    def as_liuxin_metadata(self) -> _LiuXinMetadata:
        """
        Perform the as liuxin metadata test-helper operation with deterministic inputs.

        Example:
            Exercise LiuXinWEMIMetadata.as liuxin metadata through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self.liuxin

    def as_calibre_metadata(self) -> _CalibreMetadata:
        """
        Perform the as calibre metadata test-helper operation with deterministic inputs.

        Example:
            Exercise LiuXinWEMIMetadata.as calibre metadata through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self.calibre


class _MetadataObjectGetter(MetadataObjectGetterAPI):
    """
    Provide the MetadataObjectGetter test fixture or double with explicit deterministic behavior.

    Example:
        Exercise MetadataObjectGetter through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py
    """
    def __init__(self) -> None:
        """
        Initialize the MetadataObjectGetter test double.

        Example:
            Exercise MetadataObjectGetter.init through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :return: None; the function records state or raises through its assertions.
        """
        super().__init__(db=None)
        self.requested_item_id: int | None = None
        self.requested_source_row: dict[str, int] | None = None
        self.metadata = _LiuXinWEMIMetadata()

    def get_liuxin_wemi_metadata(
        self,
        item_id: int | None = None,
        source_row: dict[str, int] | None = None,
    ) -> _LiuXinWEMIMetadata:
        """
        Return liuxin wemi metadata from deterministic test state.

        Example:
            Exercise MetadataObjectGetter.get liuxin wemi metadata through its owning regression module::

                python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


        :param item_id: Value supplied for item id in the focused test operation.
        :param source_row: Value supplied for source row in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.requested_item_id = item_id
        self.requested_source_row = source_row
        return self.metadata


def test_metadata_object_getter_derives_liuxin_metadata_from_wemi_slice() -> None:
    """
    Verify metadata object getter derives liuxin metadata from wemi slice.

    Example:
        Exercise test metadata object getter derives liuxin metadata from wemi slice through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


    :return: None; the function records state or raises through its assertions.
    """
    getter = _MetadataObjectGetter()
    source_row = {"item_id": 10}

    metadata = getter.get_liuxin_metadata(source_row=source_row)

    assert metadata is getter.metadata.liuxin
    assert getter.requested_item_id is None
    assert getter.requested_source_row == source_row


def test_metadata_object_getter_derives_calibre_metadata_from_wemi_slice() -> None:
    """
    Verify metadata object getter derives calibre metadata from wemi slice.

    Example:
        Exercise test metadata object getter derives calibre metadata from wemi slice through its owning regression module::

            python -m pytest -q tests/metadata/api/test_metadata_hydrator_api.py


    :return: None; the function records state or raises through its assertions.
    """
    getter = _MetadataObjectGetter()

    metadata = getter.get_calibre_metadata(item_id=10)

    assert metadata is getter.metadata.calibre
    assert getter.requested_item_id == 10
    assert getter.requested_source_row is None
