"""
Define host operations required to persist catalog writer updates.

The module keeps validation, normalization and host mutation boundaries explicit.

Example:
    Exercise host api through its owning regression module::

        python -m pytest -q tests/catalog/test_writer_factory.py
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from LiuXin_alpha.catalog.write.column_update import CatalogColumnUpdate
from LiuXin_alpha.catalog.write.link_update import LinkUpdate
from LiuXin_alpha.catalog.write.owned_row_update import CatalogOwnedRowUpdate
from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
from LiuXin_alpha.databases.db_types import SrcTableID
from LiuXin_alpha.databases.macro_types import LinkRow


class CatalogWriterHostAPI(Protocol):
    """
    Minimal Catalog surface required by schema-driven writers.

    Example:
        Exercise CatalogWriterHostAPI through its owning regression module::

            python -m pytest -q tests/catalog/test_writer_factory.py
    """

    db: DatabaseAPI

    def write_link_update(
        self,
        update: LinkUpdate,
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """
        Apply write link update through the catalog writer host boundary.

        Example:
            Exercise CatalogWriterHostAPI.write link update through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
        """
        ...

    def write_column_update(
        self,
        update: CatalogColumnUpdate[object],
    ) -> Mapping[SrcTableID, object]:
        """
        Apply write column update through the catalog writer host boundary.

        Example:
            Exercise CatalogWriterHostAPI.write column update through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
        """
        ...

    def write_owned_row_update(
        self,
        update: CatalogOwnedRowUpdate[object],
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """
        Apply write owned row update through the catalog writer host boundary.

        Example:
            Exercise CatalogWriterHostAPI.write owned row update through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
        """
        ...


__all__ = ["CatalogWriterHostAPI"]
