"""
Define the private host operations required to persist catalog writer updates.

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
from LiuXin_alpha.databases.db_types.database_types import SrcTableID
from LiuXin_alpha.databases.db_types.macro_types import LinkRow


class CatalogWriterHostAPI(Protocol):
    """
    Internal Catalog surface required by schema-driven writers.

    This protocol is an implementation seam between configured writers and their
    host. Applications use repository or mutation APIs, or the public
    ``Catalog.create_writer``, ``Catalog.write``, and ``Catalog.write_one`` entry
    points; they do not submit prepared update objects directly.

    Example:
        Exercise CatalogWriterHostAPI through its owning regression module::

            python -m pytest -q tests/catalog/test_writer_factory.py
    """

    db: DatabaseAPI

    def _apply_link_update(
        self,
        update: LinkUpdate,
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """
        Apply a prepared link update through the private writer-host boundary.

        Example:
            Exercise CatalogWriterHostAPI._apply_link_update through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
        """
        ...

    def _apply_column_update(
        self,
        update: CatalogColumnUpdate[object],
    ) -> Mapping[SrcTableID, object]:
        """
        Apply a prepared column update through the private writer-host boundary.

        Example:
            Exercise CatalogWriterHostAPI._apply_column_update through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
        """
        ...

    def _apply_owned_row_update(
        self,
        update: CatalogOwnedRowUpdate[object],
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """
        Apply a prepared owned-row update through the private writer-host boundary.

        Example:
            Exercise CatalogWriterHostAPI._apply_owned_row_update through its owning regression module::

                python -m pytest -q tests/catalog/test_writer_factory.py


        :param update: Prepared catalog update to validate or apply.
        :return: The deterministic value, row, identity or collection described above.
        """
        ...


__all__ = ["CatalogWriterHostAPI"]
