"""
Support retained legacy database-fixture tools behavior.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise tools through a consuming regression::

        python -m pytest -q tests/databases/test_test_resources_manager.py
"""

from __future__ import annotations

from LiuXin_alpha.catalog.metadata_tools import Add
from LiuXin_alpha.catalog.metadata_tools.apply import Apply
from LiuXin_alpha.catalog.metadata_tools import Ensure
from LiuXin_alpha.catalog.metadata_tools.intralinker import Intralinker


class BasicMetadataFramework(object):
    """
    Small compatibility wrapper around common metadata tools.

    Example:
        Exercise BasicMetadataFramework through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py
    """

    def __init__(self, db):
        """
        Initialize and validate the BasicMetadataFramework test-support state.

        Example:
            Exercise BasicMetadataFramework.  init   through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param db: Database connection, wrapper or fixture addressed by the operation.
        :return: None; completion is expressed through state changes or assertions.
        """
        self.db = db
        self.add = Add(database=self.db)
        self.ensure = Ensure(database=self.db)
        self.apply = Apply(database=self.db)
        self.intralink = Intralinker(database=self.db)

        self.add.ensure = self.ensure
        self.add.apply = self.apply
        self.apply.add = self.add
        self.apply.ensure = self.ensure


class DatabaseValidator(object):
    """
    Narrow validator surface still used by legacy DB builders.

    Example:
        Exercise DatabaseValidator through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py
    """

    def __init__(self, db):
        """
        Initialize and validate the DatabaseValidator test-support state.

        Example:
            Exercise DatabaseValidator.  init   through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param db: Database connection, wrapper or fixture addressed by the operation.
        :return: None; completion is expressed through state changes or assertions.
        """
        self.db = db

    def validate_every_folder_has_name(self):
        """
        Validate every folder has name under the fixture contract.

        Example:
            Exercise DatabaseValidator.validate every folder has name through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :return: None; fixture state or the supplied destination is updated in place.
        """
        for folder_row in self.db.get_all_rows("folders"):
            assert str(folder_row["folder_name"]).lower() != "none"
