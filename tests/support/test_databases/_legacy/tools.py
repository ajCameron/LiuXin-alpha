"""Minimal legacy tool shims used by DB-support builders."""

from __future__ import annotations

from LiuXin_alpha.databases.metadata_tools.add import Add
from LiuXin_alpha.databases.metadata_tools.apply import Apply
from LiuXin_alpha.databases.metadata_tools.ensure import Ensure
from LiuXin_alpha.databases.metadata_tools.intralinker import Intralinker


class BasicMetadataFramework(object):
    """Small compatibility wrapper around common metadata tools."""

    def __init__(self, db):
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
    """Narrow validator surface still used by legacy DB builders."""

    def __init__(self, db):
        self.db = db

    def validate_every_folder_has_name(self):
        for folder_row in self.db.get_all_rows("folders"):
            assert str(folder_row["folder_name"]).lower() != "none"
