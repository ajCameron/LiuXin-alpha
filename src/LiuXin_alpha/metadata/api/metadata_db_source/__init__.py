
"""
Responsible for reading and return metadata containers from the db.
"""

from typing import TYPE_CHECKING

from LiuXin_alpha.metadata.api.metadata_db_source.wemi_sources.agents_sources import AgentMetadataGetterAPI

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI


class DBMetadataSourceAPI(AgentMetadataGetterAPI):
    """
    Single source for all metadata objects.
    """
    db: "DatabaseAPI"

    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Startup the metadata source.

        :param db:
        """
        super().__init__(db)



