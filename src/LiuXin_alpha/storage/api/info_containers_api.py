
"""
Containers for info about storage and stores.
"""

from typing import TYPE_CHECKING

import dataclasses

if TYPE_CHECKING:
    from LiuXin_alpha.storage.storage_types import FileID
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI


@dataclasses.dataclass
class ReplicationCluster:
    """
    A replication cluster is a collection of files which are - nominally - identical.

    These files
     - will have different ids
     - should have the same hash and size

    For internal use by the manager - as a rule, provides more granular detail than should be needed.
    """

    file_locs: dict[FileID, "StoreLocationMixinAPI"]

    replication_level: int

    file_hash: str

    @property
    def file_ids(self) -> set[FileID]:
        """
        Get all the FileIDs in the replication cluster.

        :return:
        """
        return set(_ for _ in self.file_locs.keys())




