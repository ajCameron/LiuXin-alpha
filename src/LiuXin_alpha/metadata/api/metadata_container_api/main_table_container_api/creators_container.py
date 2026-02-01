
"""
Creators can be linked to a titles, series and linked back to itself with a intralink table.

This file contains the specialized containers.
"""

from typing import Iterable

from LiuXin_alpha.metadata.api.metadata_container_api.basic_container_api.many_many_container_api import (
    ManyToManyPriorityTypedMetadataContainerAPI
)


class CreatorsContainerAPI(ManyToManyPriorityTypedMetadataContainerAPI):
    """
    API for the creator's container.

    We want to enable sensible access methods for all the metadata contained here.
    So this class presents properties like
     - authors - gets you a LIST of the authors of the work
     - authors_sort - gets you a SORT STRING for the authors of the work
     -
    """
    def marc_creator_roles(self) -> Iterable[str]:
        """
        All the creator's possible roles.

        :return:
        """

    # Prototype for the MARC creator roles dict






