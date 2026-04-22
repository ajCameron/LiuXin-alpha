
"""
Getter to retrieve Item metadata containers.
"""

import abc

from typing import TYPE_CHECKING

if TYPE_CHECKING:

    from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api.items_container_api import ItemContainerAPI


class ItemMetadataGetterAPI(abc.ABC):
    """
    Responsible for retrieving metadata about an Item on the system.
    """

