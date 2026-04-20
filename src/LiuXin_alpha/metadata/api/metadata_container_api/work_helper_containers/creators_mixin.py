
"""
This API allows expected creator access.
"""

import abc


class CreatorManipMixin(abc.ABC):
    """
    Mixin to all working with creator type information.

    Motivation - the user might want to do something intuitive like call [metadata_object].authors
    - and get a useful result back.
    """
    @abc.abstractmethod
    def get_marc_roles(self) -> set[str]:
        """
        All the MARC roles known to the system.

        :return:
        """

    @abc.abstractmethod
    def standardize_marc_roles(self, marc_role: str) -> str:
        """
        Standardize the MARC role.

        :param marc_role:
        :return:
        """

    # -----------------------------------------
    # - FILL IN THE REST OF THE MARC ROLES HERE

    # (this way we can actually get functional typing going)
