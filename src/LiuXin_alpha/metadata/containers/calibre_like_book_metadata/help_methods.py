
"""
Meatdata helper methods - explains a metadata property of a class.
"""
from LiuXin_alpha.metadata.constants import METADATA_EXPLANATIONS


class BookMetadataHelpMixin:
    """

    """

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - HELP METHODS START HERE

    @staticmethod
    def explain_field(key):
        """
        Returns an explanation for the given key.
        :param key:
        :return explanation:
        """
        if key in METADATA_EXPLANATIONS:
            return METADATA_EXPLANATIONS[key]

        raise ValueError("No available explanation.")

    #
    # ------------------------------------------------------------------------------------------------------------------
