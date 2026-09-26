
"""
Expose the legacy metadata-field explanation table through a lookup helper.

Example:
    Exercise the owning behavior with pytest::

        python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_calibre_book_metadata.py
"""
from LiuXin_alpha.metadata.constants import METADATA_EXPLANATIONS


class BookMetadataHelpMixin:
    """
    Provide a static lookup for descriptions in METADATA_EXPLANATIONS.

    Example:
        Exercise the owning behavior with pytest::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_calibre_book_metadata.py
    """

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - HELP METHODS START HERE

    @staticmethod
    def explain_field(key):
        """
        Look up an exact metadata-field description, producing ValueError when absent.

        Example:
            >>> BookMetadataHelpMixin.explain_field('title')
            'The title of the work'


        :param key: Exact explanation-table field name; no alias normalization occurs.
        :return: Description string from METADATA_EXPLANATIONS.
        """
        if key in METADATA_EXPLANATIONS:
            return METADATA_EXPLANATIONS[key]

        raise ValueError("No available explanation.")

    #
    # ------------------------------------------------------------------------------------------------------------------
