
"""
Mixin to provide creator functionality to metadata objects.
"""

from typing import Union

from LiuXin_alpha.metadata.standardize import standardize_id_name, standardize_creator_category, string_to_authors, standardize_lang, standardize_internal_id_name, standardize_rating_type, standardize_tag

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata.calibre_creators_mixin import CreatorsMethodsMixin


class CreatorsMetadataMixin(CreatorsMethodsMixin):
    """
    Mixin to provide creator functionality to metadata objects.

    Creators are stored in the form of a dict of OrderedDicts.
    Keyed with the creator type and valued with the creator value.

    """

    def check_for_creator(self, key: str, value: Union[str, list[str]]) -> bool:
        """
        Checks to see if we're trying to add a creator.

        If we are trying, then return the status.
        :param key:
        :param value:
        :return:
        """
        valid_creator_category = standardize_creator_category(key)
        if not valid_creator_category:
            return False

        self._set_creator_from_normed_key(
            creator_key=valid_creator_category,
            value=value,
        )
        return True










