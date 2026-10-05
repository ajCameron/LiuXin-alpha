"""
Define the legacy category-icon key set and its conventional image resource names.

TagsIcons copies required entries from a supplied mapping; it neither loads the
filename resources in category_icon_map nor validates icon objects. Category
generation may later extend an instance with custom/user-category aliases.
"""





class TagsIcons(dict):
    """
    Copy required category-icon entries into a mutable dict accepted by legacy category generation.

    Every name in category_icons must exist in the input, but None values are
    accepted and extra input keys are not copied. Instances remain ordinary
    mutable dicts afterward; the invariant is checked only during initialization.

    Example:
        >>> icons = TagsIcons({name: None for name in TagsIcons.category_icons})
        >>> len(icons), icons["tags"] is None
        (13, True)
    """

    category_icons = [
        "authors",
        "series",
        "formats",
        "publisher",
        "rating",
        "news",
        "tags",
        "custom:",
        "user:",
        "search",
        "identifiers",
        "languages",
        "gst",
    ]

    def __init__(self, icon_dict):
        """
        Copy required keys in declaration order and raise on the first absent key.

        Values are retained by reference with no non-None/type validation. Earlier
        assignments remain if a later key is missing; explicitly reinitializing an
        existing instance does not clear unrelated keys already present in it.

        Example:
            >>> TagsIcons(category_icon_map)["tags"]
            'tags.png'


        :param icon_dict: Mapping containing every required category icon key; additional input keys are ignored.
        :return: None after assigning each required icon value to this dict instance.
        :raises ValueError: At the first missing key, after any preceding assignments.
        """
        for a in self.category_icons:
            if a not in icon_dict:
                raise ValueError("Missing category icon [%s]" % a)
            self[a] = icon_dict[a]


category_icon_map = {
    "authors": "user_profile.png",
    "series": "series.png",
    "formats": "book.png",
    "publisher": "publisher.png",
    "rating": "rating.png",
    "news": "news.png",
    "tags": "tags.png",
    "custom:": "column.png",
    "user:": "tb_folder.png",
    "search": "search.png",
    "identifiers": "identifiers.png",
    "gst": "catalog.png",
    "languages": "languages.png",
}
