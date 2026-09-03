"""Resolve category and tag names to presentation icon resources."""





class TagsIcons(dict):
    """
    If the client wants icons to be in the tag structure, this class must be
    instantiated and filled in with real icons. If this class is instantiated
    and passed to get_categories, All items must be given a value not None
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
