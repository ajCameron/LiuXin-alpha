


# Todo: All link tables should have an entry in here - creator_synopsis_links seemed to escape
# Todo: In the test suite, generate test database and check them against the stored ones/run the tests on the,
# Todo: creeator_title links should probably allow a person to play more than one role in their own work

from LiuXin_alpha.metadata.constants import CREATOR_DROP_REGEX_SET, CREATOR_CATEGORIES, CREATOR_TYPES, CREATOR_TYPE_CAT_DIR, EXTERNAL_EBOOK_ID_SCHEMA, EXTERNAL_EBOOK_REKEY_SCHEME
from LiuXin_alpha.metadata.constants import INTERNAL_EBOOK_ID_SCHEMA, EXTERNAL_EBOOK_REKEY_SCHEME, EXTERNAL_EBOOK_ID_SCHEMA, RATING_TYPES
from LiuXin_alpha.metadata.constants import INTERNAL_EBOOK_REKEY_SCHEME
from LiuXin_alpha.metadata.constants import METADATA_NULL_VALUES



__INTERLINK_TABLE_CONSTRAINTS__ = {
    # This information, and the request columns - should be all that's needed to construct the SQL to build the table
    "book_cover_links": {
        "primary": "books",
        "secondary": "covers",
        "link_type": "many_many",
    },
    "book_digital_asset_links": {
        "primary": "books",
        "secondary": "digital_assets",
        "link_type": "one_many",
    },
    "book_folder_links": {
        "primary": "books",
        "secondary": "folders",
        "link_type": "many_many",
    },
    "comment_creator_links": {
        "primary": "creators",
        "secondary": "comments",
        "link_type": "one_many",
    },
    "comment_series_links": {
        "primary": "series",
        "secondary": "comments",
        "link_type": "one_many",
    },
    "comment_title_links": {
        "primary": "titles",
        "secondary": "comments",
        "link_type": "one_many",
    },
    "cover_creator_links": {
        "primary": "creators",
        "secondary": "covers",
        "link_type": "many_many",
    },
    "cover_series_links": {
        "primary": "series",
        "secondary": "covers",
        "link_type": "many_many",
    },
    "creator_folder_links": {
        "primary": "creators",
        "secondary": "folders",
        "link_type": "many_many",
    },
    "creator_language_links": {
        "primary": "creators",
        "secondary": "languages",
        "link_type": "many_one",
    },
    "creator_note_links": {
        "primary": "creators",
        "secondary": "notes",
        "link_type": "one_many",
    },
    # Todo: This restriction is silly, but leaving it in for the moment
    "creator_series_links": {
        "primary": "series",
        "secondary": "creators",
        "link_type": "many_one",
    },
    "creator_synopsis_links": {
        "primary": "creators",
        "secondary": "synopses",
        "link_type": "one_many",
    },
    "creator_tag_links": {
        "primary": "creators",
        "secondary": "tags",
        "link_type": "many_many",
    },
    "creator_title_links": {
        "primary": "titles",
        "secondary": "creators",
        "link_type": "many_many",
    },
    # Todo: Is not actually one_one - it's more like one_many_single_val
    # Todo: Your us9ing link type two different ways to mean two different things - consider a rename - mapping_type?
    # Todo: This REALLY needs a set of types
    "device_digital_asset_links": {
        "primary": "digital_assets",
        "secondary": "devices",
        "link_type": "many_many",
    },
    "device_note_links": {
        "primary": "devices",
        "secondary": "notes",
        "link_type": "one_many",
    },
    "digital_asset_folder_links": {
        "primary": "digital_assets",
        "secondary": "folders",
        "link_type": "many_one",
    },
    "digital_asset_identifier_links": {
        "primary": "digital_assets",
        "secondary": "identifiers",
        "link_type": "one_many",
    },
    # Todo: Really need to ship with a languages table
    "digital_asset_language_links": {
        "primary": "digital_assets",
        "secondary": "languages",
        "link_type": "many_one",
    },
    "digital_asset_publisher_links": {
        "primary": "digital_assets",
        "secondary": "publishers",
        "link_type": "many_one",
    },
    "folder_series_links": {
        "primary": "folders",
        "secondary": "series",
        "link_type": "many_many",
    },
    "folder_store_note_links": {
        "primary": "folder_stores",
        "secondary": "notes",
        "link_type": "one_many",
    },
    "genre_series_links": {
        "primary": "series",
        "secondary": "genres",
        "link_type": "many_many",
    },
    "genre_title_links": {
        "primary": "titles",
        "secondary": "genres",
        "link_type": "many_many",
    },
    "identifier_title_links": {
        "primary": "titles",
        "secondary": "identifiers",
        "link_type": "one_many",
    },
    # Todo: Come back and rethink this some
    "language_title_links": {
        "primary": "titles",
        "secondary": "languages",
        "link_type": "many_many_non_exclusive",
    },
    "note_publisher_links": {
        "primary": "publishers",
        "secondary": "notes",
        "link_type": "one_many",
    },
    "note_series_links": {
        "primary": "series",
        "secondary": "notes",
        "link_type": "one_many",
    },
    "note_title_links": {
        "primary": "titles",
        "secondary": "notes",
        "link_type": "one_many",
    },
    "publisher_title_links": {
        "primary": "titles",
        "secondary": "publishers",
        "link_type": "many_many",
    },
    "rating_title_links": {
        "primary": "titles",
        "secondary": "ratings",
        "link_type": "rating",
    },
    "series_synopsis_links": {
        "primary": "series",
        "secondary": "synopses",
        "link_type": "one_many",
    },
    "series_tag_links": {
        "primary": "series",
        "secondary": "tags",
        "link_type": "many_many",
    },
    "series_title_links": {
        "primary": "titles",
        "secondary": "series",
        "link_type": "many_many",
    },
    "subject_title_links": {
        "primary": "titles",
        "secondary": "subjects",
        "link_type": "many_many",
    },
    "synopsis_title_links": {
        "primary": "titles",
        "secondary": "synopses",
        "link_type": "one_many",
    },
    "tag_title_links": {
        "primary": "titles",
        "secondary": "tags",
        "link_type": "many_many",
    },
}
__INTERLINK_REQUESTED_COLS__ = {
    "book_cover_links": {"priority", "type"},
    "book_digital_asset_links": {
        "priority",
    },
    "book_folder_links": {
        "priority",
    },
    "comment_creator_links": {"priority", "type"},
    "comment_series_links": {"priority", "type"},
    "comment_title_links": {"priority", "type"},
    "cover_creator_links": {
        "priority",
    },
    "cover_series_links": {
        "priority",
    },
    "creator_folder_links": {
        "priority",
    },
    "creator_language_links": None,
    "creator_note_links": {"priority", "type"},
    "creator_series_links": {
        "type",
    },
    "creator_synopsis_links": None,
    "creator_tag_links": None,
    "creator_title_links": {"priority", "type"},
    "device_digital_asset_links": {
        "type",
    },
    "device_note_links": {
        "priority",
    },
    "digital_asset_folder_links": None,
    "digital_asset_identifier_links": {"type", "priority"},
    "digital_asset_language_links": None,
    "digital_asset_publisher_links": None,
    "folder_series_links": {
        "priority",
    },
    "folder_store_note_links": {
        "priority",
    },
    "genre_series_links": {
        "priority",
    },
    "genre_title_links": {
        "priority",
    },
    "identifier_title_links": {"type", "priority"},
    "language_title_links": {"type", "priority"},
    "note_publisher_links": {
        "priority",
    },
    "note_series_links": {
        "priority",
    },
    "note_title_links": {"priority", "type"},
    "series_synopsis_links": {
        "priority",
    },
    "series_tag_links": None,
    "series_title_links": {"priority", "index"},
    "publisher_title_links": {
        "priority",
    },
    "rating_title_links": {
        "type",
    },
    "subject_title_links": {
        "priority",
    },
    "synopsis_title_links": {
        "priority",
    },
    "tag_title_links": None,
}
__ALLOWED_INTERLINK_TYPE_VAL_DICT__ = {
    "book_cover_links": {"from_file", "from_web"},
    "comment_creator_links": ("amazon", "google"),
    "comment_series_links": ("synopsis", "reading_notes", "dramatis_persona"),
    "comment_title_links": ("synopsis", "reading_notes", "dramatis_persona"),
    "creator_note_links": ("bio", "bibliography"),
    "creator_series_links": tuple(ct for ct in CREATOR_CATEGORIES),
    "creator_title_links": tuple(ct for ct in CREATOR_CATEGORIES),
    "digital_asset_identifier_links": tuple(idt for idt in EXTERNAL_EBOOK_ID_SCHEMA),
    "device_digital_asset_links": ("load_when_can", "ensure_on_device", "delete_when_possible"),
    "identifier_title_links": tuple(
        [idt for idt in EXTERNAL_EBOOK_ID_SCHEMA] + [idt for idt in INTERNAL_EBOOK_ID_SCHEMA]
    ),
    "language_title_links": (
        "primary",
        "about",
        "available_language",
        "contained_in",
    ),
    "note_title_links": ("summary", "synopsis", "glossary", "marginalia"),
    "rating_title_links": tuple(rt for rt in RATING_TYPES),
}



__ALLOWED_INTRALINK_TYPE_VAL_DICT__ = {
    "creators": ("user_marked_different",),
    "covers": (
        "user_marked_different",
        "derived_from",
        "derived_from-higher_resolution_version",
        "contained_in",
        "backup",
        "mirror",
    ),
    "digital_assets": (
        "user_marked_different",
        "derived_from",
        "derived_from-higher_resolution_version",
        "contained_in",
        "backup",
        "mirror",
    ),
    "folder_stores": ("user_marked_different", "mirror", "backup"),
    "identifiers": ("differ_only_in_format",),
    "publishers": ("user_marked_different", "rename", "translated_name"),
    "tags": ("user_marked_different",),
    "titles": (
        "user_marked_different",
        "contained_in",
        "identical",
        "alt_title",
        "translation",
        "abridgement",
    ),
}
