
"""
Test DB 19 Properties.
"""


from LiuXin_tests.test_databases.test_db_properties.common_db_properties import (
    CommonDBProperties,
)


from LiuXin_alpha.utils.libraries.liuxin_six import iteritems


class TestDB19Properties(CommonDBProperties):
    """
    Properties for the test_db_19 test database.
    """

    theo_stable_words_columns = ["word_id", "word"]

    valid_ci_types = set(["incidental", "primary", "secondary"])

    theo_tables_and_columns = {
        "secondary_uuids": [
            "secondary_uuid_id",
            "secondary_uuid",
            "secondary_uuid_datestamp",
            "secondary_uuid_scratch",
        ],
        "creator_title_links": [
            "creator_title_link_id",
            "creator_title_link_creator_id",
            "creator_title_link_title_id",
            "creator_title_link_priority",
            "creator_title_link_type",
            "creator_title_link_datestamp",
            "creator_title_link_scratch",
        ],
        "tag_tag_intralinks": [
            "tag_tag_intralink_id",
            "tag_tag_intralink_primary_id",
            "tag_tag_intralink_secondary_id",
            "tag_tag_intralink_type",
            "tag_tag_intralink_datestamp",
            "tag_tag_intralink_scratch",
        ],
        "language_title_links": [
            "language_title_link_id",
            "language_title_link_language_id",
            "language_title_link_title_id",
            "language_title_link_priority",
            "language_title_link_type",
            "language_title_link_datestamp",
            "language_title_link_scratch",
        ],
        "creator_creator_intralinks": [
            "creator_creator_intralink_id",
            "creator_creator_intralink_primary_id",
            "creator_creator_intralink_secondary_id",
            "creator_creator_intralink_type",
            "creator_creator_intralink_datestamp",
            "creator_creator_intralink_scratch",
        ],
        "new_books": [
            "new_book_id",
            "new_book_name",
            "new_book_extension",
            "new_book_path",
            "new_book_hash_1",
            "new_book_hash_2",
            "new_book_size",
            "new_book_group_id",
            "new_book_cached",
            "new_book_cache_attempted",
            "new_book_datestamp",
            "new_book_scratch",
        ],
        "identifier_title_links": [
            "identifier_title_link_id",
            "identifier_title_link_identifier_id",
            "identifier_title_link_title_id",
            "identifier_title_link_priority",
            "identifier_title_link_type",
            "identifier_title_link_datestamp",
            "identifier_title_link_scratch",
        ],
        "allowed_types__publishers_publishers_intralinks": [
            "allowed_types__publishers_publishers_intralink_id",
            "allowed_types__publishers_publishers_intralink_type",
            "allowed_types__publishers_publishers_intralink_datestamp",
            "allowed_types__publishers_publishers_intralink_scratch",
        ],
        "allowed_types__creators_creators_intralinks": [
            "allowed_types__creators_creators_intralink_id",
            "allowed_types__creators_creators_intralink_type",
            "allowed_types__creators_creators_intralink_datestamp",
            "allowed_types__creators_creators_intralink_scratch",
        ],
        "allowed_types__note_title_links": [
            "allowed_types__note_title_link_id",
            "allowed_types__note_title_link_type",
            "allowed_types__note_title_link_datestamp",
            "allowed_types__note_title_link_scratch",
        ],
        "compressed_files": [
            "compressed_file_id",
            "compressed_file_name",
            "compressed_file_extension",
            "compressed_file_path",
            "compressed_file_hash_1",
            "compressed_file_hash_2",
            "compressed_file_size",
            "compressed_file_group_id",
            "compressed_file_folder",
            "compressed_file_cached",
            "compressed_file_cache_attempted",
            "compressed_file_datestamp",
            "compressed_file_scratch",
        ],
        "note_series_links": [
            "note_series_link_id",
            "note_series_link_note_id",
            "note_series_link_series_id",
            "note_series_link_priority",
            "note_series_link_datestamp",
            "note_series_link_scratch",
        ],
        "languages": [
            "language_id",
            "language",
            "language_code",
            "language_datestamp",
            "language_scratch",
        ],
        "content_levels": [
            "content_level_id",
            "content_level",
            "content_level_datestamp",
            "content_level_scratch",
        ],
        "database_version": [
            "database_version_id",
            "database_version_version",
            "database_version_datestamp",
        ],
        "allowed_types__identifier_title_links": [
            "allowed_types__identifier_title_link_id",
            "allowed_types__identifier_title_link_type",
            "allowed_types__identifier_title_link_datestamp",
            "allowed_types__identifier_title_link_scratch",
        ],
        "file_file_intralinks": [
            "file_file_intralink_id",
            "file_file_intralink_primary_id",
            "file_file_intralink_secondary_id",
            "file_file_intralink_type",
            "file_file_intralink_datestamp",
            "file_file_intralink_scratch",
        ],
        "words": ["word_id", "word", "word_datestamp", "word_scratch"],
        "file_folder_links": [
            "file_folder_link_id",
            "file_folder_link_file_id",
            "file_folder_link_folder_id",
            "file_folder_link_datestamp",
            "file_folder_link_scratch",
        ],
        "loc_shelf_number_title_links": [
            "loc_shelf_number_title_link_id",
            "loc_shelf_number_title_link_loc_shelf_number_id",
            "loc_shelf_number_title_link_title_id",
            "loc_shelf_number_title_link_priority",
            "loc_shelf_number_title_link_primary",
            "loc_shelf_number_title_link_type",
            "loc_shelf_number_title_link_index",
            "loc_shelf_number_title_link_datestamp",
            "loc_shelf_number_title_link_scratch",
        ],
        "title_title_intralinks": [
            "title_title_intralink_id",
            "title_title_intralink_primary_id",
            "title_title_intralink_secondary_id",
            "title_title_intralink_type",
            "title_title_intralink_datestamp",
            "title_title_intralink_scratch",
        ],
        "notes": ["note_id", "note", "note_datestamp", "note_scratch"],
        "metadata_dirtied_books": [
            "metadata_dirtied_book_id",
            "metadata_dirtied_book",
        ],
        "conversion_options": [
            "conversion_option_id",
            "conversion_option_format",
            "conversion_option_book",
            "conversion_option_data",
            "conversion_option_datestamp",
        ],
        "allowed_types__files_files_intralinks": [
            "allowed_types__files_files_intralink_id",
            "allowed_types__files_files_intralink_type",
            "allowed_types__files_files_intralink_datestamp",
            "allowed_types__files_files_intralink_scratch",
        ],
        "publisher_publisher_owner_links": [
            "publisher_publisher_owner_link_id",
            "publisher_publisher_owner_link_publisher_id",
            "publisher_publisher_owner_link_publisher_owner_id",
            "publisher_publisher_owner_link_priority",
            "publisher_publisher_owner_link_primary",
            "publisher_publisher_owner_link_type",
            "publisher_publisher_owner_link_index",
            "publisher_publisher_owner_link_datestamp",
            "publisher_publisher_owner_link_scratch",
        ],
        "feeds": [
            "feed_id",
            "feed_title",
            "feed_script",
            "feed_created_datestamp",
            "feed_datestamp",
            "feed_scratch",
        ],
        "library_id": ["library_id", "library_id_uuid", "library_id_datestamp"],
        "publishers": [
            "publisher_id",
            "publisher",
            "publisher_sort",
            "publisher_phash",
            "publisher_wikipedia",
            "publisher_website",
            "publisher_parent",
            "publisher_position",
            "publisher_tree_id",
            "publisher_full",
            "publisher_created_datestamp",
            "publisher_datestamp",
            "publisher_scratch",
        ],
        "series": [
            "series_id",
            "series",
            "series_sort",
            "series_phash",
            "series_over_author",
            "series_parent",
            "series_parent_position",
            "series_tree_id",
            "series_full",
            "series_datestamp",
            "series_scratch",
        ],
        "allowed_types__folder_stores_folder_stores_intralinks": [
            "allowed_types__folder_stores_folder_stores_intralink_id",
            "allowed_types__folder_stores_folder_stores_intralink_type",
            "allowed_types__folder_stores_folder_stores_intralink_datestamp",
            "allowed_types__folder_stores_folder_stores_intralink_scratch",
        ],
        "books": [
            "book_id",
            "book_sort",
            "book_flags",
            "book_pubdate",
            "book_copyright_date",
            "book_uuid",
            "book_has_cover",
            "book_has_local_cover",
            "book_last_modified",
            "book_fingerprint",
            "book_paths",
            "book_size",
            "book_rating",
            "book_created_datestamp",
            "book_datestamp",
            "book_scratch",
        ],
        "devices": [
            "device_id",
            "device_type",
            "device_created_datestamp",
            "device_datestamp",
            "device_scratch",
        ],
        "preferences": [
            "preference_id",
            "preference_key",
            "preference_value",
            "preference_value_type",
            "preference_parent_LiuXin_instance",
            "preference_datestamp",
            "preference_scratch",
        ],
        "publisher_title_links": [
            "publisher_title_link_id",
            "publisher_title_link_publisher_id",
            "publisher_title_link_title_id",
            "publisher_title_link_priority",
            "publisher_title_link_datestamp",
            "publisher_title_link_scratch",
        ],
        "comments": [
            "comment_id",
            "comment",
            "comment_datestamp",
            "comment_scratch",
        ],
        "loc_shelf_numbers": [
            "loc_shelf_number_id",
            "loc_shelf_number",
            "loc_shelf_number_datestamp",
            "loc_shelf_number_scratch",
        ],
        "book_folder_links": [
            "book_folder_link_id",
            "book_folder_link_book_id",
            "book_folder_link_folder_id",
            "book_folder_link_priority",
            "book_folder_link_datestamp",
            "book_folder_link_scratch",
        ],
        "series_synopsis_links": [
            "series_synopsis_link_id",
            "series_synopsis_link_series_id",
            "series_synopsis_link_synopsis_id",
            "series_synopsis_link_priority",
            "series_synopsis_link_datestamp",
            "series_synopsis_link_scratch",
        ],
        "allowed_types__creator_series_links": [
            "allowed_types__creator_series_link_id",
            "allowed_types__creator_series_link_type",
            "allowed_types__creator_series_link_datestamp",
            "allowed_types__creator_series_link_scratch",
        ],
        "books_plugin_data": [
            "book_plugin_data_id",
            "book_plugin_data_book",
            "book_plugin_data_name",
            "book_plugin_data_val",
            "book_plugin_created_datestamp",
            "book_plugin_datestamp",
            "book_plugin_scratch",
        ],
        "series_series_character_introduction_links": [
            "series_series_character_introduction_link_id",
            "series_series_character_introduction_link_series_id",
            "series_series_character_introduction_link_series_character_introduction_id",
            "series_series_character_introduction_link_priority",
            "series_series_character_introduction_link_primary",
            "series_series_character_introduction_link_type",
            "series_series_character_introduction_link_index",
            "series_series_character_introduction_link_datestamp",
            "series_series_character_introduction_link_scratch",
        ],
        "tags": ["tag_id", "tag", "tag_phash", "tag_datestamp", "tag_scratch"],
        "synopsis_title_links": [
            "synopsis_title_link_id",
            "synopsis_title_link_synopsis_id",
            "synopsis_title_link_title_id",
            "synopsis_title_link_priority",
            "synopsis_title_link_datestamp",
            "synopsis_title_link_scratch",
        ],
        "comment_creator_links": [
            "comment_creator_link_id",
            "comment_creator_link_comment_id",
            "comment_creator_link_creator_id",
            "comment_creator_link_priority",
            "comment_creator_link_type",
            "comment_creator_link_datestamp",
            "comment_creator_link_scratch",
        ],
        "synopses": [
            "synopsis_id",
            "synopsis",
            "synopsis_datestamp",
            "synopsis_scratch",
        ],
        "content_level_title_links": [
            "content_level_title_link_id",
            "content_level_title_link_content_level_id",
            "content_level_title_link_title_id",
            "content_level_title_link_priority",
            "content_level_title_link_primary",
            "content_level_title_link_type",
            "content_level_title_link_index",
            "content_level_title_link_datestamp",
            "content_level_title_link_scratch",
        ],
        "database_metadata": [
            "database_metadata_id",
            "database_metadata_unique_id",
            "database_metadata_parent_LiuXin_instance",
            "database_metadata_db_name",
            "database_metadata_datestamp",
            "database_metadata_scratch",
        ],
        "allowed_types__comment_series_links": [
            "allowed_types__comment_series_link_id",
            "allowed_types__comment_series_link_type",
            "allowed_types__comment_series_link_datestamp",
            "allowed_types__comment_series_link_scratch",
        ],
        "folder_stores": [
            "folder_store_id",
            "folder_store",
            "folder_store_identifier",
            "folder_store_path",
            "folder_store_path_os_type",
            "folder_store_created",
            "folder_store_media_types",
            "folder_store_forbidden_print",
            "folder_store_preferred_print",
            "folder_store_cache",
            "folder_store_storage",
            "folder_store_cf_expansion",
            "folder_store_cache_priority",
            "folder_store_max_path_length",
            "folder_store_user_name",
            "folder_store_password",
            "folder_store_type",
            "folder_store_marker_name",
            "folder_store_marker_text",
            "folder_store_marker_path",
            "folder_store_marker_path_os_type",
            "folder_store_dedicated_drive",
            "folder_store_max_size",
            "folder_store_found",
            "folder_store_readable",
            "folder_store_writeable",
            "folder_store_size",
            "folder_store_free_size",
            "folder_store_creation_date",
            "folder_store_created_datestamp",
            "folder_store_datestamp",
            "folder_store_scratch",
        ],
        "creator_language_links": [
            "creator_language_link_id",
            "creator_language_link_creator_id",
            "creator_language_link_language_id",
            "creator_language_link_datestamp",
            "creator_language_link_scratch",
        ],
        "identifier_identifier_intralinks": [
            "identifier_identifier_intralink_id",
            "identifier_identifier_intralink_primary_id",
            "identifier_identifier_intralink_secondary_id",
            "identifier_identifier_intralink_type",
            "identifier_identifier_intralink_datestamp",
            "identifier_identifier_intralink_scratch",
        ],
        "creator_note_links": [
            "creator_note_link_id",
            "creator_note_link_creator_id",
            "creator_note_link_note_id",
            "creator_note_link_priority",
            "creator_note_link_type",
            "creator_note_link_datestamp",
            "creator_note_link_scratch",
        ],
        "allowed_types__language_title_links": [
            "allowed_types__language_title_link_id",
            "allowed_types__language_title_link_type",
            "allowed_types__language_title_link_datestamp",
            "allowed_types__language_title_link_scratch",
        ],
        "book_books_secondary_uuid_links": [
            "book_books_secondary_uuid_link_id",
            "book_books_secondary_uuid_link_book_id",
            "book_books_secondary_uuid_link_books_secondary_uuid_id",
            "book_books_secondary_uuid_link_priority",
            "book_books_secondary_uuid_link_primary",
            "book_books_secondary_uuid_link_type",
            "book_books_secondary_uuid_link_index",
            "book_books_secondary_uuid_link_datestamp",
            "book_books_secondary_uuid_link_scratch",
        ],
        "book_year_first_published_links": [
            "book_year_first_published_link_id",
            "book_year_first_published_link_book_id",
            "book_year_first_published_link_year_first_published_id",
            "book_year_first_published_link_priority",
            "book_year_first_published_link_primary",
            "book_year_first_published_link_type",
            "book_year_first_published_link_index",
            "book_year_first_published_link_datestamp",
            "book_year_first_published_link_scratch",
        ],
        "publisher_publisher_intralinks": [
            "publisher_publisher_intralink_id",
            "publisher_publisher_intralink_primary_id",
            "publisher_publisher_intralink_secondary_id",
            "publisher_publisher_intralink_type",
            "publisher_publisher_intralink_datestamp",
            "publisher_publisher_intralink_scratch",
        ],
        "hashes": ["hash_id", "hash"],
        "book_year_reprinted_links": [
            "book_year_reprinted_link_id",
            "book_year_reprinted_link_book_id",
            "book_year_reprinted_link_year_reprinted_id",
            "book_year_reprinted_link_priority",
            "book_year_reprinted_link_primary",
            "book_year_reprinted_link_type",
            "book_year_reprinted_link_index",
            "book_year_reprinted_link_datestamp",
            "book_year_reprinted_link_scratch",
        ],
        "custom_columns": [
            "custom_column_id",
            "custom_column_mark_for_delete",
            "custom_column_in_table",
            "custom_column_label",
            "custom_column_name",
            "custom_column_datatype",
            "custom_column_db_datatype",
            "custom_column_editable",
            "custom_column_display",
            "custom_column_normalized",
            "custom_column_is_multiple",
            "custom_column_ordered",
            "custom_column_datestamp",
            "custom_column_scratch",
        ],
        "series_character_introductions": [
            "series_character_introduction_id",
            "series_character_introduction",
            "series_character_introduction_datestamp",
            "series_character_introduction_scratch",
        ],
        "folder_store_note_links": [
            "folder_store_note_link_id",
            "folder_store_note_link_folder_store_id",
            "folder_store_note_link_note_id",
            "folder_store_note_link_priority",
            "folder_store_note_link_datestamp",
            "folder_store_note_link_scratch",
        ],
        "genre_title_links": [
            "genre_title_link_id",
            "genre_title_link_genre_id",
            "genre_title_link_title_id",
            "genre_title_link_priority",
            "genre_title_link_datestamp",
            "genre_title_link_scratch",
        ],
        "comment_title_links": [
            "comment_title_link_id",
            "comment_title_link_comment_id",
            "comment_title_link_title_id",
            "comment_title_link_priority",
            "comment_title_link_type",
            "comment_title_link_datestamp",
            "comment_title_link_scratch",
        ],
        "title_word_links": [
            "title_word_link_id",
            "title_word_link_title_id",
            "title_word_link_word_id",
            "title_word_link_priority",
            "title_word_link_primary",
            "title_word_link_type",
            "title_word_link_index",
            "title_word_link_datestamp",
            "title_word_link_scratch",
        ],
        "series_tag_links": [
            "series_tag_link_id",
            "series_tag_link_series_id",
            "series_tag_link_tag_id",
            "series_tag_link_datestamp",
            "series_tag_link_scratch",
        ],
        "allowed_types__file_identifier_links": [
            "allowed_types__file_identifier_link_id",
            "allowed_types__file_identifier_link_type",
            "allowed_types__file_identifier_link_datestamp",
            "allowed_types__file_identifier_link_scratch",
        ],
        "folder_store_folder_store_intralinks": [
            "folder_store_folder_store_intralink_id",
            "folder_store_folder_store_intralink_primary_id",
            "folder_store_folder_store_intralink_secondary_id",
            "folder_store_folder_store_intralink_type",
            "folder_store_folder_store_intralink_datestamp",
            "folder_store_folder_store_intralink_scratch",
        ],
        "files": [
            "file_id",
            "file_name",
            "file_base_name",
            "file_tag",
            "file_extension",
            "file_path",
            "file_auto_name",
            "file_use_auto_name",
            "file_size",
            "file_hash",
            "file_new_hash",
            "file_corrupt",
            "file_base_folder",
            "file_acquired_timestamp",
            "file_source",
            "file_original_name",
            "file_original_path",
            "file_phash",
            "file_anthology",
            "file_critical",
            "file_parent",
            "file_conversion_settings",
            "file_processed",
            "file_created_datestamp",
            "file_datestamp",
            "file_scratch",
        ],
        "creator_folder_links": [
            "creator_folder_link_id",
            "creator_folder_link_creator_id",
            "creator_folder_link_folder_id",
            "creator_folder_link_priority",
            "creator_folder_link_datestamp",
            "creator_folder_link_scratch",
        ],
        "allowed_types__rating_title_links": [
            "allowed_types__rating_title_link_id",
            "allowed_types__rating_title_link_type",
            "allowed_types__rating_title_link_datestamp",
            "allowed_types__rating_title_link_scratch",
        ],
        "note_title_links": [
            "note_title_link_id",
            "note_title_link_note_id",
            "note_title_link_title_id",
            "note_title_link_priority",
            "note_title_link_type",
            "note_title_link_datestamp",
            "note_title_link_scratch",
        ],
        "allowed_types__comment_creator_links": [
            "allowed_types__comment_creator_link_id",
            "allowed_types__comment_creator_link_type",
            "allowed_types__comment_creator_link_datestamp",
            "allowed_types__comment_creator_link_scratch",
        ],
        "tag_title_links": [
            "tag_title_link_id",
            "tag_title_link_tag_id",
            "tag_title_link_title_id",
            "tag_title_link_datestamp",
            "tag_title_link_scratch",
        ],
        "character_introduction_title_links": [
            "character_introduction_title_link_id",
            "character_introduction_title_link_character_introduction_id",
            "character_introduction_title_link_title_id",
            "character_introduction_title_link_priority",
            "character_introduction_title_link_primary",
            "character_introduction_title_link_type",
            "character_introduction_title_link_index",
            "character_introduction_title_link_datestamp",
            "character_introduction_title_link_scratch",
        ],
        "creator_tag_links": [
            "creator_tag_link_id",
            "creator_tag_link_creator_id",
            "creator_tag_link_tag_id",
            "creator_tag_link_datestamp",
            "creator_tag_link_scratch",
        ],
        "note_publisher_links": [
            "note_publisher_link_id",
            "note_publisher_link_note_id",
            "note_publisher_link_publisher_id",
            "note_publisher_link_priority",
            "note_publisher_link_datestamp",
            "note_publisher_link_scratch",
        ],
        "file_publisher_links": [
            "file_publisher_link_id",
            "file_publisher_link_file_id",
            "file_publisher_link_publisher_id",
            "file_publisher_link_datestamp",
            "file_publisher_link_scratch",
        ],
        "file_language_links": [
            "file_language_link_id",
            "file_language_link_file_id",
            "file_language_link_language_id",
            "file_language_link_datestamp",
            "file_language_link_scratch",
        ],
        "titles": [
            "title_id",
            "title",
            "title_sort",
            "title_phash",
            "title_creator_sort",
            "title_pub_date",
            "title_copyright_date",
            "title_wikipedia",
            "title_fiction_length_category",
            "title_type",
            "title_wordcount",
            "title_source",
            "title_source_path",
            "title_source_name",
            "title_created_datestamp",
            "title_datestamp",
            "title_last_modified",
            "title_scratch",
        ],
        "allowed_types__titles_titles_intralinks": [
            "allowed_types__titles_titles_intralink_id",
            "allowed_types__titles_titles_intralink_type",
            "allowed_types__titles_titles_intralink_datestamp",
            "allowed_types__titles_titles_intralink_scratch",
        ],
        "allowed_types__creator_note_links": [
            "allowed_types__creator_note_link_id",
            "allowed_types__creator_note_link_type",
            "allowed_types__creator_note_link_datestamp",
            "allowed_types__creator_note_link_scratch",
        ],
        "creator_synopsis_links": [
            "creator_synopsis_link_id",
            "creator_synopsis_link_creator_id",
            "creator_synopsis_link_synopsis_id",
            "creator_synopsis_link_datestamp",
            "creator_synopsis_link_scratch",
        ],
        "allowed_types__tags_tags_intralinks": [
            "allowed_types__tags_tags_intralink_id",
            "allowed_types__tags_tags_intralink_type",
            "allowed_types__tags_tags_intralink_datestamp",
            "allowed_types__tags_tags_intralink_scratch",
        ],
        "rating_title_links": [
            "rating_title_link_id",
            "rating_title_link_rating_id",
            "rating_title_link_title_id",
            "rating_title_link_type",
            "rating_title_link_datestamp",
            "rating_title_link_scratch",
        ],
        "allowed_types__covers_covers_intralinks": [
            "allowed_types__covers_covers_intralink_id",
            "allowed_types__covers_covers_intralink_type",
            "allowed_types__covers_covers_intralink_datestamp",
            "allowed_types__covers_covers_intralink_scratch",
        ],
        "covers": [
            "cover_id",
            "cover_name",
            "cover_extension",
            "cover_path",
            "cover_use_auto_name",
            "cover_hash",
            "cover_new_hash",
            "cover_corrupt",
            "cover_original_path",
            "cover_local",
            "cover_base_folder",
            "cover_created_datestamp",
            "cover_datestamp",
            "cover_scratch",
        ],
        "book_file_links": [
            "book_file_link_id",
            "book_file_link_book_id",
            "book_file_link_file_id",
            "book_file_link_priority",
            "book_file_link_datestamp",
            "book_file_link_scratch",
        ],
        "file_identifier_links": [
            "file_identifier_link_id",
            "file_identifier_link_file_id",
            "file_identifier_link_identifier_id",
            "file_identifier_link_priority",
            "file_identifier_link_type",
            "file_identifier_link_datestamp",
            "file_identifier_link_scratch",
        ],
        "character_introductions": [
            "character_introduction_id",
            "character_introduction",
            "character_introduction_datestamp",
            "character_introduction_scratch",
        ],
        "secondary_uuid_title_links": [
            "secondary_uuid_title_link_id",
            "secondary_uuid_title_link_secondary_uuid_id",
            "secondary_uuid_title_link_title_id",
            "secondary_uuid_title_link_priority",
            "secondary_uuid_title_link_primary",
            "secondary_uuid_title_link_type",
            "secondary_uuid_title_link_index",
            "secondary_uuid_title_link_datestamp",
            "secondary_uuid_title_link_scratch",
        ],
        "cover_creator_links": [
            "cover_creator_link_id",
            "cover_creator_link_cover_id",
            "cover_creator_link_creator_id",
            "cover_creator_link_priority",
            "cover_creator_link_datestamp",
            "cover_creator_link_scratch",
        ],
        "ratings": [
            "rating_id",
            "rating",
            "rating_source",
            "rating_datestamp",
            "rating_scratch",
        ],
        "genres": [
            "genre_id",
            "genre",
            "genre_sort",
            "genre_phash",
            "genre_parent",
            "genre_position",
            "genre_tree_id",
            "genre_full",
            "genre_datestamp",
            "genre_scratch",
        ],
        "books_secondary_uuid": [
            "books_secondary_uuid_id",
            "books_secondary_uuid",
            "books_secondary_uuid_datestamp",
            "books_secondary_uuid_scratch",
        ],
        "creator_series_links": [
            "creator_series_link_id",
            "creator_series_link_creator_id",
            "creator_series_link_series_id",
            "creator_series_link_type",
            "creator_series_link_datestamp",
            "creator_series_link_scratch",
        ],
        "last_read_positions": [
            "id",
            "book",
            "format",
            "user",
            "device",
            "cfi",
            "epoch",
            "pos_frac",
        ],
        "device_note_links": [
            "device_note_link_id",
            "device_note_link_device_id",
            "device_note_link_note_id",
            "device_note_link_priority",
            "device_note_link_datestamp",
            "device_note_link_scratch",
        ],
        "subjects": [
            "subject_id",
            "subject",
            "subject_phash",
            "subject_sort",
            "subject_parent",
            "subject_parent_position",
            "subject_tree_id",
            "subject_full",
            "subject_datestamp",
            "subject_scratch",
        ],
        "allowed_types__creator_title_links": [
            "allowed_types__creator_title_link_id",
            "allowed_types__creator_title_link_type",
            "allowed_types__creator_title_link_datestamp",
            "allowed_types__creator_title_link_scratch",
        ],
        "folders": [
            "folder_id",
            "folder_name",
            "folder_tag",
            "folder_original_name",
            "folder_auto_name",
            "folder_use_auto_name",
            "folder_path",
            "folder_auto_position",
            "folder_found",
            "folder_folder_store_id",
            "folder_parent",
            "folder_depth",
            "folder_size",
            "folder_hash",
            "folder_types",
            "folder_created_datestamp",
            "folder_datestamp",
            "folder_scratch",
        ],
        "publisher_owners": [
            "publisher_owner_id",
            "publisher_owner",
            "publisher_owner_datestamp",
            "publisher_owner_scratch",
        ],
        "allowed_types__device_file_links": [
            "allowed_types__device_file_link_id",
            "allowed_types__device_file_link_type",
            "allowed_types__device_file_link_datestamp",
            "allowed_types__device_file_link_scratch",
        ],
        "allowed_types__book_cover_links": [
            "allowed_types__book_cover_link_id",
            "allowed_types__book_cover_link_type",
            "allowed_types__book_cover_link_datestamp",
            "allowed_types__book_cover_link_scratch",
        ],
        "folder_series_links": [
            "folder_series_link_id",
            "folder_series_link_folder_id",
            "folder_series_link_series_id",
            "folder_series_link_priority",
            "folder_series_link_datestamp",
            "folder_series_link_scratch",
        ],
        "series_title_links": [
            "series_title_link_id",
            "series_title_link_series_id",
            "series_title_link_title_id",
            "series_title_link_priority",
            "series_title_link_index",
            "series_title_link_datestamp",
            "series_title_link_scratch",
        ],
        "cover_cover_intralinks": [
            "cover_cover_intralink_id",
            "cover_cover_intralink_primary_id",
            "cover_cover_intralink_secondary_id",
            "cover_cover_intralink_type",
            "cover_cover_intralink_datestamp",
            "cover_cover_intralink_scratch",
        ],
        "genre_series_links": [
            "genre_series_link_id",
            "genre_series_link_genre_id",
            "genre_series_link_series_id",
            "genre_series_link_priority",
            "genre_series_link_datestamp",
            "genre_series_link_scratch",
        ],
        "subject_title_links": [
            "subject_title_link_id",
            "subject_title_link_subject_id",
            "subject_title_link_title_id",
            "subject_title_link_priority",
            "subject_title_link_datestamp",
            "subject_title_link_scratch",
        ],
        "cover_series_links": [
            "cover_series_link_id",
            "cover_series_link_cover_id",
            "cover_series_link_series_id",
            "cover_series_link_priority",
            "cover_series_link_datestamp",
            "cover_series_link_scratch",
        ],
        "year_first_published": [
            "year_first_published_id",
            "year_first_published",
            "year_first_published_datestamp",
            "year_first_published_scratch",
        ],
        "identifiers": [
            "identifier_id",
            "identifier",
            "identifier_type",
            "identifier_datestamp",
            "identifier_scratch",
        ],
        "comment_series_links": [
            "comment_series_link_id",
            "comment_series_link_comment_id",
            "comment_series_link_series_id",
            "comment_series_link_priority",
            "comment_series_link_type",
            "comment_series_link_datestamp",
            "comment_series_link_scratch",
        ],
        "allowed_types__comment_title_links": [
            "allowed_types__comment_title_link_id",
            "allowed_types__comment_title_link_type",
            "allowed_types__comment_title_link_datestamp",
            "allowed_types__comment_title_link_scratch",
        ],
        "allowed_types__identifiers_identifiers_intralinks": [
            "allowed_types__identifiers_identifiers_intralink_id",
            "allowed_types__identifiers_identifiers_intralink_type",
            "allowed_types__identifiers_identifiers_intralink_datestamp",
            "allowed_types__identifiers_identifiers_intralink_scratch",
        ],
        "device_file_links": [
            "device_file_link_id",
            "device_file_link_device_id",
            "device_file_link_file_id",
            "device_file_link_type",
            "device_file_link_datestamp",
            "device_file_link_scratch",
        ],
        "year_reprinted": [
            "year_reprinted_id",
            "year_reprinted",
            "year_reprinted_datestamp",
            "year_reprinted_scratch",
        ],
        "creators": [
            "creator_id",
            "creator",
            "creator_sort",
            "creator_short_name",
            "creator_last_name",
            "creator_phash",
            "creator_legal_name",
            "creator_birth_date",
            "creator_death_date",
            "creator_type",
            "creator_seminal_work",
            "creator_one_person",
            "creator_wikipedia",
            "creator_imdb",
            "creator_link",
            "creator_created_datestamp",
            "creator_datestamp",
            "creator_scratch",
        ],
        "book_cover_links": [
            "book_cover_link_id",
            "book_cover_link_book_id",
            "book_cover_link_cover_id",
            "book_cover_link_priority",
            "book_cover_link_type",
            "book_cover_link_datestamp",
            "book_cover_link_scratch",
        ],
    }

    all_tables_and_column_headings = theo_tables_and_columns

    all_known_tables = sorted(theo_tables_and_columns.keys())

    db_tables = all_known_tables
    db_tables_set = set([_ for _ in db_tables])

    all_sorted_tables = sorted(all_known_tables)

    theo_db_main_tables = set(
        [
            "secondary_uuids",
            "folders",
            "words",
            "series",
            "covers",
            "books",
            "character_introductions",
            "genres",
            "custom_columns",
            "content_levels",
            "series_character_introductions",
            "comments",
            "languages",
            "loc_shelf_numbers",
            "subjects",
            "files",
            "publishers",
            "tags",
            "last_read_positions",
            "synopses",
            "folder_stores",
            "year_first_published",
            "notes",
            "identifiers",
            "devices",
            "books_secondary_uuid",
            "titles",
            "publisher_owners",
            "year_reprinted",
            "feeds",
            "creators",
        ]
    )

    from LiuXin_tests.test_setup.constants import test_asset_version

    # These properties are constants of the database - they should not be changeable
    theo_db_uuid = "test_db_19_fsm_{}".format(test_asset_version)

    theo_db_interlink_tables = set(
        [
            "creator_title_links",
            "book_year_first_published_links",
            "rating_title_links",
            "comment_title_links",
            "genre_series_links",
            "book_books_secondary_uuid_links",
            "language_title_links",
            "publisher_publisher_owner_links",
            "creator_synopsis_links",
            "file_identifier_links",
            "comment_series_links",
            "cover_creator_links",
            "book_year_reprinted_links",
            "synopsis_title_links",
            "note_series_links",
            "publisher_title_links",
            "identifier_title_links",
            "device_note_links",
            "folder_store_note_links",
            "genre_title_links",
            "creator_series_links",
            "title_word_links",
            "series_tag_links",
            "book_folder_links",
            "creator_folder_links",
            "series_series_character_introduction_links",
            "note_title_links",
            "folder_series_links",
            "tag_title_links",
            "series_title_links",
            "comment_creator_links",
            "character_introduction_title_links",
            "subject_title_links",
            "cover_series_links",
            "content_level_title_links",
            "creator_tag_links",
            "file_folder_links",
            "note_publisher_links",
            "loc_shelf_number_title_links",
            "file_publisher_links",
            "file_language_links",
            "secondary_uuid_title_links",
            "device_file_links",
            "creator_language_links",
            "creator_note_links",
            "series_synopsis_links",
            "book_cover_links",
            "book_file_links",
        ]
    )

    theo_main_tables = {
        "secondary_uuids",
        "folders",
        "series",
        "covers",
        "content_levels",
        "books",
        "character_introductions",
        "genres",
        "custom_columns",
        "last_read_positions",
        "series_character_introductions",
        "comments",
        "languages",
        "loc_shelf_numbers",
        "subjects",
        "files",
        "publishers",
        "tags",
        "synopses",
        "folder_stores",
        "year_first_published",
        "year_reprinted",
        "notes",
        "identifiers",
        "devices",
        "books_secondary_uuid",
        "titles",
        "publisher_owners",
        "feeds",
        "creators",
        "words",
    }

    # MAIN TABLE PROPERTIES
    theo_titles_count = 10
    theo_character_introductions_count = 14
    theo_character_introductions = theo_character_introductions_count
    theo_loc_shelf_numbers_count = 10
    theo_content_level_count = 10
    theo_note_count = 339
    theo_words_count = 120
    theo_series_count = 690
    theo_secondary_uuids_count = 10
    theo_note_title_link_count = 25

    # TITLES PROPERTIES
    theo_title_1_value = "t-1-f85be9c8"
    theo_title_5_value = "t-5-1d0e6376"

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - WORDS CUSTOM TABLE

    theo_title_1_word_ids = {
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        17,
        18,
        19,
        20,
        21,
        22,
        24,
        25,
        26,
        27,
        29,
        30,
        31,
        32,
        33,
        34,
        35,
        36,
        37,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        45,
        46,
        47,
        48,
        49,
        50,
        51,
        52,
        53,
        54,
        55,
        56,
        57,
        58,
        59,
        60,
        61,
        62,
        64,
        65,
        66,
        67,
        68,
        69,
        70,
        71,
        72,
        73,
        75,
        76,
        77,
        78,
        79,
        80,
        81,
        82,
        83,
        84,
        87,
        89,
        90,
        91,
        92,
        93,
        94,
        95,
        96,
        97,
        98,
        100,
        101,
        102,
        103,
        104,
        105,
        106,
        107,
        108,
        109,
        110,
        113,
        114,
        115,
        116,
        117,
        118,
        119,
        120,
    }

    theo_title_words_counts = {
        1: 108,
        2: 108,
        3: 105,
        4: 103,
        5: 110,
        6: 105,
        7: 110,
        8: 108,
        9: 105,
        10: 106,
    }

    theo_title_1_words_count = theo_title_words_counts[1]
    theo_title_1_word_count = theo_title_1_words_count

    theo_title_1_words = set(
        [
            "words - ROW 34 - fd1c0506-3859-4f96-b42a-1fe2f99e0b1e",
            "words - ROW 95 - c78882ba-397a-4677-954d-e3b330f7f16e",
            "words - ROW 69 - 79085d34-1849-4acf-802e-5580ad1c86bb",
            "words - ROW 57 - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f",
            "words - ROW 51 - 8229a8b9-9ed9-43b5-8bb7-1b7bc64b9127",
            "words - ROW 43 - 29686bc4-0955-4d4b-8717-c225be9f14db",
            "words - ROW 120 - c003f5f9-0394-48fc-afb3-88d78b9534dd",
            "words - ROW 119 - fc21000c-e59c-44f4-9624-022a7658da42",
            "words - ROW 106 - 77211d71-f8a7-411b-ac58-19af6f4ae350",
            "words - ROW 24 - 146ab80f-261f-44b3-a25a-85379e76abe9",
            "words - ROW 81 - ea24b712-4bcb-4f21-b7ce-57db9c919e33",
            "words - ROW 49 - 5a097718-2b1b-48d6-9e1c-424275d0e3c4",
            "words - ROW 18 - 7bd49e8c-2c72-4c12-98ce-84e57c71a634",
            "words - ROW 114 - 4da1e0c5-813a-4a01-8ef6-721d35650ce7",
            "words - ROW 92 - 01afd8a1-813e-4c2b-b8a2-b76b1c3067b1",
            "words - ROW 55 - d2b9f985-b203-466b-97d2-bde0527d5763",
            "words - ROW 59 - b5f0aed6-9956-481b-a11e-ab83847884d8",
            "words - ROW 96 - 5b6879c9-fe5d-4e56-be0f-081257a6fe8a",
            "words - ROW 39 - 010c49f8-eb17-4565-8818-9fdf516ef6cb",
            "words - ROW 73 - 55323a3a-bacf-4056-b5f5-e6f540b92d05",
            "words - ROW 32 - 088d5a29-3fd7-498f-92c4-4443efe66887",
            "words - ROW 70 - 2205909c-4f4e-4818-9140-95dcbeea4d16",
            "words - ROW 22 - 7bd3252a-b053-4080-904f-8292b6a7981c",
            "words - ROW 7 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
            "words - ROW 83 - da788a67-4603-4b71-8ceb-55d3b48ac79e",
            "words - ROW 113 - 8229a8b9-9ed9-43b5-8bb7-1b7bc64b9127",
            "words - ROW 5 - 4f2ab892-6a87-4d46-b1fb-a56478f84958",
            "words - ROW 93 - b12cfb91-6a49-4455-9ddf-5a797b7fe24c",
            "words - ROW 82 - b5f0aed6-9956-481b-a11e-ab83847884d8",
            "words - ROW 109 - 413455b4-d046-4da7-8761-36b8a88fe088",
            "words - ROW 15 - 1a87595e-f0a3-4ada-8a51-5e1f441bafbe",
            "words - ROW 107 - 39ce9a58-ea08-4f2c-994a-b49d61e9393d",
            "words - ROW 42 - 30b7d9de-990b-4294-b2a5-2d6855c77b23",
            "words - ROW 98 - 67013d5f-d85d-4308-a208-a5162afc51ab",
            "words - ROW 52 - 5b6879c9-fe5d-4e56-be0f-081257a6fe8a",
            "words - ROW 76 - 4bce79fc-83a2-43fb-8505-ef9a415bb35f",
            "words - ROW 97 - b5f0aed6-9956-481b-a11e-ab83847884d8",
            "words - ROW 62 - c78882ba-397a-4677-954d-e3b330f7f16e",
            "words - ROW 12 - c5c0e3b8-7f0a-4925-a326-cc5bf2c4ecd6",
            "words - ROW 38 - 1b5d91b5-1b35-4d52-9e65-5e6c606a85c9",
            "words - ROW 11 - 984d4dec-2ccf-4d81-b6e7-8420430262fd",
            "words - ROW 108 - 3f2fa934-e8cb-4635-8619-7afe9b5a6cd5",
            "words - ROW 84 - 862d3f42-f2de-4063-ad83-603da1b68b6b",
            "words - ROW 75 - 0d4aa6b0-9c1c-4a14-a661-66838257faad",
            "words - ROW 29 - 3542b654-e2d8-48a2-aaa3-7882aa50e259",
            "words - ROW 40 - 849b8e8d-95a4-4018-9510-45fed629ee65",
            "words - ROW 101 - d27ef268-7a63-4dfa-8876-03a6de8f3e93",
            "words - ROW 89 - 6e61e176-d873-4bef-a3f5-0eb41e20328d",
            "words - ROW 68 - 669ba846-6730-4fc8-9299-eb1a5c3c082c",
            "words - ROW 71 - 78affa14-a6cb-4f88-a57e-99cb308f99f1",
            "words - ROW 72 - 9b67e869-0ab7-4267-a5d3-3841d8b7145d",
            "words - ROW 91 - d442da62-b938-41e1-a069-0fb8bb85f340",
            "words - ROW 90 - d4201a09-0787-459b-9d35-0b8339042264",
            "words - ROW 3 - 78affa14-a6cb-4f88-a57e-99cb308f99f1",
            "words - ROW 9 - d442da62-b938-41e1-a069-0fb8bb85f340",
            "words - ROW 60 - a3d0b436-b5c2-4c95-848e-8fcfac8e8afa",
            "words - ROW 103 - 0c4ffde3-a02f-4461-a089-c72979297354",
            "words - ROW 115 - fe83a7fb-ac72-4c43-a868-869155fee1a1",
            "words - ROW 31 - 29686bc4-0955-4d4b-8717-c225be9f14db",
            "words - ROW 48 - 7f93e2df-1c66-4d29-a36b-3878c650c447",
            "words - ROW 6 - fbfa7e8f-a517-4e4c-96be-5fb7acef8517",
            "words - ROW 118 - 6e7e2e87-5443-40f6-aef6-552ee1ab5531",
            "words - ROW 26 - 03210848-24c6-42ac-b918-74aba63e40f3",
            "words - ROW 19 - 55085fbd-0504-4f37-8fc3-3b6f75f03d41",
            "words - ROW 45 - b0cd931d-5325-4331-8d08-c038682eb4c3",
            "words - ROW 27 - c78882ba-397a-4677-954d-e3b330f7f16e",
            "words - ROW 87 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
            "words - ROW 47 - e242db80-72b7-41d6-9fa4-d86b61e72318",
            "words - ROW 61 - 69b09912-0efb-4c72-8914-cbbd2e4a9bdd",
            "words - ROW 36 - da788a67-4603-4b71-8ceb-55d3b48ac79e",
            "words - ROW 117 - 43024a39-3cd9-4ad8-8b1b-c2d79f900c4b",
            "words - ROW 44 - 25233cf6-1e45-4ce6-a96c-297db220521c",
            "words - ROW 66 - 0cdf212c-724a-4666-ba52-8f8b4860f0f3",
            "words - ROW 79 - b12cfb91-6a49-4455-9ddf-5a797b7fe24c",
            "words - ROW 53 - c31ad47d-6f6d-46ec-bab8-96c8945056bd",
            "words - ROW 25 - fe83a7fb-ac72-4c43-a868-869155fee1a1",
            "words - ROW 33 - 078c31b9-a5f2-42ac-8621-6d9bc9355d4f",
            "words - ROW 17 - e08fb6ba-3808-4895-81e2-a9638dc29cee",
            "words - ROW 1 - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f",
            "words - ROW 46 - 184852b7-f79d-4cb3-acff-4a0597bd2f23",
            "words - ROW 37 - 8229a8b9-9ed9-43b5-8bb7-1b7bc64b9127",
            "words - ROW 104 - 83a0633c-042d-41ba-a447-a248c4a0cd0c",
            "words - ROW 4 - f4b6557c-4278-4e02-9d5f-7b2c88daa7e0",
            "words - ROW 41 - 9b88b308-40de-4ed1-ab51-b7762b950e49",
            "words - ROW 116 - a471e7c5-6a0f-415c-81b5-d69b1997b365",
            "words - ROW 54 - f8f24390-b627-423a-97a1-aa980159df44",
            "words - ROW 100 - 7bd3252a-b053-4080-904f-8292b6a7981c",
            "words - ROW 35 - d1803224-93e4-46ae-8756-bbe42796360e",
            "words - ROW 30 - 146ab80f-261f-44b3-a25a-85379e76abe9",
            "words - ROW 77 - 1d0e6376-0cb5-43b8-b909-0e74d53805fa",
            "words - ROW 64 - 7d465381-1f61-4501-98a5-b95db064e4dc",
            "words - ROW 94 - 68042e03-d2f2-41a0-b08e-645123c12597",
            "words - ROW 110 - 6e7e2e87-5443-40f6-aef6-552ee1ab5531",
            "words - ROW 65 - 83a0633c-042d-41ba-a447-a248c4a0cd0c",
            "words - ROW 14 - 69b09912-0efb-4c72-8914-cbbd2e4a9bdd",
            "words - ROW 20 - 8567479b-4856-44b3-bf9e-6ebd73476942",
            "words - ROW 78 - 03210848-24c6-42ac-b918-74aba63e40f3",
            "words - ROW 10 - b5f0aed6-9956-481b-a11e-ab83847884d8",
            "words - ROW 80 - 635c7cf4-4a46-402d-b219-af0479b5aa3e",
            "words - ROW 105 - 2f5ade60-4c28-47a2-82f9-a71ab5e05158",
            "words - ROW 102 - d2b9f985-b203-466b-97d2-bde0527d5763",
            "words - ROW 50 - b0cd931d-5325-4331-8d08-c038682eb4c3",
            "words - ROW 56 - 6e2a8e93-b7ad-4673-bc9e-589294004c38",
            "words - ROW 13 - 42d53491-dc6c-4e9b-b8c3-d303e16ba806",
            "words - ROW 58 - 25233cf6-1e45-4ce6-a96c-297db220521c",
            "words - ROW 67 - 669ba846-6730-4fc8-9299-eb1a5c3c082c",
            "words - ROW 2 - 2fa7997a-9998-47fa-85ec-3c81f6180a8b",
            "words - ROW 21 - 268b04fe-92df-4e2e-b23b-ce5b74dbafc2",
        ]
    )

    assert len(theo_title_1_words) == theo_title_1_words_count == theo_title_words_counts[1]

    theo_title_4_word_ids = set(
        [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            9,
            10,
            12,
            14,
            15,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            29,
            30,
            31,
            32,
            33,
            34,
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            42,
            43,
            44,
            46,
            47,
            48,
            49,
            50,
            52,
            53,
            54,
            55,
            56,
            57,
            58,
            60,
            61,
            62,
            63,
            64,
            65,
            66,
            67,
            68,
            69,
            71,
            72,
            73,
            75,
            76,
            77,
            78,
            79,
            80,
            81,
            82,
            83,
            84,
            87,
            88,
            89,
            90,
            91,
            92,
            93,
            94,
            95,
            97,
            98,
            100,
            101,
            103,
            104,
            105,
            106,
            107,
            109,
            110,
            111,
            113,
            114,
            115,
            116,
            117,
            118,
            119,
            120,
        ]
    )

    theo_title_4_words_count = theo_title_words_counts[4]
    theo_title_4_word_count = theo_title_4_words_count

    assert len(theo_title_4_word_ids) == theo_title_4_words_count

    theo_title_4_words = set(
        [
            "words - ROW 23 - 83a0633c-042d-41ba-a447-a248c4a0cd0c",
            "words - ROW 34 - fd1c0506-3859-4f96-b42a-1fe2f99e0b1e",
            "words - ROW 95 - c78882ba-397a-4677-954d-e3b330f7f16e",
            "words - ROW 69 - 79085d34-1849-4acf-802e-5580ad1c86bb",
            "words - ROW 57 - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f",
            "words - ROW 43 - 29686bc4-0955-4d4b-8717-c225be9f14db",
            "words - ROW 120 - c003f5f9-0394-48fc-afb3-88d78b9534dd",
            "words - ROW 119 - fc21000c-e59c-44f4-9624-022a7658da42",
            "words - ROW 106 - 77211d71-f8a7-411b-ac58-19af6f4ae350",
            "words - ROW 24 - 146ab80f-261f-44b3-a25a-85379e76abe9",
            "words - ROW 81 - ea24b712-4bcb-4f21-b7ce-57db9c919e33",
            "words - ROW 49 - 5a097718-2b1b-48d6-9e1c-424275d0e3c4",
            "words - ROW 18 - 7bd49e8c-2c72-4c12-98ce-84e57c71a634",
            "words - ROW 114 - 4da1e0c5-813a-4a01-8ef6-721d35650ce7",
            "words - ROW 92 - 01afd8a1-813e-4c2b-b8a2-b76b1c3067b1",
            "words - ROW 55 - d2b9f985-b203-466b-97d2-bde0527d5763",
            "words - ROW 39 - 010c49f8-eb17-4565-8818-9fdf516ef6cb",
            "words - ROW 73 - 55323a3a-bacf-4056-b5f5-e6f540b92d05",
            "words - ROW 32 - 088d5a29-3fd7-498f-92c4-4443efe66887",
            "words - ROW 22 - 7bd3252a-b053-4080-904f-8292b6a7981c",
            "words - ROW 7 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
            "words - ROW 83 - da788a67-4603-4b71-8ceb-55d3b48ac79e",
            "words - ROW 113 - 8229a8b9-9ed9-43b5-8bb7-1b7bc64b9127",
            "words - ROW 5 - 4f2ab892-6a87-4d46-b1fb-a56478f84958",
            "words - ROW 93 - b12cfb91-6a49-4455-9ddf-5a797b7fe24c",
            "words - ROW 82 - b5f0aed6-9956-481b-a11e-ab83847884d8",
            "words - ROW 109 - 413455b4-d046-4da7-8761-36b8a88fe088",
            "words - ROW 15 - 1a87595e-f0a3-4ada-8a51-5e1f441bafbe",
            "words - ROW 107 - 39ce9a58-ea08-4f2c-994a-b49d61e9393d",
            "words - ROW 42 - 30b7d9de-990b-4294-b2a5-2d6855c77b23",
            "words - ROW 98 - 67013d5f-d85d-4308-a208-a5162afc51ab",
            "words - ROW 52 - 5b6879c9-fe5d-4e56-be0f-081257a6fe8a",
            "words - ROW 76 - 4bce79fc-83a2-43fb-8505-ef9a415bb35f",
            "words - ROW 97 - b5f0aed6-9956-481b-a11e-ab83847884d8",
            "words - ROW 62 - c78882ba-397a-4677-954d-e3b330f7f16e",
            "words - ROW 12 - c5c0e3b8-7f0a-4925-a326-cc5bf2c4ecd6",
            "words - ROW 38 - 1b5d91b5-1b35-4d52-9e65-5e6c606a85c9",
            "words - ROW 84 - 862d3f42-f2de-4063-ad83-603da1b68b6b",
            "words - ROW 75 - 0d4aa6b0-9c1c-4a14-a661-66838257faad",
            "words - ROW 29 - 3542b654-e2d8-48a2-aaa3-7882aa50e259",
            "words - ROW 40 - 849b8e8d-95a4-4018-9510-45fed629ee65",
            "words - ROW 101 - d27ef268-7a63-4dfa-8876-03a6de8f3e93",
            "words - ROW 89 - 6e61e176-d873-4bef-a3f5-0eb41e20328d",
            "words - ROW 68 - 669ba846-6730-4fc8-9299-eb1a5c3c082c",
            "words - ROW 71 - 78affa14-a6cb-4f88-a57e-99cb308f99f1",
            "words - ROW 88 - 7f93e2df-1c66-4d29-a36b-3878c650c447",
            "words - ROW 72 - 9b67e869-0ab7-4267-a5d3-3841d8b7145d",
            "words - ROW 91 - d442da62-b938-41e1-a069-0fb8bb85f340",
            "words - ROW 90 - d4201a09-0787-459b-9d35-0b8339042264",
            "words - ROW 3 - 78affa14-a6cb-4f88-a57e-99cb308f99f1",
            "words - ROW 9 - d442da62-b938-41e1-a069-0fb8bb85f340",
            "words - ROW 60 - a3d0b436-b5c2-4c95-848e-8fcfac8e8afa",
            "words - ROW 103 - 0c4ffde3-a02f-4461-a089-c72979297354",
            "words - ROW 115 - fe83a7fb-ac72-4c43-a868-869155fee1a1",
            "words - ROW 26 - 03210848-24c6-42ac-b918-74aba63e40f3",
            "words - ROW 48 - 7f93e2df-1c66-4d29-a36b-3878c650c447",
            "words - ROW 6 - fbfa7e8f-a517-4e4c-96be-5fb7acef8517",
            "words - ROW 118 - 6e7e2e87-5443-40f6-aef6-552ee1ab5531",
            "words - ROW 31 - 29686bc4-0955-4d4b-8717-c225be9f14db",
            "words - ROW 19 - 55085fbd-0504-4f37-8fc3-3b6f75f03d41",
            "words - ROW 27 - c78882ba-397a-4677-954d-e3b330f7f16e",
            "words - ROW 87 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
            "words - ROW 47 - e242db80-72b7-41d6-9fa4-d86b61e72318",
            "words - ROW 61 - 69b09912-0efb-4c72-8914-cbbd2e4a9bdd",
            "words - ROW 36 - da788a67-4603-4b71-8ceb-55d3b48ac79e",
            "words - ROW 58 - 25233cf6-1e45-4ce6-a96c-297db220521c",
            "words - ROW 44 - 25233cf6-1e45-4ce6-a96c-297db220521c",
            "words - ROW 66 - 0cdf212c-724a-4666-ba52-8f8b4860f0f3",
            "words - ROW 79 - b12cfb91-6a49-4455-9ddf-5a797b7fe24c",
            "words - ROW 53 - c31ad47d-6f6d-46ec-bab8-96c8945056bd",
            "words - ROW 25 - fe83a7fb-ac72-4c43-a868-869155fee1a1",
            "words - ROW 33 - 078c31b9-a5f2-42ac-8621-6d9bc9355d4f",
            "words - ROW 17 - e08fb6ba-3808-4895-81e2-a9638dc29cee",
            "words - ROW 1 - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f",
            "words - ROW 46 - 184852b7-f79d-4cb3-acff-4a0597bd2f23",
            "words - ROW 37 - 8229a8b9-9ed9-43b5-8bb7-1b7bc64b9127",
            "words - ROW 104 - 83a0633c-042d-41ba-a447-a248c4a0cd0c",
            "words - ROW 63 - 9b9e8537-31cf-4683-af4b-5e2a33b75e8a",
            "words - ROW 111 - c187b301-de7e-4f91-94dc-57cab3996a95",
            "words - ROW 41 - 9b88b308-40de-4ed1-ab51-b7762b950e49",
            "words - ROW 116 - a471e7c5-6a0f-415c-81b5-d69b1997b365",
            "words - ROW 54 - f8f24390-b627-423a-97a1-aa980159df44",
            "words - ROW 100 - 7bd3252a-b053-4080-904f-8292b6a7981c",
            "words - ROW 35 - d1803224-93e4-46ae-8756-bbe42796360e",
            "words - ROW 30 - 146ab80f-261f-44b3-a25a-85379e76abe9",
            "words - ROW 77 - 1d0e6376-0cb5-43b8-b909-0e74d53805fa",
            "words - ROW 64 - 7d465381-1f61-4501-98a5-b95db064e4dc",
            "words - ROW 94 - 68042e03-d2f2-41a0-b08e-645123c12597",
            "words - ROW 110 - 6e7e2e87-5443-40f6-aef6-552ee1ab5531",
            "words - ROW 4 - f4b6557c-4278-4e02-9d5f-7b2c88daa7e0",
            "words - ROW 65 - 83a0633c-042d-41ba-a447-a248c4a0cd0c",
            "words - ROW 14 - 69b09912-0efb-4c72-8914-cbbd2e4a9bdd",
            "words - ROW 20 - 8567479b-4856-44b3-bf9e-6ebd73476942",
            "words - ROW 78 - 03210848-24c6-42ac-b918-74aba63e40f3",
            "words - ROW 10 - b5f0aed6-9956-481b-a11e-ab83847884d8",
            "words - ROW 80 - 635c7cf4-4a46-402d-b219-af0479b5aa3e",
            "words - ROW 105 - 2f5ade60-4c28-47a2-82f9-a71ab5e05158",
            "words - ROW 50 - b0cd931d-5325-4331-8d08-c038682eb4c3",
            "words - ROW 56 - 6e2a8e93-b7ad-4673-bc9e-589294004c38",
            "words - ROW 117 - 43024a39-3cd9-4ad8-8b1b-c2d79f900c4b",
            "words - ROW 67 - 669ba846-6730-4fc8-9299-eb1a5c3c082c",
            "words - ROW 2 - 2fa7997a-9998-47fa-85ec-3c81f6180a8b",
            "words - ROW 21 - 268b04fe-92df-4e2e-b23b-ce5b74dbafc2",
        ]
    )

    theo_title_5_words_count = theo_title_words_counts[5]
    theo_title_5_word_count = theo_title_5_words_count

    theo_title_5_words = set(
        [
            "words - ROW 23 - 83a0633c-042d-41ba-a447-a248c4a0cd0c",
            "words - ROW 34 - fd1c0506-3859-4f96-b42a-1fe2f99e0b1e",
            "words - ROW 95 - c78882ba-397a-4677-954d-e3b330f7f16e",
            "words - ROW 69 - 79085d34-1849-4acf-802e-5580ad1c86bb",
            "words - ROW 57 - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f",
            "words - ROW 51 - 8229a8b9-9ed9-43b5-8bb7-1b7bc64b9127",
            "words - ROW 43 - 29686bc4-0955-4d4b-8717-c225be9f14db",
            "words - ROW 120 - c003f5f9-0394-48fc-afb3-88d78b9534dd",
            "words - ROW 119 - fc21000c-e59c-44f4-9624-022a7658da42",
            "words - ROW 106 - 77211d71-f8a7-411b-ac58-19af6f4ae350",
            "words - ROW 24 - 146ab80f-261f-44b3-a25a-85379e76abe9",
            "words - ROW 81 - ea24b712-4bcb-4f21-b7ce-57db9c919e33",
            "words - ROW 49 - 5a097718-2b1b-48d6-9e1c-424275d0e3c4",
            "words - ROW 16 - 54e78ee6-a8f0-4d60-869f-86fc15dfc181",
            "words - ROW 18 - 7bd49e8c-2c72-4c12-98ce-84e57c71a634",
            "words - ROW 92 - 01afd8a1-813e-4c2b-b8a2-b76b1c3067b1",
            "words - ROW 99 - 9ba48058-819a-46fe-be1b-c8e5a81203b1",
            "words - ROW 39 - 010c49f8-eb17-4565-8818-9fdf516ef6cb",
            "words - ROW 80 - 635c7cf4-4a46-402d-b219-af0479b5aa3e",
            "words - ROW 70 - 2205909c-4f4e-4818-9140-95dcbeea4d16",
            "words - ROW 22 - 7bd3252a-b053-4080-904f-8292b6a7981c",
            "words - ROW 7 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
            "words - ROW 83 - da788a67-4603-4b71-8ceb-55d3b48ac79e",
            "words - ROW 113 - 8229a8b9-9ed9-43b5-8bb7-1b7bc64b9127",
            "words - ROW 5 - 4f2ab892-6a87-4d46-b1fb-a56478f84958",
            "words - ROW 93 - b12cfb91-6a49-4455-9ddf-5a797b7fe24c",
            "words - ROW 82 - b5f0aed6-9956-481b-a11e-ab83847884d8",
            "words - ROW 109 - 413455b4-d046-4da7-8761-36b8a88fe088",
            "words - ROW 15 - 1a87595e-f0a3-4ada-8a51-5e1f441bafbe",
            "words - ROW 107 - 39ce9a58-ea08-4f2c-994a-b49d61e9393d",
            "words - ROW 42 - 30b7d9de-990b-4294-b2a5-2d6855c77b23",
            "words - ROW 98 - 67013d5f-d85d-4308-a208-a5162afc51ab",
            "words - ROW 52 - 5b6879c9-fe5d-4e56-be0f-081257a6fe8a",
            "words - ROW 76 - 4bce79fc-83a2-43fb-8505-ef9a415bb35f",
            "words - ROW 97 - b5f0aed6-9956-481b-a11e-ab83847884d8",
            "words - ROW 62 - c78882ba-397a-4677-954d-e3b330f7f16e",
            "words - ROW 12 - c5c0e3b8-7f0a-4925-a326-cc5bf2c4ecd6",
            "words - ROW 38 - 1b5d91b5-1b35-4d52-9e65-5e6c606a85c9",
            "words - ROW 11 - 984d4dec-2ccf-4d81-b6e7-8420430262fd",
            "words - ROW 108 - 3f2fa934-e8cb-4635-8619-7afe9b5a6cd5",
            "words - ROW 84 - 862d3f42-f2de-4063-ad83-603da1b68b6b",
            "words - ROW 75 - 0d4aa6b0-9c1c-4a14-a661-66838257faad",
            "words - ROW 29 - 3542b654-e2d8-48a2-aaa3-7882aa50e259",
            "words - ROW 40 - 849b8e8d-95a4-4018-9510-45fed629ee65",
            "words - ROW 101 - d27ef268-7a63-4dfa-8876-03a6de8f3e93",
            "words - ROW 89 - 6e61e176-d873-4bef-a3f5-0eb41e20328d",
            "words - ROW 68 - 669ba846-6730-4fc8-9299-eb1a5c3c082c",
            "words - ROW 71 - 78affa14-a6cb-4f88-a57e-99cb308f99f1",
            "words - ROW 88 - 7f93e2df-1c66-4d29-a36b-3878c650c447",
            "words - ROW 72 - 9b67e869-0ab7-4267-a5d3-3841d8b7145d",
            "words - ROW 91 - d442da62-b938-41e1-a069-0fb8bb85f340",
            "words - ROW 90 - d4201a09-0787-459b-9d35-0b8339042264",
            "words - ROW 3 - 78affa14-a6cb-4f88-a57e-99cb308f99f1",
            "words - ROW 9 - d442da62-b938-41e1-a069-0fb8bb85f340",
            "words - ROW 60 - a3d0b436-b5c2-4c95-848e-8fcfac8e8afa",
            "words - ROW 103 - 0c4ffde3-a02f-4461-a089-c72979297354",
            "words - ROW 115 - fe83a7fb-ac72-4c43-a868-869155fee1a1",
            "words - ROW 31 - 29686bc4-0955-4d4b-8717-c225be9f14db",
            "words - ROW 48 - 7f93e2df-1c66-4d29-a36b-3878c650c447",
            "words - ROW 6 - fbfa7e8f-a517-4e4c-96be-5fb7acef8517",
            "words - ROW 118 - 6e7e2e87-5443-40f6-aef6-552ee1ab5531",
            "words - ROW 26 - 03210848-24c6-42ac-b918-74aba63e40f3",
            "words - ROW 19 - 55085fbd-0504-4f37-8fc3-3b6f75f03d41",
            "words - ROW 45 - b0cd931d-5325-4331-8d08-c038682eb4c3",
            "words - ROW 27 - c78882ba-397a-4677-954d-e3b330f7f16e",
            "words - ROW 87 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
            "words - ROW 47 - e242db80-72b7-41d6-9fa4-d86b61e72318",
            "words - ROW 61 - 69b09912-0efb-4c72-8914-cbbd2e4a9bdd",
            "words - ROW 36 - da788a67-4603-4b71-8ceb-55d3b48ac79e",
            "words - ROW 117 - 43024a39-3cd9-4ad8-8b1b-c2d79f900c4b",
            "words - ROW 44 - 25233cf6-1e45-4ce6-a96c-297db220521c",
            "words - ROW 66 - 0cdf212c-724a-4666-ba52-8f8b4860f0f3",
            "words - ROW 79 - b12cfb91-6a49-4455-9ddf-5a797b7fe24c",
            "words - ROW 53 - c31ad47d-6f6d-46ec-bab8-96c8945056bd",
            "words - ROW 25 - fe83a7fb-ac72-4c43-a868-869155fee1a1",
            "words - ROW 33 - 078c31b9-a5f2-42ac-8621-6d9bc9355d4f",
            "words - ROW 17 - e08fb6ba-3808-4895-81e2-a9638dc29cee",
            "words - ROW 55 - d2b9f985-b203-466b-97d2-bde0527d5763",
            "words - ROW 1 - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f",
            "words - ROW 46 - 184852b7-f79d-4cb3-acff-4a0597bd2f23",
            "words - ROW 116 - a471e7c5-6a0f-415c-81b5-d69b1997b365",
            "words - ROW 104 - 83a0633c-042d-41ba-a447-a248c4a0cd0c",
            "words - ROW 4 - f4b6557c-4278-4e02-9d5f-7b2c88daa7e0",
            "words - ROW 111 - c187b301-de7e-4f91-94dc-57cab3996a95",
            "words - ROW 41 - 9b88b308-40de-4ed1-ab51-b7762b950e49",
            "words - ROW 37 - 8229a8b9-9ed9-43b5-8bb7-1b7bc64b9127",
            "words - ROW 54 - f8f24390-b627-423a-97a1-aa980159df44",
            "words - ROW 100 - 7bd3252a-b053-4080-904f-8292b6a7981c",
            "words - ROW 35 - d1803224-93e4-46ae-8756-bbe42796360e",
            "words - ROW 30 - 146ab80f-261f-44b3-a25a-85379e76abe9",
            "words - ROW 77 - 1d0e6376-0cb5-43b8-b909-0e74d53805fa",
            "words - ROW 64 - 7d465381-1f61-4501-98a5-b95db064e4dc",
            "words - ROW 94 - 68042e03-d2f2-41a0-b08e-645123c12597",
            "words - ROW 86 - 6e61e176-d873-4bef-a3f5-0eb41e20328d",
            "words - ROW 110 - 6e7e2e87-5443-40f6-aef6-552ee1ab5531",
            "words - ROW 65 - 83a0633c-042d-41ba-a447-a248c4a0cd0c",
            "words - ROW 14 - 69b09912-0efb-4c72-8914-cbbd2e4a9bdd",
            "words - ROW 20 - 8567479b-4856-44b3-bf9e-6ebd73476942",
            "words - ROW 78 - 03210848-24c6-42ac-b918-74aba63e40f3",
            "words - ROW 10 - b5f0aed6-9956-481b-a11e-ab83847884d8",
            "words - ROW 114 - 4da1e0c5-813a-4a01-8ef6-721d35650ce7",
            "words - ROW 105 - 2f5ade60-4c28-47a2-82f9-a71ab5e05158",
            "words - ROW 102 - d2b9f985-b203-466b-97d2-bde0527d5763",
            "words - ROW 50 - b0cd931d-5325-4331-8d08-c038682eb4c3",
            "words - ROW 56 - 6e2a8e93-b7ad-4673-bc9e-589294004c38",
            "words - ROW 13 - 42d53491-dc6c-4e9b-b8c3-d303e16ba806",
            "words - ROW 58 - 25233cf6-1e45-4ce6-a96c-297db220521c",
            "words - ROW 67 - 669ba846-6730-4fc8-9299-eb1a5c3c082c",
            "words - ROW 2 - 2fa7997a-9998-47fa-85ec-3c81f6180a8b",
            "words - ROW 21 - 268b04fe-92df-4e2e-b23b-ce5b74dbafc2",
        ]
    )

    theo_title_5_word_ids = set(
        [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            29,
            30,
            31,
            33,
            34,
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            42,
            43,
            44,
            45,
            46,
            47,
            48,
            49,
            50,
            51,
            52,
            53,
            54,
            55,
            56,
            57,
            58,
            60,
            61,
            62,
            64,
            65,
            66,
            67,
            68,
            69,
            70,
            71,
            72,
            75,
            76,
            77,
            78,
            79,
            80,
            81,
            82,
            83,
            84,
            86,
            87,
            88,
            89,
            90,
            91,
            92,
            93,
            94,
            95,
            97,
            98,
            99,
            100,
            101,
            102,
            103,
            104,
            105,
            106,
            107,
            108,
            109,
            110,
            111,
            113,
            114,
            115,
            116,
            117,
            118,
            119,
            120,
        ]
    )

    assert len(theo_title_5_words) == len(theo_title_5_word_ids)

    assert len(theo_title_5_words) == theo_title_5_word_count

    theo_word_1_val = "words - ROW 1 - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f"

    theo_word_5_val = "words - ROW 5 - 4f2ab892-6a87-4d46-b1fb-a56478f84958"

    theo_word_7_val = "words - ROW 7 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed"
    theo_word_8_val = "words - ROW 8 - d27ef268-7a63-4dfa-8876-03a6de8f3e93"
    theo_word_9_val = "words - ROW 9 - d442da62-b938-41e1-a069-0fb8bb85f340"

    theo_word_10_val = "words - ROW 10 - b5f0aed6-9956-481b-a11e-ab83847884d8"

    theo_word_12_val = "words - ROW 12 - c5c0e3b8-7f0a-4925-a326-cc5bf2c4ecd6"
    theo_word_13_val = "words - ROW 13 - 42d53491-dc6c-4e9b-b8c3-d303e16ba806"
    theo_word_14_val = "words - ROW 14 - 69b09912-0efb-4c72-8914-cbbd2e4a9bdd"
    theo_word_15_val = "words - ROW 15 - 1a87595e-f0a3-4ada-8a51-5e1f441bafbe"

    theo_word_23_val = "words - ROW 23 - 83a0633c-042d-41ba-a447-a248c4a0cd0c"

    theo_word_47_val = "words - ROW 47 - e242db80-72b7-41d6-9fa4-d86b61e72318"

    theo_word_63_val = "words - ROW 63 - 9b9e8537-31cf-4683-af4b-5e2a33b75e8a"
    theo_word_64_val = "words - ROW 64 - 7d465381-1f61-4501-98a5-b95db064e4dc"

    theo_word_79_val = "words - ROW 79 - b12cfb91-6a49-4455-9ddf-5a797b7fe24c"

    theo_word_100_val = "words - ROW 100 - 7bd3252a-b053-4080-904f-8292b6a7981c"

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CHARACTER INTRODUCTIONS TABLE

    theo_title_ci_counts = {1: 2, 2: 3, 3: 2, 4: 0, 5: 1, 6: 3, 7: 0, 8: 1, 9: 2, 10: 0}

    theo_ci_1_val = "character_introductions - 2fa7997a-9998-47fa-85ec-3c81f6180a8b - LINKED TO titles 1 - ROW 0"
    theo_ci_2_val = "character_introductions - f4b6557c-4278-4e02-9d5f-7b2c88daa7e0 - LINKED TO titles 1 - ROW 1"
    theo_ci_3_val = "character_introductions - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed - LINKED TO titles 2 - ROW 0"
    theo_ci_4_val = "character_introductions - d442da62-b938-41e1-a069-0fb8bb85f340 - LINKED TO titles 2 - ROW 1"
    theo_ci_5_val = "character_introductions - 984d4dec-2ccf-4d81-b6e7-8420430262fd - LINKED TO titles 2 - ROW 2"
    theo_ci_6_val = "character_introductions - 69b09912-0efb-4c72-8914-cbbd2e4a9bdd - LINKED TO titles 3 - ROW 0"
    theo_ci_7_val = "character_introductions - 54e78ee6-a8f0-4d60-869f-86fc15dfc181 - LINKED TO titles 3 - ROW 1"
    theo_ci_8_val = "character_introductions - 8567479b-4856-44b3-bf9e-6ebd73476942 - LINKED TO titles 5 - ROW 0"
    theo_ci_9_val = "character_introductions - 83a0633c-042d-41ba-a447-a248c4a0cd0c - LINKED TO titles 6 - ROW 0"
    theo_ci_10_val = "character_introductions - fe83a7fb-ac72-4c43-a868-869155fee1a1 - LINKED TO titles 6 - ROW 1"

    # TITLE 1 CI PROPERTIES

    theo_title_1_ci_count = theo_title_ci_counts[1]

    theo_title_1_cis = [
        "character_introductions - f4b6557c-4278-4e02-9d5f-7b2c88daa7e0 - LINKED TO titles 1 - ROW 1",
        "character_introductions - 2fa7997a-9998-47fa-85ec-3c81f6180a8b - LINKED TO titles 1 - ROW 0",
    ]

    theo_title_1_ci_ids = [
        2,
        1,
    ]

    assert len(theo_title_1_cis) == len(theo_title_1_ci_ids)

    theo_title_1_character_introductions = {
        "incidental": [],
        "primary": [
            "character_introductions - f4b6557c-4278-4e02-9d5f-7b2c88daa7e0 - LINKED TO titles 1 - ROW 1",
            "character_introductions - 2fa7997a-9998-47fa-85ec-3c81f6180a8b - LINKED TO titles 1 - ROW 0",
        ],
        "secondary": [],
    }

    theo_title_1_ci_vals_set = {
        id_type: set(id_val) for id_type, id_val in iteritems(theo_title_1_character_introductions)
    }

    theo_title_1_ci_ids_dict = {"incidental": [], "primary": [2, 1], "secondary": []}

    theo_title_1_ci_ids_set_dict = {id_type: set(id_val) for id_type, id_val in iteritems(theo_title_1_ci_ids_dict)}

    # TITLE 2 CI PROPERTIES
    theo_title_2_ci_count = theo_title_ci_counts[2]

    # TITLE 4 CI PROPERTIES
    theo_title_4_ci_count = theo_title_ci_counts[4]

    theo_title_4_ci_ids = []

    theo_title_4_cis = []

    assert len(theo_title_4_ci_ids) == theo_title_4_ci_count

    assert len(theo_title_4_cis) == theo_title_4_ci_count

    theo_title_4_ci_ids_dict = {
        "incidental": [],
        "primary": [],
        "secondary": [],
    }

    theo_title_4_ci_ids_set_dict = {id_type: set(id_val) for id_type, id_val in iteritems(theo_title_4_ci_ids_dict)}

    theo_title_4_character_introductions = {
        "incidental": [],
        "primary": [],
        "secondary": [],
    }

    theo_title_4_ci_vals_set = {
        id_type: set(id_val) for id_type, id_val in iteritems(theo_title_4_character_introductions)
    }

    # TITLE 5 CI PROPERTIES

    theo_title_5_ci_count = theo_title_ci_counts[5]

    theo_title_5_cis = ["character_introductions - 8567479b-4856-44b3-bf9e-6ebd73476942 - LINKED TO titles 5 - ROW 0"]

    assert theo_title_5_ci_count == len(theo_title_5_cis)

    theo_title_5_ci_ids = [
        8,
    ]

    theo_title_5_character_introductions = {
        "incidental": ["character_introductions - 8567479b-4856-44b3-bf9e-6ebd73476942 - LINKED TO titles 5 - ROW 0"],
        "primary": [],
        "secondary": [],
    }
    theo_title_5_ci_vals_set = {
        id_type: set(id_val) for id_type, id_val in iteritems(theo_title_5_character_introductions)
    }

    theo_title_5_ci_ids_dict = {
        "incidental": [8],
        "primary": [],
        "secondary": [],
    }
    theo_title_5_ci_ids_set_dict = {id_type: set(id_val) for id_type, id_val in iteritems(theo_title_5_ci_ids_dict)}

    theo_ci_1_title_ids = 1

    theo_ci_col_book_map = {
        1: 1,
        2: 1,
        3: 2,
        4: 2,
        5: 2,
        6: 3,
        7: 3,
        8: 5,
        9: 6,
        10: 6,
        11: 6,
        12: 8,
        13: 9,
        14: 9,
    }

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - SECONDARY_UUIDS

    # Secondary uuid table
    theo_secondary_uuid_row_1 = "secondary_uuids - d1f9c688-3046-4474-a99b-5d7bf1159101 - LINKED TO titles 1"

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BOOK SECONDARY UUID

    # secondary books uuid table
    theo_books_secondary_uuid_row_1 = "books_secondary_uuid - d1f9c688-3046-4474-a99b-5d7bf1159101 - LINKED TO books 1"
    theo_books_secondary_uuid_row_10 = (
        "books_secondary_uuid - ea487aed-0a6d-4d1d-9b73-dca14f96e4dd - LINKED TO books 10"
    )

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - LOC_SHELF_NUMBERS

    theo_lsn_1_value = "loc_shelf_numbers - ROW 1 - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f"
    theo_lsn_2_value = "loc_shelf_numbers - ROW 2 - 2fa7997a-9998-47fa-85ec-3c81f6180a8b"
    theo_lsn_3_value = "loc_shelf_numbers - ROW 3 - 78affa14-a6cb-4f88-a57e-99cb308f99f1"
    theo_lsn_4_value = "loc_shelf_numbers - ROW 4 - f4b6557c-4278-4e02-9d5f-7b2c88daa7e0"
    theo_lsn_5_value = "loc_shelf_numbers - ROW 5 - 4f2ab892-6a87-4d46-b1fb-a56478f84958"
    theo_lsn_6_value = "loc_shelf_numbers - ROW 6 - fbfa7e8f-a517-4e4c-96be-5fb7acef8517"
    theo_lsn_7_value = "loc_shelf_numbers - ROW 7 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed"
    theo_lsn_8_value = "loc_shelf_numbers - ROW 8 - d27ef268-7a63-4dfa-8876-03a6de8f3e93"
    theo_lsn_9_value = "loc_shelf_numbers - ROW 9 - d442da62-b938-41e1-a069-0fb8bb85f340"
    theo_lsn_10_value = "loc_shelf_numbers - ROW 10 - b5f0aed6-9956-481b-a11e-ab83847884d8"

    # row values
    theo_loc_shelf_num_1_val = theo_lsn_1_value
    theo_loc_shelf_num_2_val = theo_lsn_2_value
    theo_loc_shelf_num_3_val = theo_lsn_3_value
    theo_loc_shelf_num_4_val = theo_lsn_4_value
    theo_loc_shelf_num_5_val = theo_lsn_5_value
    theo_loc_shelf_num_6_val = theo_lsn_6_value

    theo_loc_shelf_num_7_val = theo_lsn_7_value
    theo_loc_shelf_num_8_val = theo_lsn_8_value
    theo_loc_shelf_num_9_val = theo_lsn_9_value
    theo_loc_shelf_num_10_val = theo_lsn_10_value

    theo_title_1_lsn = theo_loc_shelf_num_9_val
    theo_title_1_lsn_id = 9
    theo_title_1_lsn_count = 1

    theo_title_4_lsn = theo_loc_shelf_num_6_val
    theo_title_4_lsn_id = 6
    theo_title_4_lsn_count = 1

    theo_title_5_lsn = theo_loc_shelf_num_9_val
    theo_title_5_lsn_id = 9
    theo_title_5_lsn_count = 1

    # loc_shelf_numbers-title properties
    theo_title_lsn_count_map = {
        1: 1,
        2: 1,
        3: 1,
        4: 1,
        5: 1,
        6: 0,
        7: 1,
        8: 1,
        9: 0,
        10: 1,
    }

    lsn_1_title_id_list = [
        8,
    ]
    lsn_1_title_id_set = set(lsn_1_title_id_list)

    assert len(lsn_1_title_id_set) == len(lsn_1_title_id_list)

    lsn_4_title_id_list = [7]
    lsn_4_title_id_set = set(lsn_4_title_id_list)

    assert len(lsn_4_title_id_set) == len(lsn_4_title_id_list)

    lsn_5_title_id_list = []
    lsn_5_title_id_set = set(lsn_5_title_id_list)

    assert len(lsn_5_title_id_list) == len(lsn_5_title_id_set)

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    #  - CONTENT_LEVELS

    theo_content_level_title_link_columns = [
        "content_level_title_link_id",
        "content_level_title_link_content_level_id",
        "content_level_title_link_title_id",
        "content_level_title_link_priority",
        "content_level_title_link_primary",
        "content_level_title_link_type",
        "content_level_title_link_index",
        "content_level_title_link_datestamp",
        "content_level_title_link_scratch",
    ]

    # Maps the title_id to the number of content levels
    theo_title_cl_count_map = {
        1: 1,
        2: 1,
        3: 1,
        4: 1,
        5: 1,
        6: 1,
        7: 1,
        8: 0,
        9: 1,
        10: 1,
    }
    theo_cl_title_count_dir = {
        1: 1,
        2: 1,
        3: 1,
        4: 0,
        5: 0,
        6: 2,
        7: 0,
        8: 2,
        9: 2,
        10: 0,
    }

    # content level tables
    theo_cl_1_val = "content_levels - ROW 1 - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f"
    theo_cl_2_val = "content_levels - ROW 2 - 2fa7997a-9998-47fa-85ec-3c81f6180a8b"
    theo_cl_3_val = "content_levels - ROW 3 - 78affa14-a6cb-4f88-a57e-99cb308f99f1"
    theo_cl_4_val = "content_levels - ROW 4 - f4b6557c-4278-4e02-9d5f-7b2c88daa7e0"
    theo_cl_5_val = "content_levels - ROW 5 - 4f2ab892-6a87-4d46-b1fb-a56478f84958"
    theo_cl_6_val = "content_levels - ROW 6 - fbfa7e8f-a517-4e4c-96be-5fb7acef8517"
    theo_cl_7_val = "content_levels - ROW 7 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed"
    theo_cl_8_val = "content_levels - ROW 8 - d27ef268-7a63-4dfa-8876-03a6de8f3e93"
    theo_cl_9_val = "content_levels - ROW 9 - d442da62-b938-41e1-a069-0fb8bb85f340"
    theo_cl_10_val = "content_levels - ROW 10 - b5f0aed6-9956-481b-a11e-ab83847884d8"

    theo_title_cl_type_map = {
        1: "concerning",
        2: "concerning",
        3: "weird",
        4: "unexpected",
        5: "weird",
        6: "concerning",
        7: "surprising",
        9: "weird",
        10: "surprising",
    }

    theo_title_1_cl = theo_cl_9_val
    theo_title_1_cl_count = theo_title_cl_count_map[1]
    theo_title_1_cl_id = 9

    theo_title_4_cl = theo_cl_3_val
    theo_title_4_cl_count = theo_title_cl_count_map[4]
    theo_title_4_cl_id = 3

    theo_title_5_cl = theo_cl_8_val
    theo_title_5_cl_count = theo_title_cl_count_map[5]
    theo_title_5_cl_id = 8

    # content levels
    valid_cl_types = ["concerning", "unexpected", "weird", "surprising"]

    theo_cl_1_dict = {
        "concerning": [],
        "unexpected": [],
        "weird": [
            9,
        ],
        "surprising": [],
    }
    theo_cl_1_dict_set = {k: set(v) for k, v in iteritems(theo_cl_1_dict)}

    theo_cl_4_dict = {
        "concerning": [],
        "unexpected": [],
        "weird": [],
        "surprising": [],
    }
    theo_cl_4_dict_set = {k: set(v) for k, v in iteritems(theo_cl_4_dict)}

    theo_cl_5_dict = {
        "concerning": [],
        "unexpected": [],
        "weird": [],
        "surprising": [],
    }
    theo_cl_5_dict_set = {k: set(v) for k, v in iteritems(theo_cl_5_dict)}

    # CONTENT LEVELS
    theo_content_level_book_col_map = {
        1: 9,
        2: 6,
        3: 6,
        4: 3,
        5: 8,
        6: 8,
        7: 9,
        8: None,
        9: 1,
        10: 2,
    }

    #
    # ------------------------------------------------------------------------------------------------------------------
