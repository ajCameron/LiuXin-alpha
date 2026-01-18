from LiuXin_tests.test_databases.test_db_properties.common_db_properties import (
    CommonDBProperties,
)


# Logical as it is to have some of these classes based off each other the temptation should be resisted. It leans to
# method resolution orders which are so complex that bugs are basically inevitable.
class TestDB1Properties(CommonDBProperties):
    """
    Properties for test db 1 - which are also inherited by a lot of other tables.
    """

    from LiuXin_tests.test_setup.constants import test_asset_version

    db_uuid = "test_test_db_1_{}".format(test_asset_version)

    theo_titles_table_hash = "282fbf2e169ee56635dc2874807356be"

    theo_main_table_id_col_map = {
        "titles": "title_id",
        "series": "series_id",
        "publishers": "publisher_id",
        "genres": "genre_id",
        "tags": "tag_id",
    }

    theo_link_table_id_col_map = {
        "series_title_links": "series_title_link_id",
    }

    theo_intralink_table_id_col_map = {
        "title_title_intralinks": "title_title_intralink_id",
    }

    theo_main_table_scratch_col_map = {
        "titles": "title_scratch",
        "series": "series_scratch",
        "publishers": "publisher_scratch",
        "genres": "genre_scratch",
        "tags": "tag_scratch",
    }

    theo_link_table_scratch_col_map = {
        "series_title_links": "series_title_link_scratch",
    }

    theo_intralink_table_scratch_col_map = {
        "title_title_intralinks": "title_title_intralink_scratch",
    }

    theo_main_table_datestamp_col_map = {
        "titles": "title_datestamp",
        "series": "series_datestamp",
        "publishers": "publisher_datestamp",
    }

    theo_link_table_datestamp_col_map = {
        "series_title_links": "series_title_link_datestamp",
    }

    theo_intralink_table_datestamp_col_map = {
        "title_title_intralinks": "title_title_intralink_datestamp",
    }

    theo_main_table_parent_col_map = {
        "titles": False,
        "series": "series_parent",
        "publishers": "publisher_parent",
    }

    theo_link_table_parent_col_map = {
        "series_title_links": False,
    }

    theo_intralink_table_parent_col_map = {
        "title_title_intralinks": False,
    }

    theo_main_table_display_col_map = {
        "titles": "title",
        "series": "series",
        "publishers": "publisher",
    }

    theo_link_table_display_col_map = {
        # Todo: This does not, entirely, make sense. Need to consider what to do with this
        "series_title_links": "series_title_link_index",
    }

    theo_intralink_table_display_col_map = {
        "title_title_intralinks": "title_title_intralink_type",
    }

    theo_mt_intralink_table_map = {
        "folders": False,
        "files": "file_file_intralinks",
        "genres": False,
        "custom_columns": False,
        "folder_stores": "folder_store_folder_store_intralinks",
        "covers": "cover_cover_intralinks",
        "publishers": "publisher_publisher_intralinks",
        "series": False,
        "notes": False,
        "tags": "tag_tag_intralinks",
        "devices": False,
        "languages": False,
        "last_read_positions": False,
        "books": False,
        "comments": False,
        "synopses": False,
        "titles": "title_title_intralinks",
        "feeds": False,
        "creators": "creator_creator_intralinks",
        "subjects": False,
        "identifiers": "identifier_identifier_intralinks",
    }

    theo_main_table_interlink_tables = {
        "folders": set(["files", "series", "books", "creators"]),
        "files": set(
            [
                "folders",
                "publishers",
                "identifiers",
                "devices",
                "languages",
                "books",
            ]
        ),
        "genres": set(["series", "titles"]),
        "custom_columns": set([]),
        "folder_stores": set(["notes"]),
        "covers": set(["series", "books", "creators"]),
        "publishers": set(["files", "notes", "titles"]),
        "series": set(
            [
                "folders",
                "genres",
                "tags",
                "notes",
                "covers",
                "titles",
                "comments",
                "synopses",
                "creators",
            ]
        ),
        "notes": set(
            [
                "publishers",
                "folder_stores",
                "series",
                "devices",
                "titles",
                "creators",
            ]
        ),
        "tags": set(["series", "titles", "creators"]),
        "devices": set(["files", "notes"]),
        "languages": set(["files", "titles", "creators"]),
        "last_read_positions": set([]),
        "books": set(["folders", "files", "covers"]),
        "comments": set(["series", "titles", "creators"]),
        "synopses": set(["series", "titles", "creators"]),
        "titles": set(
            [
                "publishers",
                "genres",
                "tags",
                "series",
                "notes",
                "identifiers",
                "comments",
                "languages",
                "subjects",
                "synopses",
                "creators",
            ]
        ),
        "feeds": set([]),
        "creators": set(
            [
                "folders",
                "tags",
                "series",
                "notes",
                "comments",
                "languages",
                "titles",
                "covers",
                "synopses",
            ]
        ),
        "subjects": set(["titles"]),
        "identifiers": set(["files", "titles"]),
    }

    theo_title_linked_tables = {
        "folders": False,
        "files": False,
        "genres": "genre_title_links",
        "custom_columns": False,
        "folder_stores": False,
        "covers": False,
        "publishers": "publisher_title_links",
        "series": "series_title_links",
        "notes": "note_title_links",
        "tags": "tag_title_links",
        "devices": False,
        "languages": "language_title_links",
        "last_read_positions": False,
        "books": False,
        "comments": "comment_title_links",
        "synopses": "synopsis_title_links",
        "titles": "title_title_intralinks",
        "feeds": False,
        "creators": "creator_title_links",
        "subjects": "subject_title_links",
        "identifiers": "identifier_title_links",
    }

    theo_main_table_intralinks = {
        "folders": False,
        "files": "file_file_intralinks",
        "genres": False,
        "custom_columns": False,
        "folder_stores": "folder_store_folder_store_intralinks",
        "covers": "cover_cover_intralinks",
        "publishers": "publisher_publisher_intralinks",
        "series": False,
        "notes": False,
        "tags": "tag_tag_intralinks",
        "devices": False,
        "languages": False,
        "last_read_positions": False,
        "books": False,
        "comments": False,
        "synopses": False,
        "titles": "title_title_intralinks",
        "feeds": False,
        "creators": "creator_creator_intralinks",
        "subjects": False,
        "identifiers": "identifier_identifier_intralinks",
    }

    title_record_count = 97
    series_record_count = 34

    title_title_intralinks_record_count = 5

    # Todo: Group commkon prpoertie sinto a common properties class
    existing_triggers = [
        "update_callback_on_titles",
        "block_insert_on_database_version_table",
        "block_update_on_database_version_table",
        "block_delete_on_database_version_table",
    ]

    all_titles = [
        "Cryptonomicon",
        "The Diamond Age",
        "Snow Crash",
        "Anathem",
        "Quicksilver",
        "The Confusion",
        "The System of the World",
        "The Long Utopia",
        "The Martian",
        "The Republic of Thieves",
        "The Shepherd's Crown",
        "What If?",
        "In the beginning was the command line",
        "With This Ring",
        "Titan",
        "The Time Ships",
        "Proxima",
        "Ultima",
        "Coalescent",
        "Exultant",
        "Transcendent",
        "Resplendent",
        "Voyage",
        "Titan",
        "Moonseed",
        "The Long Earth",
        "The Long War",
        "The Long Mars",
        "Anti-Ice",
        "Wheel of Ice",
        "Pandora's Star",
        "Judas Unchained",
        "The Dreaming Void",
        "The Evolutionary Void",
        "The Temporal Void",
        "Diggers",
        "Truckers",
        "Wings",
        "The Colour of Magic",
        "The Light Fantastic",
        "Eric!",
        "The Amazing Maurice and His educated rodents",
        "Sorcery",
        "Moving Pictures",
        "Reaper Man",
        "Witches Abroad",
        "Raising Steam",
        "Making Money",
        "Going Postal",
        "Thud!",
        "Journey to the centre of the earth",
        "A second chance at eden",
        "The Abyss Beyond Dreams",
        "The Night Without Stars",
        "The Eyre Affair",
        "Shades of Grey",
        "Going Postal",
        "The Eye of Zoltar",
        "The Woman who died a lot",
        "The last Dragonslayer",
        "One of our thursdays is missing",
        "The fourth bear",
        "The Hanging Tree",
        "Rivers of London",
        "Foxglove Summer",
        "Broken Homes",
        "Whispers Underground",
        "A People's History of the United States",
        "Zero Day",
        "Trojan Horse",
        "The Man who would be King",
        "Ilium",
        "Olympos",
        "Summer of Night",
        "The Void Trilogy",
        "Liars and Outliers",
        "How to create a mind",
        "Flashman at the Charge",
        "Flashman and the Mountain of Light",
        "Alice in Wonderland",
        "The Hobbit",
        "The Lord of the Rings",
        "The Fellowship of the Ring",
        "The Two Towers",
        "The Return of the King",
        "Watchmen",
        "V For Vendetta",
        "Truman",
        "Poseidon's Wake",
        "Ancillary Justice",
        "A Natural History of Dragons",
        "Cloud Atlas",
        "The Stars My Destination",
        "Tiger! Tiger!",
        "How Much For Just The Planet?",
        "Beyond the Stars: A Planet Too Far: a space opera anthology",
        "Venatoris",
    ]

    theo_highest_title_id = 97
    theo_highest_title_title_intralink_id = 5

    all_series = [
        "Bromeliad",
        "Discworld",
        "Science of the Disc",
        "The Baroque Cycle",
        "The Long Earth",
        "Locke Lamorra",
        "Tiffany Aching",
        "The Farthest Stars",
        "Xeelee Sequence",
        "Destiny's Children",
        "The Nasa Trilogy",
        "The Commonwealth",
        "The Starflyer War",
        "The Dreams Duoloogy",
        "The Void Trilogy",
        "Flashman",
        "Middle Earth",
        "Rincewind",
        "The Witches of Lancre",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "Star Trek",
    ]

    creators_theo_book_col_map = {
        1: {"authors": set([1])},
        2: {"authors": set([1])},
        3: {"authors": set([1])},
        4: {"authors": set([1])},
        5: {"authors": set([1])},
        6: {"authors": set([1])},
        7: {"authors": set([1])},
        8: {"authors": set([2, 3])},
        9: {"authors": set([4])},
        10: {"authors": set([5])},
        11: {"authors": set([2])},
        12: {"authors": set([6])},
        13: {"authors": set([1])},
        14: {"authors": set([7])},
        15: {"authors": set([3])},
        16: {"authors": set([3])},
        17: {"authors": set([3])},
        18: {"authors": set([3])},
        19: {"authors": set([3])},
        20: {"authors": set([3])},
        21: {"authors": set([3])},
        22: {"authors": set([3])},
        23: {"authors": set([3])},
        24: {"authors": set([3])},
        25: {"authors": set([3])},
        26: {"authors": set([2, 3])},
        27: {"authors": set([2, 3])},
        28: {None: set([3]), "authors": set([2])},
        29: {"authors": set([3])},
        30: {"authors": set([3])},
        31: {"authors": set([8])},
        32: {"authors": set([8])},
        33: {"authors": set([8])},
        34: {"authors": set([8])},
        35: {"authors": set([8])},
        36: {"authors": set([2])},
        37: {"authors": set([2])},
        38: {"authors": set([2])},
        39: {"authors": set([2])},
        40: {"authors": set([2])},
        41: {"authors": set([2])},
        42: {"authors": set([2])},
        43: {"authors": set([2])},
        44: {"authors": set([2])},
        45: {"authors": set([2])},
        46: {"authors": set([2])},
        47: {"authors": set([2])},
        48: {"authors": set([2])},
        49: {"authors": set([2])},
        50: {"authors": set([2])},
        51: {"authors": set([9])},
        52: {"authors": set([8])},
        53: {"authors": set([8])},
        54: {"authors": set([8])},
        55: {"authors": set([10])},
        56: {"authors": set([10])},
        57: {"authors": set([2])},
        58: {"authors": set([10])},
        59: {"authors": set([10])},
        60: {"authors": set([10])},
        61: {"authors": set([10])},
        62: {"authors": set([10])},
        63: {"authors": set([11])},
        64: {"authors": set([11])},
        65: {"authors": set([11])},
        66: {"authors": set([11])},
        67: {"authors": set([11])},
        68: {"authors": set([12])},
        69: {"authors": set([13])},
        70: {"authors": set([13])},
        71: {"authors": set([14])},
        72: {"authors": set([15])},
        73: {"authors": set([15])},
        74: {"authors": set([15])},
        75: {"authors": set([8])},
        76: {"authors": set([16])},
        77: {"authors": set([17])},
        78: {"authors": set([18])},
        79: {"authors": set([18])},
        80: {"authors": set([19])},
        81: {"authors": set([20])},
        82: {"authors": set([20])},
        83: {"authors": set([20])},
        84: {"authors": set([20])},
        85: {"authors": set([20])},
        86: {"authors": set([21]), "colorists": set([23]), "artists": set([22])},
        87: {"authors": set([21])},
        88: {"authors": set([24])},
        89: {"authors": set([25])},
        90: {"authors": set([26])},
        91: {"authors": set([27])},
        92: {"authors": set([28])},
        93: {"authors": set([29])},
        94: {},
        95: {},
        96: {},
        97: {},
    }

    theo_creator_types = set([None, "artists", "colorists", "authors"])

    link_tables = {
        "creator_title_links",
        "rating_title_links",
        "comment_title_links",
        "language_title_links",
        "book_file_links",
        "file_identifier_links",
        "cover_creator_links",
        "synopsis_title_links",
        "note_series_links",
        "publisher_title_links",
        "creator_series_links",
        "device_note_links",
        "folder_store_note_links",
        "genre_title_links",
        "series_tag_links",
        "book_folder_links",
        "creator_folder_links",
        "note_title_links",
        "folder_series_links",
        "tag_title_links",
        "identifier_title_links",
        "series_title_links",
        "comment_creator_links",
        "genre_series_links",
        "subject_title_links",
        "cover_series_links",
        "creator_tag_links",
        "file_folder_links",
        "note_publisher_links",
        "file_publisher_links",
        "file_language_links",
        "comment_series_links",
        "device_file_links",
        "creator_language_links",
        "creator_note_links",
        "series_synopsis_links",
        "book_cover_links",
        "creator_synopsis_links",
    }

    title_row_2_complete = {
        "title_scratch": None,
        "title_created_datestamp": "2022-05-23 11:28:54",
        "title_datestamp": 1465608939,
        "title_pub_date": None,
        "title_creator_sort": None,
        "title_id": 2,
        "title_sort": None,
        "title_copyright_date": None,
        "title_wikipedia": None,
        "title": "The Diamond Age",
        "title_source": None,
        "title_source_name": None,
        "title_phash": None,
        "title_last_modified": "2022-05-23 11:28:54",
        "title_type": None,
        "title_fiction_length_category": None,
        "title_source_path": None,
        "title_wordcount": None,
    }

    title_1_meta_uuid = "01aa06c6-da59-430f-b039-a9a38808f97416.775476"

    title_1_meta_datestamp = "2016-06-11 01:35:59"

    title_1_meta_last_modified = "2022-05-23 11:28:54"

    title_1_meta_author = "Neal Stephenson"
