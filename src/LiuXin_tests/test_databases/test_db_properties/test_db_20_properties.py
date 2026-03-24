
"""
Properties for test db 20.
"""


from tests.support.test_databases.test_db_properties.common_db_properties import (
    CommonDBProperties,
)


from LiuXin_alpha.utils.libraries.liuxin_six import iteritems


class TestDB20Properties(CommonDBProperties):
    """
    Properties for the test_db_20 test database.
    """

    theo_main_tables = {
        "folders",
        "files",
        "genres",
        "custom_columns",
        "folder_stores",
        "covers",
        "publishers",
        "series",
        "notes",
        "tags",
        "devices",
        "languages",
        "last_read_positions",
        "books",
        "comments",
        "synopses",
        "titles",
        "feeds",
        "creators",
        "subjects",
        "identifiers",
    }

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - IDENTIFIERS

    theo_identifiers_count = 177
    theo_ids_titles_link_count = theo_identifiers_count

    seen_ids_type = {
        "douban",
        "doi",
        "isbn",
        "uuid",
        "lccn",
        "issn",
        "uri",
        "amazon",
        "google",
        "oclc",
        "ff",
        "goodreads",
    }

    theo_title_identifiers_map = {
        2: {
            "douban": set([36, 39, 9, 44, 18, 21, 22]),
            "google": set([8, 45]),
            "isbn": set([2, 27]),
            "uuid": set([4, 12, 29, 30, 23]),
            "lccn": set([11]),
            "issn": set([37, 40, 16, 17, 19, 26]),
            "uri": set([5, 42, 13, 46, 28, 10]),
            "amazon": set([41, 34, 25, 1, 33]),
            "doi": set([24, 3, 35, 7]),
            "oclc": set([32, 43, 14, 15]),
            "ff": set([6, 38, 31]),
            "goodreads": set([20]),
        },
        3: {
            "oclc": set([47]),
            "goodreads": set([51]),
            "isbn": set([48]),
            "uuid": set([49, 52]),
            "uri": set([50]),
        },
        4: {
            "douban": set([58, 54]),
            "google": set([60]),
            "isbn": set([56]),
            "uuid": set([53]),
            "issn": set([57, 61]),
            "oclc": set([59]),
            "goodreads": set([55]),
        },
        5: {
            "douban": set([62]),
            "amazon": set([65]),
            "doi": set([63]),
            "uri": set([64]),
            "oclc": set([66]),
        },
        6: {
            "douban": set([67, 70]),
            "goodreads": set([69]),
            "uuid": set([71]),
            "oclc": set([72, 68]),
        },
        7: {
            "douban": set([88, 81, 78]),
            "google": set([104, 73, 74]),
            "uuid": set([80, 98, 103]),
            "lccn": set([97, 82, 92, 79]),
            "issn": set([90]),
            "uri": set([76, 84, 101]),
            "amazon": set([96, 99, 93]),
            "doi": set([100, 107, 83, 85, 86, 94]),
            "oclc": set([75, 91]),
            "ff": set([105, 106, 95]),
            "goodreads": set([89, 77, 102, 87]),
        },
        8: {"doi": set([108])},
        9: {
            "douban": set([129, 130, 119, 114, 121]),
            "google": set([128, 120, 132, 116]),
            "isbn": set([112, 146]),
            "uuid": set([137, 140, 142, 144, 124, 127]),
            "lccn": set([145, 139]),
            "issn": set([125]),
            "uri": set([133, 110]),
            "amazon": set([111]),
            "doi": set([136, 115]),
            "oclc": set([122, 117, 143]),
            "ff": set([135, 109, 141, 113, 118, 126]),
            "goodreads": set([123, 138, 131, 134]),
        },
        10: {
            "douban": set([162, 171]),
            "google": set([156]),
            "isbn": set([155, 149]),
            "uuid": set([160, 151]),
            "lccn": set([164, 165, 174]),
            "issn": set([166, 159]),
            "uri": set([167, 175]),
            "amazon": set([161, 154, 172]),
            "doi": set([168, 170, 173, 177, 148, 153]),
            "oclc": set([163, 150]),
            "ff": set([176, 169]),
            "goodreads": set([152, 147, 157, 158]),
        },
    }

    title_1_isbns = None
    title_1_isbn_count = 0

    title_2_isbns = {
        "TEST EXTERNAL IDENTIFIER - TYPE isbn - TITLE 2 - ID NUM 2 - b753a41a-5dbf-482d-b56d-20adc95cf71c",
        "TEST EXTERNAL IDENTIFIER - TYPE isbn - TITLE 2 - ID NUM 27 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
    }
    title_2_isbn_count = len(title_2_isbns)

    title_5_isbns = None
    title_5_isbn_count = 0

    theo_title_isbns_map = {
        9: set(
            [
                "TEST EXTERNAL IDENTIFIER - TYPE isbn - TITLE 9 - ID NUM 4 - e55f7696-d9d7-4127-b8b3-163d9c90df8d",
                "TEST EXTERNAL IDENTIFIER - TYPE isbn - TITLE 9 - ID NUM 38 - 79085d34-1849-4acf-802e-5580ad1c86bb",
            ]
        ),
        2: set(
            [
                "TEST EXTERNAL IDENTIFIER - TYPE isbn - TITLE 2 - ID NUM 2 - b753a41a-5dbf-482d-b56d-20adc95cf71c",
                "TEST EXTERNAL IDENTIFIER - TYPE isbn - TITLE 2 - ID NUM 27 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
            ]
        ),
        3: set(["TEST EXTERNAL IDENTIFIER - TYPE isbn - TITLE 3 - ID NUM 2 - e08fb6ba-3808-4895-81e2-a9638dc29cee"]),
        4: set(["TEST EXTERNAL IDENTIFIER - TYPE isbn - TITLE 4 - ID NUM 4 - 4f2ab892-6a87-4d46-b1fb-a56478f84958"]),
        10: set(
            [
                "TEST EXTERNAL IDENTIFIER - TYPE isbn - TITLE 10 - ID NUM 9 - 08bc4130-9b0d-4e65-8a08-e7adc416c84b",
                "TEST EXTERNAL IDENTIFIER - TYPE isbn - TITLE 10 - ID NUM 3 - e58e43b6-1488-4e47-8107-32c37d8f45e9",
            ]
        ),
    }

    theo_books_with_isbn = set(theo_title_isbns_map.keys())
