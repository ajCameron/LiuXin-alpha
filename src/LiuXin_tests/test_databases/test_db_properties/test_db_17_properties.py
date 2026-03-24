
"""
Properties for the DB 17 test databas.
"""


from LiuXin_alpha.utils.libraries.liuxin_six import iteritems

from tests.support.test_databases.test_db_properties.common_db_properties import (
    CommonDBProperties,
)


class TestDB17Properties(CommonDBProperties):
    """
    Properties for the test_db_17 test database.
    """

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - MAIN TABLE PROPERTIES

    theo_note_count = 339
    theo_creator_count = 101
    theo_title_count = 10
    publisher_record_count = 723

    theo_comment_count = 288
    theo_comment_title_count = 28

    theo_identifiers_count = 25
    theo_language_count = 10
    theo_tag_title_link_count = 21

    # Should be equal to series_title_highest_id
    theo_series_count = 690
    theo_series_title_link_id_max = 25

    theo_rating_count = 11
    theo_rating_title_link_count = 21

    series_tag_link_table_record_count = 25

    theo_main_tables = {
        "files",
        "publishers",
        "genres",
        "custom_columns",
        "folder_stores",
        "covers",
        "tags",
        "series",
        "notes",
        "identifiers",
        "devices",
        "folders",
        "languages",
        "last_read_positions",
        "books",
        "comments",
        "synopses",
        "titles",
        "feeds",
        "creators",
        "subjects",
    }

    # Todo: This should not be here - it's not a property of the database
    meta_keys = {
        "title_tags",
        "rating",
        "isbn",
        "pubdate",
        "synopsis_id",
        "series",
        "series_tags",
        "creator_tags",
        "main_tags",
        "id",
        "size",
        "uuid",
        "title",
        "wikipedia",
        "comments",
        "note",
        "max_size",
        "has_cover",
        "publisher_id",
        "sort",
        "publishers",
        "note_id",
        "tags",
        "timestamp",
        "comment_id",
        "series_id",
        "last_modified",
        "authors",
        "genre",
        "path",
        "min_rating",
        "author_ids",
        "publisher",
        "series_index",
        "max_rating",
        "language",
        "lccn",
        "type",
        "author_sort",
        "synopsis",
        "flags",
        "formats",
        "genre_id",
    }

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - RATINGS

    title_1_user_ratings = None
    title_with_user_ratings_ids = [8, 9]
    theo_title_with_user_count = len(title_with_user_ratings_ids)
    theo_title_with_user_rating = title_with_user_ratings_ids[0]

    title_1_calibre_ratings = 2
    title_with_calibre_ratings_ids = [1, 2, 5]
    theo_title_with_calibre_count = len(title_with_calibre_ratings_ids)
    theo_title_with_calibre_rating = title_with_calibre_ratings_ids[0]

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TITLES

    # titles properties
    all_titles = [
        "t-1-f85be9c8",
        "t-2-4f266348",
        "t-3-c187b301",
        "t-4-fd5ae8b5",
        "t-5-1d0e6376",
        "t-6-0d4d18dd",
        "t-7-ae50b90e",
        "t-8-078c31b9",
        "t-9-b753a41a",
        "t-10-3939f972",
    ]
    titles_highest_id = len(all_titles)
    title_1_title = all_titles[0]
    theo_title_1_creator_sort = "TEST TITLE CREATOR SORT - 1 - DELETE ME - 5331f31b-dca6-4c50-9162-4444663c2728"

    title_primary_intralink_ids = {1, 2, 5, 7, 9, 10}
    title_secondary_intralink_ids = {8, 9, 2, 3, 4}

    title_title_highest_id = 6
    title_title_record_count = title_title_highest_id

    # This is a field store in the titles table - hence why it's here
    creator_title_sort_map = {
        1: "TEST TITLE CREATOR SORT - 1 - DELETE ME - 5331f31b-dca6-4c50-9162-4444663c2728",
        2: "TEST TITLE CREATOR SORT - 2 - DELETE ME - 03210848-24c6-42ac-b918-74aba63e40f3",
        3: "TEST TITLE CREATOR SORT - 3 - DELETE ME - 3939f972-fa38-45e1-9f3f-be69a8618ee2",
        4: "TEST TITLE CREATOR SORT - 4 - DELETE ME - afaa77d3-ce17-4bc8-806f-4b2f1b278473",
        5: "TEST TITLE CREATOR SORT - 5 - DELETE ME - 54e78ee6-a8f0-4d60-869f-86fc15dfc181",
        6: "TEST TITLE CREATOR SORT - 6 - DELETE ME - 4324fdc0-8cb1-4aa7-bb25-1714dd39cdca",
        7: "TEST TITLE CREATOR SORT - 7 - DELETE ME - 49178b1c-8c0e-46c9-a739-42a7c26271b5",
        8: "TEST TITLE CREATOR SORT - 8 - DELETE ME - 93acce3a-7b3c-4afb-b717-b63150537fa6",
        9: "TEST TITLE CREATOR SORT - 9 - DELETE ME - 43024a39-3cd9-4ad8-8b1b-c2d79f900c4b",
        10: "TEST TITLE CREATOR SORT - 10 - DELETE ME - a471e7c5-6a0f-415c-81b5-d69b1997b365",
    }

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - SERIES

    theo_series_field_book_col_map = {
        1: [349],
        2: [114, 643],
        3: [647, 577, 89, 248, 604],
        4: [37, 239],
        5: [456, 525, 197],
        6: [523, 542, 169, 372],
        7: [283, 447],
        8: [],
        9: [444],
        10: [458, 133, 142, 528, 111],
    }

    all_series = [
        "s-1-25233cf6",
        "s-2-3542b654",
        "s-3-fdddde7e",
        "s-4-efda05b4",
        "s-5-650530d5",
        "s-6-c187b301",
        "s-7-d759e0bc",
        "s-8-c47407dd",
        "s-9-ab2f7513",
        "s-10-9b88b308",
        "TEST series - IN TREE - series ID 11 - 268b04fe-92df-4e2e-b23b-ce5b74dbafc2",
        "TEST series - IN TREE - series ID 12 - f8f24390-b627-423a-97a1-aa980159df44",
        "TEST series - IN TREE - series ID 13 - d73b1842-5312-4ddc-9277-da8e378bb8ab",
        "TEST series - IN TREE - series ID 14 - 1a169e8f-c7a3-45f4-8ea6-b3c5ee25d0e3",
        "TEST series - IN TREE - series ID 15 - 6e61e176-d873-4bef-a3f5-0eb41e20328d",
        "TEST series - IN TREE - series ID 16 - 7bd49e8c-2c72-4c12-98ce-84e57c71a634",
        "TEST series - IN TREE - series ID 17 - 0d4aa6b0-9c1c-4a14-a661-66838257faad",
        "TEST series - IN TREE - series ID 18 - 3939f972-fa38-45e1-9f3f-be69a8618ee2",
        "TEST series - IN TREE - series ID 19 - fc21000c-e59c-44f4-9624-022a7658da42",
        "TEST series - IN TREE - series ID 20 - fd1c0506-3859-4f96-b42a-1fe2f99e0b1e",
        "TEST series - IN TREE - series ID 21 - ee292414-a0f9-4400-b987-75669a211ca9",
        "TEST series - IN TREE - series ID 22 - ae50b90e-fbba-492b-bfbb-edfde40520b1",
        "TEST series - IN TREE - series ID 23 - c47407dd-bd9e-478b-adfa-0585e8dee677",
        "TEST series - IN TREE - series ID 24 - c282ee09-9acc-40be-b540-ab3613e6e818",
        "TEST series - IN TREE - series ID 25 - 8229a8b9-9ed9-43b5-8bb7-1b7bc64b9127",
        "TEST series - IN TREE - series ID 26 - 1d30e9e6-3ed4-4897-93e9-0b2929ff27f7",
        "TEST series - IN TREE - series ID 27 - 02c1e800-e4b1-41ea-9c03-9f1e945725f3",
        "TEST series - IN TREE - series ID 28 - 546667c5-de19-4c85-9d79-14ee90d9188a",
        "TEST series - IN TREE - series ID 29 - ea24b712-4bcb-4f21-b7ce-57db9c919e33",
        "TEST series - IN TREE - series ID 30 - d1f9c688-3046-4474-a99b-5d7bf1159101",
        "TEST series - IN TREE - series ID 31 - 5e3c2ba7-f3f0-46fe-a2b6-9a3162506d70",
        "TEST series - IN TREE - series ID 32 - 6e7e2e87-5443-40f6-aef6-552ee1ab5531",
        "TEST series - IN TREE - series ID 33 - eda46b9e-b66e-49bd-9458-68e86cc0d3d1",
        "TEST series - IN TREE - series ID 34 - 4bce79fc-83a2-43fb-8505-ef9a415bb35f",
        "TEST series - IN TREE - series ID 35 - fdddde7e-5574-478c-9608-4640e45f3ec6",
        "TEST series - IN TREE - series ID 36 - 28ef047e-f466-4721-b71f-dfb858e0b34a",
        "TEST series - IN TREE - series ID 37 - 60c2ab8f-ced7-4f8d-a977-9f2fb4be941c",
        "TEST series - IN TREE - series ID 38 - c31ad47d-6f6d-46ec-bab8-96c8945056bd",
        "TEST series - IN TREE - series ID 39 - fbfa7e8f-a517-4e4c-96be-5fb7acef8517",
        "TEST series - IN TREE - series ID 40 - c003f5f9-0394-48fc-afb3-88d78b9534dd",
        "TEST series - IN TREE - series ID 41 - 49178b1c-8c0e-46c9-a739-42a7c26271b5",
        "TEST series - IN TREE - series ID 42 - 39ce9a58-ea08-4f2c-994a-b49d61e9393d",
        "TEST series - IN TREE - series ID 43 - afaa77d3-ce17-4bc8-806f-4b2f1b278473",
        "TEST series - IN TREE - series ID 44 - 4da1e0c5-813a-4a01-8ef6-721d35650ce7",
        "TEST series - IN TREE - series ID 45 - 090a79e4-b4cf-40fa-9163-36773b820b92",
        "TEST series - IN TREE - series ID 46 - d759e0bc-426c-4831-80b2-4bf9460f5cf3",
        "TEST series - IN TREE - series ID 47 - 146ab80f-261f-44b3-a25a-85379e76abe9",
        "TEST series - IN TREE - series ID 48 - 49458593-07f1-48f6-834b-fbafdfab119d",
        "TEST series - IN TREE - series ID 49 - 52ee46ad-4981-44e1-98af-cf9379e9de5c",
        "TEST series - IN TREE - series ID 50 - c5c0e3b8-7f0a-4925-a326-cc5bf2c4ecd6",
        "TEST series - IN TREE - series ID 51 - 55323a3a-bacf-4056-b5f5-e6f540b92d05",
        "TEST series - IN TREE - series ID 52 - 96ebca6d-fd03-4449-995b-210a0fccf6a3",
        "TEST series - IN TREE - series ID 53 - f4217e2f-2e03-455d-bfbf-ed5964ae7ac0",
        "TEST series - IN TREE - series ID 54 - fd5ae8b5-3833-4466-ac34-2efb7dc13bc4",
        "TEST series - IN TREE - series ID 55 - 8cf64bff-18a0-4795-b44a-8ac350c5afa5",
        "TEST series - IN TREE - series ID 56 - c6bc06b8-530c-44d0-9acf-a5cc20b3a221",
        "TEST series - IN TREE - series ID 57 - 0f2d241b-1c38-48a3-9bce-49c99c47081e",
        "TEST series - IN TREE - series ID 58 - ef74e5a1-bc3a-465d-9d9b-db985dbe8b0c",
        "TEST series - IN TREE - series ID 59 - aec6fe91-2a11-4608-af38-a01d0e7cab1d",
        "TEST series - IN TREE - series ID 60 - f85be9c8-af5a-41d6-a47a-77baf31c5308",
        "TEST series - IN TREE - series ID 61 - 67013d5f-d85d-4308-a208-a5162afc51ab",
        "TEST series - IN TREE - series ID 62 - f4b6557c-4278-4e02-9d5f-7b2c88daa7e0",
        "TEST series - IN TREE - series ID 63 - 9b88b308-40de-4ed1-ab51-b7762b950e49",
        "TEST series - IN TREE - series ID 64 - 01627073-b7f5-4602-819a-b85ba07394ee",
        "TEST series - IN TREE - series ID 65 - 3dd78a6f-ff02-4916-a795-f35b7b86e84c",
        "TEST series - IN TREE - series ID 66 - 83c6871c-1ac1-4cec-9519-d6c5ad761b3b",
        "TEST series - IN TREE - series ID 67 - 3f2fa934-e8cb-4635-8619-7afe9b5a6cd5",
        "TEST series - IN TREE - series ID 68 - 184852b7-f79d-4cb3-acff-4a0597bd2f23",
        "TEST series - IN TREE - series ID 69 - d1803224-93e4-46ae-8756-bbe42796360e",
        "TEST series - IN TREE - series ID 70 - 65851d79-e6d6-4a89-9780-55b118cf0858",
        "TEST series - IN TREE - series ID 71 - 4f2ab892-6a87-4d46-b1fb-a56478f84958",
        "TEST series - IN TREE - series ID 72 - 6e2a8e93-b7ad-4673-bc9e-589294004c38",
        "TEST series - IN TREE - series ID 73 - 669ba846-6730-4fc8-9299-eb1a5c3c082c",
        "TEST series - IN TREE - series ID 74 - ba565492-f0f0-46c3-b50d-59fefdf04aca",
        "TEST series - IN TREE - series ID 75 - d6c55d6e-41ce-47d6-ac02-6311716cef04",
        "TEST series - IN TREE - series ID 76 - 79085d34-1849-4acf-802e-5580ad1c86bb",
        "TEST series - IN TREE - series ID 77 - 0c4ffde3-a02f-4461-a089-c72979297354",
        "TEST series - IN TREE - series ID 78 - e58e43b6-1488-4e47-8107-32c37d8f45e9",
        "TEST series - IN TREE - series ID 79 - add9e315-3502-4f4b-95f7-0ad22e5592e4",
        "TEST series - IN TREE - series ID 80 - 7b3e4793-3302-4af2-bd2d-9d903258d151",
        "TEST series - IN TREE - series ID 81 - 862d3f42-f2de-4063-ad83-603da1b68b6b",
        "TEST series - IN TREE - series ID 82 - d442da62-b938-41e1-a069-0fb8bb85f340",
        "TEST series - IN TREE - series ID 83 - efda05b4-aee0-4dcf-9eb6-463c2bbee461",
        "TEST series - IN TREE - series ID 84 - 8567479b-4856-44b3-bf9e-6ebd73476942",
        "TEST series - IN TREE - series ID 85 - e242db80-72b7-41d6-9fa4-d86b61e72318",
        "TEST series - IN TREE - series ID 86 - 30b7d9de-990b-4294-b2a5-2d6855c77b23",
        "TEST series - IN TREE - series ID 87 - 54e78ee6-a8f0-4d60-869f-86fc15dfc181",
        "TEST series - IN TREE - series ID 88 - a46b35bb-6de6-438f-b946-6c95a4b9fb0c",
        "TEST series - IN TREE - series ID 89 - 8327f6d6-7c16-4e27-87d7-453f66dacab3",
        "TEST series - IN TREE - series ID 90 - 0da80909-5fd7-4240-9fe9-bf48686dc11e",
        "TEST series - IN TREE - series ID 91 - a3d0b436-b5c2-4c95-848e-8fcfac8e8afa",
        "TEST series - IN TREE - series ID 92 - 49b21b3f-83fa-4640-af1d-ec8aabf55331",
        "TEST series - IN TREE - series ID 93 - 77211d71-f8a7-411b-ac58-19af6f4ae350",
        "TEST series - IN TREE - series ID 94 - b5f0aed6-9956-481b-a11e-ab83847884d8",
        "TEST series - IN TREE - series ID 95 - a96c4b26-4c7f-4b43-ac8b-f77b9fce7c55",
        "TEST series - IN TREE - series ID 96 - 849b8e8d-95a4-4018-9510-45fed629ee65",
        "TEST series - IN TREE - series ID 97 - b2dc4d71-e66d-4431-9266-2a4bc7b0bb7f",
        "TEST series - IN TREE - series ID 98 - 5211863b-3764-4ed2-b938-40ec0abc7b44",
        "TEST series - IN TREE - series ID 99 - 9ba48058-819a-46fe-be1b-c8e5a81203b1",
        "TEST series - IN TREE - series ID 100 - 562c6380-4c1f-413a-8cfa-612c7d9119ab",
        "TEST series - IN TREE - series ID 101 - f576fc20-e058-4859-a027-3c586d8e43c2",
        "TEST series - IN TREE - series ID 102 - 2f5ade60-4c28-47a2-82f9-a71ab5e05158",
        "TEST series - IN TREE - series ID 103 - 0cdf212c-724a-4666-ba52-8f8b4860f0f3",
        "TEST series - IN TREE - series ID 104 - 4a8dcea2-7a07-4dbd-a2e6-339e1418b0ec",
        "TEST series - IN TREE - series ID 105 - 68042e03-d2f2-41a0-b08e-645123c12597",
        "TEST series - IN TREE - series ID 106 - 8d705893-e596-4e92-a839-4dffe4373177",
        "TEST series - IN TREE - series ID 107 - c187b301-de7e-4f91-94dc-57cab3996a95",
        "TEST series - IN TREE - series ID 108 - 03210848-24c6-42ac-b918-74aba63e40f3",
        "TEST series - IN TREE - series ID 109 - e82c3c35-faae-40b2-a4a6-d31b4b5a2719",
        "TEST series - IN TREE - series ID 110 - a471e7c5-6a0f-415c-81b5-d69b1997b365",
        "TEST series - IN TREE - series ID 111 - 1d0e6376-0cb5-43b8-b909-0e74d53805fa",
        "TEST series - IN TREE - series ID 112 - e5c7c4b6-b38c-47d0-9c25-f6620896a795",
        "TEST series - IN TREE - series ID 113 - 56f272d2-bd4e-447d-8a3d-171357a5f6e2",
        "TEST series - IN TREE - series ID 114 - 29f18462-21ec-4886-9c1d-18e8dbeb54e8",
        "TEST series - IN TREE - series ID 115 - 29686bc4-0955-4d4b-8717-c225be9f14db",
        "TEST series - IN TREE - series ID 116 - d76513c3-8c70-422c-9fc2-793ad6b03180",
        "TEST series - IN TREE - series ID 117 - 585a2004-1ec7-458d-84c6-e84807aba9b2",
        "TEST series - IN TREE - series ID 118 - 5ec2291a-111e-4e61-b177-5dbd744007be",
        "TEST series - IN TREE - series ID 119 - c7a73b65-4115-45fd-adf2-7dab4327dc34",
        "TEST series - IN TREE - series ID 120 - c9e23001-d802-4f9b-91e1-cda7bb685c62",
        "TEST series - IN TREE - series ID 121 - 94d9c81a-9f6a-4669-9e65-78db57bab3a6",
        "TEST series - IN TREE - series ID 122 - f4fdb004-c2af-442e-8f47-7f353a522ef8",
        "TEST series - IN TREE - series ID 123 - 44e87e9e-d716-4e10-89ba-db03682f20ed",
        "TEST series - IN TREE - series ID 124 - 0ea3cee6-359e-4c61-b414-461d61057b3d",
        "TEST series - IN TREE - series ID 125 - 1c69f042-1254-4c69-b804-c9b03be73ef7",
        "TEST series - IN TREE - series ID 126 - 01afd8a1-813e-4c2b-b8a2-b76b1c3067b1",
        "TEST series - IN TREE - series ID 127 - ab2f7513-926d-4184-a03a-5534b59e62fd",
        "TEST series - IN TREE - series ID 128 - dec025b3-cf1d-4039-8a93-fd3fc51e416d",
        "TEST series - IN TREE - series ID 129 - 4622c89f-0e05-4ef0-9b18-1319209ee674",
        "TEST series - IN TREE - series ID 130 - 25233cf6-1e45-4ce6-a96c-297db220521c",
        "TEST series - IN TREE - series ID 131 - 5331f31b-dca6-4c50-9162-4444663c2728",
        "TEST series - IN TREE - series ID 132 - 56dd1b6a-0342-453b-b779-e12492319aa9",
        "TEST series - IN TREE - series ID 133 - 8cb78344-d95e-4166-aa5a-5029c68a1f40",
        "TEST series - IN TREE - series ID 134 - 3542b654-e2d8-48a2-aaa3-7882aa50e259",
        "TEST series - IN TREE - series ID 135 - 4324fdc0-8cb1-4aa7-bb25-1714dd39cdca",
        "TEST series - IN TREE - series ID 136 - 92472632-d0b9-4497-a5f1-852a8a168b22",
        "TEST series - IN TREE - series ID 137 - 0d4d18dd-7cb7-423b-b2d9-8845b2eed393",
        "TEST series - IN TREE - series ID 138 - d926b73e-bace-4d92-9406-a38e4b5de7b7",
        "TEST series - IN TREE - series ID 139 - d27ef268-7a63-4dfa-8876-03a6de8f3e93",
        "TEST series - IN TREE - series ID 140 - da788a67-4603-4b71-8ceb-55d3b48ac79e",
        "TEST series - IN TREE - series ID 141 - 08bc4130-9b0d-4e65-8a08-e7adc416c84b",
        "TEST series - IN TREE - series ID 142 - 2fa7997a-9998-47fa-85ec-3c81f6180a8b",
        "TEST series - IN TREE - series ID 143 - 9b67e869-0ab7-4267-a5d3-3841d8b7145d",
        "TEST series - IN TREE - series ID 144 - 3cce4d59-b8a0-4f83-9b59-9dd3500a8a8c",
        "TEST series - IN TREE - series ID 145 - 635c7cf4-4a46-402d-b219-af0479b5aa3e",
        "TEST series - IN TREE - series ID 146 - 93acce3a-7b3c-4afb-b717-b63150537fa6",
        "TEST series - IN TREE - series ID 147 - 69b09912-0efb-4c72-8914-cbbd2e4a9bdd",
        "TEST series - IN TREE - series ID 148 - 83a0633c-042d-41ba-a447-a248c4a0cd0c",
        "TEST series - IN TREE - series ID 149 - bcb2e4f4-516b-44a0-8279-a83af218b493",
        "TEST series - IN TREE - series ID 150 - cdf28d51-749d-4f0f-bff8-2668abd652a1",
        "TEST series - IN TREE - series ID 151 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
        "TEST series - IN TREE - series ID 152 - 34adf441-21e4-48db-ad38-bb43d16839e3",
        "TEST series - IN TREE - series ID 153 - b753a41a-5dbf-482d-b56d-20adc95cf71c",
        "TEST series - IN TREE - series ID 154 - 984d4dec-2ccf-4d81-b6e7-8420430262fd",
        "TEST series - IN TREE - series ID 155 - 378357dc-0b5e-4a91-afc9-37c1b37a98fc",
        "TEST series - IN TREE - series ID 156 - 42d53491-dc6c-4e9b-b8c3-d303e16ba806",
        "TEST series - IN TREE - series ID 157 - 4cd96070-5028-4c4b-bb86-b6c020deb4a2",
        "TEST series - IN TREE - series ID 158 - 413455b4-d046-4da7-8761-36b8a88fe088",
        "TEST series - IN TREE - series ID 159 - 1a87595e-f0a3-4ada-8a51-5e1f441bafbe",
        "TEST series - IN TREE - series ID 160 - 54b87228-9090-4ff8-bf90-4431cdd25519",
        "TEST series - IN TREE - series ID 161 - d4201a09-0787-459b-9d35-0b8339042264",
        "TEST series - IN TREE - series ID 162 - d64415c9-1fa5-4edf-a61e-4e3d7a041699",
        "TEST series - IN TREE - series ID 163 - dd2de1d2-9d42-4c07-9447-b0f2ac941e86",
        "TEST series - IN TREE - series ID 164 - 4c16166c-7b4f-4b32-a7c9-f039a9876785",
        "TEST series - IN TREE - series ID 165 - 2b58aad6-98ac-49ac-8eb1-b1346fb23a4c",
        "TEST series - IN TREE - series ID 166 - 43e08bcd-9ebc-4c58-b290-aa280c66e3df",
        "TEST series - IN TREE - series ID 167 - 654b7773-b95b-43b0-8c5b-820065463e47",
        "TEST series - IN TREE - series ID 168 - 4f266348-dde4-486a-b3e0-bee0baed5b02",
        "TEST series - IN TREE - series ID 169 - 813440de-8da1-4b21-a687-407407d0daeb",
        "TEST series - IN TREE - series ID 170 - 2205909c-4f4e-4818-9140-95dcbeea4d16",
        "TEST series - IN TREE - series ID 171 - 43024a39-3cd9-4ad8-8b1b-c2d79f900c4b",
        "TEST series - IN TREE - series ID 172 - 5a232c19-64ad-4ecb-894c-d020cc352438",
        "TEST series - IN TREE - series ID 173 - 7d465381-1f61-4501-98a5-b95db064e4dc",
        "TEST series - IN TREE - series ID 174 - 1b5d91b5-1b35-4d52-9e65-5e6c606a85c9",
        "TEST series - IN TREE - series ID 175 - 313e007a-16ac-4e4d-9732-6be97e7bd1d8",
        "TEST series - IN TREE - series ID 176 - 957fe462-e26e-4421-a5e7-bd3c08469145",
        "TEST series - IN TREE - series ID 177 - e08fb6ba-3808-4895-81e2-a9638dc29cee",
        "TEST series - IN TREE - series ID 178 - 5a097718-2b1b-48d6-9e1c-424275d0e3c4",
        "TEST series - IN TREE - series ID 179 - 088d5a29-3fd7-498f-92c4-4443efe66887",
        "TEST series - IN TREE - series ID 180 - 0862542f-22bc-417c-81a7-0053304412e2",
        "TEST series - IN TREE - series ID 181 - 650530d5-59f1-49ea-9ab2-f8c48538f0e3",
        "TEST series - IN TREE - series ID 182 - b16e638f-80ce-43fa-87de-2b8066c3c3e8",
        "TEST series - IN TREE - series ID 183 - 1cd4b089-63e0-4340-b387-4275a9e18a51",
        "TEST series - IN TREE - series ID 184 - fe882ed3-25fc-49f3-939d-3ee29634cf1d",
        "TEST series - IN TREE - series ID 185 - b12cfb91-6a49-4455-9ddf-5a797b7fe24c",
        "TEST series - IN TREE - series ID 186 - 5b6879c9-fe5d-4e56-be0f-081257a6fe8a",
        "TEST series - IN TREE - series ID 187 - 7f93e2df-1c66-4d29-a36b-3878c650c447",
        "TEST series - IN TREE - series ID 188 - ea487aed-0a6d-4d1d-9b73-dca14f96e4dd",
        "TEST series - IN TREE - series ID 189 - 9b9e8537-31cf-4683-af4b-5e2a33b75e8a",
        "TEST series - IN TREE - series ID 190 - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f",
        "TEST series - IN TREE - series ID 191 - 8fceae7a-5ab0-4ce9-9ca8-c21dd908f377",
        "TEST series - IN TREE - series ID 192 - 16298153-d0c1-4ea2-af5f-0c78cd6e46ba",
        "TEST series - IN TREE - series ID 193 - 55085fbd-0504-4f37-8fc3-3b6f75f03d41",
        "TEST series - IN TREE - series ID 194 - fe83a7fb-ac72-4c43-a868-869155fee1a1",
        "TEST series - IN TREE - series ID 195 - 78affa14-a6cb-4f88-a57e-99cb308f99f1",
        "TEST series - IN TREE - series ID 196 - 8615b37b-1481-4388-8dea-0f5c266c3612",
        "TEST series - IN TREE - series ID 197 - be054193-535b-4bb9-b6e2-897d7a494156",
        "TEST series - IN TREE - series ID 198 - 9f6a4336-c74a-40d0-be03-1b57556a3d5e",
        "TEST series - IN TREE - series ID 199 - 7bd3252a-b053-4080-904f-8292b6a7981c",
        "TEST series - IN TREE - series ID 200 - c78882ba-397a-4677-954d-e3b330f7f16e",
        "TEST series - IN TREE - series ID 201 - 59055099-0f20-46b5-91c5-be0a0cdd0313",
        "TEST series - IN TREE - series ID 202 - 078c31b9-a5f2-42ac-8621-6d9bc9355d4f",
        "TEST series - IN TREE - series ID 203 - b8336bb0-8f93-49f5-9433-03e5d542d635",
        "TEST series - IN TREE - series ID 204 - e55f7696-d9d7-4127-b8b3-163d9c90df8d",
        "TEST series - IN TREE - series ID 205 - b0cd931d-5325-4331-8d08-c038682eb4c3",
        "TEST series - IN TREE - series ID 206 - f9a27800-4067-4b82-a3a7-e9d18a9b8bcf",
        "TEST series - IN TREE - series ID 207 - d2b9f985-b203-466b-97d2-bde0527d5763",
        "TEST series - IN TREE - series ID 208 - 010c49f8-eb17-4565-8818-9fdf516ef6cb",
        "TEST series - IN TREE - series ID 209 - 2f7de989-32fd-4263-8020-e99d9358a89a",
        "TEST series - IN TREE - series ID 210 - a2a2b752-5f8e-4702-9e49-3eaaa4a1fd5a",
        "TEST series - IN TREE - series ID 211 - 268b04fe-92df-4e2e-b23b-ce5b74dbafc2",
        "TEST series - IN TREE - series ID 212 - f8f24390-b627-423a-97a1-aa980159df44",
        "TEST series - IN TREE - series ID 213 - d73b1842-5312-4ddc-9277-da8e378bb8ab",
        "TEST series - IN TREE - series ID 214 - 1a169e8f-c7a3-45f4-8ea6-b3c5ee25d0e3",
        "TEST series - IN TREE - series ID 215 - 6e61e176-d873-4bef-a3f5-0eb41e20328d",
        "TEST series - IN TREE - series ID 216 - 7bd49e8c-2c72-4c12-98ce-84e57c71a634",
        "TEST series - IN TREE - series ID 217 - 0d4aa6b0-9c1c-4a14-a661-66838257faad",
        "TEST series - IN TREE - series ID 218 - 3939f972-fa38-45e1-9f3f-be69a8618ee2",
        "TEST series - IN TREE - series ID 219 - fc21000c-e59c-44f4-9624-022a7658da42",
        "TEST series - IN TREE - series ID 220 - fd1c0506-3859-4f96-b42a-1fe2f99e0b1e",
        "TEST series - IN TREE - series ID 221 - ee292414-a0f9-4400-b987-75669a211ca9",
        "TEST series - IN TREE - series ID 222 - ae50b90e-fbba-492b-bfbb-edfde40520b1",
        "TEST series - IN TREE - series ID 223 - c47407dd-bd9e-478b-adfa-0585e8dee677",
        "TEST series - IN TREE - series ID 224 - c282ee09-9acc-40be-b540-ab3613e6e818",
        "TEST series - IN TREE - series ID 225 - 8229a8b9-9ed9-43b5-8bb7-1b7bc64b9127",
        "TEST series - IN TREE - series ID 226 - 1d30e9e6-3ed4-4897-93e9-0b2929ff27f7",
        "TEST series - IN TREE - series ID 227 - 02c1e800-e4b1-41ea-9c03-9f1e945725f3",
        "TEST series - IN TREE - series ID 228 - 546667c5-de19-4c85-9d79-14ee90d9188a",
        "TEST series - IN TREE - series ID 229 - ea24b712-4bcb-4f21-b7ce-57db9c919e33",
        "TEST series - IN TREE - series ID 230 - d1f9c688-3046-4474-a99b-5d7bf1159101",
        "TEST series - IN TREE - series ID 231 - 5e3c2ba7-f3f0-46fe-a2b6-9a3162506d70",
        "TEST series - IN TREE - series ID 232 - 6e7e2e87-5443-40f6-aef6-552ee1ab5531",
        "TEST series - IN TREE - series ID 233 - eda46b9e-b66e-49bd-9458-68e86cc0d3d1",
        "TEST series - IN TREE - series ID 234 - 4bce79fc-83a2-43fb-8505-ef9a415bb35f",
        "TEST series - IN TREE - series ID 235 - fdddde7e-5574-478c-9608-4640e45f3ec6",
        "TEST series - IN TREE - series ID 236 - 28ef047e-f466-4721-b71f-dfb858e0b34a",
        "TEST series - IN TREE - series ID 237 - 60c2ab8f-ced7-4f8d-a977-9f2fb4be941c",
        "TEST series - IN TREE - series ID 238 - c31ad47d-6f6d-46ec-bab8-96c8945056bd",
        "TEST series - IN TREE - series ID 239 - fbfa7e8f-a517-4e4c-96be-5fb7acef8517",
        "TEST series - IN TREE - series ID 240 - c003f5f9-0394-48fc-afb3-88d78b9534dd",
        "TEST series - IN TREE - series ID 241 - 49178b1c-8c0e-46c9-a739-42a7c26271b5",
        "TEST series - IN TREE - series ID 242 - 39ce9a58-ea08-4f2c-994a-b49d61e9393d",
        "TEST series - IN TREE - series ID 243 - afaa77d3-ce17-4bc8-806f-4b2f1b278473",
        "TEST series - IN TREE - series ID 244 - 4da1e0c5-813a-4a01-8ef6-721d35650ce7",
        "TEST series - IN TREE - series ID 245 - 090a79e4-b4cf-40fa-9163-36773b820b92",
        "TEST series - IN TREE - series ID 246 - d759e0bc-426c-4831-80b2-4bf9460f5cf3",
        "TEST series - IN TREE - series ID 247 - 146ab80f-261f-44b3-a25a-85379e76abe9",
        "TEST series - IN TREE - series ID 248 - 49458593-07f1-48f6-834b-fbafdfab119d",
        "TEST series - IN TREE - series ID 249 - 52ee46ad-4981-44e1-98af-cf9379e9de5c",
        "TEST series - IN TREE - series ID 250 - c5c0e3b8-7f0a-4925-a326-cc5bf2c4ecd6",
        "TEST series - IN TREE - series ID 251 - 55323a3a-bacf-4056-b5f5-e6f540b92d05",
        "TEST series - IN TREE - series ID 252 - 96ebca6d-fd03-4449-995b-210a0fccf6a3",
        "TEST series - IN TREE - series ID 253 - f4217e2f-2e03-455d-bfbf-ed5964ae7ac0",
        "TEST series - IN TREE - series ID 254 - fd5ae8b5-3833-4466-ac34-2efb7dc13bc4",
        "TEST series - IN TREE - series ID 255 - 8cf64bff-18a0-4795-b44a-8ac350c5afa5",
        "TEST series - IN TREE - series ID 256 - c6bc06b8-530c-44d0-9acf-a5cc20b3a221",
        "TEST series - IN TREE - series ID 257 - 0f2d241b-1c38-48a3-9bce-49c99c47081e",
        "TEST series - IN TREE - series ID 258 - ef74e5a1-bc3a-465d-9d9b-db985dbe8b0c",
        "TEST series - IN TREE - series ID 259 - aec6fe91-2a11-4608-af38-a01d0e7cab1d",
        "TEST series - IN TREE - series ID 260 - f85be9c8-af5a-41d6-a47a-77baf31c5308",
        "TEST series - IN TREE - series ID 261 - 67013d5f-d85d-4308-a208-a5162afc51ab",
        "TEST series - IN TREE - series ID 262 - f4b6557c-4278-4e02-9d5f-7b2c88daa7e0",
        "TEST series - IN TREE - series ID 263 - 9b88b308-40de-4ed1-ab51-b7762b950e49",
        "TEST series - IN TREE - series ID 264 - 01627073-b7f5-4602-819a-b85ba07394ee",
        "TEST series - IN TREE - series ID 265 - 3dd78a6f-ff02-4916-a795-f35b7b86e84c",
        "TEST series - IN TREE - series ID 266 - 83c6871c-1ac1-4cec-9519-d6c5ad761b3b",
        "TEST series - IN TREE - series ID 267 - 3f2fa934-e8cb-4635-8619-7afe9b5a6cd5",
        "TEST series - IN TREE - series ID 268 - 184852b7-f79d-4cb3-acff-4a0597bd2f23",
        "TEST series - IN TREE - series ID 269 - d1803224-93e4-46ae-8756-bbe42796360e",
        "TEST series - IN TREE - series ID 270 - 65851d79-e6d6-4a89-9780-55b118cf0858",
        "TEST series - IN TREE - series ID 271 - 4f2ab892-6a87-4d46-b1fb-a56478f84958",
        "TEST series - IN TREE - series ID 272 - 6e2a8e93-b7ad-4673-bc9e-589294004c38",
        "TEST series - IN TREE - series ID 273 - 669ba846-6730-4fc8-9299-eb1a5c3c082c",
        "TEST series - IN TREE - series ID 274 - ba565492-f0f0-46c3-b50d-59fefdf04aca",
        "TEST series - IN TREE - series ID 275 - d6c55d6e-41ce-47d6-ac02-6311716cef04",
        "TEST series - IN TREE - series ID 276 - 79085d34-1849-4acf-802e-5580ad1c86bb",
        "TEST series - IN TREE - series ID 277 - 0c4ffde3-a02f-4461-a089-c72979297354",
        "TEST series - IN TREE - series ID 278 - e58e43b6-1488-4e47-8107-32c37d8f45e9",
        "TEST series - IN TREE - series ID 279 - add9e315-3502-4f4b-95f7-0ad22e5592e4",
        "TEST series - IN TREE - series ID 280 - 7b3e4793-3302-4af2-bd2d-9d903258d151",
        "TEST series - IN TREE - series ID 281 - 862d3f42-f2de-4063-ad83-603da1b68b6b",
        "TEST series - IN TREE - series ID 282 - d442da62-b938-41e1-a069-0fb8bb85f340",
        "TEST series - IN TREE - series ID 283 - efda05b4-aee0-4dcf-9eb6-463c2bbee461",
        "TEST series - IN TREE - series ID 284 - 8567479b-4856-44b3-bf9e-6ebd73476942",
        "TEST series - IN TREE - series ID 285 - e242db80-72b7-41d6-9fa4-d86b61e72318",
        "TEST series - IN TREE - series ID 286 - 30b7d9de-990b-4294-b2a5-2d6855c77b23",
        "TEST series - IN TREE - series ID 287 - 54e78ee6-a8f0-4d60-869f-86fc15dfc181",
        "TEST series - IN TREE - series ID 288 - a46b35bb-6de6-438f-b946-6c95a4b9fb0c",
        "TEST series - IN TREE - series ID 289 - 8327f6d6-7c16-4e27-87d7-453f66dacab3",
        "TEST series - IN TREE - series ID 290 - 0da80909-5fd7-4240-9fe9-bf48686dc11e",
        "TEST series - IN TREE - series ID 291 - a3d0b436-b5c2-4c95-848e-8fcfac8e8afa",
        "TEST series - IN TREE - series ID 292 - 49b21b3f-83fa-4640-af1d-ec8aabf55331",
        "TEST series - IN TREE - series ID 293 - 77211d71-f8a7-411b-ac58-19af6f4ae350",
        "TEST series - IN TREE - series ID 294 - b5f0aed6-9956-481b-a11e-ab83847884d8",
        "TEST series - IN TREE - series ID 295 - a96c4b26-4c7f-4b43-ac8b-f77b9fce7c55",
        "TEST series - IN TREE - series ID 296 - 849b8e8d-95a4-4018-9510-45fed629ee65",
        "TEST series - IN TREE - series ID 297 - b2dc4d71-e66d-4431-9266-2a4bc7b0bb7f",
        "TEST series - IN TREE - series ID 298 - 5211863b-3764-4ed2-b938-40ec0abc7b44",
        "TEST series - IN TREE - series ID 299 - 9ba48058-819a-46fe-be1b-c8e5a81203b1",
        "TEST series - IN TREE - series ID 300 - 562c6380-4c1f-413a-8cfa-612c7d9119ab",
        "TEST series - IN TREE - series ID 301 - f576fc20-e058-4859-a027-3c586d8e43c2",
        "TEST series - IN TREE - series ID 302 - 2f5ade60-4c28-47a2-82f9-a71ab5e05158",
        "TEST series - IN TREE - series ID 303 - 0cdf212c-724a-4666-ba52-8f8b4860f0f3",
        "TEST series - IN TREE - series ID 304 - 4a8dcea2-7a07-4dbd-a2e6-339e1418b0ec",
        "TEST series - IN TREE - series ID 305 - 68042e03-d2f2-41a0-b08e-645123c12597",
        "TEST series - IN TREE - series ID 306 - 8d705893-e596-4e92-a839-4dffe4373177",
        "TEST series - IN TREE - series ID 307 - c187b301-de7e-4f91-94dc-57cab3996a95",
        "TEST series - IN TREE - series ID 308 - 03210848-24c6-42ac-b918-74aba63e40f3",
        "TEST series - IN TREE - series ID 309 - e82c3c35-faae-40b2-a4a6-d31b4b5a2719",
        "TEST series - IN TREE - series ID 310 - a471e7c5-6a0f-415c-81b5-d69b1997b365",
        "TEST series - IN TREE - series ID 311 - 1d0e6376-0cb5-43b8-b909-0e74d53805fa",
        "TEST series - IN TREE - series ID 312 - e5c7c4b6-b38c-47d0-9c25-f6620896a795",
        "TEST series - IN TREE - series ID 313 - 56f272d2-bd4e-447d-8a3d-171357a5f6e2",
        "TEST series - IN TREE - series ID 314 - 29f18462-21ec-4886-9c1d-18e8dbeb54e8",
        "TEST series - IN TREE - series ID 315 - 29686bc4-0955-4d4b-8717-c225be9f14db",
        "TEST series - IN TREE - series ID 316 - d76513c3-8c70-422c-9fc2-793ad6b03180",
        "TEST series - IN TREE - series ID 317 - 585a2004-1ec7-458d-84c6-e84807aba9b2",
        "TEST series - IN TREE - series ID 318 - 5ec2291a-111e-4e61-b177-5dbd744007be",
        "TEST series - IN TREE - series ID 319 - c7a73b65-4115-45fd-adf2-7dab4327dc34",
        "TEST series - IN TREE - series ID 320 - c9e23001-d802-4f9b-91e1-cda7bb685c62",
        "TEST series - IN TREE - series ID 321 - 94d9c81a-9f6a-4669-9e65-78db57bab3a6",
        "TEST series - IN TREE - series ID 322 - f4fdb004-c2af-442e-8f47-7f353a522ef8",
        "TEST series - IN TREE - series ID 323 - 44e87e9e-d716-4e10-89ba-db03682f20ed",
        "TEST series - IN TREE - series ID 324 - 0ea3cee6-359e-4c61-b414-461d61057b3d",
        "TEST series - IN TREE - series ID 325 - 1c69f042-1254-4c69-b804-c9b03be73ef7",
        "TEST series - IN TREE - series ID 326 - 01afd8a1-813e-4c2b-b8a2-b76b1c3067b1",
        "TEST series - IN TREE - series ID 327 - ab2f7513-926d-4184-a03a-5534b59e62fd",
        "TEST series - IN TREE - series ID 328 - dec025b3-cf1d-4039-8a93-fd3fc51e416d",
        "TEST series - IN TREE - series ID 329 - 4622c89f-0e05-4ef0-9b18-1319209ee674",
        "TEST series - IN TREE - series ID 330 - 25233cf6-1e45-4ce6-a96c-297db220521c",
        "TEST series - IN TREE - series ID 331 - 5331f31b-dca6-4c50-9162-4444663c2728",
        "TEST series - IN TREE - series ID 332 - 56dd1b6a-0342-453b-b779-e12492319aa9",
        "TEST series - IN TREE - series ID 333 - 8cb78344-d95e-4166-aa5a-5029c68a1f40",
        "TEST series - IN TREE - series ID 334 - 3542b654-e2d8-48a2-aaa3-7882aa50e259",
        "TEST series - IN TREE - series ID 335 - 4324fdc0-8cb1-4aa7-bb25-1714dd39cdca",
        "TEST series - IN TREE - series ID 336 - 92472632-d0b9-4497-a5f1-852a8a168b22",
        "TEST series - IN TREE - series ID 337 - 0d4d18dd-7cb7-423b-b2d9-8845b2eed393",
        "TEST series - IN TREE - series ID 338 - d926b73e-bace-4d92-9406-a38e4b5de7b7",
        "TEST series - IN TREE - series ID 339 - d27ef268-7a63-4dfa-8876-03a6de8f3e93",
        "TEST series - IN TREE - series ID 340 - da788a67-4603-4b71-8ceb-55d3b48ac79e",
        "TEST series - IN TREE - series ID 341 - 08bc4130-9b0d-4e65-8a08-e7adc416c84b",
        "TEST series - IN TREE - series ID 342 - 2fa7997a-9998-47fa-85ec-3c81f6180a8b",
        "TEST series - IN TREE - series ID 343 - 9b67e869-0ab7-4267-a5d3-3841d8b7145d",
        "TEST series - IN TREE - series ID 344 - 3cce4d59-b8a0-4f83-9b59-9dd3500a8a8c",
        "TEST series - IN TREE - series ID 345 - 635c7cf4-4a46-402d-b219-af0479b5aa3e",
        "TEST series - IN TREE - series ID 346 - 93acce3a-7b3c-4afb-b717-b63150537fa6",
        "TEST series - IN TREE - series ID 347 - 69b09912-0efb-4c72-8914-cbbd2e4a9bdd",
        "TEST series - IN TREE - series ID 348 - 83a0633c-042d-41ba-a447-a248c4a0cd0c",
        "TEST series - IN TREE - series ID 349 - bcb2e4f4-516b-44a0-8279-a83af218b493",
        "TEST series - IN TREE - series ID 350 - cdf28d51-749d-4f0f-bff8-2668abd652a1",
        "TEST series - IN TREE - series ID 351 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
        "TEST series - IN TREE - series ID 352 - 34adf441-21e4-48db-ad38-bb43d16839e3",
        "TEST series - IN TREE - series ID 353 - b753a41a-5dbf-482d-b56d-20adc95cf71c",
        "TEST series - IN TREE - series ID 354 - 984d4dec-2ccf-4d81-b6e7-8420430262fd",
        "TEST series - IN TREE - series ID 355 - 378357dc-0b5e-4a91-afc9-37c1b37a98fc",
        "TEST series - IN TREE - series ID 356 - 42d53491-dc6c-4e9b-b8c3-d303e16ba806",
        "TEST series - IN TREE - series ID 357 - 4cd96070-5028-4c4b-bb86-b6c020deb4a2",
        "TEST series - IN TREE - series ID 358 - 413455b4-d046-4da7-8761-36b8a88fe088",
        "TEST series - IN TREE - series ID 359 - 1a87595e-f0a3-4ada-8a51-5e1f441bafbe",
        "TEST series - IN TREE - series ID 360 - 54b87228-9090-4ff8-bf90-4431cdd25519",
        "TEST series - IN TREE - series ID 361 - d4201a09-0787-459b-9d35-0b8339042264",
        "TEST series - IN TREE - series ID 362 - d64415c9-1fa5-4edf-a61e-4e3d7a041699",
        "TEST series - IN TREE - series ID 363 - dd2de1d2-9d42-4c07-9447-b0f2ac941e86",
        "TEST series - IN TREE - series ID 364 - 4c16166c-7b4f-4b32-a7c9-f039a9876785",
        "TEST series - IN TREE - series ID 365 - 2b58aad6-98ac-49ac-8eb1-b1346fb23a4c",
        "TEST series - IN TREE - series ID 366 - 43e08bcd-9ebc-4c58-b290-aa280c66e3df",
        "TEST series - IN TREE - series ID 367 - 654b7773-b95b-43b0-8c5b-820065463e47",
        "TEST series - IN TREE - series ID 368 - 4f266348-dde4-486a-b3e0-bee0baed5b02",
        "TEST series - IN TREE - series ID 369 - 813440de-8da1-4b21-a687-407407d0daeb",
        "TEST series - IN TREE - series ID 370 - 2205909c-4f4e-4818-9140-95dcbeea4d16",
        "TEST series - IN TREE - series ID 371 - 43024a39-3cd9-4ad8-8b1b-c2d79f900c4b",
        "TEST series - IN TREE - series ID 372 - 5a232c19-64ad-4ecb-894c-d020cc352438",
        "TEST series - IN TREE - series ID 373 - 7d465381-1f61-4501-98a5-b95db064e4dc",
        "TEST series - IN TREE - series ID 374 - 1b5d91b5-1b35-4d52-9e65-5e6c606a85c9",
        "TEST series - IN TREE - series ID 375 - 313e007a-16ac-4e4d-9732-6be97e7bd1d8",
        "TEST series - IN TREE - series ID 376 - 957fe462-e26e-4421-a5e7-bd3c08469145",
        "TEST series - IN TREE - series ID 377 - e08fb6ba-3808-4895-81e2-a9638dc29cee",
        "TEST series - IN TREE - series ID 378 - 5a097718-2b1b-48d6-9e1c-424275d0e3c4",
        "TEST series - IN TREE - series ID 379 - 088d5a29-3fd7-498f-92c4-4443efe66887",
        "TEST series - IN TREE - series ID 380 - 0862542f-22bc-417c-81a7-0053304412e2",
        "TEST series - IN TREE - series ID 381 - 650530d5-59f1-49ea-9ab2-f8c48538f0e3",
        "TEST series - IN TREE - series ID 382 - b16e638f-80ce-43fa-87de-2b8066c3c3e8",
        "TEST series - IN TREE - series ID 383 - 1cd4b089-63e0-4340-b387-4275a9e18a51",
        "TEST series - IN TREE - series ID 384 - fe882ed3-25fc-49f3-939d-3ee29634cf1d",
        "TEST series - IN TREE - series ID 385 - b12cfb91-6a49-4455-9ddf-5a797b7fe24c",
        "TEST series - IN TREE - series ID 386 - 5b6879c9-fe5d-4e56-be0f-081257a6fe8a",
        "TEST series - IN TREE - series ID 387 - 7f93e2df-1c66-4d29-a36b-3878c650c447",
        "TEST series - IN TREE - series ID 388 - ea487aed-0a6d-4d1d-9b73-dca14f96e4dd",
        "TEST series - IN TREE - series ID 389 - 9b9e8537-31cf-4683-af4b-5e2a33b75e8a",
        "TEST series - IN TREE - series ID 390 - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f",
        "TEST series - IN TREE - series ID 391 - 8fceae7a-5ab0-4ce9-9ca8-c21dd908f377",
        "TEST series - IN TREE - series ID 392 - 16298153-d0c1-4ea2-af5f-0c78cd6e46ba",
        "TEST series - IN TREE - series ID 393 - 55085fbd-0504-4f37-8fc3-3b6f75f03d41",
        "TEST series - IN TREE - series ID 394 - fe83a7fb-ac72-4c43-a868-869155fee1a1",
        "TEST series - IN TREE - series ID 395 - 78affa14-a6cb-4f88-a57e-99cb308f99f1",
        "TEST series - IN TREE - series ID 396 - 8615b37b-1481-4388-8dea-0f5c266c3612",
        "TEST series - IN TREE - series ID 397 - be054193-535b-4bb9-b6e2-897d7a494156",
        "TEST series - IN TREE - series ID 398 - 9f6a4336-c74a-40d0-be03-1b57556a3d5e",
        "TEST series - IN TREE - series ID 399 - 7bd3252a-b053-4080-904f-8292b6a7981c",
        "TEST series - IN TREE - series ID 400 - c78882ba-397a-4677-954d-e3b330f7f16e",
        "TEST series - IN TREE - series ID 401 - 59055099-0f20-46b5-91c5-be0a0cdd0313",
        "TEST series - IN TREE - series ID 402 - 078c31b9-a5f2-42ac-8621-6d9bc9355d4f",
        "TEST series - IN TREE - series ID 403 - b8336bb0-8f93-49f5-9433-03e5d542d635",
        "TEST series - IN TREE - series ID 404 - e55f7696-d9d7-4127-b8b3-163d9c90df8d",
        "TEST series - IN TREE - series ID 405 - b0cd931d-5325-4331-8d08-c038682eb4c3",
        "TEST series - IN TREE - series ID 406 - f9a27800-4067-4b82-a3a7-e9d18a9b8bcf",
        "TEST series - IN TREE - series ID 407 - d2b9f985-b203-466b-97d2-bde0527d5763",
        "TEST series - IN TREE - series ID 408 - 010c49f8-eb17-4565-8818-9fdf516ef6cb",
        "TEST series - IN TREE - series ID 409 - 2f7de989-32fd-4263-8020-e99d9358a89a",
        "TEST series - IN TREE - series ID 410 - a2a2b752-5f8e-4702-9e49-3eaaa4a1fd5a",
        "TEST series - IN TREE - series ID 411 - 268b04fe-92df-4e2e-b23b-ce5b74dbafc2",
        "TEST series - IN TREE - series ID 412 - f8f24390-b627-423a-97a1-aa980159df44",
        "TEST series - IN TREE - series ID 413 - d73b1842-5312-4ddc-9277-da8e378bb8ab",
        "TEST series - IN TREE - series ID 414 - 1a169e8f-c7a3-45f4-8ea6-b3c5ee25d0e3",
        "TEST series - IN TREE - series ID 415 - 6e61e176-d873-4bef-a3f5-0eb41e20328d",
        "TEST series - IN TREE - series ID 416 - 7bd49e8c-2c72-4c12-98ce-84e57c71a634",
        "TEST series - IN TREE - series ID 417 - 0d4aa6b0-9c1c-4a14-a661-66838257faad",
        "TEST series - IN TREE - series ID 418 - 3939f972-fa38-45e1-9f3f-be69a8618ee2",
        "TEST series - IN TREE - series ID 419 - fc21000c-e59c-44f4-9624-022a7658da42",
        "TEST series - IN TREE - series ID 420 - fd1c0506-3859-4f96-b42a-1fe2f99e0b1e",
        "TEST series - IN TREE - series ID 421 - ee292414-a0f9-4400-b987-75669a211ca9",
        "TEST series - IN TREE - series ID 422 - ae50b90e-fbba-492b-bfbb-edfde40520b1",
        "TEST series - IN TREE - series ID 423 - c47407dd-bd9e-478b-adfa-0585e8dee677",
        "TEST series - IN TREE - series ID 424 - c282ee09-9acc-40be-b540-ab3613e6e818",
        "TEST series - IN TREE - series ID 425 - 8229a8b9-9ed9-43b5-8bb7-1b7bc64b9127",
        "TEST series - IN TREE - series ID 426 - 1d30e9e6-3ed4-4897-93e9-0b2929ff27f7",
        "TEST series - IN TREE - series ID 427 - 02c1e800-e4b1-41ea-9c03-9f1e945725f3",
        "TEST series - IN TREE - series ID 428 - 546667c5-de19-4c85-9d79-14ee90d9188a",
        "TEST series - IN TREE - series ID 429 - ea24b712-4bcb-4f21-b7ce-57db9c919e33",
        "TEST series - IN TREE - series ID 430 - d1f9c688-3046-4474-a99b-5d7bf1159101",
        "TEST series - IN TREE - series ID 431 - 5e3c2ba7-f3f0-46fe-a2b6-9a3162506d70",
        "TEST series - IN TREE - series ID 432 - 6e7e2e87-5443-40f6-aef6-552ee1ab5531",
        "TEST series - IN TREE - series ID 433 - eda46b9e-b66e-49bd-9458-68e86cc0d3d1",
        "TEST series - IN TREE - series ID 434 - 4bce79fc-83a2-43fb-8505-ef9a415bb35f",
        "TEST series - IN TREE - series ID 435 - fdddde7e-5574-478c-9608-4640e45f3ec6",
        "TEST series - IN TREE - series ID 436 - 28ef047e-f466-4721-b71f-dfb858e0b34a",
        "TEST series - IN TREE - series ID 437 - 60c2ab8f-ced7-4f8d-a977-9f2fb4be941c",
        "TEST series - IN TREE - series ID 438 - c31ad47d-6f6d-46ec-bab8-96c8945056bd",
        "TEST series - IN TREE - series ID 439 - fbfa7e8f-a517-4e4c-96be-5fb7acef8517",
        "TEST series - IN TREE - series ID 440 - c003f5f9-0394-48fc-afb3-88d78b9534dd",
        "TEST series - IN TREE - series ID 441 - 49178b1c-8c0e-46c9-a739-42a7c26271b5",
        "TEST series - IN TREE - series ID 442 - 39ce9a58-ea08-4f2c-994a-b49d61e9393d",
        "TEST series - IN TREE - series ID 443 - afaa77d3-ce17-4bc8-806f-4b2f1b278473",
        "TEST series - IN TREE - series ID 444 - 4da1e0c5-813a-4a01-8ef6-721d35650ce7",
        "TEST series - IN TREE - series ID 445 - 090a79e4-b4cf-40fa-9163-36773b820b92",
        "TEST series - IN TREE - series ID 446 - d759e0bc-426c-4831-80b2-4bf9460f5cf3",
        "TEST series - IN TREE - series ID 447 - 146ab80f-261f-44b3-a25a-85379e76abe9",
        "TEST series - IN TREE - series ID 448 - 49458593-07f1-48f6-834b-fbafdfab119d",
        "TEST series - IN TREE - series ID 449 - 52ee46ad-4981-44e1-98af-cf9379e9de5c",
        "TEST series - IN TREE - series ID 450 - c5c0e3b8-7f0a-4925-a326-cc5bf2c4ecd6",
        "TEST series - IN TREE - series ID 451 - 55323a3a-bacf-4056-b5f5-e6f540b92d05",
        "TEST series - IN TREE - series ID 452 - 96ebca6d-fd03-4449-995b-210a0fccf6a3",
        "TEST series - IN TREE - series ID 453 - f4217e2f-2e03-455d-bfbf-ed5964ae7ac0",
        "TEST series - IN TREE - series ID 454 - fd5ae8b5-3833-4466-ac34-2efb7dc13bc4",
        "TEST series - IN TREE - series ID 455 - 8cf64bff-18a0-4795-b44a-8ac350c5afa5",
        "TEST series - IN TREE - series ID 456 - c6bc06b8-530c-44d0-9acf-a5cc20b3a221",
        "TEST series - IN TREE - series ID 457 - 0f2d241b-1c38-48a3-9bce-49c99c47081e",
        "TEST series - IN TREE - series ID 458 - ef74e5a1-bc3a-465d-9d9b-db985dbe8b0c",
        "TEST series - IN TREE - series ID 459 - aec6fe91-2a11-4608-af38-a01d0e7cab1d",
        "TEST series - IN TREE - series ID 460 - f85be9c8-af5a-41d6-a47a-77baf31c5308",
        "TEST series - IN TREE - series ID 461 - 67013d5f-d85d-4308-a208-a5162afc51ab",
        "TEST series - IN TREE - series ID 462 - f4b6557c-4278-4e02-9d5f-7b2c88daa7e0",
        "TEST series - IN TREE - series ID 463 - 9b88b308-40de-4ed1-ab51-b7762b950e49",
        "TEST series - IN TREE - series ID 464 - 01627073-b7f5-4602-819a-b85ba07394ee",
        "TEST series - IN TREE - series ID 465 - 3dd78a6f-ff02-4916-a795-f35b7b86e84c",
        "TEST series - IN TREE - series ID 466 - 83c6871c-1ac1-4cec-9519-d6c5ad761b3b",
        "TEST series - IN TREE - series ID 467 - 3f2fa934-e8cb-4635-8619-7afe9b5a6cd5",
        "TEST series - IN TREE - series ID 468 - 184852b7-f79d-4cb3-acff-4a0597bd2f23",
        "TEST series - IN TREE - series ID 469 - d1803224-93e4-46ae-8756-bbe42796360e",
        "TEST series - IN TREE - series ID 470 - 65851d79-e6d6-4a89-9780-55b118cf0858",
        "TEST series - IN TREE - series ID 471 - 4f2ab892-6a87-4d46-b1fb-a56478f84958",
        "TEST series - IN TREE - series ID 472 - 6e2a8e93-b7ad-4673-bc9e-589294004c38",
        "TEST series - IN TREE - series ID 473 - 669ba846-6730-4fc8-9299-eb1a5c3c082c",
        "TEST series - IN TREE - series ID 474 - ba565492-f0f0-46c3-b50d-59fefdf04aca",
        "TEST series - IN TREE - series ID 475 - d6c55d6e-41ce-47d6-ac02-6311716cef04",
        "TEST series - IN TREE - series ID 476 - 79085d34-1849-4acf-802e-5580ad1c86bb",
        "TEST series - IN TREE - series ID 477 - 0c4ffde3-a02f-4461-a089-c72979297354",
        "TEST series - IN TREE - series ID 478 - e58e43b6-1488-4e47-8107-32c37d8f45e9",
        "TEST series - IN TREE - series ID 479 - add9e315-3502-4f4b-95f7-0ad22e5592e4",
        "TEST series - IN TREE - series ID 480 - 7b3e4793-3302-4af2-bd2d-9d903258d151",
        "TEST series - IN TREE - series ID 481 - 862d3f42-f2de-4063-ad83-603da1b68b6b",
        "TEST series - IN TREE - series ID 482 - d442da62-b938-41e1-a069-0fb8bb85f340",
        "TEST series - IN TREE - series ID 483 - efda05b4-aee0-4dcf-9eb6-463c2bbee461",
        "TEST series - IN TREE - series ID 484 - 8567479b-4856-44b3-bf9e-6ebd73476942",
        "TEST series - IN TREE - series ID 485 - e242db80-72b7-41d6-9fa4-d86b61e72318",
        "TEST series - IN TREE - series ID 486 - 30b7d9de-990b-4294-b2a5-2d6855c77b23",
        "TEST series - IN TREE - series ID 487 - 54e78ee6-a8f0-4d60-869f-86fc15dfc181",
        "TEST series - IN TREE - series ID 488 - a46b35bb-6de6-438f-b946-6c95a4b9fb0c",
        "TEST series - IN TREE - series ID 489 - 8327f6d6-7c16-4e27-87d7-453f66dacab3",
        "TEST series - IN TREE - series ID 490 - 0da80909-5fd7-4240-9fe9-bf48686dc11e",
        "TEST series - IN TREE - series ID 491 - a3d0b436-b5c2-4c95-848e-8fcfac8e8afa",
        "TEST series - IN TREE - series ID 492 - 49b21b3f-83fa-4640-af1d-ec8aabf55331",
        "TEST series - IN TREE - series ID 493 - 77211d71-f8a7-411b-ac58-19af6f4ae350",
        "TEST series - IN TREE - series ID 494 - b5f0aed6-9956-481b-a11e-ab83847884d8",
        "TEST series - IN TREE - series ID 495 - a96c4b26-4c7f-4b43-ac8b-f77b9fce7c55",
        "TEST series - IN TREE - series ID 496 - 849b8e8d-95a4-4018-9510-45fed629ee65",
        "TEST series - IN TREE - series ID 497 - b2dc4d71-e66d-4431-9266-2a4bc7b0bb7f",
        "TEST series - IN TREE - series ID 498 - 5211863b-3764-4ed2-b938-40ec0abc7b44",
        "TEST series - IN TREE - series ID 499 - 9ba48058-819a-46fe-be1b-c8e5a81203b1",
        "TEST series - IN TREE - series ID 500 - 562c6380-4c1f-413a-8cfa-612c7d9119ab",
        "TEST series - IN TREE - series ID 501 - f576fc20-e058-4859-a027-3c586d8e43c2",
        "TEST series - IN TREE - series ID 502 - 2f5ade60-4c28-47a2-82f9-a71ab5e05158",
        "TEST series - IN TREE - series ID 503 - 0cdf212c-724a-4666-ba52-8f8b4860f0f3",
        "TEST series - IN TREE - series ID 504 - 4a8dcea2-7a07-4dbd-a2e6-339e1418b0ec",
        "TEST series - IN TREE - series ID 505 - 68042e03-d2f2-41a0-b08e-645123c12597",
        "TEST series - IN TREE - series ID 506 - 8d705893-e596-4e92-a839-4dffe4373177",
        "TEST series - IN TREE - series ID 507 - c187b301-de7e-4f91-94dc-57cab3996a95",
        "TEST series - IN TREE - series ID 508 - 03210848-24c6-42ac-b918-74aba63e40f3",
        "TEST series - IN TREE - series ID 509 - e82c3c35-faae-40b2-a4a6-d31b4b5a2719",
        "TEST series - IN TREE - series ID 510 - a471e7c5-6a0f-415c-81b5-d69b1997b365",
        "TEST series - IN TREE - series ID 511 - 1d0e6376-0cb5-43b8-b909-0e74d53805fa",
        "TEST series - IN TREE - series ID 512 - e5c7c4b6-b38c-47d0-9c25-f6620896a795",
        "TEST series - IN TREE - series ID 513 - 56f272d2-bd4e-447d-8a3d-171357a5f6e2",
        "TEST series - IN TREE - series ID 514 - 29f18462-21ec-4886-9c1d-18e8dbeb54e8",
        "TEST series - IN TREE - series ID 515 - 29686bc4-0955-4d4b-8717-c225be9f14db",
        "TEST series - IN TREE - series ID 516 - d76513c3-8c70-422c-9fc2-793ad6b03180",
        "TEST series - IN TREE - series ID 517 - 585a2004-1ec7-458d-84c6-e84807aba9b2",
        "TEST series - IN TREE - series ID 518 - 5ec2291a-111e-4e61-b177-5dbd744007be",
        "TEST series - IN TREE - series ID 519 - c7a73b65-4115-45fd-adf2-7dab4327dc34",
        "TEST series - IN TREE - series ID 520 - c9e23001-d802-4f9b-91e1-cda7bb685c62",
        "TEST series - IN TREE - series ID 521 - 94d9c81a-9f6a-4669-9e65-78db57bab3a6",
        "TEST series - IN TREE - series ID 522 - f4fdb004-c2af-442e-8f47-7f353a522ef8",
        "TEST series - IN TREE - series ID 523 - 44e87e9e-d716-4e10-89ba-db03682f20ed",
        "TEST series - IN TREE - series ID 524 - 0ea3cee6-359e-4c61-b414-461d61057b3d",
        "TEST series - IN TREE - series ID 525 - 1c69f042-1254-4c69-b804-c9b03be73ef7",
        "TEST series - IN TREE - series ID 526 - 01afd8a1-813e-4c2b-b8a2-b76b1c3067b1",
        "TEST series - IN TREE - series ID 527 - ab2f7513-926d-4184-a03a-5534b59e62fd",
        "TEST series - IN TREE - series ID 528 - dec025b3-cf1d-4039-8a93-fd3fc51e416d",
        "TEST series - IN TREE - series ID 529 - 4622c89f-0e05-4ef0-9b18-1319209ee674",
        "TEST series - IN TREE - series ID 530 - 25233cf6-1e45-4ce6-a96c-297db220521c",
        "TEST series - IN TREE - series ID 531 - 5331f31b-dca6-4c50-9162-4444663c2728",
        "TEST series - IN TREE - series ID 532 - 56dd1b6a-0342-453b-b779-e12492319aa9",
        "TEST series - IN TREE - series ID 533 - 8cb78344-d95e-4166-aa5a-5029c68a1f40",
        "TEST series - IN TREE - series ID 534 - 3542b654-e2d8-48a2-aaa3-7882aa50e259",
        "TEST series - IN TREE - series ID 535 - 4324fdc0-8cb1-4aa7-bb25-1714dd39cdca",
        "TEST series - IN TREE - series ID 536 - 92472632-d0b9-4497-a5f1-852a8a168b22",
        "TEST series - IN TREE - series ID 537 - 0d4d18dd-7cb7-423b-b2d9-8845b2eed393",
        "TEST series - IN TREE - series ID 538 - d926b73e-bace-4d92-9406-a38e4b5de7b7",
        "TEST series - IN TREE - series ID 539 - d27ef268-7a63-4dfa-8876-03a6de8f3e93",
        "TEST series - IN TREE - series ID 540 - da788a67-4603-4b71-8ceb-55d3b48ac79e",
        "TEST series - IN TREE - series ID 541 - 08bc4130-9b0d-4e65-8a08-e7adc416c84b",
        "TEST series - IN TREE - series ID 542 - 2fa7997a-9998-47fa-85ec-3c81f6180a8b",
        "TEST series - IN TREE - series ID 543 - 9b67e869-0ab7-4267-a5d3-3841d8b7145d",
        "TEST series - IN TREE - series ID 544 - 3cce4d59-b8a0-4f83-9b59-9dd3500a8a8c",
        "TEST series - IN TREE - series ID 545 - 635c7cf4-4a46-402d-b219-af0479b5aa3e",
        "TEST series - IN TREE - series ID 546 - 93acce3a-7b3c-4afb-b717-b63150537fa6",
        "TEST series - IN TREE - series ID 547 - 69b09912-0efb-4c72-8914-cbbd2e4a9bdd",
        "TEST series - IN TREE - series ID 548 - 83a0633c-042d-41ba-a447-a248c4a0cd0c",
        "TEST series - IN TREE - series ID 549 - bcb2e4f4-516b-44a0-8279-a83af218b493",
        "TEST series - IN TREE - series ID 550 - cdf28d51-749d-4f0f-bff8-2668abd652a1",
        "TEST series - IN TREE - series ID 551 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
        "TEST series - IN TREE - series ID 552 - 34adf441-21e4-48db-ad38-bb43d16839e3",
        "TEST series - IN TREE - series ID 553 - b753a41a-5dbf-482d-b56d-20adc95cf71c",
        "TEST series - IN TREE - series ID 554 - 984d4dec-2ccf-4d81-b6e7-8420430262fd",
        "TEST series - IN TREE - series ID 555 - 378357dc-0b5e-4a91-afc9-37c1b37a98fc",
        "TEST series - IN TREE - series ID 556 - 42d53491-dc6c-4e9b-b8c3-d303e16ba806",
        "TEST series - IN TREE - series ID 557 - 4cd96070-5028-4c4b-bb86-b6c020deb4a2",
        "TEST series - IN TREE - series ID 558 - 413455b4-d046-4da7-8761-36b8a88fe088",
        "TEST series - IN TREE - series ID 559 - 1a87595e-f0a3-4ada-8a51-5e1f441bafbe",
        "TEST series - IN TREE - series ID 560 - 54b87228-9090-4ff8-bf90-4431cdd25519",
        "TEST series - IN TREE - series ID 561 - d4201a09-0787-459b-9d35-0b8339042264",
        "TEST series - IN TREE - series ID 562 - d64415c9-1fa5-4edf-a61e-4e3d7a041699",
        "TEST series - IN TREE - series ID 563 - dd2de1d2-9d42-4c07-9447-b0f2ac941e86",
        "TEST series - IN TREE - series ID 564 - 4c16166c-7b4f-4b32-a7c9-f039a9876785",
        "TEST series - IN TREE - series ID 565 - 2b58aad6-98ac-49ac-8eb1-b1346fb23a4c",
        "TEST series - IN TREE - series ID 566 - 43e08bcd-9ebc-4c58-b290-aa280c66e3df",
        "TEST series - IN TREE - series ID 567 - 654b7773-b95b-43b0-8c5b-820065463e47",
        "TEST series - IN TREE - series ID 568 - 4f266348-dde4-486a-b3e0-bee0baed5b02",
        "TEST series - IN TREE - series ID 569 - 813440de-8da1-4b21-a687-407407d0daeb",
        "TEST series - IN TREE - series ID 570 - 2205909c-4f4e-4818-9140-95dcbeea4d16",
        "TEST series - IN TREE - series ID 571 - 43024a39-3cd9-4ad8-8b1b-c2d79f900c4b",
        "TEST series - IN TREE - series ID 572 - 5a232c19-64ad-4ecb-894c-d020cc352438",
        "TEST series - IN TREE - series ID 573 - 7d465381-1f61-4501-98a5-b95db064e4dc",
        "TEST series - IN TREE - series ID 574 - 1b5d91b5-1b35-4d52-9e65-5e6c606a85c9",
        "TEST series - IN TREE - series ID 575 - 313e007a-16ac-4e4d-9732-6be97e7bd1d8",
        "TEST series - IN TREE - series ID 576 - 957fe462-e26e-4421-a5e7-bd3c08469145",
        "TEST series - IN TREE - series ID 577 - e08fb6ba-3808-4895-81e2-a9638dc29cee",
        "TEST series - IN TREE - series ID 578 - 5a097718-2b1b-48d6-9e1c-424275d0e3c4",
        "TEST series - IN TREE - series ID 579 - 088d5a29-3fd7-498f-92c4-4443efe66887",
        "TEST series - IN TREE - series ID 580 - 0862542f-22bc-417c-81a7-0053304412e2",
        "TEST series - IN TREE - series ID 581 - 650530d5-59f1-49ea-9ab2-f8c48538f0e3",
        "TEST series - IN TREE - series ID 582 - b16e638f-80ce-43fa-87de-2b8066c3c3e8",
        "TEST series - IN TREE - series ID 583 - 1cd4b089-63e0-4340-b387-4275a9e18a51",
        "TEST series - IN TREE - series ID 584 - fe882ed3-25fc-49f3-939d-3ee29634cf1d",
        "TEST series - IN TREE - series ID 585 - b12cfb91-6a49-4455-9ddf-5a797b7fe24c",
        "TEST series - IN TREE - series ID 586 - 5b6879c9-fe5d-4e56-be0f-081257a6fe8a",
        "TEST series - IN TREE - series ID 587 - 7f93e2df-1c66-4d29-a36b-3878c650c447",
        "TEST series - IN TREE - series ID 588 - ea487aed-0a6d-4d1d-9b73-dca14f96e4dd",
        "TEST series - IN TREE - series ID 589 - 9b9e8537-31cf-4683-af4b-5e2a33b75e8a",
        "TEST series - IN TREE - series ID 590 - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f",
        "TEST series - IN TREE - series ID 591 - 8fceae7a-5ab0-4ce9-9ca8-c21dd908f377",
        "TEST series - IN TREE - series ID 592 - 16298153-d0c1-4ea2-af5f-0c78cd6e46ba",
        "TEST series - IN TREE - series ID 593 - 55085fbd-0504-4f37-8fc3-3b6f75f03d41",
        "TEST series - IN TREE - series ID 594 - fe83a7fb-ac72-4c43-a868-869155fee1a1",
        "TEST series - IN TREE - series ID 595 - 78affa14-a6cb-4f88-a57e-99cb308f99f1",
        "TEST series - IN TREE - series ID 596 - 8615b37b-1481-4388-8dea-0f5c266c3612",
        "TEST series - IN TREE - series ID 597 - be054193-535b-4bb9-b6e2-897d7a494156",
        "TEST series - IN TREE - series ID 598 - 9f6a4336-c74a-40d0-be03-1b57556a3d5e",
        "TEST series - IN TREE - series ID 599 - 7bd3252a-b053-4080-904f-8292b6a7981c",
        "TEST series - IN TREE - series ID 600 - c78882ba-397a-4677-954d-e3b330f7f16e",
        "TEST series - IN TREE - series ID 601 - 59055099-0f20-46b5-91c5-be0a0cdd0313",
        "TEST series - IN TREE - series ID 602 - 078c31b9-a5f2-42ac-8621-6d9bc9355d4f",
        "TEST series - IN TREE - series ID 603 - b8336bb0-8f93-49f5-9433-03e5d542d635",
        "TEST series - IN TREE - series ID 604 - e55f7696-d9d7-4127-b8b3-163d9c90df8d",
        "TEST series - IN TREE - series ID 605 - b0cd931d-5325-4331-8d08-c038682eb4c3",
        "TEST series - IN TREE - series ID 606 - f9a27800-4067-4b82-a3a7-e9d18a9b8bcf",
        "TEST series - IN TREE - series ID 607 - d2b9f985-b203-466b-97d2-bde0527d5763",
        "TEST series - IN TREE - series ID 608 - 010c49f8-eb17-4565-8818-9fdf516ef6cb",
        "TEST series - IN TREE - series ID 609 - 2f7de989-32fd-4263-8020-e99d9358a89a",
        "TEST series - IN TREE - series ID 610 - a2a2b752-5f8e-4702-9e49-3eaaa4a1fd5a",
        "TEST series - IN TREE - series ID 611 - 268b04fe-92df-4e2e-b23b-ce5b74dbafc2",
        "TEST series - IN TREE - series ID 612 - f8f24390-b627-423a-97a1-aa980159df44",
        "TEST series - IN TREE - series ID 613 - d73b1842-5312-4ddc-9277-da8e378bb8ab",
        "TEST series - IN TREE - series ID 614 - 1a169e8f-c7a3-45f4-8ea6-b3c5ee25d0e3",
        "TEST series - IN TREE - series ID 615 - 6e61e176-d873-4bef-a3f5-0eb41e20328d",
        "TEST series - IN TREE - series ID 616 - 7bd49e8c-2c72-4c12-98ce-84e57c71a634",
        "TEST series - IN TREE - series ID 617 - 0d4aa6b0-9c1c-4a14-a661-66838257faad",
        "TEST series - IN TREE - series ID 618 - 3939f972-fa38-45e1-9f3f-be69a8618ee2",
        "TEST series - IN TREE - series ID 619 - fc21000c-e59c-44f4-9624-022a7658da42",
        "TEST series - IN TREE - series ID 620 - fd1c0506-3859-4f96-b42a-1fe2f99e0b1e",
        "TEST series - IN TREE - series ID 621 - ee292414-a0f9-4400-b987-75669a211ca9",
        "TEST series - IN TREE - series ID 622 - ae50b90e-fbba-492b-bfbb-edfde40520b1",
        "TEST series - IN TREE - series ID 623 - c47407dd-bd9e-478b-adfa-0585e8dee677",
        "TEST series - IN TREE - series ID 624 - c282ee09-9acc-40be-b540-ab3613e6e818",
        "TEST series - IN TREE - series ID 625 - 8229a8b9-9ed9-43b5-8bb7-1b7bc64b9127",
        "TEST series - IN TREE - series ID 626 - 1d30e9e6-3ed4-4897-93e9-0b2929ff27f7",
        "TEST series - IN TREE - series ID 627 - 02c1e800-e4b1-41ea-9c03-9f1e945725f3",
        "TEST series - IN TREE - series ID 628 - 546667c5-de19-4c85-9d79-14ee90d9188a",
        "TEST series - IN TREE - series ID 629 - ea24b712-4bcb-4f21-b7ce-57db9c919e33",
        "TEST series - IN TREE - series ID 630 - d1f9c688-3046-4474-a99b-5d7bf1159101",
        "TEST series - IN TREE - series ID 631 - 5e3c2ba7-f3f0-46fe-a2b6-9a3162506d70",
        "TEST series - IN TREE - series ID 632 - 6e7e2e87-5443-40f6-aef6-552ee1ab5531",
        "TEST series - IN TREE - series ID 633 - eda46b9e-b66e-49bd-9458-68e86cc0d3d1",
        "TEST series - IN TREE - series ID 634 - 4bce79fc-83a2-43fb-8505-ef9a415bb35f",
        "TEST series - IN TREE - series ID 635 - fdddde7e-5574-478c-9608-4640e45f3ec6",
        "TEST series - IN TREE - series ID 636 - 28ef047e-f466-4721-b71f-dfb858e0b34a",
        "TEST series - IN TREE - series ID 637 - 60c2ab8f-ced7-4f8d-a977-9f2fb4be941c",
        "TEST series - IN TREE - series ID 638 - c31ad47d-6f6d-46ec-bab8-96c8945056bd",
        "TEST series - IN TREE - series ID 639 - fbfa7e8f-a517-4e4c-96be-5fb7acef8517",
        "TEST series - IN TREE - series ID 640 - c003f5f9-0394-48fc-afb3-88d78b9534dd",
        "TEST series - IN TREE - series ID 641 - 49178b1c-8c0e-46c9-a739-42a7c26271b5",
        "TEST series - IN TREE - series ID 642 - 39ce9a58-ea08-4f2c-994a-b49d61e9393d",
        "TEST series - IN TREE - series ID 643 - afaa77d3-ce17-4bc8-806f-4b2f1b278473",
        "TEST series - IN TREE - series ID 644 - 4da1e0c5-813a-4a01-8ef6-721d35650ce7",
        "TEST series - IN TREE - series ID 645 - 090a79e4-b4cf-40fa-9163-36773b820b92",
        "TEST series - IN TREE - series ID 646 - d759e0bc-426c-4831-80b2-4bf9460f5cf3",
        "TEST series - IN TREE - series ID 647 - 146ab80f-261f-44b3-a25a-85379e76abe9",
        "TEST series - IN TREE - series ID 648 - 49458593-07f1-48f6-834b-fbafdfab119d",
        "TEST series - IN TREE - series ID 649 - 52ee46ad-4981-44e1-98af-cf9379e9de5c",
        "TEST series - IN TREE - series ID 650 - c5c0e3b8-7f0a-4925-a326-cc5bf2c4ecd6",
        "TEST series - IN TREE - series ID 651 - 55323a3a-bacf-4056-b5f5-e6f540b92d05",
        "TEST series - IN TREE - series ID 652 - 96ebca6d-fd03-4449-995b-210a0fccf6a3",
        "TEST series - IN TREE - series ID 653 - f4217e2f-2e03-455d-bfbf-ed5964ae7ac0",
        "TEST series - IN TREE - series ID 654 - fd5ae8b5-3833-4466-ac34-2efb7dc13bc4",
        "TEST series - IN TREE - series ID 655 - 8cf64bff-18a0-4795-b44a-8ac350c5afa5",
        "TEST series - IN TREE - series ID 656 - c6bc06b8-530c-44d0-9acf-a5cc20b3a221",
        "TEST series - IN TREE - series ID 657 - 0f2d241b-1c38-48a3-9bce-49c99c47081e",
        "TEST series - IN TREE - series ID 658 - ef74e5a1-bc3a-465d-9d9b-db985dbe8b0c",
        "TEST series - IN TREE - series ID 659 - aec6fe91-2a11-4608-af38-a01d0e7cab1d",
        "TEST series - IN TREE - series ID 660 - f85be9c8-af5a-41d6-a47a-77baf31c5308",
        "TEST series - IN TREE - series ID 661 - 67013d5f-d85d-4308-a208-a5162afc51ab",
        "TEST series - IN TREE - series ID 662 - f4b6557c-4278-4e02-9d5f-7b2c88daa7e0",
        "TEST series - IN TREE - series ID 663 - 9b88b308-40de-4ed1-ab51-b7762b950e49",
        "TEST series - IN TREE - series ID 664 - 01627073-b7f5-4602-819a-b85ba07394ee",
        "TEST series - IN TREE - series ID 665 - 3dd78a6f-ff02-4916-a795-f35b7b86e84c",
        "TEST series - IN TREE - series ID 666 - 83c6871c-1ac1-4cec-9519-d6c5ad761b3b",
        "TEST series - IN TREE - series ID 667 - 3f2fa934-e8cb-4635-8619-7afe9b5a6cd5",
        "TEST series - IN TREE - series ID 668 - 184852b7-f79d-4cb3-acff-4a0597bd2f23",
        "TEST series - IN TREE - series ID 669 - d1803224-93e4-46ae-8756-bbe42796360e",
        "TEST series - IN TREE - series ID 670 - 65851d79-e6d6-4a89-9780-55b118cf0858",
        "TEST series - IN TREE - series ID 671 - 4f2ab892-6a87-4d46-b1fb-a56478f84958",
        "TEST series - IN TREE - series ID 672 - 6e2a8e93-b7ad-4673-bc9e-589294004c38",
        "TEST series - IN TREE - series ID 673 - 669ba846-6730-4fc8-9299-eb1a5c3c082c",
        "TEST series - IN TREE - series ID 674 - ba565492-f0f0-46c3-b50d-59fefdf04aca",
        "TEST series - IN TREE - series ID 675 - d6c55d6e-41ce-47d6-ac02-6311716cef04",
        "TEST series - IN TREE - series ID 676 - 79085d34-1849-4acf-802e-5580ad1c86bb",
        "TEST series - IN TREE - series ID 677 - 0c4ffde3-a02f-4461-a089-c72979297354",
        "TEST series - IN TREE - series ID 678 - e58e43b6-1488-4e47-8107-32c37d8f45e9",
        "TEST series - IN TREE - series ID 679 - add9e315-3502-4f4b-95f7-0ad22e5592e4",
        "TEST series - IN TREE - series ID 680 - 7b3e4793-3302-4af2-bd2d-9d903258d151",
        "TEST series - IN TREE - series ID 681 - 862d3f42-f2de-4063-ad83-603da1b68b6b",
        "TEST series - IN TREE - series ID 682 - d442da62-b938-41e1-a069-0fb8bb85f340",
        "TEST series - IN TREE - series ID 683 - efda05b4-aee0-4dcf-9eb6-463c2bbee461",
        "TEST series - IN TREE - series ID 684 - 8567479b-4856-44b3-bf9e-6ebd73476942",
        "TEST series - IN TREE - series ID 685 - e242db80-72b7-41d6-9fa4-d86b61e72318",
        "TEST series - IN TREE - series ID 686 - 30b7d9de-990b-4294-b2a5-2d6855c77b23",
        "TEST series - IN TREE - series ID 687 - 54e78ee6-a8f0-4d60-869f-86fc15dfc181",
        "TEST series - IN TREE - series ID 688 - a46b35bb-6de6-438f-b946-6c95a4b9fb0c",
        "TEST series - IN TREE - series ID 689 - 8327f6d6-7c16-4e27-87d7-453f66dacab3",
    ]

    series_5_tree_ids = set(
        [
            5,
            521,
            522,
            523,
            524,
            525,
            526,
            527,
            528,
            529,
            530,
            531,
            532,
            533,
            534,
            535,
            536,
            537,
            538,
            539,
            540,
            541,
            542,
            543,
            544,
            545,
            546,
            547,
            548,
            549,
            550,
            551,
            552,
            553,
            554,
            555,
            556,
            557,
            558,
            559,
            560,
            561,
            562,
            563,
            564,
            565,
            566,
            567,
            568,
            569,
            570,
            571,
            572,
            573,
            574,
            575,
            576,
            577,
            578,
            579,
            580,
            581,
            582,
            583,
            584,
            585,
            586,
            587,
            588,
            589,
            590,
            591,
            592,
            593,
            594,
            595,
            596,
            597,
            598,
            599,
            600,
            601,
            602,
            603,
            604,
            605,
            606,
            607,
            608,
            609,
            610,
            611,
            612,
            613,
            614,
            615,
            616,
            617,
            618,
            619,
            620,
            621,
            622,
            623,
            624,
            625,
            626,
            627,
            628,
            629,
            630,
            631,
            632,
            633,
            634,
            635,
            636,
            637,
            638,
            639,
            640,
            641,
            642,
            643,
            644,
            645,
            646,
            647,
            648,
            649,
            650,
            651,
            652,
            653,
            654,
            655,
            656,
            657,
            658,
            659,
            660,
            661,
            662,
            663,
            664,
            665,
            666,
            667,
            668,
            669,
            670,
            671,
            672,
            673,
            674,
            675,
            676,
            677,
            678,
            679,
            680,
            681,
            682,
            683,
            684,
            685,
            686,
            687,
            688,
            689,
        ]
    )

    title_5_series_count = 3

    title_5_series_id = 456
    title_5_primary_series_index = "14"

    # highest id from the series title link table
    series_title_highest_id = 25

    theo_title_1_series_strs = ["TEST series - IN TREE - series ID 349 - bcb2e4f4-516b-44a0-8279-a83af218b493"]

    theo_title_series_count_map = {
        1: 1,
        2: 2,
        3: 5,
        4: 2,
        5: 3,
        6: 4,
        7: 2,
        8: 0,
        9: 1,
        10: 5,
    }

    theo_title_1_series_index_pairs = [(349, 5)]
    # Sorted by priority
    theo_title_1_indexes = [
        5,
    ]
    theo_title_1_primary_series = "TEST series - IN TREE - series ID 349 - bcb2e4f4-516b-44a0-8279-a83af218b493"
    theo_title_1_series_ids = [
        349,
    ]

    theo_title_2_series_ids = [114, 643]

    theo_title_2_series_index_pairs = [(114, 1), (643, 18)]
    theo_title_2_indexes = [1, 18]
    theo_title_2_primary_series = "TEST series - IN TREE - series ID 114 - 29f18462-21ec-4886-9c1d-18e8dbeb54e8"

    theo_title_3_series_ids = [647, 577, 89, 248, 604]

    theo_title_5_series_index_pairs = [(456, 14), (525, 10), (197, 14)]
    theo_title_5_indexes = [_[1] for _ in theo_title_5_series_index_pairs]
    theo_title_5_primary_series = "TEST series - IN TREE - series ID 456 - c6bc06b8-530c-44d0-9acf-a5cc20b3a221"
    theo_title_5_series_ids = [_[0] for _ in theo_title_5_series_index_pairs]

    title_primary_series_map = {
        1: "TEST series - IN TREE - series ID 116 - 3f2fa934-e8cb-4635-8619-7afe9b5a6cd5",
        2: "TEST series - IN TREE - series ID 347 - 5211863b-3764-4ed2-b938-40ec0abc7b44",
        3: "TEST series - IN TREE - series ID 79 - d1f9c688-3046-4474-a99b-5d7bf1159101",
        4: None,
        5: "TEST series - IN TREE - series ID 245 - 8615b37b-1481-4388-8dea-0f5c266c3612",
        6: "TEST series - IN TREE - series ID 363 - 29f18462-21ec-4886-9c1d-18e8dbeb54e8",
        7: "TEST series - IN TREE - series ID 137 - a46b35bb-6de6-438f-b946-6c95a4b9fb0c",
        8: "TEST series - IN TREE - series ID 355 - 8d705893-e596-4e92-a839-4dffe4373177",
        9: "TEST series - IN TREE - series ID 228 - 088d5a29-3fd7-498f-92c4-4443efe66887",
        10: "TEST series - IN TREE - series ID 211 - d64415c9-1fa5-4edf-a61e-4e3d7a041699",
    }

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BOOKS

    book_1_uuid = "e5c7c4b6-b38c-47d0-9c25-f6620896a795"

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CREATORS

    # creator properties
    creator_title_link_types = frozenset(
        [
            "composers",
            "directors",
            "cover_artists",
            "colorists",
            "artists",
            "book_producer",
            "editors",
            "producers",
            "illustrators",
            "translators",
            "authors",
        ]
    )

    theo_creator_id_val_map = {
        1: "c-1-6e61e1",
        2: "c-2-ef74e5",
        3: "c-3-02c1e8",
        4: "c-4-c282ee",
        5: "c-5-dd2de1",
        6: "c-6-25233c",
        7: "c-7-c6bc06",
        8: "c-8-010c49",
        9: "c-9-d926b7",
        10: "c-10-635c7c",
        11: "c-11-546667",
        12: "c-12-f4b655",
        13: "c-13-68042e",
        14: "c-14-0cdf21",
        15: "c-15-590550",
        16: "c-16-a2a2b7",
        17: "c-17-b753a4",
        18: "c-18-aec6fe",
        19: "c-19-9b9e85",
        20: "c-20-7d4653",
        21: "c-21-56dd1b",
        22: "c-22-f576fc",
        23: "c-23-7b3e47",
        24: "c-24-184852",
        25: "c-25-aec6fe",
        26: "c-26-29f184",
        27: "c-27-c003f5",
        28: "c-28-d27ef2",
        29: "c-29-3542b6",
        30: "c-30-268b04",
        31: "c-31-e58e43",
        32: "c-32-79085d",
        33: "c-33-849b8e",
        34: "c-34-4cb1d3",
        35: "c-35-016270",
        36: "c-36-d1f9c6",
        37: "c-37-b5f0ae",
        38: "c-38-521186",
        39: "c-39-83c687",
        40: "c-40-5b6879",
        41: "c-41-fbfa7e",
        42: "c-42-b12cfb",
        43: "c-43-b2dc4d",
        44: "c-44-585a20",
        45: "c-45-a46b35",
        46: "c-46-6e2a8e",
        47: "c-47-fd5ae8",
        48: "c-48-fc2100",
        49: "c-49-4f2663",
        50: "c-50-da788a",
        51: "c-51-162981",
        52: "c-52-010c49",
        53: "c-53-c5c0e3",
        54: "c-54-b5f0ae",
        55: "c-55-9f6a43",
        56: "c-56-dec025",
        57: "c-57-ef74e5",
        58: "c-58-078c31",
        59: "c-59-8327f6",
        60: "c-60-be0541",
        61: "c-61-c5c0e3",
        62: "c-62-184852",
        63: "c-63-dd2de1",
        64: "c-64-8229a8",
        65: "c-65-d76513",
        66: "c-66-9b9e85",
        67: "c-67-dec025",
        68: "c-68-957fe4",
        69: "c-69-28ef04",
        70: "c-70-cdf28d",
        71: "c-71-b2dc4d",
        72: "c-72-56dd1b",
        73: "c-73-220590",
        74: "c-74-4f2ab8",
        75: "c-75-7d4653",
        76: "c-76-60c2ab",
        77: "c-77-b8336b",
        78: "c-78-eda46b",
        79: "c-79-032108",
        80: "c-80-7f93e2",
        81: "c-81-25233c",
        82: "c-82-590550",
        83: "c-83-bcb2e4",
        84: "c-84-957fe4",
        85: "c-85-fe882e",
        86: "c-86-fc2100",
        87: "c-87-e55f76",
        88: "c-88-862d3f",
        89: "c-89-77211d",
        90: "c-90-8229a8",
        91: "c-91-ab2f75",
        92: "c-92-ae50b9",
        93: "c-93-54b872",
        94: "c-94-268b04",
        95: "c-95-4f2663",
        96: "c-96-d926b7",
        97: "c-97-39ce9a",
        98: "c-98-0ea3ce",
        99: "c-99-c187b3",
        100: "c-100-1a169e",
    }

    creator_sort_map = {
        1: "cs-1-5a0977",
        2: "cs-2-f9a278",
        3: "cs-3-d18032",
        4: "cs-4-b753a4",
        5: "cs-5-957fe4",
        6: "cs-6-f576fc",
        7: "cs-7-654b77",
        8: "cs-8-60c2ab",
        9: "cs-9-4cb1d3",
        10: "cs-10-55085f",
        11: "cs-11-a96c4b",
        12: "cs-12-078c31",
        13: "cs-13-b2dc4d",
        14: "cs-14-02c1e8",
        15: "cs-15-78affa",
        16: "cs-16-036a5f",
        17: "cs-17-862d3f",
        18: "cs-18-0ea3ce",
        19: "cs-19-56f272",
        20: "cs-20-e55f76",
        21: "cs-21-83a063",
        22: "cs-22-f4fdb0",
        23: "cs-23-ea487a",
        24: "cs-24-d2b9f9",
        25: "cs-25-4bce79",
        26: "cs-26-c47407",
        27: "cs-27-d76513",
        28: "cs-28-7bd49e",
        29: "cs-29-2f7de9",
        30: "cs-30-016270",
        31: "cs-31-090a79",
        32: "cs-32-fe882e",
        33: "cs-33-9b67e8",
        34: "cs-34-c187b3",
        35: "cs-35-55323a",
        36: "cs-36-585a20",
        37: "cs-37-856747",
        38: "cs-38-5331f3",
        39: "cs-39-96ebca",
        40: "cs-40-79085d",
        41: "cs-41-25233c",
        42: "cs-42-a3d0b4",
        43: "cs-43-9ba480",
        44: "cs-44-c9e230",
        45: "cs-45-b0cd93",
        46: "cs-46-fd5ae8",
        47: "cs-47-b8336b",
        48: "cs-48-494585",
        49: "cs-49-d4201a",
        50: "cs-50-78affa",
        51: "cs-51-c7a73b",
        52: "cs-52-d4201a",
        53: "cs-53-da788a",
        54: "cs-54-8327f6",
        55: "cs-55-fd1c05",
        56: "cs-56-0f2d24",
        57: "cs-57-65851d",
        58: "cs-58-83c687",
        59: "cs-59-01afd8",
        60: "cs-60-ea487a",
        61: "cs-61-ea487a",
        62: "cs-62-dd2de1",
        63: "cs-63-220590",
        64: "cs-64-0c4ffd",
        65: "cs-65-da788a",
        66: "cs-66-c7a73b",
        67: "cs-67-d1f9c6",
        68: "cs-68-f8f243",
        69: "cs-69-e58e43",
        70: "cs-70-7bd325",
        71: "cs-71-546667",
        72: "cs-72-d6c55d",
        73: "cs-73-4324fd",
        74: "cs-74-cdf28d",
        75: "cs-75-0d4d18",
        76: "cs-76-d926b7",
        77: "cs-77-7bd49e",
        78: "cs-78-ee2924",
        79: "cs-79-856747",
        80: "cs-80-1d0e63",
        81: "cs-81-d27ef2",
        82: "cs-82-6e2a8e",
        83: "cs-83-f576fc",
        84: "cs-84-984d4d",
        85: "cs-85-55323a",
        86: "cs-86-813440",
        87: "cs-87-56dd1b",
        88: "cs-88-5a232c",
        89: "cs-89-c5c0e3",
        90: "cs-90-5ec229",
        91: "cs-91-984d4d",
        92: "cs-92-4cd960",
        93: "cs-93-49b21b",
        94: "cs-94-010c49",
        95: "cs-95-0ea3ce",
        96: "cs-96-ea24b7",
        97: "cs-97-3542b6",
        98: "cs-98-54b872",
        99: "cs-99-184852",
        100: "cs-100-6e2a8e",
    }

    title_1_author_names = (
        "c-26-29f184",
        "c-31-e58e43",
        "c-41-fbfa7e",
        "c-71-b2dc4d",
        "c-94-268b04",
    )
    title_1_author_ids = (26, 31, 41, 71, 94)

    assert len(title_1_author_names) == len(title_1_author_ids)

    title_4_author_names = ("c-12-f4b655",)
    title_4_author_ids = (12,)

    assert len(title_4_author_names) == len(title_4_author_ids)

    title_5_author_names = ("c-23-7b3e47", "c-64-8229a8")
    title_5_author_ids = (23, 64)

    assert len(title_5_author_names) == len(title_5_author_ids)

    creators_theo_book_col_map = {
        1: {
            "composers": set([12, 87]),
            "cover_artists": set([33, 5, 22]),
            "illustrators": set([52]),
            "colorists": set([89]),
            "authors": set([41, 26, 31, 94, 71]),
            "editors": set([24, 58, 62, 23]),
            "producers": set([48, 98, 3, 37, 55]),
            "directors": set([73, 11, 29]),
            "translators": set([66, 91, 69, 78, 21]),
            "artists": set([1, 7]),
        },
        2: {
            "composers": set([72, 9, 45, 62]),
            "cover_artists": set([64, 48, 74, 34]),
            "illustrators": set([41, 5, 70]),
            "colorists": set([89, 26, 52, 36, 57]),
            "authors": set([73, 90, 11]),
            "book_producer": set([88, 42, 58, 2]),
            "producers": set([3, 12]),
            "directors": set([17, 60, 77, 78, 47]),
            "translators": set([59]),
            "artists": set([16, 76, 54]),
        },
        3: {
            "composers": set([32, 97]),
            "cover_artists": set([52, 21, 86]),
            "illustrators": set([41, 53, 46, 61]),
            "colorists": set([62, 3, 84, 22, 70]),
            "authors": set([82, 10, 37, 31]),
            "editors": set([50]),
            "book_producer": set([91, 87]),
            "producers": set([93, 54, 15]),
            "directors": set([94, 71]),
            "translators": set([64, 51, 45, 77, 29]),
            "artists": set([8, 49]),
        },
        4: {
            "composers": set([40, 74, 76, 69]),
            "cover_artists": set([82, 2, 70, 29]),
            "illustrators": set([83]),
            "colorists": set([30]),
            "authors": set([12]),
            "editors": set([8, 91, 51]),
            "book_producer": set([67, 36, 71]),
            "producers": set([66]),
            "directors": set([61]),
            "translators": set([14]),
            "artists": set([47]),
        },
        5: {
            "composers": set([48, 52, 34, 76, 14]),
            "cover_artists": set([30]),
            "illustrators": set([83]),
            "colorists": set([54]),
            "authors": set([64, 23]),
            "editors": set([67, 92, 29]),
            "book_producer": set([69, 74, 87, 24, 58, 28]),
            "producers": set([1, 79, 38, 22, 89]),
            "directors": set([88, 57, 90, 25, 55]),
            "translators": set([17, 45, 9]),
            "artists": set([37, 46]),
        },
        6: {
            "composers": set([97, 21]),
            "cover_artists": set([48, 81]),
            "illustrators": set([8, 17, 28, 29]),
            "colorists": set([90, 15, 84, 62, 63]),
            "authors": set([80, 68, 26, 52]),
            "editors": set([65, 3, 38, 25]),
            "book_producer": set([11, 1, 82, 87, 39]),
            "producers": set([98]),
            "directors": set([9, 30, 14, 78]),
            "artists": set([50, 55]),
        },
        7: {
            "composers": set([34, 86, 70, 48, 18, 54]),
            "cover_artists": set([88, 64, 13, 94, 7]),
            "illustrators": set([40, 57, 36, 23]),
            "colorists": set([25, 90, 71, 44, 79]),
            "authors": set([35, 72, 43, 45, 87, 55]),
            "editors": set([6]),
            "book_producer": set([32]),
            "producers": set([8, 1]),
            "directors": set([80, 58, 62]),
            "translators": set([9, 51]),
            "artists": set([61, 31]),
        },
        8: {
            "composers": set([43, 100]),
            "cover_artists": set([17, 90]),
            "illustrators": set([67, 23, 76, 79]),
            "colorists": set([3, 72, 16, 99, 52, 95]),
            "authors": set([6]),
            "editors": set([36, 39, 41, 84, 22, 30]),
            "book_producer": set([25, 81]),
            "producers": set([66, 11, 68, 58, 13]),
            "directors": set([34]),
            "artists": set([24, 73, 98, 59, 65]),
        },
        9: {
            "composers": set([91, 79]),
            "cover_artists": set([82, 3, 20, 53]),
            "illustrators": set([11]),
            "colorists": set([23, 47, 28, 31]),
            "authors": set([40, 73, 25]),
            "editors": set([17, 66, 84, 38, 94]),
            "book_producer": set([41, 61, 95]),
            "producers": set([76]),
            "directors": set([9, 50, 65, 54, 87]),
            "translators": set([81, 75, 49]),
            "artists": set([80]),
        },
        10: {
            "composers": set([20, 29]),
            "cover_artists": set([65, 66, 82, 30]),
            "illustrators": set([8, 90, 5, 45]),
            "colorists": set([62, 63]),
            "authors": set([40, 44, 93, 6, 55]),
            "editors": set([46]),
            "book_producer": set([48, 57, 31, 47, 71]),
            "producers": set([88]),
            "directors": set([9]),
            "translators": set([100, 51, 68, 13, 54]),
            "artists": set([42]),
        },
    }

    creators_theo_book_col_map_ordered = {
        1: {
            "composers": [87, 12],
            "cover_artists": [5, 33, 22],
            "translators": [21, 91, 78, 66, 69],
            "colorists": [89],
            "authors": [26, 31, 41, 71, 94],
            "editors": [24, 62, 58, 23],
            "book_producer": [],
            "producers": [37, 98, 55, 48, 3],
            "illustrators": [52],
            "directors": [11, 29, 73],
            "artists": [1, 7],
        },
        2: {
            "composers": [72, 45, 9, 62],
            "cover_artists": [34, 48, 74, 64],
            "translators": [59],
            "colorists": [36, 26, 52, 57, 89],
            "authors": [11, 90, 73],
            "book_producer": [58, 2, 88, 42],
            "producers": [3, 12],
            "illustrators": [5, 70, 41],
            "directors": [17, 47, 60, 77, 78],
            "artists": [76, 16, 54],
        },
        3: {
            "composers": [32, 97],
            "cover_artists": [21, 86, 52],
            "translators": [64, 77, 51, 29, 45],
            "colorists": [62, 3, 84, 22, 70],
            "authors": [31, 10, 37, 82],
            "book_producer": [91, 87],
            "editors": [50],
            "producers": [15, 54, 93],
            "illustrators": [41, 61, 46, 53],
            "directors": [94, 71],
            "artists": [8, 49],
        },
        4: {
            "composers": [74, 76, 69, 40],
            "cover_artists": [29, 70, 2, 82],
            "translators": [14],
            "colorists": [30],
            "authors": [12],
            "book_producer": [71, 36, 67],
            "editors": [91, 51, 8],
            "producers": [66],
            "illustrators": [83],
            "directors": [61],
            "artists": [47],
        },
        5: {
            "composers": [52, 48, 14, 34, 76],
            "cover_artists": [30],
            "translators": [45, 9, 17],
            "colorists": [54],
            "authors": [23, 64],
            "book_producer": [24, 28, 74, 58, 69, 87],
            "editors": [29, 67, 92],
            "producers": [79, 38, 89, 22, 1],
            "illustrators": [83],
            "directors": [25, 57, 90, 55, 88],
            "artists": [37, 46],
        },
        6: {
            "composers": [97, 21],
            "cover_artists": [81, 48],
            "colorists": [15, 63, 90, 62, 84],
            "authors": [68, 26, 80, 52],
            "book_producer": [82, 11, 1, 87, 39],
            "editors": [25, 38, 3, 65],
            "producers": [98],
            "illustrators": [29, 17, 28, 8],
            "directors": [30, 14, 9, 78],
            "artists": [50, 55],
        },
        7: {
            "composers": [54, 86, 18, 70, 34, 48],
            "cover_artists": [64, 88, 94, 13, 7],
            "translators": [9, 51],
            "colorists": [71, 44, 25, 90, 79],
            "authors": [72, 35, 87, 45, 55, 43],
            "book_producer": [32],
            "editors": [6],
            "producers": [8, 1],
            "illustrators": [23, 36, 57, 40],
            "directors": [80, 62, 58],
            "artists": [61, 31],
        },
        8: {
            "composers": [100, 43],
            "cover_artists": [90, 17],
            "colorists": [72, 95, 3, 52, 99, 16],
            "authors": [6],
            "book_producer": [81, 25],
            "editors": [39, 30, 22, 41, 84, 36],
            "producers": [13, 68, 58, 66, 11],
            "illustrators": [76, 67, 23, 79],
            "directors": [34],
            "artists": [65, 59, 24, 98, 73],
        },
        9: {
            "composers": [79, 91],
            "cover_artists": [53, 20, 3, 82],
            "translators": [75, 49, 81],
            "colorists": [23, 28, 47, 31],
            "authors": [25, 73, 40],
            "book_producer": [61, 95, 41],
            "editors": [17, 84, 38, 94, 66],
            "producers": [76],
            "illustrators": [11],
            "directors": [65, 9, 87, 54, 50],
            "artists": [80],
        },
        10: {
            "composers": [20, 29],
            "cover_artists": [30, 82, 65, 66],
            "translators": [100, 13, 51, 54, 68],
            "colorists": [62, 63],
            "authors": [40, 55, 6, 93, 44],
            "book_producer": [47, 57, 31, 48, 71],
            "editors": [46],
            "producers": [88],
            "illustrators": [45, 90, 5, 8],
            "directors": [9],
            "artists": [42],
        },
    }

    # title-creator properties
    theo_title_1_creator_count = 31
    theo_title_1_author_count = 5
    theo_title_1_creator_list = [
        "c-21-56dd1b",
        "c-91-ab2f75",
        "c-78-eda46b",
        "c-66-9b9e85",
        "c-69-28ef04",
        "c-5-dd2de1",
        "c-33-849b8e",
        "c-22-f576fc",
        "c-11-546667",
        "c-29-3542b6",
        "c-73-220590",
        "c-37-b5f0ae",
        "c-98-0ea3ce",
        "c-55-9f6a43",
        "c-48-fc2100",
        "c-3-02c1e8",
        "c-87-e55f76",
        "c-12-f4b655",
        "c-24-184852",
        "c-62-184852",
        "c-58-078c31",
        "c-23-7b3e47",
        "c-52-010c49",
        "c-26-29f184",
        "c-31-e58e43",
        "c-41-fbfa7e",
        "c-71-b2dc4d",
        "c-94-268b04",
        "c-1-6e61e1",
        "c-7-c6bc06",
        "c-89-77211d",
    ]
    theo_title_1_creator_ids_list = [
        21,
        91,
        78,
        66,
        69,
        5,
        33,
        22,
        11,
        29,
        73,
        37,
        98,
        55,
        48,
        3,
        87,
        12,
        24,
        62,
        58,
        23,
        52,
        26,
        31,
        41,
        71,
        94,
        1,
        7,
        89,
    ]

    theo_title_1_creator_data = creators_theo_book_col_map_ordered[1]
    theo_title_1_creator_data_set = {lt: set(lv) for lt, lv in iteritems(theo_title_1_creator_data)}

    theo_title_1_creator_vals = dict()
    for _ in theo_title_1_creator_data:
        theo_title_1_creator_vals[_] = []
        for __ in theo_title_1_creator_data[_]:
            theo_title_1_creator_vals[_].append(theo_creator_id_val_map[__])

    theo_title_1_creator_vals_set = {lt: set(lv) for lt, lv in iteritems(theo_title_1_creator_vals)}

    theo_title_4_author_count = len(creators_theo_book_col_map_ordered[4]["authors"])
    theo_title_4_editor_count = len(creators_theo_book_col_map_ordered[4]["editors"])

    theo_title_4_creator_data = creators_theo_book_col_map_ordered[4]
    theo_title_4_creator_data_set = {lt: set(lv) for lt, lv in iteritems(theo_title_4_creator_data)}

    theo_title_4_creator_vals = dict()
    for _ in theo_title_4_creator_data:
        theo_title_4_creator_vals[_] = []
        for __ in theo_title_4_creator_data[_]:
            theo_title_4_creator_vals[_].append(theo_creator_id_val_map[__])

    theo_title_4_creator_vals_set = {lt: set(lv) for lt, lv in iteritems(theo_title_4_creator_vals)}

    theo_title_4_creator_ids_list = [
        14,
        29,
        70,
        2,
        82,
        61,
        66,
        71,
        36,
        67,
        74,
        76,
        69,
        40,
        91,
        51,
        8,
        83,
        12,
        47,
        30,
    ]
    theo_title_4_creator_count = len(theo_title_4_creator_ids_list)
    theo_title_4_creator_list = []
    for _ in theo_title_4_creator_ids_list:
        theo_title_4_creator_list.append(theo_creator_id_val_map[_])

    theo_title_5_creator_ids_list = [
        45,
        9,
        17,
        30,
        25,
        57,
        90,
        55,
        88,
        79,
        38,
        89,
        22,
        1,
        24,
        28,
        74,
        58,
        69,
        87,
        52,
        48,
        14,
        34,
        76,
        29,
        67,
        92,
        83,
        23,
        64,
        37,
        46,
        54,
    ]
    theo_title_5_creator_list = []
    for _ in theo_title_5_creator_ids_list:
        theo_title_5_creator_list.append(theo_creator_id_val_map[_])

    theo_title_5_creator_count = len(theo_title_5_creator_ids_list)

    theo_title_5_creator_data = {
        "composers": [52, 48, 14, 34, 76],
        "cover_artists": [30],
        "illustrators": [83],
        "colorists": [54],
        "artists": [37, 46],
        "book_producer": [24, 28, 74, 58, 69, 87],
        "editors": [29, 67, 92],
        "producers": [79, 38, 89, 22, 1],
        "directors": [25, 57, 90, 55, 88],
        "translators": [45, 9, 17],
        "authors": [23, 64],
    }
    theo_title_5_creator_data_set = {lt: set(lv) for lt, lv in iteritems(theo_title_5_creator_data)}
    theo_title_5_creator_vals = dict()
    for _ in theo_title_5_creator_data:
        theo_title_5_creator_vals[_] = []
        for __ in theo_title_5_creator_data[_]:
            theo_title_5_creator_vals[_].append(theo_creator_id_val_map[__])

    theo_title_5_creator_vals_set = {lt: set(lv) for lt, lv in iteritems(theo_title_5_creator_vals)}

    theo_creator_1_val = theo_creator_id_val_map[1]
    theo_creator_2_val = theo_creator_id_val_map[2]
    theo_creator_3_val = theo_creator_id_val_map[3]
    theo_creator_4_val = theo_creator_id_val_map[4]
    theo_creator_5_val = theo_creator_id_val_map[5]
    theo_creator_6_val = theo_creator_id_val_map[6]
    theo_creator_7_val = theo_creator_id_val_map[7]
    theo_creator_8_val = theo_creator_id_val_map[8]
    theo_creator_9_val = theo_creator_id_val_map[9]
    theo_creator_10_val = theo_creator_id_val_map[10]
    theo_creator_11_val = theo_creator_id_val_map[11]
    theo_creator_12_val = theo_creator_id_val_map[12]
    theo_creator_13_val = theo_creator_id_val_map[13]
    theo_creator_14_val = theo_creator_id_val_map[14]
    theo_creator_15_val = theo_creator_id_val_map[15]
    theo_creator_16_val = theo_creator_id_val_map[16]
    theo_creator_17_val = theo_creator_id_val_map[17]
    theo_creator_18_val = theo_creator_id_val_map[18]
    theo_creator_19_val = theo_creator_id_val_map[19]
    theo_creator_20_val = theo_creator_id_val_map[20]

    theo_creator_23_val = theo_creator_id_val_map[23]

    theo_creator_25_val = theo_creator_id_val_map[25]
    theo_creator_26_val = theo_creator_id_val_map[26]
    theo_creator_27_val = theo_creator_id_val_map[27]

    theo_creator_29_val = theo_creator_id_val_map[29]

    theo_creator_32_val = theo_creator_id_val_map[32]

    theo_creator_34_val = theo_creator_id_val_map[34]
    theo_creator_35_val = theo_creator_id_val_map[35]
    theo_creator_36_val = theo_creator_id_val_map[36]

    theo_creator_38_val = theo_creator_id_val_map[38]
    theo_creator_39_val = theo_creator_id_val_map[39]

    theo_creator_41_val = theo_creator_id_val_map[41]
    theo_creator_42_val = theo_creator_id_val_map[42]
    theo_creator_43_val = theo_creator_id_val_map[43]

    theo_creator_47_val = theo_creator_id_val_map[47]
    theo_creator_48_val = theo_creator_id_val_map[48]

    theo_creator_55_val = theo_creator_id_val_map[55]
    theo_creator_56_val = theo_creator_id_val_map[56]
    theo_creator_57_val = theo_creator_id_val_map[57]
    theo_creator_58_val = theo_creator_id_val_map[58]

    theo_creator_62_val = theo_creator_id_val_map[62]
    theo_creator_63_val = theo_creator_id_val_map[63]
    theo_creator_64_val = theo_creator_id_val_map[64]

    theo_creator_67_val = theo_creator_id_val_map[67]

    theo_creator_69_val = theo_creator_id_val_map[69]

    theo_creator_71_val = theo_creator_id_val_map[71]

    theo_creator_76_val = theo_creator_id_val_map[76]

    theo_creator_79_val = theo_creator_id_val_map[79]
    theo_creator_80_val = theo_creator_id_val_map[80]

    theo_creator_81_val = theo_creator_id_val_map[81]
    theo_creator_82_val = theo_creator_id_val_map[82]
    theo_creator_83_val = theo_creator_id_val_map[83]

    theo_creator_86_val = theo_creator_id_val_map[86]
    theo_creator_87_val = theo_creator_id_val_map[87]
    theo_creator_88_val = theo_creator_id_val_map[88]
    theo_creator_89_val = theo_creator_id_val_map[89]
    theo_creator_90_val = theo_creator_id_val_map[90]
    theo_creator_91_val = theo_creator_id_val_map[91]
    theo_creator_92_val = theo_creator_id_val_map[92]
    theo_creator_93_val = theo_creator_id_val_map[93]
    theo_creator_94_val = theo_creator_id_val_map[94]

    theo_creator_95_val = theo_creator_id_val_map[95]

    theo_creator_99_val = theo_creator_id_val_map[99]
    theo_creator_100_val = theo_creator_id_val_map[100]

    creator_1_title_ids_list = [7, 6, 5, 1]
    creator_1_title_ids_dict = {
        "producers": [7, 5],
        "book_producer": [6],
        "artists": [1],
    }
    creator_1_title_ids_dict_set = {k: set(v) for k, v in iteritems(creator_1_title_ids_dict)}

    creator_4_title_ids_list = []
    creator_4_title_ids_dict = dict()
    creator_4_title_ids_dict_set = {k: set(v) for k, v in iteritems(creator_4_title_ids_dict)}

    creator_5_title_ids_list = [10, 2, 1]
    creator_5_title_ids_dict = {"cover_artists": [1], "illustrators": [10, 2]}
    creator_5_title_ids_dict_set = {k: set(v) for k, v in iteritems(creator_5_title_ids_dict)}

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - COMMENTS

    # comment properties
    title_1_comment_count = 5
    title_1_comment = "TEST COMMENT - TITLE 1 - NOTE NUM - 5 - 378357dc-0b5e-4a91-afc9-37c1b37a98fc"
    theo_cache_title_1_comment_ids = (265,)

    title_2_comment_count = 0
    title_2_comment = None

    title_3_comment_count = 4
    title_3_comment = "TEST COMMENT - TITLE 3 - NOTE NUM - 4 - 8cf64bff-18a0-4795-b44a-8ac350c5afa5"

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - PUBLISHERS

    publisher_theo_book_col_map = {
        1: (692, 334),
        2: (428, 83, 625),
        3: (58, 223, 518, 1, 26, 127),
        4: (154, 339, 169),
        5: (469, 85, 39, 637, 244),
        6: (378,),
        7: (592, 606, 473, 266, 318),
        8: (382, 423, 493, 120, 365, 609),
        9: (463, 537),
        10: (345, 415),
    }
    publisher_theo_col_book_map = {
        1: set([3]),
        518: set([3]),
        266: set([7]),
        493: set([8]),
        345: set([10]),
        537: set([9]),
        154: set([4]),
        26: set([3]),
        415: set([10]),
        39: set([5]),
        169: set([4]),
        428: set([2]),
        692: set([1]),
        58: set([3]),
        318: set([7]),
        334: set([1]),
        463: set([9]),
        592: set([7]),
        339: set([4]),
        469: set([5]),
        473: set([7]),
        606: set([7]),
        223: set([3]),
        609: set([8]),
        423: set([8]),
        365: set([8]),
        625: set([2]),
        83: set([2]),
        244: set([5]),
        120: set([8]),
        378: set([6]),
        127: set([3]),
        637: set([5]),
        382: set([8]),
        85: set([5]),
    }

    theo_title_1_publishers_count = 2
    theo_title_1_publishers = (
        "TEST publishers - IN TREE - publishers ID 692 - dd2de1d2-9d42-4c07-9447-b0f2ac941e86",
        "TEST publishers - IN TREE - publishers ID 334 - b0cd931d-5325-4331-8d08-c038682eb4c3",
    )
    theo_title_1_publisher_ids = (692, 334)

    theo_title_2_publishers_count = 3

    theo_title_5_publishers_count = 5
    theo_title_5_publishers = (
        "TEST publishers - IN TREE - publishers ID 469 - da788a67-4603-4b71-8ceb-55d3b48ac79e",
        "TEST publishers - IN TREE - publishers ID 85 - 42d53491-dc6c-4e9b-b8c3-d303e16ba806",
        "TEST publishers - IN TREE - publishers ID 39 - a471e7c5-6a0f-415c-81b5-d69b1997b365",
        "TEST publishers - IN TREE - publishers ID 637 - 03210848-24c6-42ac-b918-74aba63e40f3",
        "TEST publishers - IN TREE - publishers ID 244 - 29686bc4-0955-4d4b-8717-c225be9f14db",
    )
    theo_title_5_publisher_ids = (469, 85, 39, 637, 244)

    theo_title_pub_map = {1: 2, 2: 3, 3: 6, 4: 3, 5: 5, 6: 1, 7: 5, 8: 6, 9: 2, 10: 2}

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - IDENTIFIERS

    title_1_ids_count = 0
    title_1_isbn_count = 0

    title_2_ids_count = 1
    title_2_isbn_count = 0

    theo_id_row_1_val = (
        "TEST EXTERNAL IDENTIFIER - TYPE amazon - TITLE 2 - ID NUM 1 - 8615b37b-1481-4388-8dea-0f5c266c3612"
    )

    # Identifiers properties
    theo_books_with_isbns = {10, 7}

    title_identifiers_map = {
        1: {},
        2: {"amazon": set([1])},
        3: {},
        4: {"doi": set([2, 6]), "uri": set([4]), "ff": set([5]), "uuid": set([3])},
        5: {
            "lccn": set([7]),
            "douban": set([9]),
            "google": set([10]),
            "ff": set([8]),
            "goodreads": set([11]),
        },
        6: {},
        7: {"amazon": set([14]), "isbn": set([13]), "ff": set([12])},
        8: {"douban": set([18, 15]), "issn": set([16]), "goodreads": set([17])},
        9: {"google": set([20]), "ff": set([19])},
        10: {
            "amazon": set([21]),
            "issn": set([22]),
            "isbn": set([23]),
            "uuid": set([25]),
            "uri": set([24]),
        },
    }

    title_1_isbns = title_identifiers_map[1].get("isbn", None)

    title_9_isbn_ids = title_identifiers_map[9].get("isbn", None)
    title_9_isbn_count = len(title_9_isbn_ids) if title_9_isbn_ids is not None else 0
    title_9_isbns = set()

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - LANGUAGES

    theo_title_1_lang_count = 4

    theo_lang_1_row_value = "TEST LANGUAGE - 1 - DELETE ME - d76513c3-8c70-422c-9fc2-793ad6b03180"

    title_1_lang_code = "TEST LANGUAGE CODE - 9 - DELETE ME - 1cd4b089-63e0-4340-b387-4275a9e18a51"

    title_primary_lang_map = {
        1: "TEST LANGUAGE CODE - 9 - DELETE ME - 1cd4b089-63e0-4340-b387-4275a9e18a51",
        2: "TEST LANGUAGE CODE - 8 - DELETE ME - 010c49f8-eb17-4565-8818-9fdf516ef6cb",
        3: "TEST LANGUAGE CODE - 8 - DELETE ME - 010c49f8-eb17-4565-8818-9fdf516ef6cb",
        4: "TEST LANGUAGE CODE - 3 - DELETE ME - 1cd4b089-63e0-4340-b387-4275a9e18a51",
        5: "TEST LANGUAGE CODE - 9 - DELETE ME - 1cd4b089-63e0-4340-b387-4275a9e18a51",
        6: "TEST LANGUAGE CODE - 2 - DELETE ME - 67013d5f-d85d-4308-a208-a5162afc51ab",
        7: "TEST LANGUAGE CODE - 4 - DELETE ME - 83a0633c-042d-41ba-a447-a248c4a0cd0c",
        8: "TEST LANGUAGE CODE - 5 - DELETE ME - fdddde7e-5574-478c-9608-4640e45f3ec6",
        9: "TEST LANGUAGE CODE - 7 - DELETE ME - e08fb6ba-3808-4895-81e2-a9638dc29cee",
        10: "TEST LANGUAGE CODE - 2 - DELETE ME - 67013d5f-d85d-4308-a208-a5162afc51ab",
    }

    theo_known_language_types = set(["available_language", "contained_in", "primary"])
    theo_title_1_language_types = set(["available_language", "primary"])

    theo_title_1_lang_link_pairs = {
        (1, 2),
        (1, 4),
        (1, 9),
    }

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TAGS

    theo_tag_id_val_map = {
        1: "TEST TAG - 1 - f85be9c8-af5a-41d6-a47a-77baf31c5308",
        2: "TEST TAG - 2 - 79085d34-1849-4acf-802e-5580ad1c86bb",
        3: "TEST TAG - 3 - 5e3c2ba7-f3f0-46fe-a2b6-9a3162506d70",
        4: "TEST TAG - 4 - 3cce4d59-b8a0-4f83-9b59-9dd3500a8a8c",
        5: "TEST TAG - 5 - 9f6a4336-c74a-40d0-be03-1b57556a3d5e",
        6: "TEST TAG - 6 - ea487aed-0a6d-4d1d-9b73-dca14f96e4dd",
        7: "TEST TAG - 7 - c78882ba-397a-4677-954d-e3b330f7f16e",
        8: "TEST TAG - 8 - e5c7c4b6-b38c-47d0-9c25-f6620896a795",
        9: "TEST TAG - 9 - d926b73e-bace-4d92-9406-a38e4b5de7b7",
        10: "TEST TAG - 10 - 03210848-24c6-42ac-b918-74aba63e40f3",
    }

    theo_tag_1_titles = frozenset([1, 7, 9])
    theo_tag_1_title_count = len(theo_tag_1_titles)

    theo_tag_5_titles = frozenset(
        [
            6,
        ]
    )
    theo_tag_5_title_count = len(theo_tag_5_titles)

    theo_book_1_tag_ids = {8, 1, 3}
    theo_book_1_tag_values = {
        "TEST TAG - 8 - e5c7c4b6-b38c-47d0-9c25-f6620896a795",
        "TEST TAG - 1 - f85be9c8-af5a-41d6-a47a-77baf31c5308",
        "TEST TAG - 3 - 5e3c2ba7-f3f0-46fe-a2b6-9a3162506d70",
    }

    theo_book_5_tag_ids = {9, 7}
    theo_book_5_tag_values = {
        "TEST TAG - 9 - d926b73e-bace-4d92-9406-a38e4b5de7b7",
        "TEST TAG - 7 - c78882ba-397a-4677-954d-e3b330f7f16e",
    }

    theo_all_tag_ids = frozenset([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    theo_tag_id_tag_val_map = {
        1: "TEST TAG - 1 - f85be9c8-af5a-41d6-a47a-77baf31c5308",
        2: "TEST TAG - 2 - 79085d34-1849-4acf-802e-5580ad1c86bb",
        3: "TEST TAG - 3 - 5e3c2ba7-f3f0-46fe-a2b6-9a3162506d70",
        4: "TEST TAG - 4 - 3cce4d59-b8a0-4f83-9b59-9dd3500a8a8c",
        5: "TEST TAG - 5 - 9f6a4336-c74a-40d0-be03-1b57556a3d5e",
        6: "TEST TAG - 6 - ea487aed-0a6d-4d1d-9b73-dca14f96e4dd",
        7: "TEST TAG - 7 - c78882ba-397a-4677-954d-e3b330f7f16e",
        8: "TEST TAG - 8 - e5c7c4b6-b38c-47d0-9c25-f6620896a795",
        9: "TEST TAG - 9 - d926b73e-bace-4d92-9406-a38e4b5de7b7",
        10: "TEST TAG - 10 - 03210848-24c6-42ac-b918-74aba63e40f3",
    }

    theo_titles_with_tags = set([1, 3, 4, 5, 6, 7, 9, 10])

    theo_tags_searchable_values_map = {
        "TEST TAG - 4 - 3cce4d59-b8a0-4f83-9b59-9dd3500a8a8c": set([10, 7]),
        "TEST TAG - 5 - 9f6a4336-c74a-40d0-be03-1b57556a3d5e": set([6]),
        "TEST TAG - 10 - 03210848-24c6-42ac-b918-74aba63e40f3": set([3]),
        "TEST TAG - 7 - c78882ba-397a-4677-954d-e3b330f7f16e": set([9, 5, 7]),
        "TEST TAG - 8 - e5c7c4b6-b38c-47d0-9c25-f6620896a795": set([1]),
        "TEST TAG - 9 - d926b73e-bace-4d92-9406-a38e4b5de7b7": set([5, 7]),
        "TEST TAG - 1 - f85be9c8-af5a-41d6-a47a-77baf31c5308": set([1, 9, 7]),
        "TEST TAG - 3 - 5e3c2ba7-f3f0-46fe-a2b6-9a3162506d70": set([1]),
        "TEST TAG - 2 - 79085d34-1849-4acf-802e-5580ad1c86bb": set([9, 10, 6]),
        "TEST TAG - 6 - ea487aed-0a6d-4d1d-9b73-dca14f96e4dd": set([9, 10, 3, 4]),
    }

    theo_tag_title_counts = {
        1: 3,
        2: 3,
        3: 1,
        4: 2,
        5: 1,
        6: 4,
        7: 3,
        8: 1,
        9: 2,
        10: 1,
    }
    tags_count_map_dict = {
        1: set([8, 10, 3, 5]),
        2: set([9, 4]),
        3: set([1, 2, 7]),
        4: set([6]),
    }

    theo_tags_count_map = [
        (1, set([3, 5, 8, 10])),
        (2, set([4, 9])),
        (3, set([1, 2, 7])),
        (4, set([6])),
    ]

    theo_tag_1_titles = set([1, 7, 9])

    series_1_tags = frozenset(
        [
            "TEST TAG - 5 - 9f6a4336-c74a-40d0-be03-1b57556a3d5e",
            "TEST TAG - 6 - ea487aed-0a6d-4d1d-9b73-dca14f96e4dd",
            "TEST TAG - 1 - f85be9c8-af5a-41d6-a47a-77baf31c5308",
            "TEST TAG - 7 - c78882ba-397a-4677-954d-e3b330f7f16e",
        ]
    )

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - NOTES

    # note properties
    theo_title_1_note_count = 1
    theo_title_1_notes = ["TEST NOTE - TITLE 1 - NOTE NUM 1 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed"]
    theo_title_1_note_ids = [315]

    theo_title_4_note_count = 1
    theo_title_4_notes = ["TEST NOTE - TITLE 4 - NOTE NUM 1 - 0d4aa6b0-9c1c-4a14-a661-66838257faad"]
    theo_title_4_note_ids = [
        322,
    ]

    theo_title_5_note_count = 1
    theo_title_5_notes = ["TEST NOTE - TITLE 5 - NOTE NUM 1 - 56dd1b6a-0342-453b-b779-e12492319aa9"]
    theo_title_5_note_ids = [
        323,
    ]

    theo_note_val_map = {
        1: "TEST NOTE - CREATOR 1 - NOTE NUM - 1 - 96ebca6d-fd03-4449-995b-210a0fccf6a3",
        2: "TEST NOTE - CREATOR 1 - NOTE NUM - 2 - 96ebca6d-fd03-4449-995b-210a0fccf6a3",
        3: "TEST NOTE - CREATOR 2 - NOTE NUM - 1 - 49b21b3f-83fa-4640-af1d-ec8aabf55331",
        4: "TEST NOTE - CREATOR 2 - NOTE NUM - 2 - fc21000c-e59c-44f4-9624-022a7658da42",
        5: "TEST NOTE - CREATOR 2 - NOTE NUM - 3 - 4cd96070-5028-4c4b-bb86-b6c020deb4a2",
        6: "TEST NOTE - CREATOR 3 - NOTE NUM - 1 - c9e23001-d802-4f9b-91e1-cda7bb685c62",
        7: "TEST NOTE - CREATOR 3 - NOTE NUM - 2 - 8327f6d6-7c16-4e27-87d7-453f66dacab3",
        8: "TEST NOTE - CREATOR 3 - NOTE NUM - 3 - add9e315-3502-4f4b-95f7-0ad22e5592e4",
        9: "TEST NOTE - CREATOR 4 - NOTE NUM - 1 - 4cd96070-5028-4c4b-bb86-b6c020deb4a2",
        10: "TEST NOTE - CREATOR 5 - NOTE NUM - 1 - 44e87e9e-d716-4e10-89ba-db03682f20ed",
        11: "TEST NOTE - CREATOR 5 - NOTE NUM - 2 - 4622c89f-0e05-4ef0-9b18-1319209ee674",
        12: "TEST NOTE - CREATOR 5 - NOTE NUM - 3 - d6c55d6e-41ce-47d6-ac02-6311716cef04",
        13: "TEST NOTE - CREATOR 5 - NOTE NUM - 4 - 5211863b-3764-4ed2-b938-40ec0abc7b44",
        14: "TEST NOTE - CREATOR 5 - NOTE NUM - 5 - 9b88b308-40de-4ed1-ab51-b7762b950e49",
        15: "TEST NOTE - CREATOR 6 - NOTE NUM - 1 - 8fceae7a-5ab0-4ce9-9ca8-c21dd908f377",
        16: "TEST NOTE - CREATOR 6 - NOTE NUM - 2 - afaa77d3-ce17-4bc8-806f-4b2f1b278473",
        17: "TEST NOTE - CREATOR 6 - NOTE NUM - 3 - 378357dc-0b5e-4a91-afc9-37c1b37a98fc",
        18: "TEST NOTE - CREATOR 6 - NOTE NUM - 4 - dd2de1d2-9d42-4c07-9447-b0f2ac941e86",
        19: "TEST NOTE - CREATOR 6 - NOTE NUM - 5 - 5a097718-2b1b-48d6-9e1c-424275d0e3c4",
        20: "TEST NOTE - CREATOR 7 - NOTE NUM - 1 - 635c7cf4-4a46-402d-b219-af0479b5aa3e",
        21: "TEST NOTE - CREATOR 7 - NOTE NUM - 2 - d759e0bc-426c-4831-80b2-4bf9460f5cf3",
        22: "TEST NOTE - CREATOR 7 - NOTE NUM - 3 - 5331f31b-dca6-4c50-9162-4444663c2728",
        23: "TEST NOTE - CREATOR 7 - NOTE NUM - 4 - 49b21b3f-83fa-4640-af1d-ec8aabf55331",
        24: "TEST NOTE - CREATOR 8 - NOTE NUM - 1 - 5b6879c9-fe5d-4e56-be0f-081257a6fe8a",
        25: "TEST NOTE - CREATOR 8 - NOTE NUM - 2 - 03210848-24c6-42ac-b918-74aba63e40f3",
        26: "TEST NOTE - CREATOR 8 - NOTE NUM - 3 - 03210848-24c6-42ac-b918-74aba63e40f3",
        27: "TEST NOTE - CREATOR 9 - NOTE NUM - 1 - 54b87228-9090-4ff8-bf90-4431cdd25519",
        28: "TEST NOTE - CREATOR 9 - NOTE NUM - 2 - c7a73b65-4115-45fd-adf2-7dab4327dc34",
        29: "TEST NOTE - CREATOR 9 - NOTE NUM - 3 - ee292414-a0f9-4400-b987-75669a211ca9",
        30: "TEST NOTE - CREATOR 9 - NOTE NUM - 4 - dec025b3-cf1d-4039-8a93-fd3fc51e416d",
        31: "TEST NOTE - CREATOR 9 - NOTE NUM - 5 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
        32: "TEST NOTE - CREATOR 10 - NOTE NUM - 1 - 93acce3a-7b3c-4afb-b717-b63150537fa6",
        33: "TEST NOTE - CREATOR 10 - NOTE NUM - 2 - 0cdf212c-724a-4666-ba52-8f8b4860f0f3",
        34: "TEST NOTE - CREATOR 10 - NOTE NUM - 3 - 5a097718-2b1b-48d6-9e1c-424275d0e3c4",
        35: "TEST NOTE - CREATOR 11 - NOTE NUM - 1 - ba565492-f0f0-46c3-b50d-59fefdf04aca",
        36: "TEST NOTE - CREATOR 12 - NOTE NUM - 1 - e242db80-72b7-41d6-9fa4-d86b61e72318",
        37: "TEST NOTE - CREATOR 12 - NOTE NUM - 2 - d442da62-b938-41e1-a069-0fb8bb85f340",
        38: "TEST NOTE - CREATOR 14 - NOTE NUM - 1 - 52ee46ad-4981-44e1-98af-cf9379e9de5c",
        39: "TEST NOTE - CREATOR 14 - NOTE NUM - 2 - a471e7c5-6a0f-415c-81b5-d69b1997b365",
        40: "TEST NOTE - CREATOR 14 - NOTE NUM - 3 - 7b3e4793-3302-4af2-bd2d-9d903258d151",
        41: "TEST NOTE - CREATOR 15 - NOTE NUM - 1 - 957fe462-e26e-4421-a5e7-bd3c08469145",
        42: "TEST NOTE - CREATOR 15 - NOTE NUM - 2 - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f",
        43: "TEST NOTE - CREATOR 15 - NOTE NUM - 3 - 2f7de989-32fd-4263-8020-e99d9358a89a",
        44: "TEST NOTE - CREATOR 16 - NOTE NUM - 1 - d76513c3-8c70-422c-9fc2-793ad6b03180",
        45: "TEST NOTE - CREATOR 16 - NOTE NUM - 2 - f576fc20-e058-4859-a027-3c586d8e43c2",
        46: "TEST NOTE - CREATOR 17 - NOTE NUM - 1 - 4bce79fc-83a2-43fb-8505-ef9a415bb35f",
        47: "TEST NOTE - CREATOR 17 - NOTE NUM - 2 - c7a73b65-4115-45fd-adf2-7dab4327dc34",
        48: "TEST NOTE - CREATOR 18 - NOTE NUM - 1 - 6e2a8e93-b7ad-4673-bc9e-589294004c38",
        49: "TEST NOTE - CREATOR 18 - NOTE NUM - 2 - 0d4d18dd-7cb7-423b-b2d9-8845b2eed393",
        50: "TEST NOTE - CREATOR 18 - NOTE NUM - 3 - ab2f7513-926d-4184-a03a-5534b59e62fd",
        51: "TEST NOTE - CREATOR 19 - NOTE NUM - 1 - 9f6a4336-c74a-40d0-be03-1b57556a3d5e",
        52: "TEST NOTE - CREATOR 19 - NOTE NUM - 2 - 42d53491-dc6c-4e9b-b8c3-d303e16ba806",
        53: "TEST NOTE - CREATOR 19 - NOTE NUM - 3 - 146ab80f-261f-44b3-a25a-85379e76abe9",
        54: "TEST NOTE - CREATOR 19 - NOTE NUM - 4 - f4217e2f-2e03-455d-bfbf-ed5964ae7ac0",
        55: "TEST NOTE - CREATOR 19 - NOTE NUM - 5 - e08fb6ba-3808-4895-81e2-a9638dc29cee",
        56: "TEST NOTE - CREATOR 20 - NOTE NUM - 1 - 30b7d9de-990b-4294-b2a5-2d6855c77b23",
        57: "TEST NOTE - CREATOR 20 - NOTE NUM - 2 - 4da1e0c5-813a-4a01-8ef6-721d35650ce7",
        58: "TEST NOTE - CREATOR 21 - NOTE NUM - 1 - 54b87228-9090-4ff8-bf90-4431cdd25519",
        59: "TEST NOTE - CREATOR 21 - NOTE NUM - 2 - d64415c9-1fa5-4edf-a61e-4e3d7a041699",
        60: "TEST NOTE - CREATOR 22 - NOTE NUM - 1 - 6e2a8e93-b7ad-4673-bc9e-589294004c38",
        61: "TEST NOTE - CREATOR 22 - NOTE NUM - 2 - c282ee09-9acc-40be-b540-ab3613e6e818",
        62: "TEST NOTE - CREATOR 22 - NOTE NUM - 3 - e55f7696-d9d7-4127-b8b3-163d9c90df8d",
        63: "TEST NOTE - CREATOR 23 - NOTE NUM - 1 - 2f5ade60-4c28-47a2-82f9-a71ab5e05158",
        64: "TEST NOTE - CREATOR 23 - NOTE NUM - 2 - bcb2e4f4-516b-44a0-8279-a83af218b493",
        65: "TEST NOTE - CREATOR 23 - NOTE NUM - 3 - d73b1842-5312-4ddc-9277-da8e378bb8ab",
        66: "TEST NOTE - CREATOR 24 - NOTE NUM - 1 - d442da62-b938-41e1-a069-0fb8bb85f340",
        67: "TEST NOTE - CREATOR 25 - NOTE NUM - 1 - e55f7696-d9d7-4127-b8b3-163d9c90df8d",
        68: "TEST NOTE - CREATOR 25 - NOTE NUM - 2 - 42d53491-dc6c-4e9b-b8c3-d303e16ba806",
        69: "TEST NOTE - CREATOR 26 - NOTE NUM - 1 - 59055099-0f20-46b5-91c5-be0a0cdd0313",
        70: "TEST NOTE - CREATOR 26 - NOTE NUM - 2 - 55085fbd-0504-4f37-8fc3-3b6f75f03d41",
        71: "TEST NOTE - CREATOR 26 - NOTE NUM - 3 - d6c55d6e-41ce-47d6-ac02-6311716cef04",
        72: "TEST NOTE - CREATOR 26 - NOTE NUM - 4 - 0d4d18dd-7cb7-423b-b2d9-8845b2eed393",
        73: "TEST NOTE - CREATOR 28 - NOTE NUM - 1 - f4b6557c-4278-4e02-9d5f-7b2c88daa7e0",
        74: "TEST NOTE - CREATOR 29 - NOTE NUM - 1 - 0cdf212c-724a-4666-ba52-8f8b4860f0f3",
        75: "TEST NOTE - CREATOR 29 - NOTE NUM - 2 - a3d0b436-b5c2-4c95-848e-8fcfac8e8afa",
        76: "TEST NOTE - CREATOR 29 - NOTE NUM - 3 - 4da1e0c5-813a-4a01-8ef6-721d35650ce7",
        77: "TEST NOTE - CREATOR 30 - NOTE NUM - 1 - a471e7c5-6a0f-415c-81b5-d69b1997b365",
        78: "TEST NOTE - CREATOR 30 - NOTE NUM - 2 - 654b7773-b95b-43b0-8c5b-820065463e47",
        79: "TEST NOTE - CREATOR 30 - NOTE NUM - 3 - cdf28d51-749d-4f0f-bff8-2668abd652a1",
        80: "TEST NOTE - CREATOR 31 - NOTE NUM - 1 - c47407dd-bd9e-478b-adfa-0585e8dee677",
        81: "TEST NOTE - CREATOR 31 - NOTE NUM - 2 - 078c31b9-a5f2-42ac-8621-6d9bc9355d4f",
        82: "TEST NOTE - CREATOR 31 - NOTE NUM - 3 - bcb2e4f4-516b-44a0-8279-a83af218b493",
        83: "TEST NOTE - CREATOR 31 - NOTE NUM - 4 - eda46b9e-b66e-49bd-9458-68e86cc0d3d1",
        84: "TEST NOTE - CREATOR 32 - NOTE NUM - 1 - 1a87595e-f0a3-4ada-8a51-5e1f441bafbe",
        85: "TEST NOTE - CREATOR 32 - NOTE NUM - 2 - 52ee46ad-4981-44e1-98af-cf9379e9de5c",
        86: "TEST NOTE - CREATOR 32 - NOTE NUM - 3 - 378357dc-0b5e-4a91-afc9-37c1b37a98fc",
        87: "TEST NOTE - CREATOR 33 - NOTE NUM - 1 - 8567479b-4856-44b3-bf9e-6ebd73476942",
        88: "TEST NOTE - CREATOR 33 - NOTE NUM - 2 - 3939f972-fa38-45e1-9f3f-be69a8618ee2",
        89: "TEST NOTE - CREATOR 33 - NOTE NUM - 3 - 4f2ab892-6a87-4d46-b1fb-a56478f84958",
        90: "TEST NOTE - CREATOR 33 - NOTE NUM - 4 - fc21000c-e59c-44f4-9624-022a7658da42",
        91: "TEST NOTE - CREATOR 33 - NOTE NUM - 5 - d6c55d6e-41ce-47d6-ac02-6311716cef04",
        92: "TEST NOTE - CREATOR 34 - NOTE NUM - 1 - ea487aed-0a6d-4d1d-9b73-dca14f96e4dd",
        93: "TEST NOTE - CREATOR 34 - NOTE NUM - 2 - 5a097718-2b1b-48d6-9e1c-424275d0e3c4",
        94: "TEST NOTE - CREATOR 34 - NOTE NUM - 3 - fdddde7e-5574-478c-9608-4640e45f3ec6",
        95: "TEST NOTE - CREATOR 35 - NOTE NUM - 1 - e242db80-72b7-41d6-9fa4-d86b61e72318",
        96: "TEST NOTE - CREATOR 35 - NOTE NUM - 2 - 68042e03-d2f2-41a0-b08e-645123c12597",
        97: "TEST NOTE - CREATOR 37 - NOTE NUM - 1 - 5a097718-2b1b-48d6-9e1c-424275d0e3c4",
        98: "TEST NOTE - CREATOR 37 - NOTE NUM - 2 - 7f93e2df-1c66-4d29-a36b-3878c650c447",
        99: "TEST NOTE - CREATOR 38 - NOTE NUM - 1 - 813440de-8da1-4b21-a687-407407d0daeb",
        100: "TEST NOTE - CREATOR 38 - NOTE NUM - 2 - e82c3c35-faae-40b2-a4a6-d31b4b5a2719",
        101: "TEST NOTE - CREATOR 38 - NOTE NUM - 3 - 9ba48058-819a-46fe-be1b-c8e5a81203b1",
        102: "TEST NOTE - CREATOR 39 - NOTE NUM - 1 - d1f9c688-3046-4474-a99b-5d7bf1159101",
        103: "TEST NOTE - CREATOR 39 - NOTE NUM - 2 - e5c7c4b6-b38c-47d0-9c25-f6620896a795",
        104: "TEST NOTE - CREATOR 39 - NOTE NUM - 3 - 94d9c81a-9f6a-4669-9e65-78db57bab3a6",
        105: "TEST NOTE - CREATOR 40 - NOTE NUM - 1 - 8567479b-4856-44b3-bf9e-6ebd73476942",
        106: "TEST NOTE - CREATOR 40 - NOTE NUM - 2 - 5b6879c9-fe5d-4e56-be0f-081257a6fe8a",
        107: "TEST NOTE - CREATOR 41 - NOTE NUM - 1 - 68042e03-d2f2-41a0-b08e-645123c12597",
        108: "TEST NOTE - CREATOR 41 - NOTE NUM - 2 - 984d4dec-2ccf-4d81-b6e7-8420430262fd",
        109: "TEST NOTE - CREATOR 41 - NOTE NUM - 3 - d1803224-93e4-46ae-8756-bbe42796360e",
        110: "TEST NOTE - CREATOR 42 - NOTE NUM - 1 - 43024a39-3cd9-4ad8-8b1b-c2d79f900c4b",
        111: "TEST NOTE - CREATOR 42 - NOTE NUM - 2 - fc21000c-e59c-44f4-9624-022a7658da42",
        112: "TEST NOTE - CREATOR 44 - NOTE NUM - 1 - ef74e5a1-bc3a-465d-9d9b-db985dbe8b0c",
        113: "TEST NOTE - CREATOR 44 - NOTE NUM - 2 - 5e3c2ba7-f3f0-46fe-a2b6-9a3162506d70",
        114: "TEST NOTE - CREATOR 46 - NOTE NUM - 1 - 67013d5f-d85d-4308-a208-a5162afc51ab",
        115: "TEST NOTE - CREATOR 46 - NOTE NUM - 2 - 2f5ade60-4c28-47a2-82f9-a71ab5e05158",
        116: "TEST NOTE - CREATOR 46 - NOTE NUM - 3 - ea487aed-0a6d-4d1d-9b73-dca14f96e4dd",
        117: "TEST NOTE - CREATOR 46 - NOTE NUM - 4 - 6e7e2e87-5443-40f6-aef6-552ee1ab5531",
        118: "TEST NOTE - CREATOR 46 - NOTE NUM - 5 - 29f18462-21ec-4886-9c1d-18e8dbeb54e8",
        119: "TEST NOTE - CREATOR 47 - NOTE NUM - 1 - 54b87228-9090-4ff8-bf90-4431cdd25519",
        120: "TEST NOTE - CREATOR 47 - NOTE NUM - 2 - 3dd78a6f-ff02-4916-a795-f35b7b86e84c",
        121: "TEST NOTE - CREATOR 47 - NOTE NUM - 3 - 28ef047e-f466-4721-b71f-dfb858e0b34a",
        122: "TEST NOTE - CREATOR 47 - NOTE NUM - 4 - 650530d5-59f1-49ea-9ab2-f8c48538f0e3",
        123: "TEST NOTE - CREATOR 47 - NOTE NUM - 5 - cdf28d51-749d-4f0f-bff8-2668abd652a1",
        124: "TEST NOTE - CREATOR 48 - NOTE NUM - 1 - 8fceae7a-5ab0-4ce9-9ca8-c21dd908f377",
        125: "TEST NOTE - CREATOR 48 - NOTE NUM - 2 - 0d4d18dd-7cb7-423b-b2d9-8845b2eed393",
        126: "TEST NOTE - CREATOR 50 - NOTE NUM - 1 - bcb2e4f4-516b-44a0-8279-a83af218b493",
        127: "TEST NOTE - CREATOR 50 - NOTE NUM - 2 - 4cd96070-5028-4c4b-bb86-b6c020deb4a2",
        128: "TEST NOTE - CREATOR 50 - NOTE NUM - 3 - 546667c5-de19-4c85-9d79-14ee90d9188a",
        129: "TEST NOTE - CREATOR 50 - NOTE NUM - 4 - 5e3c2ba7-f3f0-46fe-a2b6-9a3162506d70",
        130: "TEST NOTE - CREATOR 50 - NOTE NUM - 5 - d759e0bc-426c-4831-80b2-4bf9460f5cf3",
        131: "TEST NOTE - CREATOR 51 - NOTE NUM - 1 - bcb2e4f4-516b-44a0-8279-a83af218b493",
        132: "TEST NOTE - CREATOR 51 - NOTE NUM - 2 - 30b7d9de-990b-4294-b2a5-2d6855c77b23",
        133: "TEST NOTE - CREATOR 51 - NOTE NUM - 3 - 9ba48058-819a-46fe-be1b-c8e5a81203b1",
        134: "TEST NOTE - CREATOR 51 - NOTE NUM - 4 - f576fc20-e058-4859-a027-3c586d8e43c2",
        135: "TEST NOTE - CREATOR 53 - NOTE NUM - 1 - 2205909c-4f4e-4818-9140-95dcbeea4d16",
        136: "TEST NOTE - CREATOR 53 - NOTE NUM - 2 - 0da80909-5fd7-4240-9fe9-bf48686dc11e",
        137: "TEST NOTE - CREATOR 53 - NOTE NUM - 3 - fc21000c-e59c-44f4-9624-022a7658da42",
        138: "TEST NOTE - CREATOR 54 - NOTE NUM - 1 - 29f18462-21ec-4886-9c1d-18e8dbeb54e8",
        139: "TEST NOTE - CREATOR 54 - NOTE NUM - 2 - 2f7de989-32fd-4263-8020-e99d9358a89a",
        140: "TEST NOTE - CREATOR 54 - NOTE NUM - 3 - 5331f31b-dca6-4c50-9162-4444663c2728",
        141: "TEST NOTE - CREATOR 55 - NOTE NUM - 1 - 957fe462-e26e-4421-a5e7-bd3c08469145",
        142: "TEST NOTE - CREATOR 55 - NOTE NUM - 2 - 7b3e4793-3302-4af2-bd2d-9d903258d151",
        143: "TEST NOTE - CREATOR 55 - NOTE NUM - 3 - d6c55d6e-41ce-47d6-ac02-6311716cef04",
        144: "TEST NOTE - CREATOR 56 - NOTE NUM - 1 - 7d465381-1f61-4501-98a5-b95db064e4dc",
        145: "TEST NOTE - CREATOR 56 - NOTE NUM - 2 - 650530d5-59f1-49ea-9ab2-f8c48538f0e3",
        146: "TEST NOTE - CREATOR 56 - NOTE NUM - 3 - 16298153-d0c1-4ea2-af5f-0c78cd6e46ba",
        147: "TEST NOTE - CREATOR 56 - NOTE NUM - 4 - 078c31b9-a5f2-42ac-8621-6d9bc9355d4f",
        148: "TEST NOTE - CREATOR 57 - NOTE NUM - 1 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
        149: "TEST NOTE - CREATOR 59 - NOTE NUM - 1 - 42d53491-dc6c-4e9b-b8c3-d303e16ba806",
        150: "TEST NOTE - CREATOR 60 - NOTE NUM - 1 - 862d3f42-f2de-4063-ad83-603da1b68b6b",
        151: "TEST NOTE - CREATOR 60 - NOTE NUM - 2 - d76513c3-8c70-422c-9fc2-793ad6b03180",
        152: "TEST NOTE - CREATOR 60 - NOTE NUM - 3 - 83c6871c-1ac1-4cec-9519-d6c5ad761b3b",
        153: "TEST NOTE - CREATOR 61 - NOTE NUM - 1 - 8d705893-e596-4e92-a839-4dffe4373177",
        154: "TEST NOTE - CREATOR 61 - NOTE NUM - 2 - 6e2a8e93-b7ad-4673-bc9e-589294004c38",
        155: "TEST NOTE - CREATOR 61 - NOTE NUM - 3 - 4da1e0c5-813a-4a01-8ef6-721d35650ce7",
        156: "TEST NOTE - CREATOR 62 - NOTE NUM - 1 - 43024a39-3cd9-4ad8-8b1b-c2d79f900c4b",
        157: "TEST NOTE - CREATOR 62 - NOTE NUM - 2 - 54b87228-9090-4ff8-bf90-4431cdd25519",
        158: "TEST NOTE - CREATOR 62 - NOTE NUM - 3 - 4a8dcea2-7a07-4dbd-a2e6-339e1418b0ec",
        159: "TEST NOTE - CREATOR 62 - NOTE NUM - 4 - 65851d79-e6d6-4a89-9780-55b118cf0858",
        160: "TEST NOTE - CREATOR 62 - NOTE NUM - 5 - ef74e5a1-bc3a-465d-9d9b-db985dbe8b0c",
        161: "TEST NOTE - CREATOR 63 - NOTE NUM - 1 - c9e23001-d802-4f9b-91e1-cda7bb685c62",
        162: "TEST NOTE - CREATOR 63 - NOTE NUM - 2 - 8d705893-e596-4e92-a839-4dffe4373177",
        163: "TEST NOTE - CREATOR 63 - NOTE NUM - 3 - 0da80909-5fd7-4240-9fe9-bf48686dc11e",
        164: "TEST NOTE - CREATOR 63 - NOTE NUM - 4 - 2fa7997a-9998-47fa-85ec-3c81f6180a8b",
        165: "TEST NOTE - CREATOR 63 - NOTE NUM - 5 - 4a8dcea2-7a07-4dbd-a2e6-339e1418b0ec",
        166: "TEST NOTE - CREATOR 64 - NOTE NUM - 1 - 078c31b9-a5f2-42ac-8621-6d9bc9355d4f",
        167: "TEST NOTE - CREATOR 64 - NOTE NUM - 2 - 49458593-07f1-48f6-834b-fbafdfab119d",
        168: "TEST NOTE - CREATOR 64 - NOTE NUM - 3 - 4c16166c-7b4f-4b32-a7c9-f039a9876785",
        169: "TEST NOTE - CREATOR 64 - NOTE NUM - 4 - b753a41a-5dbf-482d-b56d-20adc95cf71c",
        170: "TEST NOTE - CREATOR 67 - NOTE NUM - 1 - add9e315-3502-4f4b-95f7-0ad22e5592e4",
        171: "TEST NOTE - CREATOR 68 - NOTE NUM - 1 - 3cce4d59-b8a0-4f83-9b59-9dd3500a8a8c",
        172: "TEST NOTE - CREATOR 69 - NOTE NUM - 1 - 268b04fe-92df-4e2e-b23b-ce5b74dbafc2",
        173: "TEST NOTE - CREATOR 70 - NOTE NUM - 1 - 268b04fe-92df-4e2e-b23b-ce5b74dbafc2",
        174: "TEST NOTE - CREATOR 70 - NOTE NUM - 2 - ef74e5a1-bc3a-465d-9d9b-db985dbe8b0c",
        175: "TEST NOTE - CREATOR 71 - NOTE NUM - 1 - 4bce79fc-83a2-43fb-8505-ef9a415bb35f",
        176: "TEST NOTE - CREATOR 71 - NOTE NUM - 2 - 29686bc4-0955-4d4b-8717-c225be9f14db",
        177: "TEST NOTE - CREATOR 71 - NOTE NUM - 3 - fe882ed3-25fc-49f3-939d-3ee29634cf1d",
        178: "TEST NOTE - CREATOR 72 - NOTE NUM - 1 - b12cfb91-6a49-4455-9ddf-5a797b7fe24c",
        179: "TEST NOTE - CREATOR 72 - NOTE NUM - 2 - 55323a3a-bacf-4056-b5f5-e6f540b92d05",
        180: "TEST NOTE - CREATOR 72 - NOTE NUM - 3 - ee292414-a0f9-4400-b987-75669a211ca9",
        181: "TEST NOTE - CREATOR 72 - NOTE NUM - 4 - 16298153-d0c1-4ea2-af5f-0c78cd6e46ba",
        182: "TEST NOTE - CREATOR 72 - NOTE NUM - 5 - d64415c9-1fa5-4edf-a61e-4e3d7a041699",
        183: "TEST NOTE - CREATOR 74 - NOTE NUM - 1 - b753a41a-5dbf-482d-b56d-20adc95cf71c",
        184: "TEST NOTE - CREATOR 74 - NOTE NUM - 2 - 01afd8a1-813e-4c2b-b8a2-b76b1c3067b1",
        185: "TEST NOTE - CREATOR 74 - NOTE NUM - 3 - 79085d34-1849-4acf-802e-5580ad1c86bb",
        186: "TEST NOTE - CREATOR 74 - NOTE NUM - 4 - e5c7c4b6-b38c-47d0-9c25-f6620896a795",
        187: "TEST NOTE - CREATOR 74 - NOTE NUM - 5 - 1a169e8f-c7a3-45f4-8ea6-b3c5ee25d0e3",
        188: "TEST NOTE - CREATOR 75 - NOTE NUM - 1 - 79085d34-1849-4acf-802e-5580ad1c86bb",
        189: "TEST NOTE - CREATOR 76 - NOTE NUM - 1 - 4bce79fc-83a2-43fb-8505-ef9a415bb35f",
        190: "TEST NOTE - CREATOR 77 - NOTE NUM - 1 - 5a097718-2b1b-48d6-9e1c-424275d0e3c4",
        191: "TEST NOTE - CREATOR 77 - NOTE NUM - 2 - 6e7e2e87-5443-40f6-aef6-552ee1ab5531",
        192: "TEST NOTE - CREATOR 78 - NOTE NUM - 1 - 957fe462-e26e-4421-a5e7-bd3c08469145",
        193: "TEST NOTE - CREATOR 78 - NOTE NUM - 2 - d76513c3-8c70-422c-9fc2-793ad6b03180",
        194: "TEST NOTE - CREATOR 78 - NOTE NUM - 3 - 39ce9a58-ea08-4f2c-994a-b49d61e9393d",
        195: "TEST NOTE - CREATOR 79 - NOTE NUM - 1 - 5331f31b-dca6-4c50-9162-4444663c2728",
        196: "TEST NOTE - CREATOR 79 - NOTE NUM - 2 - 088d5a29-3fd7-498f-92c4-4443efe66887",
        197: "TEST NOTE - CREATOR 80 - NOTE NUM - 1 - 5ec2291a-111e-4e61-b177-5dbd744007be",
        198: "TEST NOTE - CREATOR 80 - NOTE NUM - 2 - dec025b3-cf1d-4039-8a93-fd3fc51e416d",
        199: "TEST NOTE - CREATOR 81 - NOTE NUM - 1 - 39ce9a58-ea08-4f2c-994a-b49d61e9393d",
        200: "TEST NOTE - CREATOR 81 - NOTE NUM - 2 - 65851d79-e6d6-4a89-9780-55b118cf0858",
        201: "TEST NOTE - CREATOR 81 - NOTE NUM - 3 - 4f266348-dde4-486a-b3e0-bee0baed5b02",
        202: "TEST NOTE - CREATOR 81 - NOTE NUM - 4 - 78affa14-a6cb-4f88-a57e-99cb308f99f1",
        203: "TEST NOTE - CREATOR 82 - NOTE NUM - 1 - b16e638f-80ce-43fa-87de-2b8066c3c3e8",
        204: "TEST NOTE - CREATOR 83 - NOTE NUM - 1 - 5b6879c9-fe5d-4e56-be0f-081257a6fe8a",
        205: "TEST NOTE - CREATOR 83 - NOTE NUM - 2 - 090a79e4-b4cf-40fa-9163-36773b820b92",
        206: "TEST NOTE - CREATOR 83 - NOTE NUM - 3 - 68042e03-d2f2-41a0-b08e-645123c12597",
        207: "TEST NOTE - CREATOR 83 - NOTE NUM - 4 - 68042e03-d2f2-41a0-b08e-645123c12597",
        208: "TEST NOTE - CREATOR 84 - NOTE NUM - 1 - 5ec2291a-111e-4e61-b177-5dbd744007be",
        209: "TEST NOTE - CREATOR 84 - NOTE NUM - 2 - b12cfb91-6a49-4455-9ddf-5a797b7fe24c",
        210: "TEST NOTE - CREATOR 84 - NOTE NUM - 3 - 65851d79-e6d6-4a89-9780-55b118cf0858",
        211: "TEST NOTE - CREATOR 84 - NOTE NUM - 4 - d1f9c688-3046-4474-a99b-5d7bf1159101",
        212: "TEST NOTE - CREATOR 84 - NOTE NUM - 5 - 2f7de989-32fd-4263-8020-e99d9358a89a",
        213: "TEST NOTE - CREATOR 85 - NOTE NUM - 1 - 4cd96070-5028-4c4b-bb86-b6c020deb4a2",
        214: "TEST NOTE - CREATOR 85 - NOTE NUM - 2 - 59055099-0f20-46b5-91c5-be0a0cdd0313",
        215: "TEST NOTE - CREATOR 85 - NOTE NUM - 3 - 654b7773-b95b-43b0-8c5b-820065463e47",
        216: "TEST NOTE - CREATOR 86 - NOTE NUM - 1 - 02c1e800-e4b1-41ea-9c03-9f1e945725f3",
        217: "TEST NOTE - CREATOR 86 - NOTE NUM - 2 - 49b21b3f-83fa-4640-af1d-ec8aabf55331",
        218: "TEST NOTE - CREATOR 87 - NOTE NUM - 1 - c187b301-de7e-4f91-94dc-57cab3996a95",
        219: "TEST NOTE - CREATOR 87 - NOTE NUM - 2 - 96ebca6d-fd03-4449-995b-210a0fccf6a3",
        220: "TEST NOTE - CREATOR 87 - NOTE NUM - 3 - 7d465381-1f61-4501-98a5-b95db064e4dc",
        221: "TEST NOTE - CREATOR 87 - NOTE NUM - 4 - 6e2a8e93-b7ad-4673-bc9e-589294004c38",
        222: "TEST NOTE - CREATOR 87 - NOTE NUM - 5 - 9b9e8537-31cf-4683-af4b-5e2a33b75e8a",
        223: "TEST NOTE - CREATOR 89 - NOTE NUM - 1 - 3939f972-fa38-45e1-9f3f-be69a8618ee2",
        224: "TEST NOTE - CREATOR 89 - NOTE NUM - 2 - a471e7c5-6a0f-415c-81b5-d69b1997b365",
        225: "TEST NOTE - CREATOR 90 - NOTE NUM - 1 - 52ee46ad-4981-44e1-98af-cf9379e9de5c",
        226: "TEST NOTE - CREATOR 90 - NOTE NUM - 2 - ba565492-f0f0-46c3-b50d-59fefdf04aca",
        227: "TEST NOTE - CREATOR 90 - NOTE NUM - 3 - 25233cf6-1e45-4ce6-a96c-297db220521c",
        228: "TEST NOTE - CREATOR 91 - NOTE NUM - 1 - 088d5a29-3fd7-498f-92c4-4443efe66887",
        229: "TEST NOTE - CREATOR 91 - NOTE NUM - 2 - 49b21b3f-83fa-4640-af1d-ec8aabf55331",
        230: "TEST NOTE - CREATOR 92 - NOTE NUM - 1 - fd5ae8b5-3833-4466-ac34-2efb7dc13bc4",
        231: "TEST NOTE - CREATOR 92 - NOTE NUM - 2 - 585a2004-1ec7-458d-84c6-e84807aba9b2",
        232: "TEST NOTE - CREATOR 92 - NOTE NUM - 3 - 1a169e8f-c7a3-45f4-8ea6-b3c5ee25d0e3",
        233: "TEST NOTE - CREATOR 92 - NOTE NUM - 4 - add9e315-3502-4f4b-95f7-0ad22e5592e4",
        234: "TEST NOTE - CREATOR 93 - NOTE NUM - 1 - 378357dc-0b5e-4a91-afc9-37c1b37a98fc",
        235: "TEST NOTE - CREATOR 93 - NOTE NUM - 2 - ab2f7513-926d-4184-a03a-5534b59e62fd",
        236: "TEST NOTE - CREATOR 93 - NOTE NUM - 3 - 0862542f-22bc-417c-81a7-0053304412e2",
        237: "TEST NOTE - CREATOR 93 - NOTE NUM - 4 - ee292414-a0f9-4400-b987-75669a211ca9",
        238: "TEST NOTE - CREATOR 94 - NOTE NUM - 1 - da788a67-4603-4b71-8ceb-55d3b48ac79e",
        239: "TEST NOTE - CREATOR 94 - NOTE NUM - 2 - 562c6380-4c1f-413a-8cfa-612c7d9119ab",
        240: "TEST NOTE - CREATOR 94 - NOTE NUM - 3 - e5c7c4b6-b38c-47d0-9c25-f6620896a795",
        241: "TEST NOTE - CREATOR 95 - NOTE NUM - 1 - b16e638f-80ce-43fa-87de-2b8066c3c3e8",
        242: "TEST NOTE - CREATOR 95 - NOTE NUM - 2 - fd5ae8b5-3833-4466-ac34-2efb7dc13bc4",
        243: "TEST NOTE - CREATOR 95 - NOTE NUM - 3 - e5c7c4b6-b38c-47d0-9c25-f6620896a795",
        244: "TEST NOTE - CREATOR 95 - NOTE NUM - 4 - add9e315-3502-4f4b-95f7-0ad22e5592e4",
        245: "TEST NOTE - CREATOR 95 - NOTE NUM - 5 - c31ad47d-6f6d-46ec-bab8-96c8945056bd",
        246: "TEST NOTE - CREATOR 96 - NOTE NUM - 1 - 52ee46ad-4981-44e1-98af-cf9379e9de5c",
        247: "TEST NOTE - CREATOR 97 - NOTE NUM - 1 - da788a67-4603-4b71-8ceb-55d3b48ac79e",
        248: "TEST NOTE - CREATOR 97 - NOTE NUM - 2 - 6e7e2e87-5443-40f6-aef6-552ee1ab5531",
        249: "TEST NOTE - CREATOR 97 - NOTE NUM - 3 - b16e638f-80ce-43fa-87de-2b8066c3c3e8",
        250: "TEST NOTE - CREATOR 97 - NOTE NUM - 4 - b8336bb0-8f93-49f5-9433-03e5d542d635",
        251: "TEST NOTE - CREATOR 98 - NOTE NUM - 1 - c003f5f9-0394-48fc-afb3-88d78b9534dd",
        252: "TEST NOTE - CREATOR 98 - NOTE NUM - 2 - 49458593-07f1-48f6-834b-fbafdfab119d",
        253: "TEST NOTE - CREATOR 98 - NOTE NUM - 3 - d926b73e-bace-4d92-9406-a38e4b5de7b7",
        254: "TEST NOTE - CREATOR 99 - NOTE NUM - 1 - 43024a39-3cd9-4ad8-8b1b-c2d79f900c4b",
        255: "TEST NOTE - CREATOR 99 - NOTE NUM - 2 - 77211d71-f8a7-411b-ac58-19af6f4ae350",
        256: "TEST NOTE - CREATOR 99 - NOTE NUM - 3 - 7f93e2df-1c66-4d29-a36b-3878c650c447",
        257: "TEST NOTE - CREATOR 99 - NOTE NUM - 4 - 56f272d2-bd4e-447d-8a3d-171357a5f6e2",
        258: "TEST NOTE - CREATOR 99 - NOTE NUM - 5 - eda46b9e-b66e-49bd-9458-68e86cc0d3d1",
        259: "TEST NOTE - CREATOR 100 - NOTE NUM - 1 - 2f7de989-32fd-4263-8020-e99d9358a89a",
        260: "TEST NOTE - CREATOR 100 - NOTE NUM - 2 - 3dd78a6f-ff02-4916-a795-f35b7b86e84c",
        261: "TEST NOTE - CREATOR 100 - NOTE NUM - 3 - 4324fdc0-8cb1-4aa7-bb25-1714dd39cdca",
        262: "TEST NOTE - PUBLISHER TEST PUBLISHER - 1 - DELETE ME - b2dc4d71-e66d-4431-9266-2a4bc7b0bb7f - NOTE NUM 1 - d73b1842-5312-4ddc-9277-da8e378bb8ab",
        263: "TEST NOTE - PUBLISHER TEST PUBLISHER - 1 - DELETE ME - b2dc4d71-e66d-4431-9266-2a4bc7b0bb7f - NOTE NUM 2 - 090a79e4-b4cf-40fa-9163-36773b820b92",
        264: "TEST NOTE - PUBLISHER TEST PUBLISHER - 1 - DELETE ME - b2dc4d71-e66d-4431-9266-2a4bc7b0bb7f - NOTE NUM 3 - 54b87228-9090-4ff8-bf90-4431cdd25519",
        265: "TEST NOTE - PUBLISHER TEST PUBLISHER - 2 - DELETE ME - 29686bc4-0955-4d4b-8717-c225be9f14db - NOTE NUM 1 - a471e7c5-6a0f-415c-81b5-d69b1997b365",
        266: "TEST NOTE - PUBLISHER TEST PUBLISHER - 2 - DELETE ME - 29686bc4-0955-4d4b-8717-c225be9f14db - NOTE NUM 2 - 2f7de989-32fd-4263-8020-e99d9358a89a",
        267: "TEST NOTE - PUBLISHER TEST PUBLISHER - 3 - DELETE ME - e242db80-72b7-41d6-9fa4-d86b61e72318 - NOTE NUM 1 - 29686bc4-0955-4d4b-8717-c225be9f14db",
        268: "TEST NOTE - PUBLISHER TEST PUBLISHER - 3 - DELETE ME - e242db80-72b7-41d6-9fa4-d86b61e72318 - NOTE NUM 2 - 3542b654-e2d8-48a2-aaa3-7882aa50e259",
        269: "TEST NOTE - PUBLISHER TEST PUBLISHER - 3 - DELETE ME - e242db80-72b7-41d6-9fa4-d86b61e72318 - NOTE NUM 3 - fbfa7e8f-a517-4e4c-96be-5fb7acef8517",
        270: "TEST NOTE - PUBLISHER TEST PUBLISHER - 4 - DELETE ME - e55f7696-d9d7-4127-b8b3-163d9c90df8d - NOTE NUM 1 - b8336bb0-8f93-49f5-9433-03e5d542d635",
        271: "TEST NOTE - PUBLISHER TEST PUBLISHER - 4 - DELETE ME - e55f7696-d9d7-4127-b8b3-163d9c90df8d - NOTE NUM 2 - e82c3c35-faae-40b2-a4a6-d31b4b5a2719",
        272: "TEST NOTE - PUBLISHER TEST PUBLISHER - 4 - DELETE ME - e55f7696-d9d7-4127-b8b3-163d9c90df8d - NOTE NUM 3 - d1f9c688-3046-4474-a99b-5d7bf1159101",
        273: "TEST NOTE - PUBLISHER TEST PUBLISHER - 4 - DELETE ME - e55f7696-d9d7-4127-b8b3-163d9c90df8d - NOTE NUM 4 - 146ab80f-261f-44b3-a25a-85379e76abe9",
        274: "TEST NOTE - PUBLISHER TEST PUBLISHER - 4 - DELETE ME - e55f7696-d9d7-4127-b8b3-163d9c90df8d - NOTE NUM 5 - 1b5d91b5-1b35-4d52-9e65-5e6c606a85c9",
        275: "TEST NOTE - PUBLISHER TEST PUBLISHER - 5 - DELETE ME - c78882ba-397a-4677-954d-e3b330f7f16e - NOTE NUM 1 - 92472632-d0b9-4497-a5f1-852a8a168b22",
        276: "TEST NOTE - PUBLISHER TEST PUBLISHER - 5 - DELETE ME - c78882ba-397a-4677-954d-e3b330f7f16e - NOTE NUM 2 - 669ba846-6730-4fc8-9299-eb1a5c3c082c",
        277: "TEST NOTE - PUBLISHER TEST PUBLISHER - 5 - DELETE ME - c78882ba-397a-4677-954d-e3b330f7f16e - NOTE NUM 3 - cdf28d51-749d-4f0f-bff8-2668abd652a1",
        278: "TEST NOTE - PUBLISHER TEST PUBLISHER - 6 - DELETE ME - 9b67e869-0ab7-4267-a5d3-3841d8b7145d - NOTE NUM 1 - a46b35bb-6de6-438f-b946-6c95a4b9fb0c",
        279: "TEST NOTE - PUBLISHER TEST PUBLISHER - 6 - DELETE ME - 9b67e869-0ab7-4267-a5d3-3841d8b7145d - NOTE NUM 2 - 1cd4b089-63e0-4340-b387-4275a9e18a51",
        280: "TEST NOTE - PUBLISHER TEST PUBLISHER - 6 - DELETE ME - 9b67e869-0ab7-4267-a5d3-3841d8b7145d - NOTE NUM 3 - 77211d71-f8a7-411b-ac58-19af6f4ae350",
        281: "TEST NOTE - PUBLISHER TEST PUBLISHER - 7 - DELETE ME - 313e007a-16ac-4e4d-9732-6be97e7bd1d8 - NOTE NUM 1 - 7bd3252a-b053-4080-904f-8292b6a7981c",
        282: "TEST NOTE - PUBLISHER TEST PUBLISHER - 7 - DELETE ME - 313e007a-16ac-4e4d-9732-6be97e7bd1d8 - NOTE NUM 2 - d27ef268-7a63-4dfa-8876-03a6de8f3e93",
        283: "TEST NOTE - PUBLISHER TEST PUBLISHER - 7 - DELETE ME - 313e007a-16ac-4e4d-9732-6be97e7bd1d8 - NOTE NUM 3 - 8cf64bff-18a0-4795-b44a-8ac350c5afa5",
        284: "TEST NOTE - PUBLISHER TEST PUBLISHER - 8 - DELETE ME - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f - NOTE NUM 1 - a471e7c5-6a0f-415c-81b5-d69b1997b365",
        285: "TEST NOTE - PUBLISHER TEST PUBLISHER - 8 - DELETE ME - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f - NOTE NUM 2 - 67013d5f-d85d-4308-a208-a5162afc51ab",
        286: "TEST NOTE - PUBLISHER TEST PUBLISHER - 8 - DELETE ME - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f - NOTE NUM 3 - 03210848-24c6-42ac-b918-74aba63e40f3",
        287: "TEST NOTE - PUBLISHER TEST PUBLISHER - 8 - DELETE ME - 4cb1d31d-8bc8-4c7b-b7e4-c7865e477d6f - NOTE NUM 4 - f9a27800-4067-4b82-a3a7-e9d18a9b8bcf",
        288: "TEST NOTE - PUBLISHER TEST PUBLISHER - 9 - DELETE ME - 2f7de989-32fd-4263-8020-e99d9358a89a - NOTE NUM 1 - aec6fe91-2a11-4608-af38-a01d0e7cab1d",
        289: "TEST NOTE - PUBLISHER TEST PUBLISHER - 9 - DELETE ME - 2f7de989-32fd-4263-8020-e99d9358a89a - NOTE NUM 2 - 52ee46ad-4981-44e1-98af-cf9379e9de5c",
        290: "TEST NOTE - PUBLISHER TEST PUBLISHER - 9 - DELETE ME - 2f7de989-32fd-4263-8020-e99d9358a89a - NOTE NUM 3 - 54e78ee6-a8f0-4d60-869f-86fc15dfc181",
        291: "TEST NOTE - PUBLISHER TEST PUBLISHER - 9 - DELETE ME - 2f7de989-32fd-4263-8020-e99d9358a89a - NOTE NUM 4 - 5a097718-2b1b-48d6-9e1c-424275d0e3c4",
        292: "TEST NOTE - PUBLISHER TEST PUBLISHER - 9 - DELETE ME - 2f7de989-32fd-4263-8020-e99d9358a89a - NOTE NUM 5 - 4cd96070-5028-4c4b-bb86-b6c020deb4a2",
        293: "TEST NOTE - PUBLISHER TEST PUBLISHER - 10 - DELETE ME - 4bce79fc-83a2-43fb-8505-ef9a415bb35f - NOTE NUM 1 - 3542b654-e2d8-48a2-aaa3-7882aa50e259",
        294: "TEST NOTE - PUBLISHER TEST PUBLISHER - 10 - DELETE ME - 4bce79fc-83a2-43fb-8505-ef9a415bb35f - NOTE NUM 2 - c47407dd-bd9e-478b-adfa-0585e8dee677",
        295: "TEST NOTE - SERIES s-2-3542b654 - NOTE NUM 1 - a2a2b752-5f8e-4702-9e49-3eaaa4a1fd5a",
        296: "TEST NOTE - SERIES s-2-3542b654 - NOTE NUM 2 - 146ab80f-261f-44b3-a25a-85379e76abe9",
        297: "TEST NOTE - SERIES s-3-fdddde7e - NOTE NUM 1 - 078c31b9-a5f2-42ac-8621-6d9bc9355d4f",
        298: "TEST NOTE - SERIES s-3-fdddde7e - NOTE NUM 2 - 3dd78a6f-ff02-4916-a795-f35b7b86e84c",
        299: "TEST NOTE - SERIES s-3-fdddde7e - NOTE NUM 3 - d27ef268-7a63-4dfa-8876-03a6de8f3e93",
        300: "TEST NOTE - SERIES s-4-efda05b4 - NOTE NUM 1 - d926b73e-bace-4d92-9406-a38e4b5de7b7",
        301: "TEST NOTE - SERIES s-5-650530d5 - NOTE NUM 1 - 1cd4b089-63e0-4340-b387-4275a9e18a51",
        302: "TEST NOTE - SERIES s-6-c187b301 - NOTE NUM 1 - 0ea3cee6-359e-4c61-b414-461d61057b3d",
        303: "TEST NOTE - SERIES s-6-c187b301 - NOTE NUM 2 - 16298153-d0c1-4ea2-af5f-0c78cd6e46ba",
        304: "TEST NOTE - SERIES s-6-c187b301 - NOTE NUM 3 - 4324fdc0-8cb1-4aa7-bb25-1714dd39cdca",
        305: "TEST NOTE - SERIES s-6-c187b301 - NOTE NUM 4 - cdf28d51-749d-4f0f-bff8-2668abd652a1",
        306: "TEST NOTE - SERIES s-6-c187b301 - NOTE NUM 5 - 9b9e8537-31cf-4683-af4b-5e2a33b75e8a",
        307: "TEST NOTE - SERIES s-7-d759e0bc - NOTE NUM 1 - c78882ba-397a-4677-954d-e3b330f7f16e",
        308: "TEST NOTE - SERIES s-7-d759e0bc - NOTE NUM 2 - 56f272d2-bd4e-447d-8a3d-171357a5f6e2",
        309: "TEST NOTE - SERIES s-7-d759e0bc - NOTE NUM 3 - 79085d34-1849-4acf-802e-5580ad1c86bb",
        310: "TEST NOTE - SERIES s-8-c47407dd - NOTE NUM 1 - 0f2d241b-1c38-48a3-9bce-49c99c47081e",
        311: "TEST NOTE - SERIES s-10-9b88b308 - NOTE NUM 1 - 44e87e9e-d716-4e10-89ba-db03682f20ed",
        312: "TEST NOTE - SERIES s-10-9b88b308 - NOTE NUM 2 - 49458593-07f1-48f6-834b-fbafdfab119d",
        313: "TEST NOTE - SERIES s-10-9b88b308 - NOTE NUM 3 - d6c55d6e-41ce-47d6-ac02-6311716cef04",
        314: "TEST NOTE - SERIES s-10-9b88b308 - NOTE NUM 4 - 0c4ffde3-a02f-4461-a089-c72979297354",
        315: "TEST NOTE - TITLE 1 - NOTE NUM 1 - 036a5fbb-570b-42a6-b4d0-30d46a05a4ed",
        316: "TEST NOTE - TITLE 2 - NOTE NUM 1 - 984d4dec-2ccf-4d81-b6e7-8420430262fd",
        317: "TEST NOTE - TITLE 2 - NOTE NUM 2 - 08bc4130-9b0d-4e65-8a08-e7adc416c84b",
        318: "TEST NOTE - TITLE 2 - NOTE NUM 3 - 1a87595e-f0a3-4ada-8a51-5e1f441bafbe",
        319: "TEST NOTE - TITLE 2 - NOTE NUM 4 - 5211863b-3764-4ed2-b938-40ec0abc7b44",
        320: "TEST NOTE - TITLE 3 - NOTE NUM 1 - 849b8e8d-95a4-4018-9510-45fed629ee65",
        321: "TEST NOTE - TITLE 3 - NOTE NUM 2 - 6e2a8e93-b7ad-4673-bc9e-589294004c38",
        322: "TEST NOTE - TITLE 4 - NOTE NUM 1 - 0d4aa6b0-9c1c-4a14-a661-66838257faad",
        323: "TEST NOTE - TITLE 5 - NOTE NUM 1 - 56dd1b6a-0342-453b-b779-e12492319aa9",
        324: "TEST NOTE - TITLE 6 - NOTE NUM 1 - d6c55d6e-41ce-47d6-ac02-6311716cef04",
        325: "TEST NOTE - TITLE 7 - NOTE NUM 1 - 43024a39-3cd9-4ad8-8b1b-c2d79f900c4b",
        326: "TEST NOTE - TITLE 7 - NOTE NUM 2 - b5f0aed6-9956-481b-a11e-ab83847884d8",
        327: "TEST NOTE - TITLE 7 - NOTE NUM 3 - 55323a3a-bacf-4056-b5f5-e6f540b92d05",
        328: "TEST NOTE - TITLE 8 - NOTE NUM 1 - 635c7cf4-4a46-402d-b219-af0479b5aa3e",
        329: "TEST NOTE - TITLE 8 - NOTE NUM 2 - 01627073-b7f5-4602-819a-b85ba07394ee",
        330: "TEST NOTE - TITLE 8 - NOTE NUM 3 - 8567479b-4856-44b3-bf9e-6ebd73476942",
        331: "TEST NOTE - TITLE 9 - NOTE NUM 1 - a2a2b752-5f8e-4702-9e49-3eaaa4a1fd5a",
        332: "TEST NOTE - TITLE 9 - NOTE NUM 2 - 090a79e4-b4cf-40fa-9163-36773b820b92",
        333: "TEST NOTE - TITLE 9 - NOTE NUM 3 - fd1c0506-3859-4f96-b42a-1fe2f99e0b1e",
        334: "TEST NOTE - TITLE 9 - NOTE NUM 4 - 93acce3a-7b3c-4afb-b717-b63150537fa6",
        335: "TEST NOTE - TITLE 9 - NOTE NUM 5 - 3542b654-e2d8-48a2-aaa3-7882aa50e259",
        336: "TEST NOTE - TITLE 10 - NOTE NUM 1 - 96ebca6d-fd03-4449-995b-210a0fccf6a3",
        337: "TEST NOTE - TITLE 10 - NOTE NUM 2 - 78affa14-a6cb-4f88-a57e-99cb308f99f1",
        338: "TEST NOTE - TITLE 10 - NOTE NUM 3 - 02c1e800-e4b1-41ea-9c03-9f1e945725f3",
        339: "TEST NOTE - TITLE 10 - NOTE NUM 4 - 546667c5-de19-4c85-9d79-14ee90d9188a",
    }

    # note values
    theo_note_1_val = theo_note_val_map[1]
    theo_note_2_val = theo_note_val_map[2]
    theo_note_3_val = theo_note_val_map[3]
    theo_note_4_val = theo_note_val_map[4]
    theo_note_5_val = theo_note_val_map[5]
    theo_note_6_val = theo_note_val_map[6]
    theo_note_7_val = theo_note_val_map[7]
    theo_note_8_val = theo_note_val_map[8]
    theo_note_9_val = theo_note_val_map[9]
    theo_note_10_val = theo_note_val_map[10]

    theo_note_12_val = theo_note_val_map[12]
    theo_note_13_val = theo_note_val_map[13]
    theo_note_14_val = theo_note_val_map[14]
    theo_note_15_val = theo_note_val_map[15]

    theo_note_28_value = theo_note_val_map[28]

    theo_note_79_val = theo_note_val_map[79]

    theo_note_100_val = theo_note_val_map[100]

    theo_note_201_val = theo_note_val_map[201]
    theo_note_202_val = theo_note_val_map[202]
    theo_note_203_val = theo_note_val_map[203]
    theo_note_204_val = theo_note_val_map[204]
    theo_note_205_val = theo_note_val_map[205]

    theo_note_207_val = theo_note_val_map[207]
    theo_note_208_val = theo_note_val_map[208]
    theo_note_209_val = theo_note_val_map[209]
    theo_note_210_val = theo_note_val_map[210]

    theo_note_212_val = theo_note_val_map[212]
    theo_note_213_val = theo_note_val_map[213]
    theo_note_214_val = theo_note_val_map[214]
    theo_note_215_val = theo_note_val_map[215]

    theo_note_279_val = theo_note_val_map[279]

    theo_note_300_val = theo_note_val_map[300]

    theo_note_312_value = theo_note_val_map[312]

    theo_note_316_val = theo_note_val_map[316]

    theo_note_320_val = theo_note_val_map[320]

    theo_note_323_val = theo_note_val_map[323]
    theo_note_324_val = theo_note_val_map[324]
    theo_note_325_val = theo_note_val_map[325]

    theo_title_note_count_map = {
        1: 1,
        2: 4,
        3: 2,
        4: 1,
        5: 1,
        6: 1,
        7: 3,
        8: 3,
        9: 5,
        10: 4,
    }

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - SUBJECTS

    theo_title_subject_count_map = {
        1: 4,
        2: 3,
        3: 3,
        4: 0,
        5: 4,
        6: 3,
        7: 5,
        8: 3,
        9: 4,
        10: 0,
    }

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - SYNOPSES

    theo_title_synopsis_count_map = {
        1: 0,
        2: 2,
        3: 3,
        4: 1,
        5: 1,
        6: 5,
        7: 3,
        8: 1,
        9: 0,
        10: 4,
    }

    #
    # ------------------------------------------------------------------------------------------------------------------

    # creator-title properties
