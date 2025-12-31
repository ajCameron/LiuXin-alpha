
"""
Tests basic calling methods.
"""

import pytest



class TestCalibreCreatorMethods:
    """
    Tests the book metadata class - which holds data for a book.
    """
    def test_creator_methods__getattr__(self) -> None:
        """
        Tests that we can actually init the class.

        :return:
        """
        from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData

        test_metadata = CalibreLikeLiuXinBookMetaData()
        assert test_metadata is not None

        assert test_metadata.creators == {'artists': [],
 'authors': [],
 'book_producer': [],
 'colorists': [],
 'composers': [],
 'cover_artists': [],
 'directors': [],
 'editors': [],
 'illustrators': [],
 'producers': [],
 'translators': []}

    def test_creator_methods_set_creators(self) -> None:
        """
        Attempting to just directly set the creators dict should fail.

        :return:
        """
        from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData

        test_metadata = CalibreLikeLiuXinBookMetaData()
        assert test_metadata is not None

        with pytest.raises(AttributeError):
            test_metadata.creators = None
