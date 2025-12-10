
from collections import OrderedDict


"""
Tests the book metadata class - which holds data for a book.
"""


class TestBookMetadataClass:
    """
    Tests the book metadata class - which holds data for a book.
    """
    def test_metadata_class_init(self) -> None:
        """
        Tests that we can actually init the class.

        :return:
        """
        from LiuXin_alpha.metadata.containers.calibre_book_metadata import CalibreLikeBookMetaData

        test_metadata = CalibreLikeBookMetaData()
        assert test_metadata is not None

    def test_metadata_get_methods(self) -> None:
        """
        Tests the get methods on a metadata object.

        :return:
        """
        from LiuXin_alpha.metadata.containers.calibre_book_metadata import CalibreLikeBookMetaData

        test_metadata = CalibreLikeBookMetaData()
        assert test_metadata is not None

        assert test_metadata.get(field="tags", default="Not This") == OrderedDict()