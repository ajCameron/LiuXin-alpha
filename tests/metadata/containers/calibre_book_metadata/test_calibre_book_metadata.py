
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
        from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData

        test_metadata = CalibreLikeLiuXinBookMetaData()
        assert test_metadata is not None

    def test_metadata_get_methods(self) -> None:
        """
        Tests the get methods on a metadata object.

        :return:
        """
        from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData

        test_metadata = CalibreLikeLiuXinBookMetaData()
        assert test_metadata is not None

        assert test_metadata.get(field="tags", default="Not This") == OrderedDict()

    def test_metadata_add_isbn(self) -> None:
        """
        Tests adding an ISBN to the metadata.

        :return:
        """
        from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData

        test_metadata = CalibreLikeLiuXinBookMetaData()
        test_metadata.isbn = "978-1234-5678"

        assert test_metadata is not None
        assert test_metadata.isbn == OrderedDict({'978-1234-5678': None})

    def test_metadata_add_asin(self) -> None:
        """
        Tests adding an ISBN to the metadata.

        :return:
        """
        from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData

        test_metadata = CalibreLikeLiuXinBookMetaData()
        test_metadata.asin = "978-1234-5678"

        assert test_metadata is not None
        assert test_metadata.asin == OrderedDict({'978-1234-5678': None})
