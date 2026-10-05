
"""
Verify Calibre-like book metadata construction and compatibility behavior.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test calibre book metadata through its owning regression module::

        python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_calibre_book_metadata.py
"""
from collections import OrderedDict


"""
Tests the book metadata class - which holds data for a book.
"""


class TestBookMetadataClass:
    """
    Tests the book metadata class - which holds data for a book.

    Example:
        Exercise TestBookMetadataClass through its owning regression module::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_calibre_book_metadata.py
    """
    def test_metadata_class_init(self) -> None:
        """
        Tests that we can actually init the class.

        Example:
            Exercise TestBookMetadataClass.test metadata class init through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_calibre_book_metadata.py


        :return: None; the function records state or raises through its assertions.
        """
        from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData

        test_metadata = CalibreLikeLiuXinBookMetaData()
        assert test_metadata is not None

    def test_metadata_get_methods(self) -> None:
        """
        Tests the get methods on a metadata object.

        Example:
            Exercise TestBookMetadataClass.test metadata get methods through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_calibre_book_metadata.py


        :return: None; the function records state or raises through its assertions.
        """
        from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData

        test_metadata = CalibreLikeLiuXinBookMetaData()
        assert test_metadata is not None

        assert test_metadata.get(field="tags", default="Not This") == OrderedDict()

    def test_metadata_add_isbn(self) -> None:
        """
        Tests adding an ISBN to the metadata.

        Example:
            Exercise TestBookMetadataClass.test metadata add isbn through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_calibre_book_metadata.py


        :return: None; the function records state or raises through its assertions.
        """
        from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData

        test_metadata = CalibreLikeLiuXinBookMetaData()
        test_metadata.isbn = "978-1234-5678"

        assert test_metadata is not None
        assert test_metadata.isbn == OrderedDict({'978-1234-5678': None})

    def test_metadata_add_asin(self) -> None:
        """
        Tests adding an ISBN to the metadata.

        Example:
            Exercise TestBookMetadataClass.test metadata add asin through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_calibre_book_metadata.py


        :return: None; the function records state or raises through its assertions.
        """
        from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData

        test_metadata = CalibreLikeLiuXinBookMetaData()
        test_metadata.asin = "978-1234-5678"

        assert test_metadata is not None
        assert test_metadata.asin == OrderedDict({'978-1234-5678': None})
