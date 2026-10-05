
"""
Verify creator mutation and role handling on Calibre-like metadata.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test calibre creator methods through its owning regression module::

        python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_calibre_creator_methods.py
"""

import pytest



class TestCalibreCreatorMethods:
    """
    Tests the book metadata class - which holds data for a book.

    Example:
        Exercise TestCalibreCreatorMethods through its owning regression module::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_calibre_creator_methods.py
    """
    def test_creator_methods__getattr__(self) -> None:
        """
        Tests that we can actually init the class.

        Example:
            Exercise TestCalibreCreatorMethods.test creator methods  getattr through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_calibre_creator_methods.py


        :return: None; the function records state or raises through its assertions.
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

        Example:
            Exercise TestCalibreCreatorMethods.test creator methods set creators through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_calibre_creator_methods.py


        :return: None; the function records state or raises through its assertions.
        """
        from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData

        test_metadata = CalibreLikeLiuXinBookMetaData()
        assert test_metadata is not None

        with pytest.raises(AttributeError):
            test_metadata.creators = None
