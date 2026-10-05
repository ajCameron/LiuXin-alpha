
"""
Provide test sane hash utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test sane hash through a consuming regression::

        python -m pytest -q tests/utils/storage/hashes/test_sane_hash.py
"""
from LiuXin_alpha.utils.storage.hashes import sane_hash



class TestSaneHash:
    """
    Tests the sane hash function.

    Example:
        Exercise TestSaneHash through a consuming regression::

            python -m pytest -q tests/utils/storage/hashes/test_sane_hash.py
    """
    def test_sane_hash_rwe(self) -> None:
        """
        Tests sane hash function.

        Example:
            Exercise TestSaneHash.test sane hash rwe through a consuming regression::

                python -m pytest -q tests/utils/storage/hashes/test_sane_hash.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        str_hashed =sane_hash(data=b"This is a test and this should be a hash.")

        assert str_hashed == '9fa6399becc3913bf8b61fc00aa8a6df965c6c9ea5705a3c218cd95209432188'
