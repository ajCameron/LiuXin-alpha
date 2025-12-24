
from LiuXin_alpha.utils.storage.hashes import sane_hash



class TestSaneHash:
    """
    Tests the sane hash function.
    """
    def test_sane_hash_rwe(self) -> None:
        """
        Tests sane hash function.

        :return:
        """
        str_hashed =sane_hash(data=b"This is a test and this should be a hash.")

        assert str_hashed == '9fa6399becc3913bf8b61fc00aa8a6df965c6c9ea5705a3c218cd95209432188'