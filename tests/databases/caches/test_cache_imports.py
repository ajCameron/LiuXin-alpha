



class TestCacheImportAPIs:
    """
    Tests that we can actually import cache objects at all.
    """
    def test_cache_imports_smple(self) -> None:
        """
        Tries to import cache objects.

        :return:
        """
        from LiuXin_alpha.library.caches.base_calibre.fields import BaseField

        assert BaseField is not None