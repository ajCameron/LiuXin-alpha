"""
Check notification and dirtied callbacks for embedded and standalone database doubles.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_notify.py
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.databases.notify import dummy_dirtied, dummy_notify


class _FakeCCClassNotEmbedded:
    """
    Supply embed=False for the standalone callback branch.

    Example:
        >>> _FakeCCClassNotEmbedded.embed
        False
    """
    embed = False


class _FakeCCClassEmbedded:
    """
    Supply embed=True for the unsupported embedded callback branch.

    Example:
        >>> _FakeCCClassEmbedded.embed
        True
    """
    embed = True


class TestDummyNotify:
    """
    Check no-op notifications and rejection of embedded notifications.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_notify.py
    """
    def test_no_op_when_not_embedded(self) -> None:
        # Should return None silently
        """
        Check that standalone notification returns None.

        Example:
            >>> TestDummyNotify().test_no_op_when_not_embedded()


        :return: None; failed expectations raise AssertionError.
        """
        result = dummy_notify("added", [1, 2, 3], _FakeCCClassNotEmbedded())
        assert result is None

    def test_raises_not_implemented_when_embedded(self) -> None:
        """
        Check that embedded notification raises NotImplementedError.

        Example:
            >>> TestDummyNotify().test_raises_not_implemented_when_embedded()


        :return: None; failed expectations raise AssertionError.
        """
        with pytest.raises(NotImplementedError):
            dummy_notify("added", [1], _FakeCCClassEmbedded())

    def test_accepts_arbitrary_event_names(self) -> None:
        """
        Exercise standalone deleted and updated notifications without an exception.

        Example:
            >>> TestDummyNotify().test_accepts_arbitrary_event_names()


        :return: None; failed expectations raise AssertionError.
        """
        dummy_notify("deleted", [], _FakeCCClassNotEmbedded())
        dummy_notify("updated", [99], _FakeCCClassNotEmbedded())

    def test_accepts_empty_ids_list(self) -> None:
        """
        Exercise a standalone notification with no IDs.

        Example:
            >>> TestDummyNotify().test_accepts_empty_ids_list()


        :return: None; failed expectations raise AssertionError.
        """
        dummy_notify("any_event", [], _FakeCCClassNotEmbedded())


class TestDummyDirtied:
    """
    Check dirtied callbacks with embedded flags, empty IDs, and commit choices.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_notify.py
    """
    def test_no_op_when_not_embedded(self) -> None:
        """
        Check that the standalone dirtied callback returns None with commit enabled.

        Example:
            >>> TestDummyDirtied().test_no_op_when_not_embedded()


        :return: None; failed expectations raise AssertionError.
        """
        result = dummy_dirtied([1, 2], commit=True, cc_class=_FakeCCClassNotEmbedded())
        assert result is None

    def test_raises_not_implemented_when_embedded(self) -> None:
        """
        Check that the embedded dirtied callback rejects commit=False.

        Example:
            >>> TestDummyDirtied().test_raises_not_implemented_when_embedded()


        :return: None; failed expectations raise AssertionError.
        """
        with pytest.raises(NotImplementedError):
            dummy_dirtied([1], commit=False, cc_class=_FakeCCClassEmbedded())

    def test_accepts_empty_ids(self) -> None:
        """
        Exercise the standalone dirtied callback with an empty ID list.

        Example:
            >>> TestDummyDirtied().test_accepts_empty_ids()


        :return: None; failed expectations raise AssertionError.
        """
        dummy_dirtied([], commit=False, cc_class=_FakeCCClassNotEmbedded())

    def test_commit_flag_does_not_affect_non_embed_path(self) -> None:
        """
        Exercise both commit flags on the standalone dirtied callback.

        Example:
            >>> TestDummyDirtied().test_commit_flag_does_not_affect_non_embed_path()


        :return: None; failed expectations raise AssertionError.
        """
        dummy_dirtied([1], commit=True, cc_class=_FakeCCClassNotEmbedded())
        dummy_dirtied([1], commit=False, cc_class=_FakeCCClassNotEmbedded())
