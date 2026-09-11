"""
Check that addresses and FileInfo do not invent filename metadata.

Selected pathlib-style name/suffix/stem operations are absent; the Store retains
an explicit opaque key after real publication.
"""

from __future__ import annotations


def test_location_does_not_infer_filename_components(store) -> None:
    """
    Resolve a multi-suffix key and require the six listed pathlib-style name/suffix/stem and
    replacement attributes to be absent.

    Example:
        >>> test_location_does_not_infer_filename_components(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    location = store.locate("opaque/foo.tar.gz")

    for attribute in ("name", "suffix", "suffixes", "stem", "with_name", "with_suffix"):
        assert not hasattr(location, attribute)


def test_file_info_does_not_invent_an_original_filename(store) -> None:
    """
    Publish bytes under an explicit opaque identifier, require that key in FileInfo.location, and
    require no name attribute on FileInfo.

    Example:
        >>> test_file_info_does_not_invent_an_original_filename(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    info = store.store_bytes(b"book", location="opaque/identifier")

    assert info.location.key == "opaque/identifier"
    assert not hasattr(info, "name")
