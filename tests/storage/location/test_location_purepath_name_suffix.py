"""Names, suffixes, and stems are placement metadata—not Location facts."""

from __future__ import annotations


def test_location_does_not_infer_filename_components(store) -> None:
    location = store.locate("opaque/foo.tar.gz")

    for attribute in ("name", "suffix", "suffixes", "stem", "with_name", "with_suffix"):
        assert not hasattr(location, attribute)


def test_file_info_does_not_invent_an_original_filename(store) -> None:
    info = store.store_bytes(b"book", location="opaque/identifier")

    assert info.location.key == "opaque/identifier"
    assert not hasattr(info, "name")
