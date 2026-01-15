from __future__ import annotations


def test_calibre_metainformation_is_available_after_install() -> None:
    from LiuXin_alpha.utils.calibre_compat.install import install_calibre_shims as install

    # Idempotent install (should be safe to call multiple times).
    install()
    install()

    from calibre.ebooks.metadata import MetaInformation, authors_to_string, string_to_authors
    from calibre.ebooks.metadata.book.base import MetaInformation as BaseMetaInformation, Metadata

    mi = MetaInformation("Some Title", ["A. Author"])

    assert isinstance(mi, Metadata)
    assert isinstance(mi, BaseMetaInformation)

    # Arbitrary attributes should be settable
    mi.publisher = "Example Publisher"
    assert mi.publisher == "Example Publisher"

    # Smoke-test helpers are exposed
    assert authors_to_string(["A. Author"])
    assert string_to_authors("A. Author") == ["A. Author"]
