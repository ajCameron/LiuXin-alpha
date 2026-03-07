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

    # calibre-style string/bool behavior should exist on compat metadata.
    assert isinstance(str(mi), str)
    assert bool(mi) is True


def test_calibre_book_constants_are_exposed_after_install() -> None:
    from LiuXin_alpha.utils.calibre_compat.install import install_calibre_shims as install

    install()

    from calibre.ebooks.metadata.book import (
        ALL_METADATA_FIELDS,
        SC_COPYABLE_FIELDS,
        SC_FIELDS_COPY_NOT_NULL,
        STANDARD_METADATA_FIELDS,
        TOP_LEVEL_IDENTIFIERS,
    )

    assert "title" in ALL_METADATA_FIELDS
    assert "authors" in STANDARD_METADATA_FIELDS
    assert "isbn" in TOP_LEVEL_IDENTIFIERS
    assert isinstance(SC_COPYABLE_FIELDS, frozenset)
    assert isinstance(SC_FIELDS_COPY_NOT_NULL, frozenset)


def test_calibre_base_module_exposes_expected_api_surface() -> None:
    from LiuXin_alpha.utils.calibre_compat.install import install_calibre_shims as install

    install()

    import calibre.ebooks.metadata.book.base as base

    expected_names = (
        "Metadata",
        "MetaInformation",
        "FieldMetadata",
        "NULL_VALUES",
        "SIMPLE_GET",
        "SIMPLE_SET",
        "ck",
        "cv",
        "field_from_string",
        "field_metadata",
        "human_readable",
        "reset_field_metadata",
        "get_model_metadata_instance",
    )
    for name in expected_names:
        assert hasattr(base, name)


def test_calibre_metadata_parity_helper_methods_exist_and_work() -> None:
    from LiuXin_alpha.utils.calibre_compat.install import install_calibre_shims as install

    install()

    from calibre.ebooks.metadata.book.base import Metadata

    mi = Metadata("Title", ["Author"])
    mi.set_null("title")
    # calibre semantics: null title is a translated Unknown placeholder.
    assert mi.title

    # Composite evaluators should be no-op safe for non-composite fields.
    mi._evaluate_composite("#does_not_exist")
    mi._evaluate_all_composites()

    mi.set_user_metadata("#x", {"name": "x", "datatype": "text", "is_multiple": False, "#value#": "v"})
    other = Metadata("Other", ["B"])
    # empty user metadata on `other` should prune `mi` custom metadata
    mi.remove_stale_user_metadata(other)
    assert "#x" not in list(mi.custom_field_keys())
