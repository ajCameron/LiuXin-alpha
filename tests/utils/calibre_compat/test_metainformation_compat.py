"""
Provide test metainformation compat utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test metainformation compat through a consuming regression::

        python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py
"""
from __future__ import annotations


def test_calibre_metainformation_imports_without_namespace_injection() -> None:

    """
    Perform the test calibre metainformation imports without namespace injection utility operation under explicit compatibility rules.

    Example:
        Exercise test calibre metainformation imports without namespace injection through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import sys

    external_before = {
        name for name in sys.modules if name == "calibre" or name.startswith("calibre.")
    }
    from LiuXin_alpha.utils.calibre_compat.ebooks.metadata import (
        MetaInformation,
        authors_to_string,
        string_to_authors,
    )
    from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import Metadata
    from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import (
        MetaInformation as BaseMetaInformation,
    )

    assert {
        name for name in sys.modules if name == "calibre" or name.startswith("calibre.")
    } == external_before
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


def test_calibre_book_constants_are_available_from_the_owner() -> None:

    """
    Perform the test calibre book constants are available from the owner utility operation under explicit compatibility rules.

    Example:
        Exercise test calibre book constants are available from the owner through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book import (
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

    """
    Perform the test calibre base module exposes expected api surface utility operation under explicit compatibility rules.

    Example:
        Exercise test calibre base module exposes expected api surface through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base as base

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

    """
    Perform the test calibre metadata parity helper methods exist and work utility operation under explicit compatibility rules.

    Example:
        Exercise test calibre metadata parity helper methods exist and work through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_metainformation_compat.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import Metadata

    mi = Metadata("Title", ["Author"])
    mi.set_null("title")
    # calibre semantics: null title is a translated Unknown placeholder.
    assert mi.title

    # Composite evaluators should be no-op safe for non-composite fields.
    mi._evaluate_composite("#does_not_exist")
    mi._evaluate_all_composites()

    mi.set_user_metadata(
        "#x", {"name": "x", "datatype": "text", "is_multiple": False, "#value#": "v"}
    )
    other = Metadata("Other", ["B"])
    # empty user metadata on `other` should prune `mi` custom metadata
    mi.remove_stale_user_metadata(other)
    assert "#x" not in list(mi.custom_field_keys())
