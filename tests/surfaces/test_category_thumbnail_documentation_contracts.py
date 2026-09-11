"""
Verify documented legacy category and thumbnail-cache behavior without changing the retained implementation.

Category providers are recording in-memory doubles. Thumbnail operations use only
pytest temporary directories, including deliberate missing/corrupt files and
orphan cleanup checks. Regression tests make Python 3 limitations explicit rather
than treating logging, deferred invalidation, or resizing as successfully repaired.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest

from LiuXin_alpha.core import CoreClientAPI
from LiuXin_alpha.surfaces import categories
from LiuXin_alpha.surfaces.core import CoreSurfaceModel
from LiuXin_alpha.surfaces.images.api import ImageBackend
from LiuXin_alpha.surfaces.tags_icons import TagsIcons, category_icon_map
from LiuXin_alpha.surfaces.thumbnail_cache import ThumbnailCache


def _metadata(definitions: dict[str, dict[str, Any]]) -> MagicMock:
    """
    Build legacy field metadata with complete defaults and recording lookup methods for category tests.

    Example:
        >>> metadata = _metadata({"tags": {}})
        >>> metadata["tags"]["in_table"]
        'books'


    :param definitions: Field-key mappings overriding category, datatype, table, and display defaults.
    :return: MagicMock exposing repeatable iteritems, keyed descriptions, labels, and custom-field flags.
    """
    records = {
        name: {
            "is_category": True,
            "kind": "field",
            "datatype": "text",
            "in_table": "books",
            "is_multiple": {},
            "display": {},
            "is_custom": False,
            **definition,
        }
        for name, definition in definitions.items()
    }
    metadata = MagicMock()
    metadata.iteritems.return_value = tuple(records.items())
    metadata.__getitem__.side_effect = records.__getitem__
    metadata.key_to_label.side_effect = {name: name for name in records}.__getitem__
    metadata.is_custom_field.side_effect = {
        name: value["is_custom"] for name, value in records.items()
    }.__getitem__
    return metadata


def _category_cache(
    definitions: dict[str, dict[str, Any]],
    rows: dict[str, list[categories.Tag]],
    *,
    preferences: dict[str, Any] | None = None,
) -> SimpleNamespace:
    """
    Supply recording field providers, preference access, proxy lookup, and saved searches to the real category assembler.

    Example:
        >>> cache = _category_cache({"rating": {"datatype": "rating"}}, {"rating": []})
        >>> categories.get_categories(cache)["rating"]
        []


    :param definitions: Legacy field metadata passed to the metadata double builder.
    :param rows: Mutable category lists returned unchanged by ordinary field providers.
    :param preferences: Optional preference mapping read through its get method without persistence side effects.
    :return: Namespace implementing the legacy cache members exercised by category generation.
    """
    preference_values = {} if preferences is None else preferences
    fields = {
        name: Mock(book_value_map={})
        for name in set(definitions) | {"rating", "languages"}
    }
    for name, provider in fields.items():
        provider.get_categories.return_value = rows.get(name, [])
    return SimpleNamespace(
        field_metadata=_metadata(definitions),
        fields=fields,
        pref=Mock(side_effect=preference_values.get),
        set_pref=Mock(),
        _all_book_ids=Mock(return_value={1, 2}),
        _get_proxy_metadata=Mock(return_value=None),
        _search_api=SimpleNamespace(saved_searches=SimpleNamespace(queries={})),
    )


def test_tag_values_share_ids_and_keep_legacy_string_recursion() -> None:
    """
    Retain caller-owned ID sets, halved ratings, independent counts, and the current failing str/repr hooks.

    Example:
        >>> test_tag_values_share_ids_and_keep_legacy_string_recursion()


    :return: None after mutable identity, field initialization, text-hook, and recursion assertions.
    """
    identifiers = {1}
    tag = categories.Tag(
        "Fiction", id=7, count=99, avg=8, category="tags", id_set=identifiers
    )
    assert tag.id_set is identifiers and tag.avg_rating == 4 and tag.count == 99
    identifiers.add(2)
    assert tag.id_set == {1, 2} and tag.is_hierarchical == ""
    assert tag.__unicode__().startswith("Fiction:99:7:0:tags:")
    assert categories.Tag("empty").id_set is not categories.Tag("empty").id_set
    for converter in (str, repr):
        with pytest.raises(RecursionError):
            converter(tag)


def test_category_discovery_respects_books_and_branch_precedence() -> None:
    """
    Keep books-table filtering and the standard-category branch ahead of composite display-category selection.

    Example:
        >>> test_category_discovery_respects_books_and_branch_precedence()


    :return: None after exact ordered category/separator/composite triples are checked.
    """
    metadata = _metadata(
        {
            "tags": {"is_multiple": {"cache_to_list": ","}},
            "nonbooks": {"in_table": "works"},
            "user": {"kind": "user"},
            "search": {"kind": "search"},
            "composite": {
                "is_category": False,
                "datatype": "composite",
                "display": {"make_category": True},
            },
            "ordinary-composite": {
                "datatype": "composite",
                "display": {"make_category": True},
            },
            "hidden-composite": {
                "is_category": False,
                "datatype": "composite",
                "display": {"make_category": True},
                "in_table": "works",
            },
        }
    )
    assert list(categories.find_categories(metadata)) == [
        ("tags", ",", False),
        ("composite", None, True),
        ("ordinary-composite", None, False),
    ]


def test_icon_maps_accept_none_and_keep_partial_initialization_and_label_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Require icon-key presence but preserve None values, partial reinitialization, and category-versus-label lookup behavior.

    Example:
        >>> with pytest.MonkeyPatch.context() as patch:
        ...     test_icon_maps_accept_none_and_keep_partial_initialization_and_label_lookup(patch)


    :param monkeypatch: Fixture replacing only the category module's author-name tweak mapping.
    :return: None after icon-copy, custom alias, partial failure, label mismatch, and author-display assertions.
    """
    icons = TagsIcons(
        {key: None for key in TagsIcons.category_icons} | {"extra": "ignored"}
    )
    assert len(icons) == 13 and "extra" not in icons and icons["tags"] is None
    icons["retained"] = "existing"
    with pytest.raises(ValueError, match="series"):
        icons.__init__({"authors": "replacement"})
    assert icons["authors"] == "replacement" and icons["retained"] == "existing"
    monkeypatch.setattr(
        categories, "tweaks", {"categories_use_field_for_author_name": "author_sort"}
    )
    metadata = _metadata(
        {
            "custom": {
                "is_custom": True,
                "is_multiple": {"cache_to_list": ","},
                "display": {"is_names": True},
            }
        }
    )
    icon = object()
    icons["custom:"] = icon
    factory = categories.create_tag_class("custom", metadata, icons)
    assert (
        factory.func is categories.Tag and factory.keywords["use_sort_as_name"] is True
    )
    assert icons["custom"] is icon and factory("Name").icon is icon
    metadata = _metadata({"tags": {}})
    metadata.key_to_label.side_effect = None
    metadata.key_to_label.return_value = "missing-label"
    with pytest.raises(KeyError, match="missing-label"):
        categories.create_tag_class("tags", metadata, TagsIcons(category_icon_map))


def test_user_category_cleaning_preserves_collision_and_save_failure_behavior() -> None:
    """
    Keep last-value wins for generated/normalized names and suppress even BaseException during preference persistence.

    Example:
        >>> test_user_category_cleaning_preserves_collision_and_save_failure_behavior()


    :return: None after normalization collisions, shared value identity, and swallowed save interruption are checked.
    """
    first, last, dotted = [], ["last"], ["dotted"]
    cache = Mock()
    cache.pref.return_value = {" ": first, "...": last, " A . B ": first, "A.B": dotted}
    cache.set_pref.side_effect = KeyboardInterrupt("simulated save interruption")
    result = categories.clean_user_categories(cache)
    assert result == {"1": last, "A.B": dotted}
    assert result["1"] is last and result["A.B"] is dotted
    cache.set_pref.assert_called_once_with("user_categories", result)


def test_category_sorting_is_in_place_and_unknown_tokens_use_name_order() -> None:
    """
    Keep stable descending count/rating sorts and the unvalidated name fallback of the standalone sorter.

    Example:
        >>> test_category_sorting_is_in_place_and_unknown_tokens_use_name_order()


    :return: None after list identity, tie order, sort-value preference, and rating precedence checks.
    """
    first = categories.Tag("z", count=2, avg=2, sort="a")
    second = categories.Tag("a", count=2, avg=8, sort="z")
    items = [first, second]
    assert categories.sort_categories(items, "popularity") is items
    assert items[0] is first
    categories.sort_categories(items, "not-validated-here")
    assert items[0] is first
    categories.sort_categories(items, "rating")
    assert items[0] is second


def test_category_assembly_keeps_rating_merge_and_empty_selection_semantics() -> None:
    """
    Retain mutation-during-rating merging, unchanged empty selection objects, and early top-level argument validation.

    Example:
        >>> test_category_assembly_keeps_rating_merge_and_empty_selection_semantics()


    :return: None after residual duplicate ratings, aggregate counts, forwarded selection, and validation ordering checks.
    """
    rating_rows = [
        categories.Tag("same", id=number, count=number, id_set={number})
        for number in range(1, 5)
    ]
    cache = _category_cache({"rating": {"datatype": "rating"}}, {"rating": rating_rows})
    empty: list[int] = []
    result = categories.get_categories(cache, book_ids=empty, first_letter_sort=True)
    assert result["rating"] is rating_rows and len(rating_rows) == 2
    assert sum(row.count for row in rating_rows) == 10
    assert cache.fields["rating"].get_categories.call_args.args[-1] is empty
    with pytest.raises(TypeError, match="icon_map"):
        categories.get_categories(object(), icon_map={})
    with pytest.raises(ValueError, match="not a valid value"):
        categories.get_categories(object(), sort="unknown")


def test_composite_metadata_reuses_non_none_values_but_retries_none() -> None:
    """
    Share per-call composite metadata lookups while treating a cached None as another provider miss.

    Example:
        >>> test_composite_metadata_reuses_non_none_values_but_retries_none()


    :return: None after composite arguments, all-book selection, and exact proxy-fetch count assertions.
    """
    cache = _category_cache(
        {
            "rating": {"datatype": "rating"},
            "composite": {
                "datatype": "composite",
                "is_category": False,
                "display": {"make_category": True},
            },
        },
        {"rating": []},
    )
    marker = object()
    cache._get_proxy_metadata.side_effect = [None, marker]

    def composite_rows(
        tag_class: Callable[..., categories.Tag],
        ratings: object,
        ids: object,
        separator: object,
        get_metadata: Callable[[int], object],
    ) -> list[categories.Tag]:
        """
        Exercise the assembler's lazy metadata callback before returning one composite category item.

        Example:
            >>> rows = composite_rows(factory, ratings, ids, separator, get_metadata)  # doctest: +SKIP


        :param tag_class: Bound Tag constructor supplied by category assembly.
        :param ratings: Original book-rating map expected by the composite provider.
        :param ids: Selected all-book ID set shared by composite generation.
        :param separator: Optional cache-to-list separator, absent in this fixture.
        :param get_metadata: Per-call callback whose None and non-None cache behavior is under test.
        :return: Single composite Tag after validating provider arguments and metadata fetch behavior.
        """
        assert (
            ratings is cache.fields["rating"].book_value_map
            and ids == {1, 2}
            and separator is None
        )
        assert (
            get_metadata(1) is None
            and get_metadata(1) is marker
            and get_metadata(1) is marker
        )
        return [tag_class("computed")]

    cache.fields["composite"].get_composite_categories.side_effect = composite_rows
    result = categories.get_categories(cache)
    assert result["composite"][0].name == "computed"
    assert cache._get_proxy_metadata.call_count == 2
    cache._all_book_ids.assert_called_once_with()


@pytest.mark.parametrize("name, expected_count", [("same", 1), ("Same", 2)])
def test_grouped_categories_keep_case_sensitive_indexing_and_shared_id_sets(
    name: str, expected_count: int
) -> None:
    """
    Preserve grouped-item casing quirks and shallow-copy ID-set aliasing while ordinary user items reuse source objects.

    Example:
        >>> test_grouped_categories_keep_case_sensitive_indexing_and_shared_id_sets("same", 1)


    :param name: Lowercase name that consolidates or mixed-case name whose original-key storage prevents consolidation.
    :param expected_count: Required grouped-item count for the chosen spelling.
    :return: None after original-user identity, grouped cardinality, and source-ID aliasing assertions.
    """
    first = categories.Tag(name, id=1, count=1, id_set={1}, category="tags")
    second = categories.Tag(name, id=2, count=1, id_set={2}, category="series")
    preferences = {
        "user_categories": {"manual": [[name, "tags", 0]]},
        "grouped_search_make_user_categories": ["grouped"],
        "grouped_search_terms": {"grouped": ["tags", "series"]},
    }
    cache = _category_cache(
        {"rating": {"datatype": "rating"}, "tags": {}, "series": {}},
        {"rating": [], "tags": [first], "series": [second]},
        preferences=preferences,
    )
    result = categories.get_categories(cache)
    grouped = result["@grouped"]
    assert result["@manual"][0] is first and len(grouped) == expected_count
    assert grouped[0] is not first and grouped[0].id_set is first.id_set
    assert first.id_set == ({1, 2} if name == "same" else {1})
    assert first.count == 1
    preferences["user_categories"] = {}
    assert "@grouped" not in categories.get_categories(cache)


def test_thumbnail_lru_budget_spans_groups_and_shutdown_does_not_close(
    tmp_path: Path,
) -> None:
    """
    Evict the oldest key across groups after read promotion, and retain post-shutdown mutability.

    Example:
        >>> test_thumbnail_lru_budget_spans_groups_and_shutdown_does_not_close(temporary_directory)  # doctest: +SKIP


    :param tmp_path: Temporary root for all cache payload and order files.
    :return: None after shared count/budget, group membership, read promotion, eviction, and later-write checks.
    """
    cache = ThumbnailCache(location=str(tmp_path), max_size=6 / (1024**2))
    cache.insert(1, 1.0, b"aa")
    cache.insert(2, 2.0, b"bb")
    cache.set_group_id("other")
    cache.insert(1, 3.0, b"cc")
    assert len(cache) == 3 and cache.current_size() == 6
    cache.set_group_id("group")
    assert cache[1] == (b"aa", 1.0)
    cache.insert(3, 4.0, b"dd")
    assert 2 not in cache and len(cache) == 3 and cache.current_size() == 6
    cache.set_group_id("other")
    assert cache[1] == (b"cc", 3.0)
    cache.shutdown()
    assert (Path(cache.location) / "order").is_file()
    cache.insert(2, 5.0, b"ee")
    assert cache[2] == (b"ee", 5.0)


def test_thumbnail_zero_budget_still_allows_empty_payloads(tmp_path: Path) -> None:
    """
    Reject oversized content before index creation while permitting zero-length entries at a zero-byte budget.

    Example:
        >>> test_thumbnail_zero_budget_still_allows_empty_payloads(temporary_directory)  # doctest: +SKIP


    :param tmp_path: Temporary root used to verify lazy directory/index creation.
    :return: None after pre-load rejection, empty insertion, zero accounting, and count assertions.
    """
    cache = ThumbnailCache(location=str(tmp_path), max_size=0)
    cache.insert(1, 1.0, b"too large")
    assert not hasattr(cache, "items") and not Path(cache.location).exists()
    cache.insert(1, 1.0, b"")
    assert cache[1] == (b"", 1.0) and len(cache) == 1 and cache.current_size() == 0


def test_thumbnail_replacement_leaves_old_files_and_reload_trusts_declared_sizes(
    tmp_path: Path,
) -> None:
    """
    Keep orphaned replacement files and duplicate-key byte accounting when rebuilding the index from filenames.

    Example:
        >>> test_thumbnail_replacement_leaves_old_files_and_reload_trusts_declared_sizes(temporary_directory)  # doctest: +SKIP


    :param tmp_path: Temporary cache root holding two distinct filenames for one logical key.
    :return: None after replacement timestamps, orphan existence, and reloaded declared-size double counting are checked.
    """
    cache = ThumbnailCache(location=str(tmp_path))
    cache.insert(7, 1.23456, b"aa")
    assert cache[7] == (b"aa", 1.23456)
    first = Path(cache.items[("group", 7)].path)
    assert "-1.23-" in first.name
    cache.insert(7, 2.0, b"bbb")
    second = Path(cache.items[("group", 7)].path)
    assert first != second and first.exists() and second.exists()
    assert cache.current_size() == 3
    second.write_bytes(b"actual bytes have a different length")
    reopened = ThumbnailCache(location=str(tmp_path))
    assert len(reopened) == 1 and reopened.current_size() == 5


def test_thumbnail_size_change_can_fail_after_partial_live_iterator_removal(
    tmp_path: Path,
) -> None:
    """
    Expose the retained OrderedDict mutation error and its partial side effects instead of claiming resize invalidation succeeds.

    Example:
        >>> test_thumbnail_size_change_can_fail_after_partial_live_iterator_removal(temporary_directory)  # doctest: +SKIP


    :param tmp_path: Temporary cache root containing two old-size entries.
    :return: None after stale membership, partial removals, retained pending flag, and final empty lookup are verified.
    """
    cache = ThumbnailCache(location=str(tmp_path))
    cache.insert(1, 1.0, b"a")
    cache.insert(2, 1.0, b"b")
    cache.set_thumbnail_size(60, 80)
    assert len(cache) == 2 and 1 in cache and cache.current_size() == 2
    with pytest.raises(RuntimeError, match="mutated"):
        cache[1]
    assert len(cache) == 1 and cache.current_size() == 1 and cache.size_changed
    assert cache[2] == (None, None)
    assert len(cache) == 0 and cache.current_size() == 0 and not cache.size_changed
    assert cache[2] == (None, None) and not cache.size_changed


def test_deferred_thumbnail_invalidation_loses_binary_records_on_reload(
    tmp_path: Path,
) -> None:
    """
    Preserve the observed missing append separator and bytes/text journal parsing failure without hiding it as invalidation success.

    Example:
        >>> test_deferred_thumbnail_invalidation_loses_binary_records_on_reload(temporary_directory)  # doctest: +SKIP


    :param tmp_path: Temporary cache root shared by initialized and cold instances.
    :return: None after concatenated journal bytes, surviving indexed payloads, and journal removal are checked.
    """
    active = ThumbnailCache(location=str(tmp_path))
    active.insert(1, 1.0, b"a")
    active.insert(2, 1.0, b"b")
    cold = ThumbnailCache(location=str(tmp_path))
    cold.invalidate([1])
    cold.invalidate([2])
    journal = Path(cold.location) / "invalidate"
    assert journal.read_bytes() == b"group 1group 2"
    assert cold[1] == (b"a", 1.0) and cold[2] == (b"b", 1.0)
    assert not journal.exists()


def test_thumbnail_error_paths_raise_for_the_unbound_diagnostic_helper(
    tmp_path: Path,
) -> None:
    """
    Confirm removal, payload-read, and corrupt-order failures encounter the current undefined as_unicode name.

    Example:
        >>> test_thumbnail_error_paths_raise_for_the_unbound_diagnostic_helper(temporary_directory)  # doctest: +SKIP


    :param tmp_path: Temporary cache root used for deliberate missing payloads and harmless corrupt pickle bytes.
    :return: None after NameError boundaries and retained read-failure index/accounting state are checked.
    """
    cache = ThumbnailCache(location=str(tmp_path), test_mode=True)
    with pytest.raises(NameError, match="as_unicode"):
        cache._do_delete(tmp_path / "missing-file")
    cache.insert(1, 1.0, b"a")
    Path(cache.items[("group", 1)].path).unlink()
    with pytest.raises(NameError, match="as_unicode"):
        cache[1]
    assert 1 in cache and cache.current_size() == 1
    (Path(cache.location) / "order").write_bytes(b"not pickle data")
    with pytest.raises(NameError, match="as_unicode"):
        cache._read_order()


def test_thumbnail_empty_removes_indexed_files_not_unparsed_artifacts(
    tmp_path: Path,
) -> None:
    """
    Empty initialized payloads and order state without pretending to recursively erase the cache directory.

    Example:
        >>> test_thumbnail_empty_removes_indexed_files_not_unparsed_artifacts(temporary_directory)  # doctest: +SKIP


    :param tmp_path: Temporary root containing an indexed payload and an unparseable sibling artifact.
    :return: None after indexed deletion, zero count/usage, and preserved unrelated artifact/directory assertions.
    """
    cache = ThumbnailCache(location=str(tmp_path))
    cache.insert(1, 1.0, b"a")
    payload = Path(cache.items[("group", 1)].path)
    artifact = payload.parent / "not-a-thumbnail"
    artifact.write_bytes(b"left alone")
    cache.shutdown()
    cache.empty()
    assert len(cache) == 0 and cache.current_size() == 0
    assert not payload.exists() and artifact.exists() and Path(cache.location).is_dir()
    assert not (Path(cache.location) / "order").exists()


def test_image_model_reuse_redirect_limits_and_shallow_alias_precedence() -> None:
    """
    Reuse concrete Core models, preserve unvalidated redirects, and keep shallow file-alias overwrite semantics.

    Example:
        >>> test_image_model_reuse_redirect_limits_and_shallow_alias_precedence()


    :return: None after model identity/fallback, blank redirect, projected aliases, and SVG dimension/initial checks.
    """
    client = Mock(spec=CoreClientAPI)
    model = CoreSurfaceModel(client)
    nested: list[str] = []
    row = {
        "image_id": 7,
        "image_name": "fallback.jpg",
        "image_store_id": 0,
        "file_store_id": 99,
        "nested": nested,
    }
    host = SimpleNamespace(
        core=client,
        read_model=SimpleNamespace(model=model),
        _row_dict=Mock(return_value=row),
        _row_primary_text=Mock(return_value="<ß & snow>"),
    )
    backend = ImageBackend(host)
    assert backend._model() is model
    host.read_model.model = object()
    assert backend._model() is not backend._model()
    host.read_model.model = model
    client.query.return_value = {
        "delivery": "redirect",
        "location": None,
        "readable": False,
    }
    target = backend.resolve_image_target(row)
    assert (
        target is not None
        and target.location == ""
        and target.download_name == "fallback.jpg"
    )
    metadata = backend.image_storage_lookup_metadata(row)
    assert (
        metadata["file_store_id"] == 0 and metadata["image_row"]["file_store_id"] == 99
    )
    assert metadata["nested"] is metadata["image_row"]["nested"] is nested
    svg = backend.placeholder_cover_svg({}, width=-1, height=0)
    assert (
        b"width='-1' height='0'" in svg
        and b">SS</text>" in svg
        and b"font-size='18'" in svg
    )
