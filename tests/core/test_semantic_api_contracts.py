"""
Characterize documented tree, row-based storage-policy, browse, and acquisition edge cases without a live database or Store.

Portable macro fakes expose fixed row snapshots and record mutation attempts.
These assertions protect traversal/validation/routing semantics; they do not
establish physical deletion, byte verification, or durable transaction behavior.
Acquisition tests use only pytest-owned temporary paths and mocked storage readers;
example HTTP URLs are never fetched.
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from LiuXin_alpha.core import CoreCommand, CoreQuery
from LiuXin_alpha.core import browse_api as browse
from LiuXin_alpha.core import database_semantics_api as trees
from LiuXin_alpha.core import storage_graph_api as graph
from LiuXin_alpha.core.errors import CoreDispatchError


def _tree_runtime(rows: dict[int, dict[str, int | None]]) -> SimpleNamespace:
    """
    Build a tree-handler host with copied rows, deterministic read macros, and recording-only mutation methods.

    Updates/deletes do not mutate the copied rows. Reconciliation returns a shallow
    receipt copy and performs no cache work.

    Example:
        >>> runtime = _tree_runtime({1: {"node_id": 1, "node_parent_id": None}})
        >>> trees._tree_get(runtime, "nodes", 1)["node_id"]
        1


    :param rows: Integer-keyed node records with node_id and node_parent_id fields.
    :return: Namespace exposing the database and service attributes needed by these handlers.
    """
    snapshot = {key: dict(value) for key, value in rows.items()}

    def get_row(table, row_id, *, id_column):
        """
        Require the fixture table/ID column and return its stored row or None.

        Example:
            >>> get_row("nodes", 1, id_column="node_id")  # doctest: +SKIP


        :param table: Must be nodes; asserted to detect an unexpected query shape.
        :param row_id: Integer identity used in the fixture snapshot lookup.
        :param id_column: Must be node_id; this fake does not support alternate keys.
        :return: Shared snapshot row dictionary, or None for an absent identity.
        """
        assert table == "nodes" and id_column == "node_id"
        return snapshot.get(row_id)

    def get_rows(table, *, where, order_by):
        """
        Return shallow child-row copies in numeric identity order using the fixture's parent equality filter.

        Example:
            >>> children = get_rows("nodes", where={"node_parent_id": 1}, order_by=("node_id",))  # doctest: +SKIP


        :param table: Must be nodes; asserted before reading the snapshot.
        :param where: Mapping whose node_parent_id value selects immediate children.
        :param order_by: Must request the single node_id ordering column.
        :return: New list of child-row dictionaries, ordered by the snapshot's sorted integer keys.
        """
        assert table == "nodes" and order_by == ("node_id",)
        return [
            dict(snapshot[key])
            for key in sorted(snapshot)
            if snapshot[key]["node_parent_id"] == where["node_parent_id"]
        ]

    macros = Mock(spec_set=["get_row", "get_rows", "update_row", "delete_row"])
    macros.get_row.side_effect = get_row
    macros.get_rows.side_effect = get_rows
    database = SimpleNamespace(
        macros=macros,
        get_column_headings=Mock(return_value=["node_id", "node_parent_id"]),
        driver_wrapper=SimpleNamespace(get_id_column=Mock(return_value="node_id")),
    )
    return SimpleNamespace(
        database=database,
        services=SimpleNamespace(reconcile=Mock(side_effect=dict)),
    )


def _branching_runtime() -> SimpleNamespace:
    """
    Build a four-node tree with two root children and one grandchild beneath the first child.

    Example:
        >>> [row["node_id"] for row in trees._tree_walk_rows(_branching_runtime(), "nodes", 1)]
        [1, 2, 3, 4]


    :return: Recording-only runtime fixture with root 1, children 2/3, and grandchild 4 beneath 2.
    """
    return _tree_runtime(
        {
            1: {"node_id": 1, "node_parent_id": None},
            2: {"node_id": 2, "node_parent_id": 1},
            3: {"node_id": 3, "node_parent_id": 1},
            4: {"node_id": 4, "node_parent_id": 2},
        }
    )


def test_tree_walk_lineage_and_search_have_distinct_ordering() -> None:
    """
    Require breadth-first walking, root-first lineage, and sorted/deduplicated search including the root.

    Example:
        >>> test_tree_walk_lineage_and_search_have_distinct_ordering()


    :return: None if all three documented ordering contracts hold against the same fixed tree.
    """
    runtime = _branching_runtime()
    assert [row["node_id"] for row in trees._tree_walk_rows(runtime, "nodes", 1)] == [
        1,
        2,
        3,
        4,
    ]
    assert [
        row["node_id"] for row in trees._tree_lineage_rows(runtime, "nodes", 4)
    ] == [
        1,
        2,
        4,
    ]
    query = CoreQuery(
        "tree.search", {"table": "nodes", "row_id": 1, "row_ids": [4, 1, 4, 99]}
    )
    assert trees.CoreDatabaseSemanticsAPI.tree_search(runtime, query) == {
        "row_ids": [1, 4]
    }


def test_tree_walk_and_lineage_reject_repeated_ids() -> None:
    """
    Require tree_cycle errors from both downward and upward traversal of a two-node cycle.

    Example:
        >>> test_tree_walk_and_lineage_reject_repeated_ids()


    :return: None if both traversal helpers detect the repeated identity instead of looping indefinitely.
    """
    runtime = _tree_runtime(
        {
            1: {"node_id": 1, "node_parent_id": 2},
            2: {"node_id": 2, "node_parent_id": 1},
        }
    )
    for operation in (trees._tree_walk_rows, trees._tree_lineage_rows):
        with pytest.raises(CoreDispatchError) as caught:
            operation(runtime, "nodes", 1)
        assert caught.value.code == "tree_cycle"


def test_tree_nest_validates_all_children_and_retains_duplicates() -> None:
    """
    Reject a later missing child before any write, then preserve duplicate valid IDs as repeated update attempts.

    Example:
        >>> test_tree_nest_validates_all_children_and_retains_duplicates()


    :return: None if validation precedes writes and successful receipts/calls retain the duplicate input IDs.
    """
    runtime = _branching_runtime()
    command = CoreCommand(
        "tree.nest", {"table": "nodes", "parent_id": 1, "child_ids": [2, 99]}
    )
    with pytest.raises(CoreDispatchError) as caught:
        trees.CoreDatabaseSemanticsAPI.tree_nest(runtime, command)
    assert caught.value.code == "row_not_found"
    runtime.database.macros.update_row.assert_not_called()

    command = CoreCommand(
        "tree.nest", {"table": "nodes", "parent_id": 1, "child_ids": [2, 2]}
    )
    result = trees.CoreDatabaseSemanticsAPI.tree_nest(runtime, command)
    assert result["child_ids"] == [2, 2]
    assert runtime.database.macros.update_row.call_count == 2
    assert [
        call.args[1] for call in runtime.database.macros.update_row.call_args_list
    ] == [2, 2]


@pytest.mark.parametrize("confirmation", [None, 1, "true"])
def test_tree_delete_requires_literal_true(confirmation: object) -> None:
    """
    Reject non-True confirmation values before reading/deleting the requested tree.

    Example:
        >>> test_tree_delete_requires_literal_true(1)


    :param confirmation: Parametrized missing-like, truthy integer, or textual confirmation value.
    :return: None if the confirmation-required error occurs without any root read or delete call.
    """
    runtime = _branching_runtime()
    command = CoreCommand(
        "tree.delete", {"table": "nodes", "row_id": 1, "confirm": confirmation}
    )
    with pytest.raises(CoreDispatchError) as caught:
        trees.CoreDatabaseSemanticsAPI.tree_delete(runtime, command)
    assert caught.value.code == "confirmation_required"
    runtime.database.macros.get_row.assert_not_called()
    runtime.database.macros.delete_row.assert_not_called()


def test_tree_delete_counts_reverse_breadth_first_attempts() -> None:
    """
    Require reverse breadth-first delete calls and count them even when the fake macro returns False.

    Example:
        >>> test_tree_delete_counts_reverse_breadth_first_attempts()


    :return: None if attempted IDs are 4/3/2/1 and the receipt reports four planned deletions.
    """
    runtime = _branching_runtime()
    runtime.database.macros.delete_row.return_value = False
    command = CoreCommand(
        "tree.delete", {"table": "nodes", "row_id": 1, "confirm": True}
    )
    result = trees.CoreDatabaseSemanticsAPI.tree_delete(runtime, command)
    assert [
        call.args[1] for call in runtime.database.macros.delete_row.call_args_list
    ] == [4, 3, 2, 1]
    assert result["deleted"] is True and result["deleted_count"] == 4


def test_tree_root_and_array_id_coercion_are_not_equivalent() -> None:
    """
    Preserve direct int coercion for root IDs but reject decimal-text conversion of floating array IDs.

    Example:
        >>> test_tree_root_and_array_id_coercion_are_not_equivalent()


    :return: None if the root helper truncates a fraction while tree-search array conversion raises ValueError.
    """
    assert trees._required_int({"row_id": 1.9}, "row_id") == 1
    query = CoreQuery("tree.search", {"table": "nodes", "row_id": 1, "row_ids": [1.0]})
    with pytest.raises(ValueError):
        trees.CoreDatabaseSemanticsAPI.tree_search(_branching_runtime(), query)


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({}, True),
        ({"asset_replica_presence_status": "OFFLINE"}, False),
        ({"asset_replica_presence_status": "offline "}, True),
        ({"asset_replica_integrity_status": "CORRUPT"}, False),
        ({"asset_replica_integrity_status": "unverified"}, True),
    ],
)
def test_graph_health_is_a_permissive_label_filter(
    row: dict[str, str], expected: bool
) -> None:
    """
    Check casefolded bad-label rejection while retaining missing, unverified, and unstripped status values.

    Example:
        >>> test_graph_health_is_a_permissive_label_filter({}, True)


    :param row: Parametrized Replica status fields, without any actual byte observation.
    :param expected: Whether the row-label helper should regard the supplied fields as healthy.
    :return: None if the permissive filter matches its documented treatment of the status fields.
    """
    assert graph._healthy_replica(row) is expected


def test_graph_plan_can_select_one_store_for_each_zero_shortfall() -> None:
    """
    Characterize post-append counting and independent family selection when both policy targets are already met.

    Example:
        >>> test_graph_plan_can_select_one_store_for_each_zero_shortfall()


    :return: None if the same candidate is suggested for active and backup despite both shortfalls being zero.
    """
    macros = Mock()
    macros.get_row.return_value = {"digital_asset_id": 7}
    replica = {"asset_replica_store_id": 1, "asset_replica_mode": "active"}
    macros.get_rows.side_effect = [
        [replica],
        [replica],
        [{"store_id": 2, "store_name": "destination"}],
    ]
    runtime = SimpleNamespace(database=SimpleNamespace(macros=macros))
    result = graph.CoreStorageGraphAPI().policy_plan(
        runtime, CoreQuery("storage.policy.plan", {"asset_id": 7})
    )
    assert result["assessment"]["replication"]["meets_target"] is True
    assert result["assessment"]["backup"]["meets_target"] is True
    assert [(value["store_id"], value["mode"]) for value in result["placements"]] == [
        (2, "active"),
        (2, "backup"),
    ]
    assert result["replication_shortfall"] == result["backup_shortfall"] == 0


def test_graph_violations_use_minimums_not_targets() -> None:
    """
    Exclude an Asset meeting minimums even when a supplied assessment says both targets remain unmet.

    Example:
        >>> test_graph_violations_use_minimums_not_targets()


    :return: None if the violations route uses minimum flags rather than target flags in its selection.
    """
    macros = Mock()
    macros.get_rows.return_value = [{"digital_asset_id": 7}]
    runtime = SimpleNamespace(database=SimpleNamespace(macros=macros))
    adapter = graph.CoreStorageGraphAPI()
    adapter._assess = Mock(
        return_value={
            "asset_id": 7,
            "replication": {"meets_minimum": True, "meets_target": False},
            "backup": {"meets_minimum": True, "meets_target": False},
        }
    )
    result = adapter.policy_violations(runtime, CoreQuery("storage.policy.violations"))
    assert result["records"] == [] and result["complete"] is True
    adapter._assess.assert_called_once_with(runtime, 7)


def test_graph_empty_headings_and_id_alias_bypass_membership_checks() -> None:
    """
    Allow unknown candidate fields without headings and the id alias regardless of headings, but reject managed timestamps.

    Example:
        >>> test_graph_empty_headings_and_id_alias_bypass_membership_checks()


    :return: None if schema-membership fallback and timestamp restrictions retain their documented boundaries.
    """
    runtime = SimpleNamespace(database=SimpleNamespace())
    spec = graph._RESOURCES["asset"]
    assert graph._normalise_values(runtime, spec, {"unknown": 3}) == {
        "digital_asset_unknown": 3
    }
    assert graph._column_name(spec, "id", headings={"unrelated"}) == "digital_asset_id"
    with pytest.raises(CoreDispatchError, match="timestamps"):
        graph._normalise_values(
            runtime, spec, {"created_timestamp_ep_k": 1}, allow_id=True
        )


def test_browse_recent_sort_direction_differs_between_routes() -> None:
    """
    Require opposite recent-ID order for the same explicit ascending=True on category-items and Works queries.

    The real Work summary runs against a read source with no relation capabilities.

    Example:
        >>> test_browse_recent_sort_direction_differs_between_routes()


    :return: None if category-items uses descending IDs while Works uses ascending IDs for the supplied options.
    """
    source = SimpleNamespace(
        get_all_rows=Mock(
            return_value=[
                {"work_id": 2, "work_title": "Two"},
                {"work_id": 1, "work_title": "One"},
            ]
        )
    )
    runtime = SimpleNamespace(
        database=object(), services=SimpleNamespace(read_source=source)
    )
    values = {"category": "all", "sort": "recent", "ascending": True}
    categories = browse.CoreBrowseAPI.category_items(
        runtime, CoreQuery("browse.category.items", values)
    )
    works = browse.CoreBrowseAPI.works(runtime, CoreQuery("browse.works", values))
    assert [row["work_id"] for row in categories["records"]] == [2, 1]
    assert [row["work_id"] for row in works["records"]] == [1, 2]


def test_browse_table_signature_retry_also_catches_internal_type_errors() -> None:
    """
    Record that a first-call TypeError triggers a no-keyword retry without proving a signature mismatch.

    Example:
        >>> test_browse_table_signature_retry_also_catches_internal_type_errors()


    :return: None if the retry succeeds and the recorded calls show keyword then no-keyword invocation.
    """
    get_tables = Mock(side_effect=[TypeError("internal first-call failure"), ["works"]])
    runtime = SimpleNamespace(
        services=SimpleNamespace(read_source=SimpleNamespace(get_tables=get_tables))
    )
    assert browse._tables(runtime) == {"works"}
    assert [call.kwargs for call in get_tables.call_args_list] == [
        {"force_refresh": False},
        {},
    ]


def test_browse_search_failure_falls_back_to_full_equality_scan() -> None:
    """
    Suppress a search error and use exact Python equality against fully read row values.

    Example:
        >>> test_browse_search_failure_falls_back_to_full_equality_scan()


    :return: None if only the integer-valued match survives the fallback rather than the textual lookalike.
    """
    source = SimpleNamespace(
        search=Mock(side_effect=RuntimeError("search failed")),
        get_all_rows=Mock(
            return_value=[
                {"item_id": 1, "item_manifestation_id": 7},
                {"item_id": 2, "item_manifestation_id": "7"},
            ]
        ),
    )
    runtime = SimpleNamespace(services=SimpleNamespace(read_source=source))
    assert browse._search_rows(runtime, "items", "item_manifestation_id", 7) == [
        {"item_id": 1, "item_manifestation_id": 7}
    ]
    source.get_all_rows.assert_called_once_with("items", iterator_return=False)


def test_acquisition_remote_hint_precedes_existing_local_bytes(tmp_path: Path) -> None:
    """
    Prefer stored HTTP source metadata over a real temporary local file and require a redirect error on read.

    No network request is made; the fixture's local bytes would be readable if the
    resolution policy selected that path.

    Example:
        >>> test_acquisition_remote_hint_precedes_existing_local_bytes(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest-owned directory for the small local acquisition fixture.
    :return: None if resolution selects the remote URL and acquisition_read rejects Core delivery.
    """
    path = tmp_path / "book.epub"
    path.write_bytes(b"local content")
    row = {
        "file_id": 7,
        "file_original_path": str(path),
        "file_source": "https://example.test/book.epub",
    }
    source = SimpleNamespace(get_row_from_id=Mock(return_value=row))
    runtime = SimpleNamespace(services=SimpleNamespace(read_source=source))
    resolved = browse._resolution(runtime, kind="legacy-file", row=row)
    assert resolved["delivery"] == "redirect" and resolved["readable"] is False
    with pytest.raises(CoreDispatchError) as caught:
        browse.CoreBrowseAPI.acquisition_read(
            runtime, CoreQuery("acquisition.read", {"kind": "legacy-file", "id": 7})
        )
    assert caught.value.code == "acquisition_redirect"
    assert caught.value.details["location"] == row["file_source"]


def test_acquisition_store_hint_does_not_prove_a_read_will_succeed(
    tmp_path: Path,
) -> None:
    """
    Mark a non-HTTP Store/key pair readable before demonstrating that neither mocked storage nor local fallback supplies bytes.

    Example:
        >>> test_acquisition_store_hint_does_not_prove_a_read_will_succeed(tmp_path)  # doctest: +SKIP


    :param tmp_path: Empty pytest-owned Store root, deliberately containing no missing.epub file.
    :return: None if resolution says Core/readable but the later byte helper raises acquisition_unavailable.
    """
    store = {
        "store_id": 1,
        "store_name": "fixture",
        "store_kind": "filesystem",
        "store_root_uri": str(tmp_path),
    }
    reader = Mock(side_effect=OSError("offline"))
    runtime = SimpleNamespace(
        database=SimpleNamespace(
            macros=SimpleNamespace(get_row=Mock(return_value=store))
        ),
        services=SimpleNamespace(
            library=SimpleNamespace(storage=SimpleNamespace(read_bytes=reader))
        ),
    )
    row = {
        "asset_replica_id": 7,
        "asset_replica_store_id": 1,
        "asset_replica_storage_key": "missing.epub",
    }
    resolved = browse._resolution(runtime, kind="replica", row=row)
    assert resolved["delivery"] == "core" and resolved["readable"] is True
    with pytest.raises(CoreDispatchError) as caught:
        browse._resource_bytes(runtime, kind="replica", row=row)
    assert caught.value.code == "acquisition_unavailable"
    reader.assert_called_once()


def test_acquisition_unavailable_resolution_uses_redirect_error_code() -> None:
    """
    Preserve the acquisition_redirect error for an unavailable resource that has no redirect location.

    Example:
        >>> test_acquisition_unavailable_resolution_uses_redirect_error_code()


    :return: None if the error code and details retain the documented unavailable-versus-redirect mismatch.
    """
    source = SimpleNamespace(get_row_from_id=Mock(return_value={"file_id": 7}))
    runtime = SimpleNamespace(services=SimpleNamespace(read_source=source))
    with pytest.raises(CoreDispatchError) as caught:
        browse.CoreBrowseAPI.acquisition_read(
            runtime, CoreQuery("acquisition.read", {"kind": "legacy-file", "id": 7})
        )
    assert caught.value.code == "acquisition_redirect"
    assert caught.value.details["delivery"] == "unavailable"
    assert "location" not in caught.value.details
