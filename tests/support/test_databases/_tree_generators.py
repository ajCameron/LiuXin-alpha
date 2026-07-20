from __future__ import annotations

from copy import deepcopy

from LiuXin_alpha.utils.libraries.liuxin_random import LiuXinBadPseudoRandomGenerator


def _advance_legacy_iterator(stream):
    """Accept both Py2-style `.next()` objects and normal Python 3 iterators."""
    if stream is None:
        raise TypeError("uuid_stream is required when row_name_str is provided")
    next_method = getattr(stream, "next", None)
    if callable(next_method):
        return next_method()
    return next(stream)


def _resolve_tree_columns(db, table: str):
    table_col_base = db.driver_wrapper.get_column_base(table)
    headings = set(db.driver_wrapper.get_column_headings(table))

    parent_col = None
    for candidate in (
        "{}_parent_id".format(table_col_base),
        "{}_parent".format(table_col_base),
    ):
        if candidate in headings:
            parent_col = candidate
            break
    if parent_col is None:
        raise KeyError("No parent column found for table {!r}".format(table))

    parent_pos = None
    for candidate in (
        "{}_parent_position".format(table_col_base),
        "{}_position".format(table_col_base),
    ):
        if candidate in headings:
            parent_pos = candidate
            break

    return table_col_base, parent_col, parent_pos


def generate_test_tree(
    root_row,
    row_name_str=None,
    uuid_stream=None,
    parent_position=True,
    seed=100,
    max_layers=5,
):
    lx_random = LiuXinBadPseudoRandomGenerator(seed)

    table = root_row.table
    db = root_row.catalog
    table_col_base, parent_col, parent_pos = _resolve_tree_columns(db, table)
    parent_positions = {1, 2, 3, 4, 5, 6, 7, 8, 9}
    layers = lx_random.randint(1, max_layers)

    previous_layer_rows = [root_row]
    for _layer in range(0, layers):
        current_layer_rows = []
        for previous_layer_row in previous_layer_rows:
            layer_count = lx_random.randint(1, 4)
            current_parent_positions = deepcopy(parent_positions)

            for _child in range(1, layer_count + 1):
                current_row = db.get_blank_row(table)
                current_row[parent_col] = previous_layer_row.row_id

                if parent_position and parent_pos is not None:
                    this_row_parent_pos = lx_random.choice([n for n in current_parent_positions])
                    current_parent_positions.remove(this_row_parent_pos)
                    current_row[parent_pos] = this_row_parent_pos

                if row_name_str is not None:
                    current_row[table_col_base] = row_name_str.format(
                        current_row.row_id, _advance_legacy_iterator(uuid_stream)
                    )

                current_row.sync()
                current_layer_rows.append(current_row)

        previous_layer_rows = current_layer_rows


def generate_test_tree_with_datestamps(
    root_row,
    datestamp_col,
    datestamp_start,
    datestamp_delta,
    row_name_str=None,
    uuid_stream=None,
    parent_position=True,
    seed=100,
    max_layers=5,
):
    lx_random = LiuXinBadPseudoRandomGenerator(seed)

    table = root_row.table
    db = root_row.catalog
    table_col_base, parent_col, parent_pos = _resolve_tree_columns(db, table)
    parent_positions = {1, 2, 3, 4, 5, 6, 7, 8, 9}
    layers = lx_random.randint(1, max_layers)
    current_datestamp = deepcopy(datestamp_start)

    previous_layer_rows = [root_row]
    for _layer in range(0, layers):
        current_layer_rows = []
        for previous_layer_row in previous_layer_rows:
            layer_count = lx_random.randint(1, 4)
            current_parent_positions = deepcopy(parent_positions)

            for _child in range(1, layer_count + 1):
                current_row = db.get_blank_row(table)
                current_row[parent_col] = previous_layer_row.row_id
                current_row[datestamp_col] = str(current_datestamp)
                current_datestamp += datestamp_delta

                if parent_position and parent_pos is not None:
                    this_row_parent_pos = lx_random.choice([n for n in current_parent_positions])
                    current_parent_positions.remove(this_row_parent_pos)
                    current_row[parent_pos] = this_row_parent_pos

                if row_name_str is not None:
                    current_row[table_col_base] = row_name_str.format(
                        current_row.row_id, _advance_legacy_iterator(uuid_stream)
                    )

                current_row.sync()
                current_layer_rows.append(current_row)

        previous_layer_rows = current_layer_rows
