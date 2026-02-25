"""Low-level database driver API contracts."""

from __future__ import annotations

import abc

from .macros import MacrosAPI

class DatabaseDriverAPI(abc.ABC):
    """API contract for low-level SQL drivers."""

    @abc.abstractmethod
    def __get_linear_index_of_columns(self, start_row, display_column):
        ...

    @abc.abstractmethod
    def __init__(self, db_metadata, db=None, set_conn=True, dirty_records_queue=None):
        ...

    @abc.abstractmethod
    def __initialize_md(self):
        ...

    @abc.abstractmethod
    def _bad_link_type_error(self, link_type: str) -> str:
        ...

    @abc.abstractmethod
    def _build_allowed_types_table_interlink(self, for_table, allowed_types):
        ...

    @abc.abstractmethod
    def _build_interlink_table_sqlite(self, table1: str, table2: str, requested_cols: Optional[Union[str, list[str]]]=None, allowed_types: Optional[Iterable[str]]=None, override_restriction_sql: Optional[str]=None) -> list[str]:
        ...

    @abc.abstractmethod
    def _canonicalise_table_name_for_cache(self, table):
        ...

    @abc.abstractmethod
    def _close_all_open_connections(self):
        ...

    @abc.abstractmethod
    def _coerce_db_value(self, value: Any, declared_type: Any) -> Any:
        ...

    @abc.abstractmethod
    def _coerce_untyped_value(self, value: Any) -> Any:
        ...

    @abc.abstractmethod
    def _get_custom_column_table_name(self, table, column_name):
        ...

    @abc.abstractmethod
    def _get_declared_types_for_table(self, table: str) -> Dict[str, str]:
        ...

    @abc.abstractmethod
    def _get_direct_link_main_tables_sqlite(self, primary_table: str, secondary_table: str, link_type: str='many_many', requested_cols: str='all', index_both: bool=True, allowed_types: Optional[Iterable[str]]=None, one_link_with_one_type: bool=True, override_restriction_sql: Optional[str]=None, nullable_fks: bool=True) -> tuple[list[str], Union[str, LiteralString]]:
        ...

    @abc.abstractmethod
    def _get_id_column(self, table, tables_and_columns=None):
        ...

    @staticmethod
    @abc.abstractmethod
    def _get_link_table_name_col_name(primary_table: str, secondary_table: str) -> tuple[str, str]:
        ...

    @abc.abstractmethod
    def _get_schema_version(self):
        ...

    @staticmethod
    @abc.abstractmethod
    def _get_table_col_base(table_name):
        ...

    @abc.abstractmethod
    def _invalidate_schema_caches(self):
        ...

    @staticmethod
    @abc.abstractmethod
    def _normalize_declared_type(declared_type: Any) -> str:
        ...

    @abc.abstractmethod
    def _register_open_connection(self, conn):
        ...

    @abc.abstractmethod
    def _row_to_dict(self, *, table: Optional[str]=None, headings: Sequence[Any], row: Sequence[Any]) -> Dict[Any, Any]:
        ...

    @staticmethod
    @abc.abstractmethod
    def _sanitize_embedded_nul_text(*, target_table: str, row_dict: dict) -> None:
        ...

    @classmethod
    @abc.abstractmethod
    def _sqlite_affinity(cls, declared_type: Any) -> str:
        ...

    @staticmethod
    @abc.abstractmethod
    def _validate_table_name(table_name):
        ...

    @abc.abstractmethod
    def _zero_prop_cache(self):
        ...

    @abc.abstractmethod
    def build_allowed_types_table_interlink(self, for_table: str, allowed_types: Optional[Iterable[str]]=None) -> list[str]:
        ...

    @abc.abstractmethod
    def build_allowed_types_table_intralink(self, for_table: str, allowed_types: Optional[Iterable[str]]=None) -> list[str]:
        ...

    @abc.abstractmethod
    def build_interlink_table_sqlite(self, table1: str, table2: str, requested_cols: Optional[Union[str, Iterable[str]]]=None, allowed_types: Optional[Iterable[str]]=None, nullable_fks: bool=True) -> list[str]:
        ...

    @abc.abstractmethod
    def build_intralink_table_sqlite(self, name: str, allowed_types: Optional[Iterable[str]]=None, requested_cols: Optional[Union[str, Iterable[str], set[str]]]=None, index_both: bool=True, nullable_fks: bool=True, symmetric: bool=False, symmetric_types: Optional[Iterable[str]]=None, use_reference_types_table: bool=False) -> list[str]:
        ...

    @abc.abstractmethod
    def call_after_table_changes(self):
        ...

    @staticmethod
    @abc.abstractmethod
    def can_index_be_transformed(target_index):
        ...

    @abc.abstractmethod
    def close(self):
        ...

    @abc.abstractmethod
    def create_interlink_types_reference_table(self, interlink_table_name: str, interlink_column_base: str, allowed_types: list[str], connection: sqlite3.Connection) -> None:
        ...

    @abc.abstractmethod
    def direct_add_multiple_simple_row_dicts(self, row_dict_list):
        ...

    @abc.abstractmethod
    def direct_add_simple_row_dict(self, row_dict):
        ...

    @abc.abstractmethod
    def direct_backup(self, path=None):
        ...

    @abc.abstractmethod
    def direct_clear_table(self, target_table):
        ...

    @abc.abstractmethod
    def direct_create_custom_column(self, in_table, column_name, data_type='TEXT', multi=False):
        ...

    @abc.abstractmethod
    def direct_create_many_many_custom_column(self, target_table, custom_column_name):
        ...

    @abc.abstractmethod
    def direct_create_many_to_one_custom_column(self, target_table, custom_column_name):
        ...

    @abc.abstractmethod
    def direct_create_new_database(self):
        ...

    @abc.abstractmethod
    def direct_create_new_main_table(self, table_name, column_headings=None, index_on='all', default_datatype='TEXT', default_unique=False):
        ...

    @abc.abstractmethod
    def direct_create_one_to_many_custom_column(self, target_table, custom_column_name, datatype='TEXT'):
        ...

    @abc.abstractmethod
    def direct_create_one_to_one_custom_column(self, target_table, custom_column_name, datatype='TEXT', normalized=False):
        ...

    @abc.abstractmethod
    def direct_delete(self, target_table, column, value, many=False):
        ...

    @abc.abstractmethod
    def direct_delete_book_group(self, group_id):
        ...

    @abc.abstractmethod
    def direct_delete_many(self, target_table, column, values):
        ...

    @abc.abstractmethod
    def direct_delete_many_by_ids(self, target_table, row_ids):
        ...

    @abc.abstractmethod
    def direct_delete_row_by_id(self, target_table, row_id):
        ...

    @abc.abstractmethod
    def direct_drop_triggers(self, triggers):
        ...

    @abc.abstractmethod
    def direct_execute(self, sql, values=None):
        ...

    @abc.abstractmethod
    def direct_executemany(self, sql, values=None):
        ...

    @abc.abstractmethod
    def direct_executescript(self, sqlscript):
        ...

    @abc.abstractmethod
    def direct_get_all_hashes(self):
        ...

    @abc.abstractmethod
    def direct_get_all_rows(self, table, sort_column=None, reverse=False):
        ...

    @abc.abstractmethod
    def direct_get_all_values(self, table, column):
        ...

    @staticmethod
    @abc.abstractmethod
    def direct_get_column_base(table_name: str) -> str:
        ...

    @abc.abstractmethod
    def direct_get_column_headings(self, table, normalize: bool=False):
        ...

    @abc.abstractmethod
    def direct_get_column_name(self, table_name):
        ...

    @abc.abstractmethod
    def direct_get_datestamp_column(self, table, tables_and_columns=None):
        ...

    @abc.abstractmethod
    def direct_get_db_unique_id(self):
        ...

    @abc.abstractmethod
    def direct_get_highest_id(self, target_table):
        ...

    @abc.abstractmethod
    def direct_get_id_column(self, table, tables_and_columns=None):
        ...

    @abc.abstractmethod
    def direct_get_max(self, column: str):
        ...

    @abc.abstractmethod
    def direct_get_min(self, column: str):
        ...

    @abc.abstractmethod
    def direct_get_next_book_group(self):
        ...

    @abc.abstractmethod
    def direct_get_null_row(self, table):
        ...

    @abc.abstractmethod
    def direct_get_random_row_dict(self, target_table, direct=False):
        ...

    @abc.abstractmethod
    def direct_get_record_count(self, target_table):
        ...

    @abc.abstractmethod
    def direct_get_root_series(self, start_row):
        ...

    @abc.abstractmethod
    def direct_get_row_count(self, table):
        ...

    @abc.abstractmethod
    def direct_get_row_dict_from_id(self, table, row_id):
        ...

    @abc.abstractmethod
    def direct_get_row_dict_iterator(self, table, sort_column=None, reverse=False):
        ...

    @abc.abstractmethod
    def direct_get_tables(self, force_refresh=False):
        ...

    @abc.abstractmethod
    def direct_get_tables_and_columns(self, force_refresh: bool=False):
        ...

    @abc.abstractmethod
    def direct_get_triggers(self):
        ...

    @abc.abstractmethod
    def direct_get_unique_values_iterator(self, target_column):
        ...

    @abc.abstractmethod
    def direct_get_unique_values_set(self, target_column):
        ...

    @abc.abstractmethod
    def direct_get_view_column_headings(self, view):
        ...

    @abc.abstractmethod
    def direct_get_view_row_dict_from_id(self, view, row_id):
        ...

    @abc.abstractmethod
    def direct_has_null_row(self, table) -> bool:
        ...

    @abc.abstractmethod
    def direct_last_modified(self):
        ...

    @abc.abstractmethod
    def direct_link_main_tables(self, primary_table, secondary_table, link_type='many_many', requested_cols='all', index_both=True, allowed_types=None, override_restriction_sql=None, nullable_fks: bool=True):
        ...

    @abc.abstractmethod
    def direct_multi_column_search(self, search_index, iterator_return=False):
        ...

    @abc.abstractmethod
    def direct_read_metadata(self, md_field_name):
        ...

    @abc.abstractmethod
    def direct_run_ta_update(self, ta_row_id):
        ...

    @abc.abstractmethod
    def direct_search_table(self, table=None, column=None, search_term=None):
        ...

    @abc.abstractmethod
    def direct_self_delete(self):
        ...

    @abc.abstractmethod
    def direct_set_db_unique_id(self, force_value=None):
        ...

    @abc.abstractmethod
    def direct_set_full_column(self, target_table):
        ...

    @abc.abstractmethod
    def direct_set_tree_ids(self, table):
        ...

    @abc.abstractmethod
    def direct_unlink_main_tables(self, primary_table, secondary_table):
        ...

    @abc.abstractmethod
    def direct_update_columns(self, id_values_map, field=None, table=None):
        ...

    @abc.abstractmethod
    def direct_update_null_row(self, table, updates=None, **fields) -> bool:
        ...

    @abc.abstractmethod
    def direct_update_row_dict(self, row_dict):
        ...

    @abc.abstractmethod
    def direct_validate_existing_table_name(self, test_name):
        ...

    @abc.abstractmethod
    def direct_write_metadata(self, md_field_name, md_field_value):
        ...

    @abc.abstractmethod
    def dirty_record(self, table, table_id, reason):
        ...

    @abc.abstractmethod
    def dump_and_restore(self, callback=lambda x: x, sql=None):
        ...

    @abc.abstractmethod
    def execute_sql(self, sql, values=None):
        ...

    @abc.abstractmethod
    def executescript(self, script):
        ...

    @abc.abstractmethod
    def exists(self):
        ...

    @abc.abstractmethod
    def get_all_tree_rows(self, start_row):
        ...

    @staticmethod
    @abc.abstractmethod
    def get_allowed_types_table_name(for_table: str) -> str:
        ...

    @abc.abstractmethod
    def get_allowed_types_table_name_intralinks(self, for_table: str) -> str:
        ...

    @abc.abstractmethod
    def get_connection(self):
        ...

    @abc.abstractmethod
    def get_display_column(self, table_name):
        ...

    @abc.abstractmethod
    def get_full_column_name(self, target_table):
        ...

    @abc.abstractmethod
    def get_id_from_row_dict(self, row_dict):
        ...

    @staticmethod
    @abc.abstractmethod
    def get_interlink_table_name(table1: str, table2: str) -> tuple[str, str]:
        ...

    @abc.abstractmethod
    def get_linear_row_index(self, start_row):
        ...

    @abc.abstractmethod
    def get_parent_column_name(self, table_name):
        ...

    @abc.abstractmethod
    def get_root_series(self, start_row):
        ...

    @abc.abstractmethod
    def get_table_sqlite(self, table, conn=None):
        ...

    @abc.abstractmethod
    def get_tree_id_column(self, target_table):
        ...

    @abc.abstractmethod
    def identify_table_from_column(self, column_heading, headings_and_columns=None, print_error=True):
        ...

    @abc.abstractmethod
    def identify_table_from_row(self, row_dict):
        ...

    @abc.abstractmethod
    def iterator_return(self, stmt, headings, table=None, bindings=None):
        ...

    @abc.abstractmethod
    def last_modified(self):
        ...

    @abc.abstractmethod
    def locational_search(self, parsed_query):
        ...

    @property
    @abc.abstractmethod
    def macros(self):
        ...

    @abc.abstractmethod
    def make_scratch(self):
        ...

    @abc.abstractmethod
    def refresh(self, reconnect: bool=False):
        ...

    @abc.abstractmethod
    def reopen(self):
        ...

    @abc.abstractmethod
    def set_database_version(self) -> None:
        ...

    @abc.abstractmethod
    def shell(self):
        ...

    @abc.abstractmethod
    def simple_print_progress_handler(self):
        ...

    @abc.abstractmethod
    def sql_dump(self):
        ...

    @abc.abstractmethod
    def sum_book_group_sizes(self, book_group):
        ...

    @staticmethod
    @abc.abstractmethod
    def transform_index(target_index):
        ...

    @abc.abstractmethod
    def tree_aggregator(self, table, table_display_column, table_row_id):
        ...

    @property
    @abc.abstractmethod
    def user_version(self):
        ...

    @abc.abstractmethod
    def validate_existing_table_name(self, test_name):
        ...

    @abc.abstractmethod
    def zero_prop_cache(self) -> None:
        ...
