"""Compatibility imports for the packaged storage CLI.

Implementation lives in ``storage_commands`` by responsibility. New commands
belong there, not in this public import boundary.
"""

from LiuXin_alpha.surfaces.cli.storage_commands import administration as _administration
from LiuXin_alpha.surfaces.cli.storage_commands import constants as _constants
from LiuXin_alpha.surfaces.cli.storage_commands import core_access as _core_access
from LiuXin_alpha.surfaces.cli.storage_commands import filesystem as _filesystem
from LiuXin_alpha.surfaces.cli.storage_commands import ingest as _ingest
from LiuXin_alpha.surfaces.cli.storage_commands import ingest_config as _ingest_config
from LiuXin_alpha.surfaces.cli.storage_commands import ingest_options as _ingest_options
from LiuXin_alpha.surfaces.cli.storage_commands import ingest_paths as _ingest_paths
from LiuXin_alpha.surfaces.cli.storage_commands import (
    ingest_preflight as _ingest_preflight,
)
from LiuXin_alpha.surfaces.cli.storage_commands import (
    ingest_reporting as _ingest_reporting,
)
from LiuXin_alpha.surfaces.cli.storage_commands import ingest_run as _ingest_run
from LiuXin_alpha.surfaces.cli.storage_commands import integrity as _integrity
from LiuXin_alpha.surfaces.cli.storage_commands import parsers as _parsers
from LiuXin_alpha.surfaces.cli.storage_commands import prompts as _prompts
from LiuXin_alpha.surfaces.cli.storage_commands import resources as _resources
from LiuXin_alpha.surfaces.cli.storage_commands import signals as _signals
from LiuXin_alpha.surfaces.cli.storage_commands import store_add as _store_add
from LiuXin_alpha.surfaces.cli.storage_commands import store_options as _store_options
from LiuXin_alpha.surfaces.cli.storage_commands import store_wizard as _store_wizard

cmd_storage_asset_show = _administration.cmd_storage_asset_show
cmd_storage_asset_verify = _administration.cmd_storage_asset_verify
cmd_storage_backends_list = _administration.cmd_storage_backends_list
cmd_storage_default_set = _administration.cmd_storage_default_set
cmd_storage_default_show = _administration.cmd_storage_default_show
cmd_storage_file_copy = _administration.cmd_storage_file_copy
cmd_storage_file_delete = _administration.cmd_storage_file_delete
cmd_storage_file_get = _administration.cmd_storage_file_get
cmd_storage_file_locate = _administration.cmd_storage_file_locate
cmd_storage_file_put = _administration.cmd_storage_file_put
cmd_storage_files_list = _administration.cmd_storage_files_list
cmd_storage_location_stat = _administration.cmd_storage_location_stat
cmd_storage_refresh = _administration.cmd_storage_refresh
cmd_storage_replica_verify = _administration.cmd_storage_replica_verify
cmd_storage_source_add = _administration.cmd_storage_source_add
cmd_storage_source_register = _administration.cmd_storage_source_register
cmd_storage_sources_list = _administration.cmd_storage_sources_list
cmd_storage_store_delete = _administration.cmd_storage_store_delete
cmd_storage_store_evacuate = _administration.cmd_storage_store_evacuate
cmd_storage_store_probe = _administration.cmd_storage_store_probe
cmd_storage_store_save = _administration.cmd_storage_store_save
cmd_storage_store_show = _administration.cmd_storage_store_show
cmd_storage_store_update = _administration.cmd_storage_store_update
cmd_storage_stores_list = _administration.cmd_storage_stores_list

_GIB = _constants._GIB
EXIT_INTERRUPTED = _constants.EXIT_INTERRUPTED
EXIT_ISSUES = _constants.EXIT_ISSUES
EXIT_OK = _constants.EXIT_OK
EXIT_TERMINATED = _constants.EXIT_TERMINATED
EXIT_USAGE = _constants.EXIT_USAGE
CLIUsageError = _constants.CLIUsageError

_bounded_file_bytes = _core_access._bounded_file_bytes
_core_json = _core_access._core_json
_storage_command = _core_access._storage_command
_storage_query = _core_access._storage_query
_store_reference = _core_access._store_reference

_fsync_directory = _filesystem._fsync_directory
_nearest_existing_parent = _filesystem._nearest_existing_parent
_path_is_within = _filesystem._path_is_within

_run_logged_command = _ingest._run_logged_command
cmd_storage_ingest = _ingest.cmd_storage_ingest
ingest_main = _ingest.ingest_main

_apply_system_root_defaults = _ingest_config._apply_system_root_defaults
_budget = _ingest_config._budget
_gib = _ingest_config._gib
_validate_early_options = _ingest_config._validate_early_options

_uuid_argument = _ingest_options._uuid_argument
add_storage_ingest_arguments = _ingest_options.add_storage_ingest_arguments

_acquire_run_lock = _ingest_paths._acquire_run_lock
_lock_path = _ingest_paths._lock_path
_log_directory = _ingest_paths._log_directory
_report_path = _ingest_paths._report_path
_validate_database_path = _ingest_paths._validate_database_path
_validate_materialization_path = _ingest_paths._validate_materialization_path
_validate_paths = _ingest_paths._validate_paths
_validate_run_control_paths = _ingest_paths._validate_run_control_paths
_validate_source_root = _ingest_paths._validate_source_root

_preflight_checks = _ingest_preflight._preflight_checks

_LOGGER = _ingest_reporting._LOGGER
_enrich_terminal_payload = _ingest_reporting._enrich_terminal_payload
_handle_failure = _ingest_reporting._handle_failure
_json_default = _ingest_reporting._json_default
_json_text = _ingest_reporting._json_text
_log = _ingest_reporting._log
_log_cli_start = _ingest_reporting._log_cli_start
_print_payload = _ingest_reporting._print_payload
_write_report = _ingest_reporting._write_report

_console_progress = _ingest_run._console_progress
_run_ingest = _ingest_run._run_ingest

cmd_storage_audit = _integrity.cmd_storage_audit
cmd_storage_policy = _integrity.cmd_storage_policy
cmd_storage_policy_set = _integrity.cmd_storage_policy_set
cmd_storage_policy_violations = _integrity.cmd_storage_policy_violations
cmd_storage_reconcile = _integrity.cmd_storage_reconcile
cmd_storage_recovery_action = _integrity.cmd_storage_recovery_action
cmd_storage_recovery_list = _integrity.cmd_storage_recovery_list
cmd_storage_repair = _integrity.cmd_storage_repair
cmd_storage_status = _integrity.cmd_storage_status

_build_storage_admin_parsers = _parsers._build_storage_admin_parsers
build_storage_parser = _parsers.build_storage_parser

_storage_prompt_choice = _prompts._storage_prompt_choice
_storage_prompt_text = _prompts._storage_prompt_text
_storage_prompt_yes_no = _prompts._storage_prompt_yes_no
_storage_stdin_is_interactive = _prompts._storage_stdin_is_interactive
_StorageAddCancelled = _prompts._StorageAddCancelled

cmd_storage_resource_delete = _resources.cmd_storage_resource_delete
cmd_storage_resource_get = _resources.cmd_storage_resource_get
cmd_storage_resource_list = _resources.cmd_storage_resource_list
cmd_storage_resource_write = _resources.cmd_storage_resource_write
cmd_storage_resources_describe = _resources.cmd_storage_resources_describe

SignalCancellation = _signals.SignalCancellation

_refresh_failure_count = _store_add._refresh_failure_count
cmd_storage_store_add = _store_add.cmd_storage_store_add

_SENSITIVE_STORE_OPTION_MARKERS = _store_options._SENSITIVE_STORE_OPTION_MARKERS
_backend_policy = _store_options._backend_policy
_default_store_role = _store_options._default_store_role
_descriptor_for_kind = _store_options._descriptor_for_kind
_parse_backend_option = _store_options._parse_backend_option
_reject_sensitive_policy = _store_options._reject_sensitive_policy
_store_add_payload = _store_options._store_add_payload

_apply_storage_add_wizard_plan = _store_wizard._apply_storage_add_wizard_plan
_default_store_name = _store_wizard._default_store_name
_print_storage_add_wizard_plan = _store_wizard._print_storage_add_wizard_plan
_run_storage_add_wizard = _store_wizard._run_storage_add_wizard
_storage_add_wizard_plan = _store_wizard._storage_add_wizard_plan
_StorageAddWizardPlan = _store_wizard._StorageAddWizardPlan
_wizard_access = _store_wizard._wizard_access
_wizard_advanced_configuration = _store_wizard._wizard_advanced_configuration
_wizard_backend = _store_wizard._wizard_backend
_wizard_post_save_actions = _store_wizard._wizard_post_save_actions
cmd_storage_add = _store_wizard.cmd_storage_add


__all__ = [
    "EXIT_INTERRUPTED",
    "EXIT_ISSUES",
    "EXIT_OK",
    "EXIT_TERMINATED",
    "EXIT_USAGE",
    "SignalCancellation",
    "add_storage_ingest_arguments",
    "build_storage_parser",
    "cmd_storage_ingest",
    "ingest_main",
]
