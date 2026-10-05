"""
Apply local-ingest profile defaults and convert CLI limits into a validated budget.

Explicit database selection without an explicit profile/root bypasses ambient
profile defaults. Applying a profile mutates the argument namespace and requires
an existing SQLite/APSW catalogue; it does not validate the catalogue's schema.
Numeric configuration is checked before log setup, with further filesystem and
backend observations owned by later validation/preflight steps.
"""

from __future__ import annotations

import argparse

from LiuXin_alpha.ingest.mixed_application import MixedIngestBudget
from LiuXin_alpha.surfaces.cli.storage_commands.constants import _GIB, CLIUsageError
from LiuXin_alpha.surfaces.system_profile import load_system_profile


def _apply_system_root_defaults(args: argparse.Namespace) -> None:
    """
    Fill absent local-ingest paths from the selected profile and require an existing database.

    Return immediately for an explicit truthy database without explicit root/profile,
    bypassing environment/persisted selection. Otherwise consult the profile loader
    with both fallbacks enabled and optional selection. Accept only SQLite/APSW
    manifest types; preserve truthy explicit paths and fill missing database/cache/log
    paths. Partial argument assignments can remain if a missing database later raises.

    Example:
        >>> args = argparse.Namespace(database="chosen.sqlite")
        >>> _apply_system_root_defaults(args)
        >>> args.database
        'chosen.sqlite'


    :param args: Mutable parsed namespace containing database, materialization_root,
        log_directory, require_existing_database, and optional system_root/profile.
    :return: None; selected defaults and require_existing_database=True are assigned in place.
    :raises CLIUsageError: Profile loading reports missing/invalid data, its type is unsupported,
        or no catalogue path remains after defaults are applied.
    """
    raw_root = getattr(args, "system_root", None)
    raw_profile = getattr(args, "profile", None)
    if args.database and not raw_root and not raw_profile:
        return
    try:
        resolved = load_system_profile(
            system_root=raw_root,
            profile=raw_profile,
            use_environment=True,
            use_persisted=True,
            required=False,
        )
    except (FileNotFoundError, ValueError) as error:
        raise CLIUsageError(str(error)) from error
    if resolved is None:
        return
    manifest_path = resolved.path
    manifest = resolved.values
    if str(manifest.get("db_type") or "SQLite").strip().lower() not in {
        "sqlite",
        "apsw",
    }:
        raise CLIUsageError(
            "mixed local ingest currently requires a SQLite/APSW system manifest"
        )
    if not args.database:
        args.database = str(manifest.get("database") or "") or None
    if not args.materialization_root:
        value = manifest.get("materialization_root")
        args.materialization_root = None if value in (None, "") else str(value)
    if not args.log_directory:
        value = manifest.get("log_directory")
        args.log_directory = None if value in (None, "") else str(value)
    if not args.database:
        raise CLIUsageError(f"system manifest has no catalogue path: {manifest_path!s}")
    args.require_existing_database = True


def _budget(args: argparse.Namespace) -> MixedIngestBudget:
    """
    Convert CLI count/time/GiB limits into the shared mixed-ingest budget record.

    Byte ceilings use binary GiB truncated to integer bytes. The budget constructor
    rejects nonpositive count/byte limits, nonfinite or sub-one expansion ratios,
    and nonfinite/nonpositive wall time. This function does not inspect available
    storage or enforce the limits against a running workflow.

    Example:
        >>> budget = _budget(parsed_ingest_args)  # doctest: +SKIP


    :param args: Namespace containing all max_* ingest safety-limit options.
    :return: New validated MixedIngestBudget with bytes, counts, ratio, and seconds fields.
    :raises ValueError: A numeric conversion or budget invariant fails, including CLIUsageError.
    :raises OverflowError: An infinite GiB value cannot be converted to integer bytes.
    """
    return MixedIngestBudget(
        max_source_files=int(args.max_source_files),
        max_containers=int(args.max_containers),
        max_container_depth=int(args.max_container_depth),
        max_members=int(args.max_members),
        max_members_per_container=int(args.max_members_per_container),
        max_member_bytes=_gib(args.max_member_gib, "--max-member-gib"),
        max_container_expanded_bytes=_gib(
            args.max_container_expanded_gib,
            "--max-container-expanded-gib",
        ),
        max_total_expanded_bytes=_gib(
            args.max_total_expanded_gib,
            "--max-total-expanded-gib",
        ),
        max_container_expansion_ratio=float(args.max_expansion_ratio),
        max_materialized_bytes=_gib(
            args.max_materialized_gib,
            "--max-materialized-gib",
        ),
        max_temporary_bytes=_gib(
            args.max_temporary_gib,
            "--max-temporary-gib",
        ),
        max_path_depth=int(args.max_path_depth),
        max_path_bytes=int(args.max_path_bytes),
        max_wall_time_s=float(args.max_wall_time_seconds),
        max_issues=int(args.max_issues),
    )


def _gib(value: object, option: str) -> int:
    """
    Convert positive numeric text/value in binary GiB to a truncated byte count.

    Only int/float/str instances are accepted, so True is also accepted as one GiB.
    A tiny positive value may return zero for the budget constructor to reject.
    NaN/infinity are not explicitly filtered: final integer conversion raises.

    Example:
        >>> _gib("0.5", "--limit")
        536870912
        >>> _gib(1e-12, "--limit")
        0


    :param value: Supported number or numeric string in units of 1,073,741,824 bytes.
    :param option: CLI flag name included in actionable numeric/positivity errors.
    :return: Truncated integer byte count; no finite-value or one-byte minimum check is added.
    :raises CLIUsageError: Type/conversion is unsupported or the parsed number is nonpositive.
    :raises ValueError: Final integer conversion receives NaN.
    :raises OverflowError: Final integer conversion receives infinity.
    """
    if not isinstance(value, (int, float, str)):
        raise CLIUsageError(f"{option} must be a number")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise CLIUsageError(f"{option} must be a number") from error
    if number <= 0:
        raise CLIUsageError(f"{option} must be positive")
    return int(number * _GIB)


def _validate_early_options(args: argparse.Namespace) -> None:
    """
    Check database presence, log/lock/backend settings, and budget before logging starts.

    Only discover_only exempts the database requirement; preflight still needs a
    catalogue selector. Construct and discard a budget for validation. Backend
    timeout uses a <=0 check, so nonfinite values are not independently rejected;
    other settings follow their explicit int/float conversions.

    Example:
        >>> _validate_early_options(parsed_ingest_args)  # doctest: +SKIP


    :param args: Complete parsed ingest namespace, normally after profile defaults.
    :return: None after checks; paths/resources are not opened or reserved here.
    :raises CLIUsageError: Required selection or an explicitly checked range is invalid.
    :raises ValueError: Numeric conversion or the constructed budget rejects a value.
    """
    if not bool(args.discover_only) and not args.database:
        raise CLIUsageError("--database is required unless --discover-only is selected")
    if int(args.log_max_mib) < 1:
        raise CLIUsageError("--log-max-mib must be positive")
    if int(args.log_backup_count) < 0:
        raise CLIUsageError("--log-backup-count must not be negative")
    if int(args.log_checkpoint_every) < 1:
        raise CLIUsageError("--log-checkpoint-every must be positive")
    if int(args.lock_timeout_seconds) < 0:
        raise CLIUsageError("--lock-timeout-seconds must not be negative")
    if float(args.backend_timeout_seconds) <= 0:
        raise CLIUsageError("--backend-timeout-seconds must be positive")
    _ = _budget(args)
