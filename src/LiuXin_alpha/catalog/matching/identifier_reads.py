"""Select matching identifier rows without changing matcher evidence or ordering."""

from LiuXin_alpha.databases.db_types.query_types import IdentifierMatch


def matching_identifier_rows(repository, prefix, candidate):
    """Filter with shared SQL normalization, or stream an adapter's fallback rows.

    Preserve all duplicate observations. The matcher retains owner filtering,
    validation, evidence and final ordering, including legacy adapter quirks.
    """
    matching = getattr(repository, "_matching_rows", None)
    if callable(matching):
        rows = matching(
            IdentifierMatch(
                f"{prefix}_scheme",
                f"{prefix}_value",
                candidate.identifier_type,
                candidate.normalised_value,
            )
        )
        if rows is not None:
            return rows
    scan = getattr(repository, "_scan_rows", repository._all_rows)
    return scan()
