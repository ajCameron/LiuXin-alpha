
"""
Expose the supported language tools compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_icu.py
"""
from LiuXin_alpha.utils.language_tools.pluralizers import singular_plural_mapper, plural_singular_mapper

# DB-backed language lookup (FRBR languages constant table)
from LiuXin_alpha.utils.language_tools.db_language_lookup import (
    best_effort_language_id,
    ensure_languages_seeded_and_locked,
    invalidate_language_caches,
    register_language_id_sql_function,
)

__all__ = [
    "singular_plural_mapper",
    "plural_singular_mapper",
    "best_effort_language_id",
    "ensure_languages_seeded_and_locked",
    "invalidate_language_caches",
    "register_language_id_sql_function",
]
