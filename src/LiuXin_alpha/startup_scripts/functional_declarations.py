"""
Provide legacy startup hooks for globally available translation and casing helpers.

The print-declaration hook is a no-op. The translation hook explicitly mutates
Python builtins; importing this module alone does not invoke either hook.
Its retained ``utils.general_ops`` imports require a legacy distribution; that
package is absent from the current checkout, so normal import fails before the
hooks become available. This module is historical startup support, not a current
importable initialization entry point.
"""

__author__ = "root"
# functions to be declared globally - some need to be declared very early

from LiuXin_alpha.utils.general_ops.io_ops import LiuXin_print as ng_LX_print
from LiuXin_alpha.utils.general_ops.io_ops import LiuXin_debug_print as ng_LX_db_print
from LiuXin_alpha.utils.general_ops.io_ops import LiuXin_warning_print as ng_LX_w_print


def declare_print_functions():
    """
    Retain the legacy print-initialization hook without installing any global names.

    The imported print helpers are available in this module, but this placeholder
    function does not copy them into builtins or otherwise configure logging.

    Example:
        >>> declare_print_functions() is None
        True


    :return: ``None``; no declarations or other state changes are performed.
    """
    pass


def declare_translation_functions():
    """
    Install identity translations and ICU casing functions into Python builtins.

    Replace ``_`` and ``__`` with identity callables, then import and assign
    ``icu_lower``, ``icu_upper``, and ``icu_title``. Existing builtins with these
    names are overwritten. This is not transactional: an ICU import failure
    leaves the two identity functions installed.

    Example:
        >>> declare_translation_functions()  # doctest: +SKIP


    :return: ``None``; the named builtins are installed for subsequent callers.
    """
    # Directly accessing the builtins to add some more functions
    import builtins as __builtin__

    # The default is for no translation to occur
    __builtin__.__dict__["_"] = lambda s: s

    # Some strings should be added to the translation tables, but shouldn't be rendered to the local.
    __builtin__.__dict__["__"] = lambda s: s

    from LiuXin_alpha.utils.icu import (
        title_case,
        lower as icu_lower,
        upper as icu_upper,
    )

    __builtin__.__dict__["icu_lower"] = icu_lower
    __builtin__.__dict__["icu_upper"] = icu_upper
    __builtin__.__dict__["icu_title"] = title_case
