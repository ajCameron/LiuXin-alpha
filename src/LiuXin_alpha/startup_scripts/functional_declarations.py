__author__ = "root"
# functions to be declared globally - some need to be declared very early

from LiuXin_alpha.utils.general_ops.io_ops import LiuXin_print as ng_LX_print
from LiuXin_alpha.utils.general_ops.io_ops import LiuXin_debug_print as ng_LX_db_print
from LiuXin_alpha.utils.general_ops.io_ops import LiuXin_warning_print as ng_LX_w_print


def declare_print_functions():
    """
    At a minimum we'll need the print functions to be defined everywhere.
    """
    pass


def declare_translation_functions():
    """
    Some other functions must just be relied on to be present everywhere.
    """
    # Directly accessing the builtins to add some more functions
    import builtins as __builtin__

    # The default is for no translation to occur
    __builtin__.__dict__["_"] = lambda s: s

    # Some strings should be added to the translation tables, but shouldn't be rendered to the local.
    __builtin__.__dict__["__"] = lambda s: s

    from LiuXin_alpha.utils.icu import title_case, lower as icu_lower, upper as icu_upper

    __builtin__.__dict__["icu_lower"] = icu_lower
    __builtin__.__dict__["icu_upper"] = icu_upper
    __builtin__.__dict__["icu_title"] = title_case
