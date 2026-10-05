"""
Initialize legacy startup preferences as module globals and print their diagnostics.

These values are not loaded from persistent settings. ``language`` and the verbose
flags are assigned only when ``declare_global_preferences`` is called; imports
alone establish just the local print aliases.
"""

from __future__ import print_function

__author__ = "root"

# Preferences for LiuXin.
# Currently just declares a list of global variables.
# To fit with LiuXin's philosophy of ease of entry for hacking, this will eventually be a text file


LiuXin_print = LiuXin_debug_print = LiuXin_warning_print = print


def declare_global_preferences():
    """
    Reset the legacy startup globals to English, verbose startup, and nonverbose debugging.

    Set ``verbose_startup=True``, ``language='eng'``, and ``verbose_debug=False``
    on this module. Every call overwrites previous values and emits three print
    checks plus a language message through the module's replaceable print aliases.

    Example:
        >>> from LiuXin_alpha.startup_scripts import preferences
        >>> preferences.declare_global_preferences()
        LiuXin_print working
        LiuXin_debug_print_working
        LiuXin_warning_print working
        Language set as eng
        >>> (preferences.language, preferences.verbose_debug)
        ('eng', False)


    :return: ``None``; module globals are assigned and startup diagnostics are printed.
    """

    # TODO: Note you need to make a variable global before assignment
    # firstly, do you want to see all the cruft that LiuXin does as it starts up?
    # I do. I wrote most of it, and seeing it imported and tested gives me a warm fuzzy feeling
    global verbose_startup
    verbose_startup = True
    if verbose_startup:
        LiuXin_print("LiuXin_print working")
        LiuXin_debug_print("LiuXin_debug_print_working")
        LiuXin_warning_print("LiuXin_warning_print working")

    # declare the local language prefernce - this will be used extensively with the translation engine
    # when it exists
    # TODO: Build translation engine
    global language
    language = "eng"
    if verbose_startup:
        print_string = "Language set as " + str(language)
        LiuXin_print(print_string)

    # verbose debug mode - LiuXin prints everything it's trying to do, and what it's trying to do it with.
    # Annoying, but useful
    global verbose_debug
    verbose_debug = False


def test():
    """
    Print the current language and debug flag as a manual startup diagnostic.

    This helper does not initialize preferences or make assertions. Call
    ``declare_global_preferences`` first so that both globals exist.

    Example:
        >>> test()  # doctest: +SKIP
        eng
        False


    :return: ``None``; the two values are printed on separate lines.
    :raises NameError: If preference initialization has not assigned the globals.
    """

    print(language)
    print(verbose_debug)
