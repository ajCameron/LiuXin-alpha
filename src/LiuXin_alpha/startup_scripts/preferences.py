from __future__ import print_function

__author__ = "root"

# Preferences for LiuXin.
# Currently just declares a list of global variables.
# To fit with LiuXin's philosophy of ease of entry for hacking, this will eventually be a text file


LiuXin_print = LiuXin_debug_print = LiuXin_warning_print = print



def declare_global_preferences():
    """
    Declares some global preferences that LiuXin tends to universally need.
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

    print(language)
    print(verbose_debug)
