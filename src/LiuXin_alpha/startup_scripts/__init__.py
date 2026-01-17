from __future__ import unicode_literals, print_function

import sys
import os

import builtins as six_builtins

# Preforms the tasks needed before LiuXin can start
_run_once = False
iswindows = False
islinux = True


if not _run_once:
    _run_once = True

    # Todo: Need to load this constant as early as possible
    if not False:  # if not isfrozen:
        # Prevent PyQt4 from being loaded
        class PyQt4Ban(object):
            def find_module(self, fullname, path=None):
                if fullname.startswith("PyQt4"):
                    return self

            def load_module(self, fullname):
                raise ImportError("Importing PyQt4 is not allowed as calibre uses PyQt5")

        sys.meta_path.insert(0, PyQt4Ban())

    def local_open(name, mode="r", bufsize=-1):
        """
        Open a file that wont be inherited by child processes

        Only supports the following modes:
            r, w, a, rb, wb, ab, r+, w+, a+, r+b, w+b, a+b
        """
        if iswindows:

            class fwrapper(object):
                def __init__(self, name, fobject):
                    object.__setattr__(self, "fobject", fobject)
                    object.__setattr__(self, "name", name)

                def __getattribute__(self, attr):
                    if attr in (
                        "name",
                        "__enter__",
                        "__str__",
                        "__unicode__",
                        "__repr__",
                        "__exit__",
                    ):
                        return object.__getattribute__(self, attr)
                    fobject = object.__getattribute__(self, "fobject")
                    return getattr(fobject, attr)

                def __setattr__(self, attr, val):
                    fobject = object.__getattribute__(self, "fobject")
                    return setattr(fobject, attr, val)

                def __repr__(self):
                    fobject = object.__getattribute__(self, "fobject")
                    name = object.__getattribute__(self, "name")
                    return re.sub(r"""['"]<fdopen>['"]""", repr(name), repr(fobject))

                def __str__(self):
                    return repr(self)

                def __unicode__(self):
                    return repr(self).decode("utf-8")

                def __enter__(self):
                    fobject = object.__getattribute__(self, "fobject")
                    fobject.__enter__()
                    return self

                def __exit__(self, *args):
                    fobject = object.__getattribute__(self, "fobject")
                    return fobject.__exit__(*args)

            m = mode[0]
            random = len(mode) > 1 and mode[1] == "+"
            binary = mode[-1] == "b"

            if m == "a":
                flags = os.O_APPEND | os.O_RDWR
                flags |= os.O_RANDOM if random else os.O_SEQUENTIAL
            elif m == "r":
                if random:
                    flags = os.O_RDWR | os.O_RANDOM
                else:
                    flags = os.O_RDONLY | os.O_SEQUENTIAL
            elif m == "w":
                if random:
                    flags = os.O_RDWR | os.O_RANDOM
                else:
                    flags = os.O_WRONLY | os.O_SEQUENTIAL
                flags |= os.O_TRUNC | os.O_CREAT
            if binary:
                flags |= os.O_BINARY
            else:
                flags |= os.O_TEXT
            flags |= os.O_NOINHERIT
            fd = os.open(name, flags)
            ans = os.fdopen(fd, mode, bufsize)
            ans = fwrapper(name, ans)
        else:
            import fcntl

            try:
                cloexec_flag = fcntl.FD_CLOEXEC
            except AttributeError:
                cloexec_flag = 1
            # Python 2.x uses fopen which on recent glibc/linux kernel at least
            # respects the 'e' mode flag. On OS X the e is ignored. So to try
            # to get atomicity where possible we pass 'e' and then only use
            # fcntl only if CLOEXEC was not set.
            if islinux:
                mode += "e"
            ans = open(name, mode, bufsize)
            old = fcntl.fcntl(ans, fcntl.F_GETFD)
            if not (old & cloexec_flag):
                fcntl.fcntl(ans, fcntl.F_SETFD, old | cloexec_flag)
        return ans

    six_builtins.__dict__["lopen"] = local_open

from LiuXin_alpha.startup_scripts.preferences import declare_global_preferences

from LiuXin_alpha.startup_scripts.prefs_folder_manager import ensure_prefs_folder, ensure_debug_folder, ensure_scratch_folder, ensure_folders


from LiuXin_alpha.constants import VERBOSE_DEBUG
from LiuXin_alpha.constants.paths import LiuXin_path, LiuXin_base_folder, LiuXin_prefs_folder


def startup():
    """
    Runs all the scripts and prepared the environment for LiuXin to start
    :return:
    """

    # declaring the special print functions.
    # declare_print_functions()

    # declaring preferences next so that they can be used (with the print functions) in the rest of the startup script
    declare_global_preferences()
    # declare_translation_functions()

    # declare other global functions
    # declare_global_functions()

    ensure_prefs_folder()
    ensure_debug_folder()
    ensure_scratch_folder()
    ensure_folders()

    if VERBOSE_DEBUG:
        print("LiuXin folder reported at", LiuXin_path)
        print("It is contained in", LiuXin_base_folder)
        print("The preferences folder is ", LiuXin_prefs_folder)


def test_lopen():
    from LiuXin.utils.calibre.ptempfile import TemporaryDirectory
    from LiuXin.utils.calibre import CurrentDir

    n = "f\xe4llen"

    with TemporaryDirectory() as tdir:
        with CurrentDir(tdir):
            with lopen(n, "w") as f:
                f.write("one")
            print("O_CREAT tested")
            with lopen(n, "w+b") as f:
                f.write("two")
            with lopen(n, "r") as f:
                if f.read() == "two":
                    print("O_TRUNC tested")
                else:
                    raise Exception("O_TRUNC failed")
            with lopen(n, "ab") as f:
                f.write("three")
            with lopen(n, "r+") as f:
                if f.read() == "twothree":
                    print("O_APPEND tested")
                else:
                    raise Exception("O_APPEND failed")
            with lopen(n, "r+") as f:
                f.seek(3)
                f.write("xxxxx")
                f.seek(0)
                if f.read() == "twoxxxxx":
                    print("O_RANDOM tested")
                else:
                    raise Exception("O_RANDOM failed")
