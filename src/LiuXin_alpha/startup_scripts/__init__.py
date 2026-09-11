"""
Retain legacy startup hooks for preferences, directory preparation, and file opening.

Importing this module installs a legacy PyQt4 import hook and overwrites the
``lopen`` builtin with ``local_open``. Calling ``startup`` separately initializes
preferences and checks folders. The opener and import-hook APIs retain historical
Python-2 assumptions; these are not current-platform compatibility guarantees.
"""

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
            """
            Reject PyQt4-prefixed imports through the legacy find_module/load_module API.

            Importing this startup module inserts an instance into ``sys.meta_path``.
            The class does not implement ``find_spec`` for modern import protocols.

            Example:
                >>> PyQt4Ban().find_module("PyQt5") is None
                True
            """

            def find_module(self, fullname, path=None):
                """
                Claim every module whose full name starts with the PyQt4 prefix.

                Other names are left to subsequent import finders. The comparison
                is a plain string prefix, not a package-boundary check.

                Example:
                    >>> blocker = PyQt4Ban()
                    >>> blocker.find_module("PyQt4.QtCore") is blocker
                    True


                :param fullname: Fully qualified module name supplied by the importer.
                :param path: Optional package search path; unused by this legacy finder.
                :return: This finder for PyQt4-prefixed names, otherwise ``None``.
                """
                if fullname.startswith("PyQt4"):
                    return self

            def load_module(self, fullname):
                """
                Reject a claimed import with the historical Qt-version diagnostic.

                The supplied name is not revalidated; every direct call raises.

                Example:
                    >>> PyQt4Ban().load_module("PyQt4")
                    Traceback (most recent call last):
                    ...
                    ImportError: Importing PyQt4 is not allowed as calibre uses PyQt5


                :param fullname: Claimed module name, unused when forming the error.
                :return: Never returns normally.
                :raises ImportError: Always, to prevent the legacy PyQt4 import.
                """
                raise ImportError(
                    "Importing PyQt4 is not allowed as calibre uses PyQt5"
                )

        sys.meta_path.insert(0, PyQt4Ban())

    def local_open(name, mode="r", bufsize=-1):
        """
        Open a file using the legacy platform-specific noninheritance mechanism.

        The Windows branch builds ``os.open`` flags and wraps ``os.fdopen`` to
        preserve the original filename. The other branch uses ``fcntl`` to set
        close-on-exec, adding the historical ``e`` fopen mode when ``islinux`` is
        true. The module's platform globals, not runtime detection, choose the path.

        Historical mode intentions include ``r/w/a``, binary variants, and update
        variants. They are not fully validated here. In particular Python 3's
        builtin ``open`` rejects the appended ``e`` mode, and the Windows wrapper
        retains Python-2 representation assumptions. This is not a portable modern
        opener; failures after opening a descriptor are not cleaned up explicitly.

        Example:
            >>> handle = local_open(filename, "rb")  # doctest: +SKIP


        :param name: Filesystem path passed to the selected opening implementation.
        :param mode: Legacy file-opening mode string used for flags and stream creation.
        :param bufsize: Buffering argument forwarded to ``open`` or ``os.fdopen``.
        :return: Open stream, or the filename-preserving wrapper on the Windows branch.
        """
        if iswindows:

            class fwrapper(object):
                """
                Proxy a Windows fdopen stream while retaining its original path for display.

                Most attribute access and every ordinary assignment go to the
                underlying stream. Context entry returns this wrapper. Historical
                representation hooks require a module-global ``re`` binding and
                retain a Python-2-only Unicode conversion.

                Example:
                    >>> wrapped.name == original_path  # doctest: +SKIP
                    True
                """

                def __init__(self, name, fobject):
                    """
                    Store the display path and wrapped stream without invoking proxy assignment.

                    Example:
                        >>> wrapped = fwrapper(original_path, stream)  # doctest: +SKIP


                    :param name: Original path to retain in the wrapper's own state.
                    :param fobject: Open file object to receive forwarded operations.
                    :return: ``None``; the wrapper retains both supplied objects.
                    """
                    object.__setattr__(self, "fobject", fobject)
                    object.__setattr__(self, "name", name)

                def __getattribute__(self, attr):
                    """
                    Read wrapper-owned display/context hooks or delegate other attributes.

                    Only ``name`` and the explicitly listed representation/context
                    methods stay on the wrapper. All other requested names, including
                    most special attributes, are looked up on the stream.

                    Example:
                        >>> data = wrapped.read(10)  # doctest: +SKIP


                    :param attr: Attribute name requested from the wrapper.
                    :return: Wrapper-owned attribute or the underlying stream's attribute.
                    """
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
                    """
                    Forward attribute assignment to the wrapped stream, including ``name``.

                    Wrapper storage is changed only through explicit base-object
                    operations such as those used during initialization.

                    Example:
                        >>> wrapped.custom_label = "source"  # doctest: +SKIP


                    :param attr: Attribute name to assign on the underlying stream.
                    :param val: Value to forward unchanged.
                    :return: ``None`` after the stream accepts the assignment.
                    """
                    fobject = object.__getattribute__(self, "fobject")
                    return setattr(fobject, attr, val)

                def __repr__(self):
                    """
                    Replace a quoted fdopen placeholder in the stream's representation with its path.

                    The regular-expression module is referenced as global ``re``;
                    this startup module does not import that name itself.

                    Example:
                        >>> label = repr(wrapped)  # doctest: +SKIP


                    :return: Stream representation with any quoted ``<fdopen>`` labels replaced.
                    :raises NameError: If the embedding environment has not supplied ``re``.
                    """
                    fobject = object.__getattribute__(self, "fobject")
                    name = object.__getattribute__(self, "name")
                    return re.sub(r"""['"]<fdopen>['"]""", repr(name), repr(fobject))

                def __str__(self):
                    """
                    Use the filename-adjusted representation as the wrapper's display string.

                    Example:
                        >>> label = str(wrapped)  # doctest: +SKIP


                    :return: Result of ``repr(self)``; representation errors propagate.
                    """
                    return repr(self)

                def __unicode__(self):
                    """
                    Decode the wrapper's representation as UTF-8 using the legacy Python-2 protocol.

                    On Python 3, ``repr`` produces a string without ``decode``;
                    this retained compatibility hook is not a working Unicode adapter.

                    Example:
                        >>> label = wrapped.__unicode__()  # doctest: +SKIP


                    :return: Decoded representation when used with the historical string protocol.
                    :raises AttributeError: If the representation does not expose ``decode``.
                    """
                    return repr(self).decode("utf-8")

                def __enter__(self):
                    """
                    Enter the underlying file context and keep this proxy as the bound handle.

                    Example:
                        >>> with wrapped as handle:  # doctest: +SKIP
                        ...     contents = handle.read()


                    :return: This wrapper after the underlying stream enters successfully.
                    """
                    fobject = object.__getattribute__(self, "fobject")
                    fobject.__enter__()
                    return self

                def __exit__(self, *args):
                    """
                    Forward context-exit arguments and preserve the stream's suppression result.

                    Example:
                        >>> result = wrapped.__exit__(None, None, None)  # doctest: +SKIP


                    :param args: Exception type, value, and traceback from the context manager.
                    :return: Underlying stream's exit result, forwarded without coercion.
                    """
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

from LiuXin_alpha.startup_scripts.prefs_folder_manager import (
    ensure_prefs_folder,
    ensure_debug_folder,
    ensure_scratch_folder,
    ensure_folders,
)


from LiuXin_alpha.constants import VERBOSE_DEBUG
from LiuXin_alpha.constants.paths import (
    LiuXin_path,
    LiuXin_base_folder,
    LiuXin_prefs_folder,
)


def startup():
    """
    Initialize legacy preferences, then ensure preference, debug, scratch, and manifest paths.

    Invoke the hooks in that order; exceptions stop subsequent preparation and
    already-created state is not rolled back. The boolean returned by
    ``ensure_folders`` is ignored. Optional path diagnostics use the imported
    ``VERBOSE_DEBUG`` constant, not the preference module's newly assigned flag.

    Example:
        >>> startup()  # doctest: +SKIP


    :return: ``None`` after all invoked startup hooks return normally.
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
    """
    Exercise historical lopen creation, truncation, append, and random-write behavior manually.

    Use a temporary working directory and a non-ASCII filename, printing successful
    stages and raising when a content comparison fails. The routine retains text
    writes to binary streams and other Python-2 assumptions; it is not a passing
    Python-3 acceptance test for the opener. Its historical
    ``utils.calibre.ptempfile`` import is also absent from the current checkout.

    Example:
        >>> test_lopen()  # doctest: +SKIP


    :return: ``None`` if every manual operation and content comparison succeeds.
    :raises Exception: If a truncation, append, or random-write comparison fails.
    :raises ModuleNotFoundError: If the historical temporary-file helper cannot be imported.
    """
    from LiuXin_alpha.utils.calibre.ptempfile import TemporaryDirectory
    from LiuXin_alpha.utils.calibre import CurrentDir

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
