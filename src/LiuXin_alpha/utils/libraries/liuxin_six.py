# Thing wrapper around six - instead of importing six directly - allows for changes to the interface
# (upgrades/modifications to six e.t.c)

from six import iteritems
from six import iterkeys
from six import itervalues



six_string_types = None
if six_string_types is None:
    from six import string_types

    six_string_types = string_types



try:
    import lzma as six_lzma
except ImportError:
    try:
        from backports import lzma as six_lzma
    except ImportError:
        # Todo: Temp patch
        six_lzma = NotImplementedError

try:
    import __builtin__ as __builtins__
except ImportError:
    # Think this is just a typo. But I can't run tests to confirm atm
    try:
        import builtin as __builtins__
    except ImportError:
        import builtins as __builtins__

try:
    import urlparse as six_urlparse
except ImportError:
    # Hopefully Py3 - but probably not
    try:
        import Urlparse as six_urlparse
    except ImportError:
        import urllib.parse as six_urlparse


def dict_iteritems(target_dict):
    return iteritems(target_dict)


def dict_iterkeys(target_dict):
    return iterkeys(target_dict)


def dict_itervalues(target_dict):
    return itervalues(target_dict)


def force_cmp(x, y):
    """
    Needed a name that was different from cmp
    :param x:
    :param y:
    :return:
    """
    try:
        return cmp(x, y)
    # Python 3 code
    except:
        return (x > y) - (x < y)


six_cmp = force_cmp


try:
    six_unicode = unicode
except:
    six_unicode = str

# Todo: Test that this actually preforms as it should
try:
    six_unichar = unichr
except:
    six_unichar = chr


try:
    memory_range = xrange
except:
    memory_range = range

# This structure to try and stop Pycharm erroneously reporting an error
try:
    import cPickle as generic_pickle
except:
    import _pickle as generic_pickle

six_pickle = generic_pickle

# Not perfect - should serve as a workaround
try:
    six_buffer = buffer
except:
    six_buffer = memoryview


try:
    user_input = raw_input
except:
    user_input = input


# Replacements for the imports generally carried out from future builtins
try:
    six_map = map
except:
    from future_builtins import map as six_map

try:
    six_zip = zip
except:
    from future_builtins import zip as six_zip

#

try:
    six_filter = filter
except:
    from future_builtins import filter as six_filter


try:
    six_long = long
except:
    six_long = int

try:
    six_input = user_input
except:
    six_input = input

# cStringIO - under Py2 - does not support unicode - hence
try:
    from cStringIO import StringIO as six_cStringIO
except:
    from io import StringIO as six_cStringIO

# StringIO - under Py2 - does support unicode
try:
    from StringIO import StringIO as six_basic_StringIO
except:
    from io import StringIO as six_basic_StringIO

# ----------------------------------------------------------------------------------------------------------------------
#
# - URLPARSE, URLLIB E.T.C

# urlparse
try:
    # Python 2 - import from the urlparse module
    from urlparse import urlparse as six_urlparse
except:
    # Python 3 - import from the urllib.parse module
    from urllib.parse import urlparse as six_urlparse

# unquote
try:
    # Python 2 - imports from the urllib2 module
    from urllib2 import unquote as six_unquote
except:
    # Python 3 - import the function, then rename it
    import urllib.parse

    six_unquote = urllib.parse.unquote

# urldefrag
try:
    # Python 2 - import from the urlparse module
    from urlparse import urldefrag as six_urldefrag
except:
    # Python 3 - import from urllib.parse, then rename
    import urllib.parse

    six_urldefrag = urllib.parse.urldefrag

# urlunparse
try:
    from urlparse import urlunparse as six_urlunparse
except:
    # Python 3 - import from urllib.parse, then rename
    import urllib.parse

    six_urlunparse = urllib.parse.urlunparse

# urljoin
try:
    from urlparse import urljoin as six_urljoin
except:
    # Python 3 - import from urllib.parse, then rename
    import urllib.parse

    six_urljoin = urllib.parse.urljoin


#
# ----------------------------------------------------------------------------------------------------------------------
