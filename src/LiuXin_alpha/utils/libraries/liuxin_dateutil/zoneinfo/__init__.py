"""
Expose the supported zoneinfo compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
"""
from dateutil.tz import tzfile
from tarfile import TarFile
import os

__author__ = "Gustavo Niemeyer <gustavo@niemeyer.net>"
__license__ = "PSF License"

__all__ = ["setcachesize", "gettz", "rebuild"]

CACHE = []
CACHESIZE = 10


class tzfile(tzfile):
    """
    Provide the tzfile utility contract with explicit state and cleanup behavior.

    Example:
        Exercise tzfile through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py
    """
    def __reduce__(self):
        """
        Perform the reduce utility operation under explicit compatibility rules.

        Example:
            Exercise tzfile.  reduce   through a consuming regression::

                python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return (gettz, (self._filename,))


def getzoneinfofile():
    """
    Perform the getzoneinfofile utility operation under explicit compatibility rules.

    Example:
        Exercise getzoneinfofile through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    filenames = os.listdir(os.path.join(os.path.dirname(__file__)))
    filenames.sort()
    filenames.reverse()
    for entry in filenames:
        if entry.startswith("zoneinfo") and ".tar." in entry:
            return os.path.join(os.path.dirname(__file__), entry)
    return None


ZONEINFOFILE = getzoneinfofile()

del getzoneinfofile


def setcachesize(size):
    """
    Perform the setcachesize utility operation under explicit compatibility rules.

    Example:
        Exercise setcachesize through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :param size: Value supplied for size under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    global CACHESIZE, CACHE
    CACHESIZE = size
    del CACHE[size:]


def gettz(name):
    """
    Perform the gettz utility operation under explicit compatibility rules.

    Example:
        Exercise gettz through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    tzinfo = None
    if ZONEINFOFILE:
        for cachedname, tzinfo in CACHE:
            if cachedname == name:
                break
        else:
            tf = TarFile.open(ZONEINFOFILE)
            try:
                zonefile = tf.extractfile(name)
            except KeyError:
                tzinfo = None
            else:
                tzinfo = tzfile(zonefile)
            tf.close()
            CACHE.insert(0, (name, tzinfo))
            del CACHE[CACHESIZE:]
    return tzinfo


def rebuild(filename, tag=None, format="gz"):
    """
    Perform the rebuild utility operation under explicit compatibility rules.

    Example:
        Exercise rebuild through a consuming regression::

            python -m pytest -q tests/utils/plugins/test_speedup_parse_date_epoch_ints.py


    :param filename: Filename used for type inference or archive output.
    :param tag: Value supplied for tag under the utility contract.
    :param format: Value supplied for format under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import tempfile, shutil

    tmpdir = tempfile.mkdtemp()
    zonedir = os.path.join(tmpdir, "zoneinfo")
    moduledir = os.path.dirname(__file__)
    if tag:
        tag = "-" + tag
    targetname = "zoneinfo%s.tar.%s" % (tag, format)
    try:
        tf = TarFile.open(filename)
        for name in tf.getnames():
            if not (name.endswith(".sh") or name.endswith(".tab") or name == "leapseconds"):
                tf.extract(name, tmpdir)
                filepath = os.path.join(tmpdir, name)
                os.system("zic -d %s %s" % (zonedir, filepath))
        tf.close()
        target = os.path.join(moduledir, targetname)
        for entry in os.listdir(moduledir):
            if entry.startswith("zoneinfo") and ".tar." in entry:
                os.unlink(os.path.join(moduledir, entry))
        tf = TarFile.open(target, "w:%s" % format)
        for entry in os.listdir(zonedir):
            entrypath = os.path.join(zonedir, entry)
            tf.add(entrypath, entry)
        tf.close()
    finally:
        shutil.rmtree(tmpdir)
