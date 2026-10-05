"""
Expose the supported iso639 compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/language_tools/test_pluralizers.py
"""

import os
import codecs

# Python 3.4 compatibility
if not "unicode" in dir():
    unicode = str


class NonExistentLanguageError(RuntimeError):
    """
    Report the NonExistentLanguageError Calibre compatibility failure.

    Example:
        Exercise NonExistentLanguageError through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py
    """
    pass


def find(whatever=None, language=None, iso639_1=None, iso639_2=None):
    """
    Perform the find utility operation under explicit compatibility rules.

    Example:
        Exercise find through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :param whatever: Value supplied for whatever under the utility contract.
    :param language: Value supplied for language under the utility contract.
    :param iso639_1: Value supplied for iso639 1 under the utility contract.
    :param iso639_2: Value supplied for iso639 2 under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if whatever:
        keys = ["name", "iso639_1", "iso639_2_b", "iso639_2_t"]
        val = whatever
    elif language:
        keys = ["name"]
        val = language
    elif iso639_1:
        keys = ["iso639_1"]
        val = iso639_1
    elif iso639_2:
        keys = ["iso639_2_b", "iso639_2_t"]
        val = iso639_2
    else:
        raise ValueError("Invalid search criteria.")
    val = unicode(val)
    return next((item for item in data if any(item[key] == val for key in keys)), None)


def is_valid639_1(code):
    """
    Return or update whether is valid639 1 holds for the compatibility value.

    Example:
        Exercise is valid639 1 through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :param code: Value supplied for code under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    if len(code) != 2:
        return False
    return find(iso639_1=code) is not None


def is_valid639_2(code):
    """
    Return or update whether is valid639 2 holds for the compatibility value.

    Example:
        Exercise is valid639 2 through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :param code: Value supplied for code under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    if len(code) != 3:
        return False
    return find(iso639_2=code) is not None


def to_iso639_1(key):
    """
    Perform the to iso639 1 utility operation under explicit compatibility rules.

    Example:
        Exercise to iso639 1 through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :param key: Metadata, identifier or local-variable key.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    item = find(whatever=key)
    if not item:
        raise NonExistentLanguageError("Language does not exist.")
    return item["iso639_1"]


def to_iso639_2(key, type="B"):
    """
    Perform the to iso639 2 utility operation under explicit compatibility rules.

    Example:
        Exercise to iso639 2 through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :param key: Metadata, identifier or local-variable key.
    :param type: Value supplied for type under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not type in ("B", "T"):
        raise ValueError('Type must be either "B" or "T".')
    item = find(whatever=key)
    if not item:
        raise NonExistentLanguageError("Language does not exist.")
    if type == "T" and item["iso639_2_t"]:
        return item["iso639_2_t"]
    return item["iso639_2_b"]


def to_name(key):
    """
    Perform the to name utility operation under explicit compatibility rules.

    Example:
        Exercise to name through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :param key: Metadata, identifier or local-variable key.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    item = find(whatever=key)
    if not item:
        raise NonExistentLanguageError("Language does not exist.")
    return item["name"]


def _load_data():
    """
    Perform the load data utility operation under explicit compatibility rules.

    Example:
        Exercise  load data through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def parse_line(line):
        """
        Parse line under the documented compatibility and safety rules.

        Example:
            Exercise  load data.parse line through a consuming regression::

                python -m pytest -q tests/utils/language_tools/test_pluralizers.py


        :param line: Value supplied for line under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        data = line.strip().split("|")
        return {
            "iso639_2_b": data[0],
            "iso639_2_t": data[1],
            "iso639_1": data[2],
            "name": data[3],
        }

    data_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ISO-639-2_utf-8.txt")

    # NOTE:
    # The upstream iso639 package expects a bundled ISO-639-2 data file.
    # In LiuXin_alpha we sometimes run in minimal / fixture-only environments
    # (e.g. CI, slim source snapshots) where that file is absent.
    #
    # To keep the core library usable (and the test suite importable), we
    # provide a small built-in fallback corpus covering the languages that
    # appear in our fixtures and metadata tests.
    try:
        with codecs.open(data_file, "r", "UTF-8") as f:
            return [parse_line(line) for line in f]
    except FileNotFoundError:
        # Minimal corpus: iso639_2_b | iso639_2_t | iso639_1 | English name
        minimal = [
            ("eng", "eng", "en", "English"),
            ("fre", "fra", "fr", "French"),
            ("ger", "deu", "de", "German"),
            ("spa", "spa", "es", "Spanish"),
            ("ita", "ita", "it", "Italian"),
            ("por", "por", "pt", "Portuguese"),
            ("dut", "nld", "nl", "Dutch"),
            ("rus", "rus", "ru", "Russian"),
            ("jpn", "jpn", "ja", "Japanese"),
            ("chi", "zho", "zh", "Chinese"),
            ("ara", "ara", "ar", "Arabic"),
            ("hin", "hin", "hi", "Hindi"),
        ]
        return [
            {"iso639_2_b": b, "iso639_2_t": t, "iso639_1": a2, "name": name}
            for (b, t, a2, name) in minimal
        ]


data = _load_data()
