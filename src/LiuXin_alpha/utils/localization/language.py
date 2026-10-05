"""
Normalize language codes, names and locale fallbacks for LiuXin callers.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise language through a consuming regression::

        python -m pytest -q tests/utils/language_tools/test_pluralizers.py
"""
__author__ = "root"


# imported directly from calibre
def get_lang():
    """
    Try to figure out what language to display the interface in

    Example:
        Exercise get lang through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from calibre.utils.config_base import prefs

    lang = prefs["language"]
    lang = os.environ.get("CALIBRE_OVERRIDE_LANG", lang)
    if lang:
        return lang
    try:
        lang = get_system_locale()
    except:
        import traceback

        traceback.print_exc()
        lang = None
    if lang:
        match = re.match("[a-z]{2,3}(_[A-Z]{2}){0,1}", lang)
        if match:
            lang = match.group()
    if lang == "zh":
        lang = "zh_CN"
    if not lang:
        lang = "en"
    return lang


# imported directly from calibre
def canonicalize_lang(raw):
    """
    Perform the canonicalize lang utility operation under explicit compatibility rules.

    Example:
        Exercise canonicalize lang through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not raw:
        return None
    if not isinstance(raw, unicode):
        raw = raw.decode("utf-8", "ignore")
    raw = raw.lower().strip()
    if not raw:
        return None
    raw = raw.replace("_", "-").partition("-")[0].strip()
    if not raw:
        return None
    iso639 = _load_iso639()
    m2to3 = iso639["2to3"]

    if len(raw) == 2:
        ans = m2to3.get(raw, None)
        if ans is not None:
            return ans
    elif len(raw) == 3:
        if raw in iso639["by_3t"]:
            return raw
        if raw in iso639["3bto3t"]:
            return iso639["3bto3t"][raw]

    return iso639["name_map"].get(raw, None)


# imported from calibre
# Todo: not working as I can't find the iso639 pickle file
def _load_iso639():
    """
    Perform the load iso639 utility operation under explicit compatibility rules.

    Example:
        Exercise  load iso639 through a consuming regression::

            python -m pytest -q tests/utils/language_tools/test_pluralizers.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    global _iso639
    if _iso639 is None:
        ip = P("localization/iso639.pickle", allow_user_override=False)
        with open(ip, "rb") as f:
            _iso639 = cPickle.load(f)
    return _iso639
