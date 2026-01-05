__author__ = "root"


# imported directly from calibre
def get_lang():
    "Try to figure out what language to display the interface in"
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
    global _iso639
    if _iso639 is None:
        ip = P("localization/iso639.pickle", allow_user_override=False)
        with open(ip, "rb") as f:
            _iso639 = cPickle.load(f)
    return _iso639
