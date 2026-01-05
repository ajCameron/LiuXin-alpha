#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import absolute_import, print_function

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


# ----------------------------------------------------------------------------------------------------------------------
#
#  - Localization systems for LiuXin. Currently does not exist.
#
# ----------------------------------------------------------------------------------------------------------------------


def _(string):
    """Takes a string, translates it to the localized language. Returns the translated string."""
    return six_unicode(string)


def trans(string):
    """
    Frontend for _ that doesn't make code checkers complain.
    :param string:
    :return:
    """
    return _(string)


def translate(lang, text):
    """
    Translate a given string from English into the given language
    :param lang:
    :param text:
    :return:
    """
    return _(text)


def __(string):
    """
    For when the string should be translated but not immediately added to the translation table.
    :param string:
    :return:
    """
    return six_unicode(string)


def delayed_trans(string):
    """
    Frontend for __ that doesn't make code checkers complain.
    :param string:
    :return:
    """
    return __(string)


def icu_lower(string):
    """
    Provides a more general mapping between upper and lowercase for unicode strings.
    Or it will.
    :param string:
    :return:
    """
    return string.lower()


# ----------------------------------------------------------------------------------------------------------------------
#
# - Copied from calibre for compatibility and modified to work with LiuXin
#
# ----------------------------------------------------------------------------------------------------------------------

import os
import locale
import re
import pickle as cPickle
from LiuXin_alpha.utils.resources import P
from gettext import GNUTranslations, NullTranslations

from LiuXin_alpha.utils.libraries.liuxin_six import six_cStringIO

_available_translations = None


def available_translations():
    global _available_translations
    if _available_translations is None:
        stats = P("localization/stats.pickle", allow_user_override=False)
        if os.path.exists(stats):
            stats = cPickle.load(open(stats, "rb"))
        else:
            stats = {}
        _available_translations = [x for x in stats if stats[x] > 0.1]
    return _available_translations


def get_system_locale():
    from LiuXin.constants import iswindows, isosx
    from LiuXin.utils.plugins import plugins

    lang = None
    if iswindows:
        try:
            from LiuXin.utils.calibre.constants import get_windows_user_locale_name

            lang = get_windows_user_locale_name()
            lang = lang.strip()
            if not lang:
                lang = None
        except:
            pass  # Windows XP does not have the GetUserDefaultLocaleName fn
    elif isosx:
        try:
            lang = plugins["usbobserver"][0].user_locale() or None
        except:
            # Fallback to environment vars if something bad happened
            import traceback

            traceback.print_exc()
    if lang is None:
        try:
            lang = locale.getdefaultlocale(["LANGUAGE", "LC_ALL", "LC_CTYPE", "LC_MESSAGES", "LANG"])[0]
        except:
            pass  # This happens on Ubuntu apparently
        if lang is None and "LANG" in os.environ:  # Needed for OS X
            try:
                lang = os.environ["LANG"]
            except:
                pass
    if lang:
        lang = lang.replace("-", "_")
        lang = "_".join(lang.split("_")[:2])
    return lang


def get_lang():
    """
    Try to figure out what language to display the interface in.
    :return lang: Default is 'en'. This should be returned even if something goes wrong.
    """
    # from LiuXin.utils.calibre.utils.config_base import prefs
    from LiuXin.preferences import preferences as prefs

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


def get_lc_messages_path(lang):
    hlang = None
    if zf_exists():
        if lang in available_translations():
            hlang = lang
        else:
            xlang = lang.split("_")[0].lower()
            if xlang in available_translations():
                hlang = xlang
    return hlang


def zf_exists():
    return os.path.exists(P("localization/locales.zip", allow_user_override=False))


_lang_trans = None


def get_all_translators():
    from zipfile import ZipFile

    with ZipFile(P("localization/locales.zip", allow_user_override=False), "r") as zf:
        for lang in available_translations():
            mpath = get_lc_messages_path(lang)
            if mpath is not None:
                buf = six_cStringIO(zf.read(mpath + "/messages.mo"))
                yield lang, GNUTranslations(buf)


lcdata = {
    "abday": ("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"),
    "abmon": (
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ),
    "d_fmt": "%m/%d/%Y",
    "d_t_fmt": "%a %d %b %Y %r %Z",
    "day": (
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ),
    "mon": (
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ),
    "noexpr": "^[nN].*",
    "radixchar": ".",
    "t_fmt": "%r",
    "t_fmt_ampm": "%I:%M:%S %p",
    "thousep": ",",
    "yesexpr": "^[yY].*",
}


def set_translators():
    global _lang_trans, lcdata
    # To test different translations invoke as
    # CALIBRE_OVERRIDE_LANG=de_DE.utf8 program
    lang = get_lang()
    t = None

    if lang:
        buf = iso639 = None
        mpath = get_lc_messages_path(lang)
        if mpath and os.access(mpath + ".po", os.R_OK):
            from translations import make

            buf = six_cStringIO()
            try:
                make(mpath + ".po", buf)
            except:
                print(("Failed to compile translations file: %s," " ignoring") % (mpath + ".po"))
                buf = None
            else:
                buf = six_cStringIO(buf.getvalue())

        if mpath is not None:
            from zipfile import ZipFile

            with ZipFile(P("localization/locales.zip", allow_user_override=False), "r") as zf:
                if buf is None:
                    buf = six_cStringIO(zf.read(mpath + "/messages.mo"))
                if mpath == "nds":
                    mpath = "de"
                isof = mpath + "/iso639.mo"
                try:
                    iso639 = six_cStringIO(zf.read(isof))
                except:
                    pass  # No iso639 translations for this lang
                if buf is not None:
                    try:
                        lcdata = cPickle.loads(zf.read(mpath + "/lcdata.pickle"))
                    except:
                        pass  # No lcdata

        if buf is not None:
            t = GNUTranslations(buf)
            if iso639 is not None:
                iso639 = _lang_trans = GNUTranslations(iso639)
                t.add_fallback(iso639)

    if t is None:
        t = NullTranslations()

    t.install(unicode=True, names=("ngettext",))
    # Now that we have installed a translator, we have to retranslate the help
    # for the global prefs object as it was instantiated in get_lang(), before
    # the translator was installed.
    from LiuXin.utils.calibre.utils.config_base import prefs

    prefs.retranslate_help()


_iso639 = None
_extra_lang_codes = {
    "pt_BR": _("Brazilian Portuguese"),
    "en_GB": _("English (UK)"),
    "zh_CN": _("Simplified Chinese"),
    "zh_TW": _("Traditional Chinese"),
    "en": _("English"),
    "en_US": _("English (United States)"),
    "en_AR": _("English (Argentina)"),
    "en_AU": _("English (Australia)"),
    "en_JP": _("English (Japan)"),
    "en_DE": _("English (Germany)"),
    "en_BG": _("English (Bulgaria)"),
    "en_EG": _("English (Egypt)"),
    "en_NZ": _("English (New Zealand)"),
    "en_CA": _("English (Canada)"),
    "en_GR": _("English (Greece)"),
    "en_IN": _("English (India)"),
    "en_NP": _("English (Nepal)"),
    "en_TH": _("English (Thailand)"),
    "en_TR": _("English (Turkey)"),
    "en_CY": _("English (Cyprus)"),
    "en_CZ": _("English (Czech Republic)"),
    "en_PH": _("English (Philippines)"),
    "en_PK": _("English (Pakistan)"),
    "en_PL": _("English (Poland)"),
    "en_HR": _("English (Croatia)"),
    "en_HU": _("English (Hungary)"),
    "en_ID": _("English (Indonesia)"),
    "en_IL": _("English (Israel)"),
    "en_RU": _("English (Russia)"),
    "en_SG": _("English (Singapore)"),
    "en_YE": _("English (Yemen)"),
    "en_IE": _("English (Ireland)"),
    "en_CN": _("English (China)"),
    "en_ZA": _("English (South Africa)"),
    "es_PY": _("Spanish (Paraguay)"),
    "es_UY": _("Spanish (Uruguay)"),
    "es_AR": _("Spanish (Argentina)"),
    "es_CR": _("Spanish (Costa Rica)"),
    "es_MX": _("Spanish (Mexico)"),
    "es_CU": _("Spanish (Cuba)"),
    "es_CL": _("Spanish (Chile)"),
    "es_EC": _("Spanish (Ecuador)"),
    "es_HN": _("Spanish (Honduras)"),
    "es_VE": _("Spanish (Venezuela)"),
    "es_BO": _("Spanish (Bolivia)"),
    "es_NI": _("Spanish (Nicaragua)"),
    "es_CO": _("Spanish (Colombia)"),
    "de_AT": _("German (AT)"),
    "fr_BE": _("French (BE)"),
    "nl": _("Dutch (NL)"),
    "nl_BE": _("Dutch (BE)"),
    "und": _("Unknown"),
}

if False:
    # Extra strings needed for Qt

    # NOTE: Ante Meridian (i.e. like 10:00 AM)
    _("AM")
    # NOTE: Post Meridian (i.e. like 10:00 PM)
    _("PM")
    # NOTE: Ante Meridian (i.e. like 10:00 am)
    _("am")
    # NOTE: Post Meridian (i.e. like 10:00 pm)
    _("pm")
    _("&Copy")
    _("Select All")
    _("Copy Link")
    _("&Select All")
    _("Copy &Link location")
    _("&Undo")
    _("&Redo")
    _("Cu&t")
    _("&Paste")
    _("Paste and Match Style")
    _("Directions")
    _("Left to Right")
    _("Right to Left")
    _("Fonts")
    _("&Step up")
    _("Step &down")

_lcase_map = {}
for k in _extra_lang_codes:
    _lcase_map[k.lower()] = k


# Todo: Crack open a calibre prefs folder and extract this method
def _load_iso639():
    global _iso639
    if _iso639 is None:
        ip = P("localization/iso639.pickle", allow_user_override=False)
        with open(ip, "rb") as f:
            _iso639 = cPickle.load(f)
    return _iso639


def get_language(lang):
    translate = _
    lang = _lcase_map.get(lang, lang)
    if lang in _extra_lang_codes:
        # The translator was not active when _extra_lang_codes was defined, so
        # re-translate
        return translate(_extra_lang_codes[lang])
    iso639 = _load_iso639()
    ans = lang
    lang = lang.split("_")[0].lower()
    if len(lang) == 2:
        ans = iso639["by_2"].get(lang, ans)
    elif len(lang) == 3:
        if lang in iso639["by_3b"]:
            ans = iso639["by_3b"][lang]
        else:
            ans = iso639["by_3t"].get(lang, ans)
    try:
        return _lang_trans.ugettext(ans)
    except AttributeError:
        return translate(ans)


def calibre_langcode_to_name(lc, localize=True):
    iso639 = _load_iso639()
    translate = _ if localize else lambda x: x
    try:
        return translate(iso639["by_3t"][lc])
    except:
        pass
    return lc


# Todo: Need to do a lot more work on this - as it isn't currently very good - should be a plugin?
def canonicalize_lang(raw):
    """
    Attempts to bring a language into a given, useful form.
    :param raw:
    :return:
    """
    # If None, returning None
    if not raw:
        return None

    if not isinstance(raw, unicode):
        raw = raw.decode("utf-8", "ignore")
    raw = raw.lower().strip()
    if not raw:
        return None

    if " " not in raw:
        raw = raw.replace("_", "-").partition("-")[0].strip()
    else:
        raw = raw.replace(" ", "-").partition("-")[0].strip()
    if not raw:
        return None

    # Todo: For the moment if something goes wrong it's ignored - fix this
    try:
        iso639 = _load_iso639()
    except:
        return raw

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


_lang_map = None


def lang_map():
    "Return mapping of ISO 639 3 letter codes to localized language names"
    iso639 = _load_iso639()
    translate = _
    global _lang_map
    if _lang_map is None:
        _lang_map = {k: translate(v) for k, v in iso639["by_3t"].iteritems()}
    return _lang_map


def langnames_to_langcodes(names):
    """
    Given a list of localized language names return a mapping of the names to 3 letter ISO 639 language codes.
    If a name is not recognized, it is mapped to None.
    :param names:
    :return:
    """
    iso639 = _load_iso639()
    translate = _
    ans = {}
    names = set(names)
    for k, v in iso639["by_3t"].iteritems():
        tv = translate(v)
        if tv in names:
            names.remove(tv)
            ans[tv] = k
        if not names:
            break
    for x in names:
        ans[x] = None

    return ans


def lang_as_iso639_1(name_or_code):
    """
    Takes a language as a iso639_2 code and tries to return it as an iso639_1 code.
    Seems to just run a lookup against a map of the 3 digit iso639_2 codes to the 2 digit iso639_1 codes and returns a
    iso_639_1 code if found.
    :param name_or_code:
    :return:
    """
    code = canonicalize_lang(name_or_code)
    if code is not None:
        try:
            iso639 = _load_iso639()
        except IOError:
            from LiuXin.utils.libraries.iso639.iso639_tools import (
                canonicalize_lang as fallback_canonicalize_lang,
            )

            return fallback_canonicalize_lang(code, iso_639_2=True)
        return iso639["3to2"].get(code, None)


_udc = None


def get_udc():
    global _udc
    if _udc is None:
        from LiuXin.file_formats.unihandecode import Unihandecoder

        _udc = Unihandecoder(lang=get_lang())
    return _udc


def localize_user_manual_link(url):
    lc = lang_as_iso639_1(get_lang())
    if lc == "en":
        return url
    import json

    try:
        stats = json.loads(
            P(
                "user-manual-translation-stats.json",
                allow_user_override=False,
                data=True,
            )
        )
    except EnvironmentError:
        return url
    if stats.get(lc, 0) < 0.3:
        return url
    from urlparse import urlparse, urlunparse

    parts = urlparse(url)
    path = re.sub(r"/generated/[a-z]+/", "/generated/%s/" % lc, parts.path or "")
    path = "/%s%s" % (lc, path)
    parts = list(parts)
    parts[2] = path
    return urlunparse(parts)
