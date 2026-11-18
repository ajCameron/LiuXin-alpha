# ruff: noqa: B007, B904, E741
# License: GPLv3 Copyright: 2010, Kovid Goyal <kovid@kovidgoyal.net>
# Derived from src/calibre/utils/formatter_functions.py at
# 5f53fce641f180cfaf0e976e703302d596e4fb2a; adapted 2026-11-17.
# Changes: retain builtin implementations; omit UI documentation/registration;
# adapt imports and expose application-only functions through explicit providers.
"""Pinned Calibre template builtin implementations for metadata search."""

from __future__ import annotations

import numbers
import os.path
import posixpath
import re
import traceback
from collections.abc import Mapping
from datetime import datetime, timedelta
from functools import partial
from math import ceil, floor, modf, trunc
from re import Match
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote as qquote

from LiuXin_alpha.metadata.book.base import calibreMetadata as Metadata
from LiuXin_alpha.metadata.book.base import field_metadata
from LiuXin_alpha.metadata.title_names import title_to_title_sort as title_sort
from LiuXin_alpha.utils.config.config_base import tweaks
from LiuXin_alpha.utils.date import UNDEFINED_DATE, format_date, parse_date
from LiuXin_alpha.utils.libraries.titlecase import titlecase
from LiuXin_alpha.utils.localization import (
    _,
    calibre_langcode_to_name,
    canonicalize_lang,
)
from LiuXin_alpha.utils.text import human_readable
from LiuXin_alpha.utils.text.icu import capitalize, sort_key, strcmp
from LiuXin_alpha.utils.text.icu import lower as icu_lower

from .formatter_support import StoredObjectType, get_database
from .upstream_support import now

if TYPE_CHECKING:
    from .upstream_formatter import Kwargs, Locals
    from .upstream_formatter import TemplateFormatter as Formatter

    Result = Any

prints = print
DATA_DIR_NAME = "data"
DATA_FILE_PATTERN = "data/**"
rendering_composite_name = "__rendering_composite__"


def only_in_gui_error(name):
    raise ValueError(f"{name} requires an explicitly supplied application provider")


class BuiltinFormatterFunction:
    object_type = StoredObjectType.PythonFunction
    aliases = ()
    cached_parse_tree = None

    def eval_(self, formatter, kwargs, mi, locals, *args):
        result = self.evaluate(formatter, kwargs, mi, locals, *args)
        if isinstance(result, (bytes, str)):
            return result
        if isinstance(result, list):
            return ",".join(result)
        if isinstance(result, (numbers.Number, bool)):
            return str(result)
        return None

    def only_in_gui_error(self):
        only_in_gui_error(self.name)

    def get_database(self, mi, formatter=None):
        return get_database(mi, self.name)


class BuiltinStrcmp(BuiltinFormatterFunction):
    name = "strcmp"
    arg_count = 5

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        x, y, lt, eq, gt = args
        v = strcmp(x, y)
        if v < 0:
            return lt
        if v == 0:
            return eq
        return gt


class BuiltinStrcmpcase(BuiltinFormatterFunction):
    name = "strcmpcase"
    arg_count = 5

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        x, y, lt, eq, gt = args
        from LiuXin_alpha.utils.text.icu import case_sensitive_strcmp as case_strcmp

        v = case_strcmp(x, y)
        if v < 0:
            return lt
        if v == 0:
            return eq
        return gt


class BuiltinCmp(BuiltinFormatterFunction):
    name = "cmp"
    arg_count = 5

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        value, y, lt, eq, gt = args
        value = float(value if value and value != "None" else 0)
        y = float(y if y and y != "None" else 0)
        if value < y:
            return lt
        if value == y:
            return eq
        return gt


class BuiltinFirstMatchingCmp(BuiltinFormatterFunction):
    name = "first_matching_cmp"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        if (len(args) % 2) != 0:
            raise ValueError(
                _("first_matching_cmp requires an even number of arguments")
            )
        val = float(args[0] if args[0] and args[0] != "None" else 0)
        for i in range(1, len(args) - 1, 2):
            c = float(args[i] if args[i] and args[i] != "None" else 0)
            if val < c:
                return args[i + 1]
        return args[len(args) - 1]


class BuiltinStrcat(BuiltinFormatterFunction):
    name = "strcat"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        i = 0
        res = ""
        for i in range(len(args)):
            res += args[i]
        return res


class BuiltinStrlen(BuiltinFormatterFunction):
    name = "strlen"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (a,) = args
        try:
            return len(a)
        except Exception:
            return -1


class BuiltinAdd(BuiltinFormatterFunction):
    name = "add"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        res = 0
        for v in args:
            v = float(v if v and v != "None" else 0)
            res += v
        return str(res)


class BuiltinSubtract(BuiltinFormatterFunction):
    name = "subtract"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        x, y = args
        x = float(x if x and x != "None" else 0)
        y = float(y if y and y != "None" else 0)
        return str(x - y)


class BuiltinMultiply(BuiltinFormatterFunction):
    name = "multiply"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        res = 1
        for v in args:
            v = float(v if v and v != "None" else 0)
            res *= v
        return str(res)


class BuiltinDivide(BuiltinFormatterFunction):
    name = "divide"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        x, y = args
        x = float(x if x and x != "None" else 0)
        y = float(y if y and y != "None" else 0)
        return str(x / y)


class BuiltinCeiling(BuiltinFormatterFunction):
    name = "ceiling"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (value,) = args
        value = float(value if value and value != "None" else 0)
        return str(ceil(value))


class BuiltinFloor(BuiltinFormatterFunction):
    name = "floor"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (value,) = args
        value = float(value if value and value != "None" else 0)
        return str(floor(value))


class BuiltinRound(BuiltinFormatterFunction):
    name = "round"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (value,) = args
        value = float(value if value and value != "None" else 0)
        return str(round(value))


class BuiltinMod(BuiltinFormatterFunction):
    name = "mod"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        value, y = args
        value = float(value if value and value != "None" else 0)
        y = float(y if y and y != "None" else 0)
        return str(int(value % y))


class BuiltinFractionalPart(BuiltinFormatterFunction):
    name = "fractional_part"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (value,) = args
        value = float(value if value and value != "None" else 0)
        return str(modf(value)[0])


class BuiltinTemplate(BuiltinFormatterFunction):
    name = "template"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (template,) = args
        template = template.replace("[[", "{").replace("]]", "}")
        new_formatter = formatter.__class__()
        new_formatter.allow_python_templates = formatter.allow_python_templates
        return new_formatter.safe_format(template, kwargs, "TEMPLATE", mi)


class BuiltinEval(BuiltinFormatterFunction):
    name = "eval"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (template,) = args
        from .upstream_formatter import EvalFormatter

        template = template.replace("[[", "{").replace("]]", "}")
        new_formatter = EvalFormatter()
        new_formatter.allow_python_templates = formatter.allow_python_templates
        return new_formatter.safe_format(template, locals, "EVAL", None)


class BuiltinAssign(BuiltinFormatterFunction):
    name = "assign"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        target, value = args
        locals[target] = value
        return value


class BuiltinListSplit(BuiltinFormatterFunction):
    name = "list_split"
    arg_count = 3

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        list_val, sep, id_prefix = args
        l = [v.strip() for v in list_val.split(sep)]
        res = ""
        for i, v in enumerate(l):
            res = locals[id_prefix + "_" + str(i)] = v
        return res


class BuiltinPrint(BuiltinFormatterFunction):
    name = "print"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        print(args)
        return ""


class BuiltinField(BuiltinFormatterFunction):
    name = "field"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (name,) = args
        return formatter.get_value(name, [], kwargs)


class BuiltinRawField(BuiltinFormatterFunction):
    name = "raw_field"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        name = args[0]
        default = args[1] if len(args) > 1 else None
        res = getattr(mi, name, None)
        if res is None and default is not None:
            return default
        if isinstance(res, list):
            fm = mi.metadata_for_field(name)
            if fm is None:
                return ", ".join(res)
            return fm["is_multiple"]["list_to_ui"].join(res)
        return str(res)


class BuiltinRawList(BuiltinFormatterFunction):
    name = "raw_list"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        name, separator = args
        res = getattr(mi, name, None)
        if not isinstance(res, list):
            return f"{name} is not a list"
        return separator.join(res)


class BuiltinSubstr(BuiltinFormatterFunction):
    name = "substr"
    arg_count = 3

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        value, start_, end_ = args
        return value[int(start_) : len(value) if int(end_) == 0 else int(end_)]


class BuiltinLookup(BuiltinFormatterFunction):
    name = "lookup"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        val, *args = all_args
        if len(args) == 2:  # here for backwards compatibility
            if val:
                return formatter.vformat("{" + args[0].strip() + "}", [], kwargs)  # ty: ignore[no-matching-overload]
            else:
                return formatter.vformat("{" + args[1].strip() + "}", [], kwargs)  # ty: ignore[no-matching-overload]
        if (len(args) % 2) != 1:
            raise ValueError(
                _("lookup requires either 2 or an odd number of arguments")
            )
        i = 0
        while i < len(args):
            if i + 1 >= len(args):
                return formatter.vformat("{" + args[i].strip() + "}", [], kwargs)  # ty: ignore[no-matching-overload]
            if re.search(args[i], val, flags=re.I):
                return formatter.vformat("{" + args[i + 1].strip() + "}", [], kwargs)  # ty: ignore[no-matching-overload]
            i += 2


class BuiltinTest(BuiltinFormatterFunction):
    name = "test"
    arg_count = 3

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, value_if_set, value_not_set = args
        if val:
            return value_if_set
        else:
            return value_not_set


class BuiltinContains(BuiltinFormatterFunction):
    name = "contains"
    arg_count = 4

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, test, value_if_present, value_if_not = args
        if re.search(test, val, flags=re.I):
            return value_if_present
        else:
            return value_if_not


class BuiltinSwitch(BuiltinFormatterFunction):
    name = "switch"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        val, *args = all_args
        if (len(args) % 2) != 1:
            raise ValueError(_("switch requires an even number of arguments"))
        i = 0
        while i < len(args):
            if i + 1 >= len(args):
                return args[i]
            if re.search(args[i], val, flags=re.I):
                return args[i + 1]
            i += 2


class BuiltinSwitchIf(BuiltinFormatterFunction):
    name = "switch_if"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        if (len(args) % 2) != 1:
            raise ValueError(_("switch_if requires an odd number of arguments"))
        # We shouldn't get here because the function is inlined. However, someone
        # might call it directly.
        i = 0
        while i < len(args):
            if i + 1 >= len(args):
                return args[i]
            if args[i]:
                return args[i + 1]
            i += 2


class BuiltinStrcatMax(BuiltinFormatterFunction):
    name = "strcat_max"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        if len(args) < 2:
            raise ValueError(_("strcat_max requires 2 or more arguments"))
        if (len(args) % 2) != 0:
            raise ValueError(_("strcat_max requires an even number of arguments"))
        try:
            max = int(args[0])
        except Exception:
            raise ValueError(_("first argument to strcat_max must be an integer"))

        i = 2
        result = args[1]
        try:
            while i < len(args):
                if (len(result) + len(args[i]) + len(args[i + 1])) > max:
                    break
                result = result + args[i] + args[i + 1]
                i += 2
        except Exception:
            pass
        return result.strip()


class BuiltinInList(BuiltinFormatterFunction):
    name = "list_contains"
    arg_count = -1
    aliases = ["in_list"]

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        val, sep, *args = all_args
        if (len(args) % 2) != 1:
            raise ValueError(_("in_list requires an odd number of arguments"))
        l = [v.strip() for v in val.split(sep) if v.strip()]
        i = 0
        while i < len(args):
            if i + 1 >= len(args):
                return args[i]
            sf = args[i]
            fv = args[i + 1]
            if l:
                for v in l:
                    if re.search(sf, v, flags=re.I):
                        return fv
            i += 2


class BuiltinStrInList(BuiltinFormatterFunction):
    name = "str_in_list"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        val, sep, *args = all_args
        if (len(args) % 2) != 1:
            raise ValueError(_("str_in_list requires an odd number of arguments"))
        l = [v.strip() for v in val.split(sep) if v.strip()]
        i = 0
        while i < len(args):
            if i + 1 >= len(args):
                return args[i]
            sf = args[i]
            fv = args[i + 1]
            c = [v.strip() for v in sf.split(sep) if v.strip()]
            if l:
                for v in l:
                    for t in c:
                        if strcmp(t, v) == 0:
                            return fv
            i += 2


class BuiltinIdentifierInList(BuiltinFormatterFunction):
    name = "identifier_in_list"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        val, ident, *args = all_args
        if len(args) == 0:
            fv_is_id = True
            nfv = ""
        elif len(args) == 2:
            fv_is_id = False
            fv = args[0]
            nfv = args[1]
        else:
            raise ValueError(_("{} requires 2 or 4 arguments").format(self.name))

        l = [v.strip() for v in val.split(",") if v.strip()]
        id_, __, regexp = ident.partition(":")
        if not id_:
            return nfv
        for candidate in l:
            i, __, v = candidate.partition(":")
            if v and i == id_:
                if not regexp or re.search(regexp, v, flags=re.I):
                    return candidate if fv_is_id else fv
        return nfv


class BuiltinRe(BuiltinFormatterFunction):
    name = "re"
    arg_count = 3

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, pattern, replacement = args
        return re.sub(pattern, replacement, val, flags=re.I)


class BuiltinReGroup(BuiltinFormatterFunction):
    name = "re_group"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        val, pattern, *args = all_args
        from .upstream_formatter import EvalFormatter

        allow_python = formatter.allow_python_templates

        def repl(mo: Match[str] | None) -> str:
            res = ""
            if mo and mo.lastindex:
                for dex in range(mo.lastindex):
                    gv = mo.group(dex + 1)
                    if gv is None:
                        continue
                    if len(args) > dex:
                        template = args[dex].replace("[[", "{").replace("]]", "}")
                        new_formatter = EvalFormatter()
                        new_formatter.allow_python_templates = allow_python
                        res += new_formatter.safe_format(
                            template, {"$": gv}, "EVAL", None, strip_results=False
                        )
                    else:
                        res += gv
            return res

        return re.sub(pattern, repl, val, flags=re.I)


class BuiltinSwapAroundComma(BuiltinFormatterFunction):
    name = "swap_around_comma"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (val,) = args
        return re.sub(r"^(.*?),\s*(.*$)", r"\2 \1", val, flags=re.I).strip()


class BuiltinIfempty(BuiltinFormatterFunction):
    name = "ifempty"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, value_if_empty = args
        if val:
            return val
        else:
            return value_if_empty


class BuiltinShorten(BuiltinFormatterFunction):
    name = "shorten"
    arg_count = 4

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, leading, center_string, trailing = args
        l = max(0, int(leading))
        t = max(0, int(trailing))
        if len(val) > l + len(center_string) + t:
            return val[0:l] + center_string + ("" if t == 0 else val[-t:])
        else:
            return val


class BuiltinCount(BuiltinFormatterFunction):
    name = "list_count"
    arg_count = 2
    aliases = ["count"]

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, sep = args
        return str(len([v for v in val.split(sep) if v]))


class BuiltinListCountMatching(BuiltinFormatterFunction):
    name = "list_count_matching"
    arg_count = 3
    aliases = ["count_matching"]

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        value, pattern, sep = args
        res = 0
        for v in [x.strip() for x in value.split(sep) if x.strip()]:
            if re.search(pattern, v, flags=re.I):
                res += 1
        return str(res)


class BuiltinListitem(BuiltinFormatterFunction):
    name = "list_item"
    arg_count = 3

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, index, sep = args
        if not val:
            return ""
        index = int(index)
        val = val.split(sep)
        try:
            return val[index].strip()
        except Exception:
            return ""


class BuiltinSelect(BuiltinFormatterFunction):
    name = "select"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, key = args
        if not val:
            return ""
        vals = [v.strip() for v in val.split(",")]
        tkey = key + ":"
        for v in vals:
            if v.startswith(tkey):
                return v[len(tkey) :]
        return ""


class BuiltinApproximateFormats(BuiltinFormatterFunction):
    name = "approximate_formats"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        if hasattr(mi, "_proxy_metadata"):
            fmt_data = mi._proxy_metadata.db_approx_formats
            if not fmt_data:
                return ""
            data = sorted(fmt_data)
            return ",".join(v.upper() for v in data)
        self.only_in_gui_error()


class BuiltinFormatsModtimes(BuiltinFormatterFunction):
    name = "formats_modtimes"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (fmt,) = args
        fmt_data = mi.get("format_metadata", {})
        try:
            # a dict or db.lazy.FormatMetadata, other values fail as before
            assert isinstance(fmt_data, Mapping)
            data = sorted(fmt_data.items(), key=lambda x: x[1]["mtime"], reverse=True)
            return ",".join(
                k.upper() + ":" + format_date(v["mtime"], fmt) for k, v in data
            )
        except Exception:
            return ""


class BuiltinFormatsSizes(BuiltinFormatterFunction):
    name = "formats_sizes"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        fmt_data = mi.get("format_metadata", {})
        try:
            # a dict or db.lazy.FormatMetadata, other values fail as before
            assert isinstance(fmt_data, Mapping)
            return ",".join(
                k.upper() + ":" + str(v["size"]) for k, v in fmt_data.items()
            )
        except Exception:
            return ""


class BuiltinFormatsPaths(BuiltinFormatterFunction):
    name = "formats_paths"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        sep = args[0] if args else ","
        fmt_data = mi.get("format_metadata", {})
        try:
            # a dict or db.lazy.FormatMetadata, other values fail as before
            assert isinstance(fmt_data, Mapping)
            return sep.join(
                k.upper() + ":" + str(v["path"]) for k, v in fmt_data.items()
            )
        except Exception:
            return ""


class BuiltinFormatsPathSegments(BuiltinFormatterFunction):
    name = "formats_path_segments"
    arg_count = 5

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        with_author, with_title, with_format, with_ext, sep = args
        fmt_metadata = mi.get("format_metadata", {})
        if fmt_metadata:
            assert isinstance(fmt_metadata, Mapping)  # a dict or db.lazy.FormatMetadata
            for v in fmt_metadata.values():
                p = v["path"]
                r, fmt = os.path.split(p)
                if with_ext == "0" or not with_ext:
                    fmt = os.path.splitext(fmt)[0]
                r, title = os.path.split(r)
                r, author = os.path.split(r)
                parts = []
                if with_author == "1":
                    parts.append(author)
                if with_title == "1":
                    parts.append(title)
                if with_format == "1":
                    parts.append(fmt)
                return sep.join(parts)
        else:
            return _("No book formats found so the path can't be generated")


class BuiltinHumanReadable(BuiltinFormatterFunction):
    name = "human_readable"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (val,) = args
        try:
            return human_readable(round(float(val)))
        except Exception:
            return ""


class BuiltinFormatNumber(BuiltinFormatterFunction):
    name = "format_number"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, template = args
        if val in {"", "None"}:
            return ""
        if "{" not in template:
            template = "{0:" + template + "}"
        try:
            v1 = float(val)
        except Exception:
            return ""
        try:  # Try formatting the value as a float
            return template.format(v1)
        except Exception:
            pass
        try:  # Try formatting the value as an int
            v2 = trunc(v1)
            if v2 == v1:
                return template.format(v2)
        except Exception:
            pass
        return ""


class BuiltinSublist(BuiltinFormatterFunction):
    name = "sublist"
    arg_count = 4

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, start_index, end_index, sep = args
        if not val:
            return ""
        si = int(start_index)
        ei = int(end_index)
        # allow empty list items so counts are what the user expects
        val = [v.strip() for v in val.split(sep)]

        if sep == ",":
            sep = ", "
        try:
            if ei == 0:
                return sep.join(val[si:])
            else:
                return sep.join(val[si:ei])
        except Exception:
            return ""


class BuiltinSubitems(BuiltinFormatterFunction):
    name = "subitems"
    arg_count = 3
    period_pattern = re.compile(r"(?<=[^\.\s])\.(?=[^\.\s])", re.U)

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, start_index, end_index = args
        if not val:
            return ""
        si = int(start_index)
        ei = int(end_index)
        has_periods = "." in val
        items = [v.strip() for v in val.split(",") if v.strip()]
        rv = set()
        for item in items:
            if has_periods and "." in item:
                components = self.period_pattern.split(item)
            else:
                components = [item]
            try:
                if ei == 0:
                    t = ".".join(components[si:]).strip()
                else:
                    t = ".".join(components[si:ei]).strip()
                if t:
                    rv.add(t)
            except Exception:
                pass
        return ", ".join(sorted(rv, key=sort_key))


class BuiltinFormatDate(BuiltinFormatterFunction):
    name = "format_date"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, format_string = args
        if not val or val == "None":
            return ""
        try:
            if format_string == "to_number":
                s = parse_date(val).timestamp()
            elif format_string.startswith("from_number"):
                val = datetime.fromtimestamp(float(val))  # ruff: ignore[call-datetime-fromtimestamp]
                f = format_string[12:]
                s = format_date(val, f or "iso")
            else:
                s = format_date(parse_date(val), format_string)
            return s
        except Exception:
            s = "BAD DATE"
        return s


class BuiltinFormatDateField(BuiltinFormatterFunction):
    name = "format_date_field"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        field, format_string = args
        try:
            # grouped search terms map to lists, which are rejected as unknown fields below
            field = cast("str", field_metadata.search_term_to_field_key(field))
            if field not in mi.all_field_keys():
                raise ValueError(
                    _("Function {0}: Unknown field '{1}'").format(
                        "format_date_field", field
                    )
                )
            val = mi.get(field, None)
            if mi.metadata_for_field(field)["datatype"] != "datetime":  # ty: ignore[not-subscriptable] None for non-field keys raises TypeError, handled below
                raise ValueError(
                    _("Function {0}: field '{1}' is not a date").format(
                        "format_date_field", field
                    )
                )
            if val is None:
                s = ""
            elif format_string == "to_number":
                assert isinstance(val, datetime)  # the value of a datetime field
                s = val.timestamp()
            elif format_string.startswith("from_number"):
                # The value of a datetime field is already a date, so there is
                # no number to convert, just apply the format
                assert isinstance(val, datetime)  # the value of a datetime field
                f = format_string[12:]
                s = format_date(val, f or "iso")
            else:
                assert isinstance(val, datetime)  # the value of a datetime field
                s = format_date(val, format_string)
            return s
        except ValueError:
            raise
        except Exception:
            traceback.print_exc()
            raise
        return s


class BuiltinUppercase(BuiltinFormatterFunction):
    name = "uppercase"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (val,) = args
        return val.upper()


class BuiltinLowercase(BuiltinFormatterFunction):
    name = "lowercase"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (val,) = args
        return val.lower()


class BuiltinTitlecase(BuiltinFormatterFunction):
    name = "titlecase"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (val,) = args
        return titlecase(val)


class BuiltinCapitalize(BuiltinFormatterFunction):
    name = "capitalize"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (val,) = args
        return capitalize(val)


class BuiltinBooksize(BuiltinFormatterFunction):
    name = "booksize"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        if hasattr(mi, "_proxy_metadata"):
            try:
                v = mi._proxy_metadata.book_size
                if v is not None:
                    return str(mi._proxy_metadata.book_size)
                return ""
            except Exception:
                pass
            return ""
        self.only_in_gui_error()


class BuiltinOndevice(BuiltinFormatterFunction):
    name = "ondevice"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        if hasattr(mi, "_proxy_metadata"):
            if mi._proxy_metadata.ondevice_col:
                return _("Yes")
            return ""
        self.only_in_gui_error()


class BuiltinAnnotationCount(BuiltinFormatterFunction):
    name = "annotation_count"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        c = self.get_database(
            mi, formatter=formatter
        ).new_api.annotation_count_for_book(mi.id)
        return "" if c == 0 else str(c)


class BuiltinIsMarked(BuiltinFormatterFunction):
    name = "is_marked"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        c = self.get_database(mi, formatter=formatter).data.get_marked(mi.id)
        return c or ""


class BuiltinSeriesSort(BuiltinFormatterFunction):
    name = "series_sort"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        if mi.series:
            langs = mi.languages
            lang = langs[0] if langs else None
            return title_sort(mi.series, lang=lang)
        return ""


class BuiltinHasCover(BuiltinFormatterFunction):
    name = "has_cover"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        if mi.has_cover:
            return _("Yes")
        return ""


class BuiltinFirstNonEmpty(BuiltinFormatterFunction):
    name = "first_non_empty"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        i = 0
        while i < len(args):
            if args[i]:
                return args[i]
            i += 1
        return ""


class BuiltinAnd(BuiltinFormatterFunction):
    name = "and"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        i = 0
        while i < len(args):
            if not args[i]:
                return ""
            i += 1
        return "1"


class BuiltinOr(BuiltinFormatterFunction):
    name = "or"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        i = 0
        while i < len(args):
            if args[i]:
                return "1"
            i += 1
        return ""


class BuiltinNot(BuiltinFormatterFunction):
    name = "not"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (val,) = args
        return "" if val else "1"


class BuiltinListJoin(BuiltinFormatterFunction):
    name = "list_join"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        with_separator, *args = all_args
        if len(args) % 2 != 0:
            raise ValueError(
                _(
                    "Invalid 'List, separator' pairs. Every list must have one associated separator"
                )
            )

        # Starting in python 3.7 dicts preserve order so we don't need OrderedDict
        result = {}
        i = 0
        while i < len(args):
            lst = [v.strip() for v in args[i].split(args[i + 1]) if v.strip()]
            result.update({item.lower(): item for item in lst})
            i += 2
        return with_separator.join(result.values())


class BuiltinListUnion(BuiltinFormatterFunction):
    name = "list_union"
    arg_count = 3
    aliases = ["merge_lists"]

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        list1, list2, separator = args
        res = {
            icu_lower(l.strip()): l.strip() for l in list2.split(separator) if l.strip()
        }
        res.update(
            {
                icu_lower(l.strip()): l.strip()
                for l in list1.split(separator)
                if l.strip()
            }
        )
        if separator == ",":
            separator = ", "
        return separator.join(res.values())


class BuiltinRange(BuiltinFormatterFunction):
    name = "range"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        limit_val = 1000
        start_val = 0
        step_val = 1
        if len(args) == 1:
            stop_val = int(args[0] if args[0] and args[0] != "None" else 0)
        elif len(args) == 2:
            start_val = int(args[0] if args[0] and args[0] != "None" else 0)
            stop_val = int(args[1] if args[1] and args[1] != "None" else 0)
        elif len(args) >= 3:
            start_val = int(args[0] if args[0] and args[0] != "None" else 0)
            stop_val = int(args[1] if args[1] and args[1] != "None" else 0)
            step_val = int(args[2] if args[2] and args[2] != "None" else 0)
            if len(args) > 3:
                limit_val = int(args[3] if args[3] and args[3] != "None" else 0)
        r = range(start_val, stop_val, step_val)
        if len(r) > limit_val:
            raise ValueError(
                _("{0}: length ({1}) longer than limit ({2})").format(
                    "range", len(r), str(limit_val)
                )
            )
        return ", ".join([str(v) for v in r])


class BuiltinListRemoveDuplicates(BuiltinFormatterFunction):
    name = "list_remove_duplicates"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        list_, separator = args
        res = {
            icu_lower(l.strip()): l.strip() for l in list_.split(separator) if l.strip()
        }
        if separator == ",":
            separator = ", "
        return separator.join(res.values())


class BuiltinListDifference(BuiltinFormatterFunction):
    name = "list_difference"
    arg_count = 3

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        list1, list2, separator = args
        l1 = [l.strip() for l in list1.split(separator) if l.strip()]
        l2 = {icu_lower(l.strip()) for l in list2.split(separator) if l.strip()}

        res = []
        for i in l1:
            if icu_lower(i) not in l2 and i not in res:
                res.append(i)
        if separator == ",":
            return ", ".join(res)
        return separator.join(res)


class BuiltinListIntersection(BuiltinFormatterFunction):
    name = "list_intersection"
    arg_count = 3

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        list1, list2, separator = args
        l1 = [l.strip() for l in list1.split(separator) if l.strip()]
        l2 = {icu_lower(l.strip()) for l in list2.split(separator) if l.strip()}

        res = []
        for i in l1:
            if icu_lower(i) in l2 and i not in res:
                res.append(i)
        if separator == ",":
            return ", ".join(res)
        return separator.join(res)


class BuiltinListSort(BuiltinFormatterFunction):
    name = "list_sort"
    arg_count = 3

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        value, direction, separator = args
        res = [l.strip() for l in value.split(separator) if l.strip()]
        if separator == ",":
            return ", ".join(sorted(res, key=sort_key, reverse=direction != "0"))
        return separator.join(sorted(res, key=sort_key, reverse=direction != "0"))


class BuiltinListEquals(BuiltinFormatterFunction):
    name = "list_equals"
    arg_count = 6

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        list1, sep1, list2, sep2, yes_val, no_val = args
        s1 = {icu_lower(l.strip()) for l in list1.split(sep1) if l.strip()}
        s2 = {icu_lower(l.strip()) for l in list2.split(sep2) if l.strip()}
        if s1 == s2:
            return yes_val
        return no_val


class BuiltinListRe(BuiltinFormatterFunction):
    name = "list_re"
    arg_count = 4

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        src_list, separator, include_re, opt_replace = args
        l = [l.strip() for l in src_list.split(separator) if l.strip()]
        res = []
        for item in l:
            if re.search(include_re, item, flags=re.I) is not None:
                if opt_replace:
                    item = re.sub(include_re, opt_replace, item)
                for i in [t.strip() for t in item.split(separator) if t.strip()]:
                    if i not in res:
                        res.append(i)
        if separator == ",":
            return ", ".join(res)
        return separator.join(res)


class BuiltinListReGroup(BuiltinFormatterFunction):
    name = "list_re_group"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        src_list, separator, include_re, search_re, *args = all_args
        from .upstream_formatter import EvalFormatter

        allow_python = formatter.allow_python_templates
        l = [l.strip() for l in src_list.split(separator) if l.strip()]
        res = []
        for item in l:

            def repl(mo: Match[str] | None) -> str:
                newval = ""
                if mo and mo.lastindex:
                    for dex in range(mo.lastindex):
                        gv = mo.group(dex + 1)
                        if gv is None:
                            continue
                        if len(args) > dex:
                            template = args[dex].replace("[[", "{").replace("]]", "}")
                            new_formatter = EvalFormatter()
                            new_formatter.allow_python_templates = allow_python
                            newval += new_formatter.safe_format(
                                template, {"$": gv}, "EVAL", None, strip_results=False
                            )
                        else:
                            newval += gv
                return newval

            if re.search(include_re, item, flags=re.I) is not None:
                item = re.sub(search_re, repl, item, flags=re.I)
                for i in [t.strip() for t in item.split(separator) if t.strip()]:
                    if i not in res:
                        res.append(i)
        if separator == ",":
            return ", ".join(res)
        return separator.join(res)


class BuiltinToday(BuiltinFormatterFunction):
    name = "today"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        return format_date(now(), "iso")


class BuiltinDaysBetween(BuiltinFormatterFunction):
    name = "days_between"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        date1, date2 = args
        try:
            d1 = parse_date(date1)
            if d1 == UNDEFINED_DATE:
                return ""
            d2 = parse_date(date2)
            if d2 == UNDEFINED_DATE:
                return ""
        except Exception:
            return ""
        i = d1 - d2
        return f"{i.days + (i.seconds / (24.0 * 60.0 * 60.0)):.1f}"


class BuiltinDateArithmetic(BuiltinFormatterFunction):
    name = "date_arithmetic"
    arg_count = -1
    calc_ops = {
        "s": lambda v: timedelta(seconds=v),
        "m": lambda v: timedelta(minutes=v),
        "h": lambda v: timedelta(hours=v),
        "d": lambda v: timedelta(days=v),
        "w": lambda v: timedelta(weeks=v),
        "y": lambda v: timedelta(days=v * 365),
    }

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        value, calc_spec = args[0], args[1]
        fmt = args[2] if len(args) > 2 else None
        try:
            d = parse_date(value)
            if d == UNDEFINED_DATE:
                return ""
            while calc_spec:
                mo = re.match(r"([-+\d]+)([smhdwy])", calc_spec)
                if mo is None:
                    raise ValueError(
                        _("{0}: invalid calculation specifier '{1}'").format(
                            "date_arithmetic", calc_spec
                        )
                    )
                d += self.calc_ops[mo[2]](int(mo[1]))
                calc_spec = calc_spec[len(mo[0]) :]
            return format_date(d, fmt or "iso")
        except ValueError:
            raise
        except Exception as e:
            traceback.print_exc()
            raise ValueError(_("{0}: error: {1}").format("date_arithmetic", str(e)))


class BuiltinLanguageStrings(BuiltinFormatterFunction):
    name = "language_strings"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        lang_codes, localize = args
        retval = []
        for c in [c.strip() for c in lang_codes.split(",") if c.strip()]:
            try:
                n = calibre_langcode_to_name(c, localize != "0")
                if n:
                    retval.append(n)
            except Exception:
                pass
        return ", ".join(retval)


class BuiltinLanguageCodes(BuiltinFormatterFunction):
    name = "language_codes"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (lang_strings,) = args
        retval = []
        for c in [c.strip() for c in lang_strings.split(",") if c.strip()]:
            try:
                cv = canonicalize_lang(c)
                if cv:
                    retval.append(canonicalize_lang(cv))
            except Exception:
                pass
        return ", ".join(retval)  # ty: ignore[no-matching-overload]


class BuiltinCurrentLibraryName(BuiltinFormatterFunction):
    name = "current_library_name"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        return self.only_in_gui_error()


class BuiltinCurrentLibraryPath(BuiltinFormatterFunction):
    name = "current_library_path"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        return self.only_in_gui_error()


class BuiltinFinishFormatting(BuiltinFormatterFunction):
    name = "finish_formatting"
    arg_count = 4

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, fmt, prefix, suffix = args
        if not val:
            return val
        return prefix + formatter._do_format(val, fmt) + suffix


class BuiltinVirtualLibraries(BuiltinFormatterFunction):
    name = "virtual_libraries"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        db = self.get_database(mi, formatter=formatter)
        try:
            a = db.data.get_virtual_libraries_for_books((mi.id,))
            return ", ".join(a[mi.id])
        except ValueError as v:
            return str(v)


class BuiltinCurrentVirtualLibraryName(BuiltinFormatterFunction):
    name = "current_virtual_library_name"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        return self.get_database(
            mi, formatter=formatter
        ).data.get_base_restriction_name()


class BuiltinUserCategories(BuiltinFormatterFunction):
    name = "user_categories"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        if hasattr(mi, "_proxy_metadata"):
            cats = {k for k, v in mi._proxy_metadata.user_categories.items() if v}
            cats = sorted(cats, key=sort_key)
            return ", ".join(cats)
        self.only_in_gui_error()


class BuiltinTransliterate(BuiltinFormatterFunction):
    name = "transliterate"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (source,) = args
        from LiuXin_alpha.utils.storage.local.filenames import ascii_text

        return ascii_text(source)


class BuiltinGetLink(BuiltinFormatterFunction):
    name = "get_link"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        field_name, field_value = args
        db = self.get_database(mi, formatter=formatter).new_api
        try:
            link = None
            item_id = db.get_item_id(field_name, field_value, case_sensitive=True)
            if item_id is not None:
                link = db.link_for(field_name, item_id)
            return link if link is not None else ""
        except Exception as e:
            traceback.print_exc()
            raise ValueError(e)


class BuiltinAuthorLinks(BuiltinFormatterFunction):
    name = "author_links"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val_sep, pair_sep = args
        if hasattr(mi, "_proxy_metadata"):
            link_data = mi._proxy_metadata.link_maps
            if not link_data:
                return ""
            link_data = link_data.get("authors")
            if not link_data:
                return ""
            names = sorted(link_data.keys(), key=sort_key)
            return pair_sep.join(n + val_sep + link_data[n] for n in names)
        self.only_in_gui_error()


class BuiltinAuthorSorts(BuiltinFormatterFunction):
    name = "author_sorts"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (val_sep,) = args
        sort_data = mi.author_sort_map
        if not sort_data:
            return ""
        from LiuXin_alpha.metadata.author_names import author_to_author_sort

        names = [
            sort_data.get(n) or author_to_author_sort(n)
            for n in mi.authors
            if n.strip()
        ]
        return val_sep.join(names)


class BuiltinConnectedDeviceName(BuiltinFormatterFunction):
    name = "connected_device_name"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        return self.only_in_gui_error()


class BuiltinConnectedDeviceUUID(BuiltinFormatterFunction):
    name = "connected_device_uuid"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        return self.only_in_gui_error()


class BuiltinCheckYesNo(BuiltinFormatterFunction):
    name = "check_yes_no"
    arg_count = 4

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        field, is_undefined, is_false, is_true = args
        res = getattr(mi, field, None)
        # Missing fields will return None. Oh well, this lets it be used everywhere,
        # not just in the GUI.
        if res is None:
            if is_undefined == "1":
                return "Yes"
            return ""
        if not isinstance(res, bool):
            raise ValueError(
                _("check_yes_no requires the field be a Yes/No custom column")
            )  # ruff: ignore[type-check-without-type-error]
        if is_false == "1" and not res:
            return "Yes"
        if is_true == "1" and res:
            return "Yes"
        return ""


class BuiltinRatingToStars(BuiltinFormatterFunction):
    name = "rating_to_stars"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        value, use_half_stars = args
        if not value:
            return ""
        err_msg = _("The rating must be a number between 0 and 5")
        try:
            v = float(value) * 2
        except Exception:
            raise ValueError(err_msg)
        if v < 0 or v > 10:
            raise ValueError(err_msg)
        return "★" * int(v // 2) + ("⯨" if use_half_stars == "1" and int(v) % 2 else "")


class BuiltinSwapAroundArticles(BuiltinFormatterFunction):
    name = "swap_around_articles"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        val, separator = args
        if not val:
            return ""
        if not separator:
            return title_sort(val).replace(",", ";")
        result = []
        try:
            for v in [x.strip() for x in val.split(separator)]:
                result.append(title_sort(v).replace(",", ";"))
        except Exception:
            traceback.print_exc()
        return separator.join(sorted(result, key=sort_key))


class BuiltinArguments(BuiltinFormatterFunction):
    name = "arguments"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        # The arguments function is implemented in-line in the formatter
        raise NotImplementedError()


class BuiltinGlobals(BuiltinFormatterFunction):
    name = "globals"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        # The globals function is implemented in-line in the formatter
        raise NotImplementedError()


class BuiltinSetGlobals(BuiltinFormatterFunction):
    name = "set_globals"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        # The globals function is implemented in-line in the formatter
        raise NotImplementedError()


class BuiltinFieldExists(BuiltinFormatterFunction):
    name = "field_exists"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (field_name,) = args
        if field_name.lower() in mi.all_field_keys():
            return "1"
        return ""


class BuiltinCharacter(BuiltinFormatterFunction):
    name = "character"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (character_name,) = args
        # The globals function is implemented in-line in the formatter
        raise NotImplementedError()


class BuiltinToHex(BuiltinFormatterFunction):
    name = "to_hex"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (val,) = args
        return val.encode().hex()


class BuiltinUrlsFromIdentifiers(BuiltinFormatterFunction):
    name = "urls_from_identifiers"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        return self.only_in_gui_error()


class BuiltinBookCount(BuiltinFormatterFunction):
    name = "book_count"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        query, use_vl = args
        rendering_composite_name = "__rendering_composite__"

        if not tweaks.get(
            "allow_template_database_functions_in_composites", False
        ) and formatter.global_vars.get(rendering_composite_name, None):
            raise ValueError(
                _("The book_count() function cannot be used in a composite column")
            )
        db = self.get_database(mi, formatter=formatter)
        try:
            if use_vl == "0":
                # use the new_api search that doesn't use virtual libraries to let
                # the function work in content server icon rules.
                ids = db.new_api.search(query, None)
            else:
                ids = db.search_getting_ids(query, None, use_virtual_library=True)  # ty: ignore[missing-attribute]
            return str(len(ids))
        except Exception:
            traceback.print_exc()


class BuiltinBookValues(BuiltinFormatterFunction):
    name = "book_values"
    arg_count = 4

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        column, query, sep, use_vl = args
        rendering_composite_name = "__rendering_composite__"

        if not tweaks.get(
            "allow_template_database_functions_in_composites", False
        ) and formatter.global_vars.get(rendering_composite_name, None):
            raise ValueError(
                _("The book_values() function cannot be used in a composite column")
            )
        db = self.get_database(mi, formatter=formatter)
        if column not in db.field_metadata:
            raise ValueError(_("The column {} doesn't exist").format(column))
        try:
            if use_vl == "0":
                ids = db.new_api.search(query, None)
            else:
                ids = db.search_getting_ids(query, None, use_virtual_library=True)  # ty: ignore[missing-attribute]
            s = set()
            for id_ in ids:
                f = db.new_api.get_proxy_metadata(id_).get(column, None)
                if isinstance(f, (tuple, list)):
                    s.update(f)
                elif f is not None:
                    s.add(str(f))
            return sep.join(s)
        except Exception as e:
            raise ValueError(e)


class BuiltinHasExtraFiles(BuiltinFormatterFunction):
    name = "has_extra_files"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        if len(args) > 1:
            raise ValueError(
                _("Incorrect number of arguments for function {0}").format(
                    "has_extra_files"
                )
            )
        pattern = args[0] if len(args) == 1 else None
        db = self.get_database(mi, formatter=formatter).new_api
        try:
            files = tuple(
                f.relpath.partition("/")[-1]
                for f in db.list_extra_files(
                    mi.id, use_cache=True, pattern=DATA_FILE_PATTERN
                )
            )
            if pattern:
                r = re.compile(pattern, re.IGNORECASE)
                files = tuple(filter(r.search, files))
            return len(files) if len(files) > 0 else ""
        except Exception as e:
            traceback.print_exc()
            raise ValueError(e)


class BuiltinExtraFileNames(BuiltinFormatterFunction):
    name = "extra_file_names"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        sep, *args = all_args
        if len(args) > 1:
            raise ValueError(
                _("Incorrect number of arguments for function {0}").format(
                    "has_extra_files"
                )
            )
        pattern = args[0] if len(args) == 1 else None
        db = self.get_database(mi, formatter=formatter).new_api
        try:
            files = tuple(
                f.relpath.partition("/")[-1]
                for f in db.list_extra_files(
                    mi.id, use_cache=True, pattern=DATA_FILE_PATTERN
                )
            )
            if pattern:
                r = re.compile(pattern, re.IGNORECASE)
                files = tuple(filter(r.search, files))
            return sep.join(files)
        except Exception as e:
            traceback.print_exc()
            raise ValueError(e)


class BuiltinExtraFileSize(BuiltinFormatterFunction):
    name = "extra_file_size"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (file_name,) = args
        db = self.get_database(mi, formatter=formatter).new_api
        try:
            q = posixpath.join(DATA_DIR_NAME, file_name)
            for f in db.list_extra_files(
                mi.id, use_cache=True, pattern=DATA_FILE_PATTERN
            ):
                if f.relpath == q:
                    return str(f.stat_result.st_size)
            return str(-1)
        except Exception as e:
            traceback.print_exc()
            raise ValueError(e)


class BuiltinExtraFileModtime(BuiltinFormatterFunction):
    name = "extra_file_modtime"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        file_name, format_string = args
        db = self.get_database(mi, formatter=formatter).new_api
        try:
            q = posixpath.join(DATA_DIR_NAME, file_name)
            for f in db.list_extra_files(
                mi.id, use_cache=True, pattern=DATA_FILE_PATTERN
            ):
                if f.relpath == q:
                    val = f.stat_result.st_mtime
                    if format_string:
                        return format_date(datetime.fromtimestamp(val), format_string)  # ruff: ignore[call-datetime-fromtimestamp]
                    return str(val)
            return str(1.0)
        except Exception as e:
            traceback.print_exc()
            raise ValueError(e)


class BuiltinGetNote(BuiltinFormatterFunction):
    name = "get_note"
    arg_count = 3

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        return self.only_in_gui_error()


class BuiltinHasNote(BuiltinFormatterFunction):
    name = "has_note"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        field_name, field_value = args
        db = self.get_database(mi, formatter=formatter).new_api
        if field_value:
            note = None
            try:
                item_id = db.get_item_id(field_name, field_value, case_sensitive=True)
                if item_id is not None:
                    note = db.notes_data_for(field_name, item_id)
            except Exception as e:
                traceback.print_exc()
                raise ValueError(str(e))
            return "1" if note is not None else ""
        try:
            notes_for_book = db.items_with_notes_in_book(mi.id)
            values = list(notes_for_book.get(field_name, {}).values())
            return (
                db.field_metadata[field_name]["is_multiple"]
                .get("list_to_ui", ", ")
                .join(values)
            )
        except Exception as e:
            traceback.print_exc()
            raise ValueError(str(e))


class BuiltinReadingProgress(BuiltinFormatterFunction):
    name = "reading_progress"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        book_id, *args = all_args
        if len(args) > 4:
            raise ValueError(
                _("Incorrect number of arguments for function {0}").format(
                    "reading_progress"
                )
            )
        book_id = int(book_id)
        for_user, output_fmt, which, fmt = "", "page_count", "most_recent", ""
        match len(args):
            case 4:
                for_user, output_fmt, which, fmt = args
            case 3:
                for_user, output_fmt, which = args
            case 2:
                for_user, output_fmt = args
            case 1:
                for_user = args[0]
        order_by = "pos_frac" if which in ("furthest", "farthest") else "epoch"
        pos_frac = 0
        with (db := self.get_database(mi, formatter=formatter).new_api).safe_read_lock:
            if records := db._get_last_read_positions(
                book_id, fmt=fmt, user=for_user, order_by=order_by, limit=1
            ):
                pos_frac = records[0]["pos_frac"]
                fmt = records[0]["format"]
            match output_fmt:
                case "percent":
                    return f"{pos_frac:.0%}"
                case "percent_number":
                    return str(round(pos_frac * 100))
                case "page_count":
                    page_count = 0
                    if pages := db._get_pages(book_id):
                        page_count = pages.pages
                    if page_count > 0:
                        return f"{int(pos_frac * page_count)} / {page_count}"
                    return f"{pos_frac:.0%}"
                case _:
                    return str(pos_frac)


class BuiltinIsDarkMode(BuiltinFormatterFunction):
    name = "is_dark_mode"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        return self.only_in_gui_error()


class BuiltinFieldListCount(BuiltinFormatterFunction):
    name = "list_count_field"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        # The globals function is implemented in-line in the formatter
        raise NotImplementedError()


class BuiltinMakeUrl(BuiltinFormatterFunction):
    name = "make_url"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        path, *args = all_args
        if (len(args) % 2) != 0:
            raise ValueError(
                _("{} requires an odd number of arguments").format("make_url")
            )
        if len(args) < 2:
            raise ValueError(_("{} requires at least 3 arguments").format("make_url"))
        query_args = []
        for i in range(0, len(args), 2):
            query_args.append(f"{args[i]}={qquote(args[i + 1].strip())}")
        return f"{path}?{'&'.join(query_args)}"


class BuiltinMakeUrlExtended(BuiltinFormatterFunction):
    name = "make_url_extended"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        scheme, authority, path, *args = all_args
        if len(args) != 1:
            if (len(args) % 2) != 0:
                raise ValueError(
                    _("{} requires an odd number of arguments").format(
                        "make_url_extended"
                    )
                )
            if len(args) < 2:
                raise ValueError(
                    _("{} requires at least 5 arguments").format("make_url_extended")
                )
            query_args = []
            for i in range(0, len(args), 2):
                query_args.append(f"{args[i]}={qquote(args[i + 1].strip())}")
            qs = "&".join(query_args)
        else:
            qs = args[0]
        if qs:
            qs = "?" + qs
        return f"{scheme}://{authority}{'/' if authority else ''}{path.removeprefix('/')}{qs}"


class BuiltinQueryString(BuiltinFormatterFunction):
    name = "query_string"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        if (len(args) % 3) != 0 or len(args) < 3:
            raise ValueError(
                _("{} requires at least one group of 3 arguments").format(
                    "query_string"
                )
            )
        funcs = [
            partial(qquote, use_plus=True),
            partial(qquote, use_plus=False),
            lambda x: x,
        ]
        query_args = []
        for i in range(0, len(args), 3):
            if (f := args[i + 2]) not in ("0", "1", "2"):
                raise ValueError(
                    _(
                        "In {} the third argument of a group must be 0, 1, or 2, not {}"
                    ).format("query_string", f)
                )
            query_args.append(f"{args[i]}={funcs[int(f)](args[i + 1].strip())}")
        return "&".join(query_args)


class BuiltinEncodeForURL(BuiltinFormatterFunction):
    name = "encode_for_url"
    arg_count = 2

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        value, use_plus = args
        if use_plus not in ("0", "1"):
            raise ValueError(
                _("In {} the second argument must be 0, or 1, not {}").format(
                    "quote_for_url", use_plus
                )
            )
        return qquote(value, use_plus=use_plus == "0")


class BuiltinFormatDuration(BuiltinFormatterFunction):
    name = "format_duration"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        value, template = args[0], args[1]
        largest_unit = args[2] if len(args) > 2 else ""
        if largest_unit not in "wdhms":
            raise ValueError(
                _("the {0} parameter must be one of {1}").format(
                    "largest_unit", "wdhms"
                )
            )

        pat = re.compile(r"\[(.)(:(.*?))?\]")

        if not largest_unit:
            highest_index = 0
            for m in pat.finditer(template):
                try:
                    # We know that m.group(1) is a single character so the only
                    # exception possible is that the character is not in the string
                    dex = "smhdw".index(m.group(1).lower())
                    highest_index = max(highest_index, dex)
                except Exception:
                    raise ValueError(
                        _("The {} format specifier is not valid").format(m.group())
                    )
            largest_unit = "smhdw"[highest_index]

        int_val = remainder = round(float(value)) if value else 0
        weeks, remainder = (
            divmod(remainder, 60 * 60 * 24 * 7)
            if largest_unit == "w"
            else (-1, remainder)
        )
        days, remainder = (
            divmod(remainder, 60 * 60 * 24) if largest_unit in "wd" else (-1, remainder)
        )
        hours, remainder = (
            divmod(remainder, 60 * 60) if largest_unit in "wdh" else (-1, remainder)
        )
        minutes, remainder = (
            divmod(remainder, 60) if largest_unit in "wdhm" else (-1, remainder)
        )
        seconds = remainder

        def repl(mo: Match[str]) -> str:
            fmt_char = mo.group(1)
            suffixes = mo.group(3)
            if suffixes is None:
                zero_suffix = one_suffix = more_suffix = fmt_char.lower() + " "
            else:
                suffixes = re.split(r"\|", suffixes)
                match len(suffixes):
                    case 1:
                        zero_suffix = one_suffix = more_suffix = suffixes[0]
                    case 2:
                        zero_suffix = more_suffix = suffixes[0]
                        one_suffix = suffixes[1]
                    case 3:
                        zero_suffix = suffixes[0]
                        one_suffix = suffixes[1]
                        more_suffix = suffixes[2]
                    case _:
                        raise ValueError(
                            _("The group {} has too many suffixes").format(fmt_char)
                        )
                        zero_suffix = one_suffix = more_suffix = "@@too many suffixes@@"

            def val_with_suffix(val: int, test_val: int) -> str:
                match val:
                    case -1:
                        return ""
                    case 0 if fmt_char.islower() and int_val < test_val:
                        return ""
                    case 0:
                        return str(val) + zero_suffix
                    case 1:
                        return str(val) + one_suffix
                    case _:
                        return str(val) + more_suffix

            match fmt_char.lower():
                case "w":
                    return val_with_suffix(weeks, 60 * 60 * 24 * 7)
                case "d":
                    return val_with_suffix(days, 60 * 60 * 24)
                case "h":
                    return val_with_suffix(hours, 60 * 60)
                case "m":
                    return val_with_suffix(minutes, 60)
                case "s":
                    return val_with_suffix(seconds, -1)
                case _:
                    raise ValueError(
                        _("The {} format specifier is not valid").format(fmt_char)
                    )

        return pat.sub(repl, template)


class BuiltinSelectedBooks(BuiltinFormatterFunction):
    name = "selected_books"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        return self.only_in_gui_error()


class BuiltinSortBookIds(BuiltinFormatterFunction):
    name = "sort_book_ids"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        return self.only_in_gui_error()


class BuiltinSelectedColumn(BuiltinFormatterFunction):
    name = "selected_column"
    arg_count = 0

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        return self.only_in_gui_error()


class BuiltinWidthFromPages(BuiltinFormatterFunction):
    name = "width_from_pages"
    arg_count = -1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *all_args: str,
    ) -> Result:
        return self.only_in_gui_error()


class BuiltinShowDialog(BuiltinFormatterFunction):
    name = "show_dialog"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        return self.only_in_gui_error()


class BuiltinFString(BuiltinFormatterFunction):
    name = "f_string"
    arg_count = 1

    def evaluate(
        self,
        formatter: Formatter,
        kwargs: Kwargs,
        mi: Metadata,
        locals: Locals,
        *args: str,
    ) -> Result:
        (fstring,) = args
        raise ValueError(
            _("This function cannot be called directly. It is built into the formatter")
        )


def builtin_functions():
    functions = {}
    for cls in tuple(globals().values()):
        if (
            isinstance(cls, type)
            and cls is not BuiltinFormatterFunction
            and issubclass(cls, BuiltinFormatterFunction)
        ):
            instance = cls()
            functions[instance.name] = instance
            for alias in instance.aliases:
                functions[alias] = instance
    return functions
