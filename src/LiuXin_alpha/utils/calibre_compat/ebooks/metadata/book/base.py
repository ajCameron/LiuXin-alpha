"""calibre.ebooks.metadata.book.base compatibility layer.

Use LiuXin's calibre-derived metadata implementation as the canonical
``Metadata`` class exposed to calibre plugins.
"""

from __future__ import annotations

import copy
from contextlib import suppress

from LiuXin_alpha.metadata.book import base as _core_base
from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book import (
    ALL_METADATA_FIELDS,
    SC_COPYABLE_FIELDS,
    SC_FIELDS_COPY_NOT_NULL,
    STANDARD_METADATA_FIELDS,
    TOP_LEVEL_IDENTIFIERS,
)
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2010, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

_CoreMetadata = _core_base.calibreMetadata
FieldMetadata = _core_base.FieldMetadata
SIMPLE_GET = _core_base.SIMPLE_GET
SIMPLE_SET = _core_base.SIMPLE_SET
NULL_VALUES = _core_base.NULL_VALUES
ck = _core_base.ck
cv = _core_base.cv
field_from_string = _core_base.field_from_string
field_metadata = _core_base.field_metadata


def reset_field_metadata():
    _core_base.reset_field_metadata()
    globals()["field_metadata"] = _core_base.field_metadata


def human_readable(size, precision=2):
    """Match calibre's display semantics for byte sizes."""
    ans = size / (1024 * 1024)
    if ans < 0.1:
        return "<0.1 MB"
    return ("%." + str(precision) + "f MB") % ans


class Metadata(_CoreMetadata):
    """Calibre-compatible Metadata object backed by LiuXin internals."""

    # Keep parity with calibre API where these constants are expected
    # to exist on module import targets.
    ALL_METADATA_FIELDS = ALL_METADATA_FIELDS
    SC_COPYABLE_FIELDS = SC_COPYABLE_FIELDS
    SC_FIELDS_COPY_NOT_NULL = SC_FIELDS_COPY_NOT_NULL
    STANDARD_METADATA_FIELDS = STANDARD_METADATA_FIELDS
    TOP_LEVEL_IDENTIFIERS = TOP_LEVEL_IDENTIFIERS

    def set_null(self, field):
        null_val = copy.copy(NULL_VALUES.get(field))
        setattr(self, field, null_val)

    def _evaluate_all_composites(self):
        custom_fields = object.__getattribute__(self, "_data")["user_metadata"]
        for field in custom_fields:
            self._evaluate_composite(field)

    def _evaluate_composite(self, field):
        f = object.__getattribute__(self, "_data")["user_metadata"].get(field, None)
        if f is not None and f.get("datatype") == "composite" and f.get("#value#") is None:
            self.get(field)

    def deepcopy(self, class_generator=lambda: None):
        if class_generator is None:
            class_generator = lambda: Metadata(None)
        return super().deepcopy(class_generator=class_generator)

    def deepcopy_metadata(self):
        m = Metadata(None)
        object.__setattr__(m, "_data", copy.deepcopy(object.__getattribute__(self, "_data")))
        return m

    def __unicode__representation__(self):
        return self.__unicode__()

    def remove_stale_user_metadata(self, other_mi):
        me = self.get_all_user_metadata(make_copy=False)
        other = set(other_mi.custom_field_keys())
        new = {}
        for k, v in me.items():
            if k in other:
                new[k] = v
        self.set_all_user_metadata(new)

    __str__ = __unicode__representation__
    __bool__ = _CoreMetadata.__nonzero__


def get_model_metadata_instance():
    """
    Return a metadata instance populated with plausible values.

    Mirrors calibre behavior and is intended for GUI-thread use.
    """
    from calibre.gui2 import is_gui_thread

    if not is_gui_thread():
        raise ValueError("get_model_metadata_instance() must only be used in the GUI thread")

    mi = Metadata(_("Title"), [_("Author")])
    mi.author_sort = _("Author Sort")
    mi.series = _("Series")
    mi.series_index = 3
    mi.rating = 4.0
    mi.tags = [_("Tag 1"), _("Tag 2")]
    mi.languages = ["eng"]
    mi.id = -1

    from calibre.gui2.ui import get_gui
    from calibre.utils.date import DEFAULT_DATE

    fm = get_gui().current_db.new_api.field_metadata
    mi.set_all_user_metadata(fm.custom_field_metadata())
    for col in mi.get_all_user_metadata(False):
        if fm[col]["datatype"] == "datetime":
            mi.set(col, DEFAULT_DATE)
        elif fm[col]["datatype"] in ("int", "float", "rating"):
            mi.set(col, 2)
        elif fm[col]["datatype"] == "bool":
            mi.set(col, True)
        elif fm[col]["datatype"] == "text":
            if fm[col]["is_multiple"]:
                mi.set(col, [_("Value 1"), _("Value 2")])
            else:
                mi.set(col, _("Some Value"))
        elif fm[col]["datatype"] == "series":
            mi.set(col, _("Series Name"), extra=1)
        elif fm[col]["datatype"] == "composite":
            with suppress(Exception):
                mi.set(col, _("Composite Value"))
    return mi


# Historically calibre exports both names from this module.
MetaInformation = Metadata

__all__ = [
    "ALL_METADATA_FIELDS",
    "SC_COPYABLE_FIELDS",
    "SC_FIELDS_COPY_NOT_NULL",
    "SIMPLE_GET",
    "SIMPLE_SET",
    "STANDARD_METADATA_FIELDS",
    "TOP_LEVEL_IDENTIFIERS",
    "ck",
    "cv",
    "FieldMetadata",
    "MetaInformation",
    "Metadata",
    "NULL_VALUES",
    "field_from_string",
    "field_metadata",
    "get_model_metadata_instance",
    "human_readable",
    "reset_field_metadata",
]
