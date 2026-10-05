# Copyright (c) 2007 Mike Higgins (Falstaff)
# Modifications from the original:
#    Copyright (C) 2007 Kovid Goyal <kovid@kovidgoyal.net>
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
#
# Current limitations and bugs:
#   Bug: Does not check if most setting values are valid unless lrf is created.
#
#   Unsupported objects: MiniPage, SimpleTextBlock, Canvas, Window,
#                        PopUpWindow, Sound, Import, SoundStream,
#                        ObjectInfo
#
#   Does not support background images for blocks or pages.
#
#   The only button type supported are JumpButtons.
#
#   None of the Japanese language tags are supported.
#
#   Other unsupported tags: PageDiv, SoundStop, Wait, pos,
#                           Plot, Image (outside of ImageBlock),
#                           EmpLine, EmpDots

"""
Construct LRS books, pages, blocks, styles, images and navigation trees.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise pylrs through a consuming regression::

        python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
"""
from __future__ import print_function
from __future__ import annotations

import typing as _typing

import os
import re
import codecs
import operator
from xml.sax.saxutils import escape
from datetime import date

try:
    from LiuXin_alpha.file_formats.lrf.pylrs.elements import Element, SubElement

    # Element, SubElement  # To make pyflakes shut up
except ImportError:
    from xml.etree.ElementTree import Element, SubElement

from LiuXin_alpha.file_formats.lrf.pylrs.elements import ElementWriter
from LiuXin_alpha.file_formats.lrf.pylrs.pylrf import (
    LrfWriter,
    LrfObject,
    LrfTag,
    LrfToc,
    STREAM_COMPRESSED,
    LrfTagStream,
    LrfStreamBase,
    IMAGE_TYPE_ENCODING,
    BINDING_DIRECTION_ENCODING,
    LINE_TYPE_ENCODING,
    LrfFileStream,
    STREAM_FORCE_COMPRESSED,
)

from LiuXin_alpha.constants import __appname__, __version__

from LiuXin_alpha.utils.calibre import entity_to_unicode
from LiuXin_alpha.utils.date import isoformat

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

basestring = six_string_types


DEFAULT_SOURCE_ENCODING = "cp1252"  # defualt is us-windows character set
DEFAULT_GENREADING = "fs"  # default is yes to both lrf and lrs


class LrsError(Exception):
    """
    Report a lrserror encountered while processing an ebook format.

    Example:
        Exercise LrsError through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    pass


class ContentError(Exception):
    """
    Report a contenterror encountered while processing an ebook format.

    Example:
        Exercise ContentError through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    pass


def _checkExists(filename: _typing.Any) -> None:
    """
    Perform the checkExists operation under explicit file-format and conversion rules.

    Example:
        Exercise  checkExists through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param filename: Filename used for type inference or archive output.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if not os.path.exists(filename):
        raise LrsError("file '%s' not found" % filename)


def _formatXml(root: _typing.Any) -> None:
    """
    A helper to make the LRS output look nicer.

    Example:
        Exercise  formatXml through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param root: Root directory that bounds path resolution or traversal.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for elem in root.iter():
        if len(elem) > 0 and (not elem.text or not elem.text.strip()):
            elem.text = "\n"
        if not elem.tail or not elem.tail.strip():
            elem.tail = "\n"


def ElementWithText(tag: _typing.Any, text: _typing.Any, **extra: _typing.Any) -> _typing.Any:
    """
    A shorthand function to create Elements with text.

    Example:
        Exercise ElementWithText through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param tag: Value supplied for tag under the utility contract.
    :param text: Text parsed, normalized or rendered.
    :param extra: Value supplied for extra under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    e = Element(tag, **extra)
    e.text = text
    return e


def ElementWithReading(tag: _typing.Any, text: _typing.Any, reading: bool = False) -> _typing.Any:
    """
    A helper function that creates reading attributes.

    Example:
        Exercise ElementWithReading through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param tag: Value supplied for tag under the utility contract.
    :param text: Text parsed, normalized or rendered.
    :param reading: Value supplied for reading under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    # note: old lrs2lrf parser only allows reading = ""

    if text is None:
        readingText = ""
    elif isinstance(text, six_string_types):
        readingText = text
    else:
        # assumed to be a sequence of (name, sortas)
        readingText = text[1]
        text = text[0]

    if not reading:
        readingText = ""
    return ElementWithText(tag, text, reading=readingText)


def appendTextElements(e: _typing.Any, contentsList: _typing.Any, se: _typing.Any) -> None:
    """
    A helper function to convert text streams into the proper elements.

    Example:
        Exercise appendTextElements through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


    :param e: Value supplied for e under the utility contract.
    :param contentsList: Value supplied for contentsList under the utility contract.
    :param se: Value supplied for se under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    def uconcat(text: _typing.Any, newText: _typing.Any, se: _typing.Any) -> _typing.Any:
        """
        Perform the uconcat operation under explicit file-format and conversion rules.

        Example:
            Exercise appendTextElements.uconcat through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param text: Text parsed, normalized or rendered.
        :param newText: Value supplied for newText under the utility contract.
        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if type(newText) != type(text):
            if type(text) is str:
                text = text.decode(se)
            else:
                newText = newText.decode(se)

        return text + newText

    e.text = ""
    last_element = None

    for content in contentsList:
        if not isinstance(content, Text):
            newElement = content.toElement(se)
            if newElement is None:
                continue
            last_element = newElement
            last_element.tail = ""
            e.append(last_element)
        else:
            if last_element is None:
                e.text = uconcat(e.text, content.text, se)
            else:
                last_element.tail = uconcat(last_element.tail, content.text, se)


class Delegator(object):
    """
    A mixin class to create delegated methods that create elements.

    Example:
        Exercise Delegator through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, delegates: _typing.Any) -> None:
        """
        Initialize and validate the delegator state.

        Example:
            Exercise Delegator.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param delegates: Value supplied for delegates under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.delegates = delegates
        self.delegatedMethods = []
        # self.delegatedSettingsDict = {}
        # self.delegatedSettings = []
        for d in delegates:
            d.parent = self
            methods = d.getMethods()
            self.delegatedMethods += methods
            for m in methods:
                setattr(self, m, getattr(d, m))

            """
            for setting in d.getSettings():
                if isinstance(setting, basestring):
                    setting = (d, setting)
                delegates = \
                        self.delegatedSettingsDict.setdefault(setting[1], [])
                delegates.append(setting[0])
                self.delegatedSettings.append(setting)
            """

    def applySetting(self: _typing.Self, name: _typing.Any, value: _typing.Any, testValid: bool = False) -> _typing.Any:
        """
        Perform the applySetting operation under explicit file-format and conversion rules.

        Example:
            Exercise Delegator.applySetting through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param value: Value normalized, stored, formatted or returned.
        :param testValid: Value supplied for testValid under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        applied = False
        if name in self.getSettings():
            setattr(self, name, value)
            applied = True

        for d in self.delegates:
            if hasattr(d, "applySetting"):
                applied = applied or d.applySetting(name, value)
            else:
                if name in d.getSettings():
                    setattr(d, name, value)
                    applied = True

        if testValid and not applied:
            raise LrsError("setting %s not valid" % name)

        return applied

    def applySettings(self: _typing.Self, settings: _typing.Any, testValid: bool = False) -> None:
        """
        Perform the applySettings operation under explicit file-format and conversion rules.

        Example:
            Exercise Delegator.applySettings through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param settings: Value supplied for settings under the utility contract.
        :param testValid: Value supplied for testValid under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for (setting, value) in settings.items():
            self.applySetting(setting, value, testValid)
            """
            if setting not in self.delegatedSettingsDict:
                raise LrsError, "setting %s not valid" % setting
            delegates = self.delegatedSettingsDict[setting]
            for d in delegates:
                setattr(d, setting, value)
            """

    def appendDelegates(self: _typing.Self, element: _typing.Any, sourceEncoding: _typing.Any) -> None:
        """
        Perform the appendDelegates operation under explicit file-format and conversion rules.

        Example:
            Exercise Delegator.appendDelegates through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param element: Value supplied for element under the utility contract.
        :param sourceEncoding: Value supplied for sourceEncoding under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for d in self.delegates:
            e = d.toElement(sourceEncoding)
            if e is not None:
                if isinstance(e, list):
                    for e1 in e:
                        element.append(e1)
                else:
                    element.append(e)

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise Delegator.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for d in self.delegates:
            d.appendReferencedObjects(parent)

    def getMethods(self: _typing.Self) -> _typing.Any:
        """
        Perform the getMethods operation under explicit file-format and conversion rules.

        Example:
            Exercise Delegator.getMethods through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.delegatedMethods

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getSettings operation under explicit file-format and conversion rules.

        Example:
            Exercise Delegator.getSettings through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []

    def toLrfDelegates(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrfDelegates operation under explicit file-format and conversion rules.

        Example:
            Exercise Delegator.toLrfDelegates through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for d in self.delegates:
            d.toLrf(lrfWriter)

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise Delegator.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.toLrfDelegates(lrfWriter)


class LrsAttributes(object):
    """
    A mixin class to handle default and user supplied attributes.

    Example:
        Exercise LrsAttributes through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, defaults: _typing.Any, alsoAllow: _typing.Any = None, **settings: _typing.Any) -> None:
        """
        Initialize and validate the lrsattributes state.

        Example:
            Exercise LrsAttributes.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param defaults: Value supplied for defaults under the utility contract.
        :param alsoAllow: Value supplied for alsoAllow under the utility contract.
        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if alsoAllow is None:
            alsoAllow = []
        self.attrs = defaults.copy()
        for (name, value) in settings.items():
            if name not in self.attrs and name not in alsoAllow:
                raise LrsError("%s does not support setting %s" % (self.__class__.__name__, name))
            if type(value) is int:
                value = str(value)
            self.attrs[name] = value


class LrsContainer(object):
    """
    This class is a mixin class for elements that are contained in or contain an unknown number of other elements.

    Example:
        Exercise LrsContainer through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, validChildren: _typing.Any) -> None:
        """
        Initialize and validate the lrscontainer state.

        Example:
            Exercise LrsContainer.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param validChildren: Value supplied for validChildren under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.parent = None
        self.contents = []
        self.validChildren = validChildren
        self.must_append = False  #: If True even an empty container is appended by append_to

    def has_text(self: _typing.Self) -> bool:
        """
        Return True iff this container has non whitespace text

        Example:
            Exercise LrsContainer.has text through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: True when the documented condition holds; otherwise False.
        """
        if hasattr(self, "text"):
            if self.text.strip():
                return True
        if hasattr(self, "contents"):
            for child in self.contents:
                if child.has_text():
                    return True
        for item in self.contents:
            if isinstance(item, (Plot, ImageBlock, Canvas, CR)):
                return True
        return False

    def append_to(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Append self to C{parent} iff self has non whitespace textual content

        Example:
            Exercise LrsContainer.append to through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.contents or self.must_append:
            parent.append(self)

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsContainer.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for c in self.contents:
            c.appendReferencedObjects(parent)

    def setParent(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the setParent operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsContainer.setParent through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.parent is not None:
            raise LrsError("object already has parent")
        self.parent = parent

    def append(self: _typing.Self, content: _typing.Any, convertText: bool = True) -> _typing.Any:
        """
        Appends valid objects to container. Can auto-covert text strings to Text objects.

        Example:
            Exercise LrsContainer.append through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param content: Value supplied for content under the utility contract.
        :param convertText: Value supplied for convertText under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for validChild in self.validChildren:
            if isinstance(content, validChild):
                break
        else:
            raise LrsError("can't append %s to %s" % (content.__class__.__name__, self.__class__.__name__))

        if convertText and isinstance(content, six_string_types):
            content = Text(content)

        content.setParent(self)

        if isinstance(content, LrsObject):
            content.assignId()

        self.contents.append(content)
        return self

    def get_all(self: _typing.Self, predicate: _typing.Callable[..., _typing.Any] = lambda x: x) -> _typing.Iterator[_typing.Any]:
        """
        Return all under the format's safety and compatibility rules.

        Example:
            Exercise LrsContainer.get all through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param predicate: Value supplied for predicate under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        for child in self.contents:
            if predicate(child):
                yield child
            if hasattr(child, "get_all"):
                for grandchild in child.get_all(predicate):
                    yield grandchild


class LrsObject(object):
    """
    A mixin class for elements that need an object id.

    Example:
        Exercise LrsObject through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    nextObjId = 0

    @classmethod
    def getNextObjId(selfClass: _typing.Any) -> _typing.Any:
        """
        Perform the getNextObjId operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsObject.getNextObjId through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        selfClass.nextObjId += 1
        return selfClass.nextObjId

    def __init__(self: _typing.Self, assignId: bool = False) -> None:
        """
        Initialize and validate the lrsobject state.

        Example:
            Exercise LrsObject.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param assignId: Value supplied for assignId under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if assignId:
            self.objId = LrsObject.getNextObjId()
        else:
            self.objId = 0

    def assignId(self: _typing.Self) -> None:
        """
        Perform the assignId operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsObject.assignId through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.objId != 0:
            raise LrsError("id already assigned to " + self.__class__.__name__)
        self.objId = LrsObject.getNextObjId()

    def lrsObjectElement(self: _typing.Self, name: _typing.Any, objlabel: str = "objlabel", labelName: _typing.Any = None, labelDecorate: bool = True, **settings: _typing.Any) -> _typing.Any:
        """
        Perform the lrsObjectElement operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsObject.lrsObjectElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param objlabel: Value supplied for objlabel under the utility contract.
        :param labelName: Value supplied for labelName under the utility contract.
        :param labelDecorate: Value supplied for labelDecorate under the utility contract.
        :param settings: Value supplied for settings under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        element = Element(name)
        element.attrib["objid"] = str(self.objId)
        if labelName is None:
            labelName = name
        if labelDecorate:
            label = "%s.%d" % (labelName, self.objId)
        else:
            label = str(self.objId)
        element.attrib[objlabel] = label
        element.attrib.update(settings)
        return element


class Book(Delegator):
    """
    Main class for any lrs or lrf. All objects must be appended to the Book class in some way or another in order to be rendered as an LRS or LRF file.

    Example:
        Exercise Book through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(
        self: _typing.Self,
        textstyledefault: _typing.Any = None,
        blockstyledefault: _typing.Any = None,
        pagestyledefault: _typing.Any = None,
        optimizeTags: bool = False,
        optimizeCompression: bool = False,
        **settings: _typing.Any
    ) -> None:

        """
        Initialize and validate the book state.

        Example:
            Exercise Book.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param textstyledefault: Value supplied for textstyledefault under the utility
            contract.
        :param blockstyledefault: Value supplied for blockstyledefault under the utility
            contract.
        :param pagestyledefault: Value supplied for pagestyledefault under the utility
            contract.
        :param optimizeTags: Value supplied for optimizeTags under the utility contract.
        :param optimizeCompression: Value supplied for optimizeCompression under the utility
            contract.
        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.parent = None  # we are the top of the parent chain

        # LRF object IDs are per-book. Reset the global counter here so
        # repeated conversions in the same process remain deterministic.
        LrsObject.nextObjId = 0

        if "thumbnail" in settings:
            _checkExists(settings["thumbnail"])

        # highly experimental -- use with caution
        self.optimizeTags = optimizeTags
        self.optimizeCompression = optimizeCompression

        pageStyle = PageStyle(**PageStyle.baseDefaults.copy())
        blockStyle = BlockStyle(**BlockStyle.baseDefaults.copy())
        textStyle = TextStyle(**TextStyle.baseDefaults.copy())

        if textstyledefault is not None:
            textStyle.update(textstyledefault)

        if blockstyledefault is not None:
            blockStyle.update(blockstyledefault)

        if pagestyledefault is not None:
            pageStyle.update(pagestyledefault)

        self.defaultPageStyle = pageStyle
        self.defaultTextStyle = textStyle
        self.defaultBlockStyle = blockStyle
        LrsObject.nextObjId += 1

        styledefault = StyleDefault()
        if ("setdefault" in settings):
            styledefault = settings.pop("setdefault")
        Delegator.__init__(
            self,
            [
                BookInformation(),
                Main(),
                Template(),
                Style(styledefault),
                Solos(),
                Objects(),
            ],
        )

        self.sourceencoding = None

        # apply default settings
        self.applySetting("genreading", DEFAULT_GENREADING)
        self.applySetting("sourceencoding", DEFAULT_SOURCE_ENCODING)

        self.applySettings(settings, testValid=True)

        self.allow_new_page = True  #: If False L{create_page} raises an exception
        self.gc_count = 0

    def set_title(self: _typing.Self, title: _typing.Any) -> None:
        """
        Set title under the format's safety and compatibility rules.

        Example:
            Exercise Book.set title through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param title: Value supplied for title under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ot = self.delegates[0].delegates[0].delegates[0].title
        self.delegates[0].delegates[0].delegates[0].title = (title, ot[1])

    def set_author(self: _typing.Self, author: _typing.Any) -> None:
        """
        Set author under the format's safety and compatibility rules.

        Example:
            Exercise Book.set author through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param author: Value supplied for author under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ot = self.delegates[0].delegates[0].delegates[0].author
        self.delegates[0].delegates[0].delegates[0].author = (author, ot[1])

    def create_text_style(self: _typing.Self, **settings: _typing.Any) -> _typing.Any:
        """
        Create text style under the format's safety and compatibility rules.

        Example:
            Exercise Book.create text style through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param settings: Value supplied for settings under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = TextStyle(**self.defaultTextStyle.attrs.copy())
        ans.update(settings)
        return ans

    def create_block_style(self: _typing.Self, **settings: _typing.Any) -> _typing.Any:
        """
        Create block style under the format's safety and compatibility rules.

        Example:
            Exercise Book.create block style through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param settings: Value supplied for settings under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = BlockStyle(**self.defaultBlockStyle.attrs.copy())
        ans.update(settings)
        return ans

    def create_page_style(self: _typing.Self, **settings: _typing.Any) -> _typing.Any:
        """
        Create page style under the format's safety and compatibility rules.

        Example:
            Exercise Book.create page style through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param settings: Value supplied for settings under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not self.allow_new_page:
            raise ContentError
        ans = PageStyle(**self.defaultPageStyle.attrs.copy())
        ans.update(settings)
        return ans

    def create_page(self: _typing.Self, pageStyle: _typing.Any = None, **settings: _typing.Any) -> _typing.Any:
        """
        Return a new L{Page}. The page has not been appended to this book.

        Example:
            Exercise Book.create page through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param pageStyle: Value supplied for pageStyle under the utility contract.
        :param settings: Value supplied for settings under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not pageStyle:
            pageStyle = self.defaultPageStyle
        return Page(pageStyle=pageStyle, **settings)

    def create_text_block(self: _typing.Self, textStyle: _typing.Any = None, blockStyle: _typing.Any = None, **settings: _typing.Any) -> _typing.Any:
        """
        Return a new L{TextBlock}. The block has not been appended to this book.

        Example:
            Exercise Book.create text block through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param textStyle: Value supplied for textStyle under the utility contract.
        :param blockStyle: Value supplied for blockStyle under the utility contract.
        :param settings: Value supplied for settings under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not textStyle:
            textStyle = self.defaultTextStyle
        if not blockStyle:
            blockStyle = self.defaultBlockStyle
        return TextBlock(textStyle=textStyle, blockStyle=blockStyle, **settings)

    def pages(self: _typing.Self) -> _typing.Any:
        """
        Return list of Page objects in this book

        Example:
            Exercise Book.pages through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = []
        for item in self.delegates:
            if isinstance(item, Main):
                for candidate in item.contents:
                    if isinstance(candidate, Page):
                        ans.append(candidate)
                break
        return ans

    def last_page(self: _typing.Self) -> _typing.Any:
        """
        Return last Page in this book

        Example:
            Exercise Book.last page through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for item in self.delegates:
            if isinstance(item, Main):
                temp = list(item.contents)
                temp.reverse()
                for candidate in temp:
                    if isinstance(candidate, Page):
                        return candidate

    def embed_font(self: _typing.Self, file: _typing.Any, facename: _typing.Any) -> None:
        """
        Perform the embed font operation under explicit file-format and conversion rules.

        Example:
            Exercise Book.embed font through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param file: Value supplied for file under the utility contract.
        :param facename: Value supplied for facename under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        f = Font(file, facename)
        self.append(f)

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getSettings operation under explicit file-format and conversion rules.

        Example:
            Exercise Book.getSettings through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ["sourceencoding"]

    def append(self: _typing.Self, content: _typing.Any) -> None:
        """
        Find and invoke the correct appender for this content.

        Example:
            Exercise Book.append through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param content: Value supplied for content under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        className = content.__class__.__name__
        try:
            method = getattr(self, "append" + className)
        except AttributeError:
            raise LrsError("can't append %s to Book" % className)
        method(content)

    def rationalize_font_sizes(self: _typing.Self, base_font_size: int = 10) -> None:
        """
        Perform the rationalize font sizes operation under explicit file-format and conversion rules.

        Example:
            Exercise Book.rationalize font sizes through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param base_font_size: Value supplied for base font size under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        base_font_size *= 10.0
        main = None
        for obj in self.delegates:
            if isinstance(obj, Main):
                main = obj
                break

        fonts = {}
        for text in main.get_all(lambda x: isinstance(x, Text)):
            fs = base_font_size
            ancestor = text.parent
            while ancestor:
                try:
                    fs = int(ancestor.attrs["fontsize"])
                    break
                except (AttributeError, KeyError):
                    pass
                try:
                    fs = int(ancestor.textSettings["fontsize"])
                    break
                except (AttributeError, KeyError):
                    pass
                try:
                    fs = int(ancestor.textStyle.attrs["fontsize"])
                    break
                except (AttributeError, KeyError):
                    pass
                ancestor = ancestor.parent
            length = len(text.text)
            fonts[fs] = fonts.get(fs, 0) + length
        if not fonts:
            print("WARNING: LRF seems to have no textual content. Cannot rationalize font sizes.")
            return

        old_base_font_size = float(max(fonts.items(), key=operator.itemgetter(1))[0])
        factor = base_font_size / old_base_font_size

        def rescale(old: _typing.Any) -> _typing.Any:
            """
            Perform the rescale operation under explicit file-format and conversion rules.

            Example:
                Exercise Book.rationalize font sizes.rescale through a consuming regression::

                    python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


            :param old: Value supplied for old under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return str(int(int(old) * factor))

        text_blocks = list(main.get_all(lambda x: isinstance(x, TextBlock)))
        for tb in text_blocks:
            if ("fontsize" in tb.textSettings):
                tb.textSettings["fontsize"] = rescale(tb.textSettings["fontsize"])
            for span in tb.get_all(lambda x: isinstance(x, Span)):
                if ("fontsize" in span.attrs):
                    span.attrs["fontsize"] = rescale(span.attrs["fontsize"])
                if ("baselineskip" in span.attrs):
                    span.attrs["baselineskip"] = rescale(span.attrs["baselineskip"])

        text_styles = set(tb.textStyle for tb in text_blocks)
        for ts in text_styles:
            ts.attrs["fontsize"] = rescale(ts.attrs["fontsize"])
            ts.attrs["baselineskip"] = rescale(ts.attrs["baselineskip"])

    def renderLrs(self: _typing.Self, lrsFile: _typing.Any, encoding: str = "UTF-8") -> None:
        """
        Perform the renderLrs operation under explicit file-format and conversion rules.

        Example:
            Exercise Book.renderLrs through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrsFile: Value supplied for lrsFile under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if isinstance(lrsFile, six_string_types):
            lrsFile = codecs.open(lrsFile, "wb", encoding=encoding)
        self.render(lrsFile, outputEncodingName=encoding)
        lrsFile.close()

    def renderLrf(self: _typing.Self, lrfFile: _typing.Any) -> None:
        """
        Perform the renderLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise Book.renderLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfFile: Value supplied for lrfFile under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.appendReferencedObjects(self)
        # Todo: Waiting until I can actually run some tests
        if isinstance(lrfFile, six_string_types):
            lrfFile = open(lrfFile, "wb")
        lrfWriter = LrfWriter(self.sourceencoding)

        lrfWriter.optimizeTags = self.optimizeTags
        lrfWriter.optimizeCompression = self.optimizeCompression

        self.toLrf(lrfWriter)
        lrfWriter.writeFile(lrfFile)
        lrfFile.close()

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Book.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        root = Element("BBeBXylog", version="1.0")
        root.append(Element("Property"))
        self.appendDelegates(root, self.sourceencoding)
        return root

    def render(self: _typing.Self, f: _typing.Any, outputEncodingName: str = "UTF-8") -> None:
        """
        Write the book as an LRS to file f.

        Example:
            Exercise Book.render through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param f: Value supplied for f under the utility contract.
        :param outputEncodingName: Value supplied for outputEncodingName under the utility
            contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """

        self.appendReferencedObjects(self)

        # create the root node, and populate with the parts of the book
        root = self.toElement(self.sourceencoding)

        # now, add some newlines to make it easier to look at
        _formatXml(root)

        writer = ElementWriter(
            root,
            header=True,
            sourceEncoding=self.sourceencoding,
            spaceBeforeClose=False,
            outputEncodingName=outputEncodingName,
        )
        writer.write(f)


class BookInformation(Delegator):
    """
    Just a container for the Info and TableOfContents elements.

    Example:
        Exercise BookInformation through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the bookinformation state.

        Example:
            Exercise BookInformation.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        Delegator.__init__(self, [Info(), TableOfContents()])

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise BookInformation.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        bi = Element("BookInformation")
        self.appendDelegates(bi, se)
        return bi


class Info(Delegator):
    """
    Just a container for the BookInfo and DocInfo elements.

    Example:
        Exercise Info through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the info state.

        Example:
            Exercise Info.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.genreading = DEFAULT_GENREADING
        Delegator.__init__(self, [BookInfo(), DocInfo()])

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getSettings operation under explicit file-format and conversion rules.

        Example:
            Exercise Info.getSettings through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ["genreading"]  # + self.delegatedSettings

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Info.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        info = Element("Info", version="1.1")
        info.append(self.delegates[0].toElement(se, reading="s" in self.genreading))
        info.append(self.delegates[1].toElement(se))
        return info

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        # this info is set in XML form in the LRF
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise Info.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        info = Element("Info", version="1.1")
        # self.appendDelegates(info)
        info.append(self.delegates[0].toElement(lrfWriter.getSourceEncoding(), reading="f" in self.genreading))
        info.append(self.delegates[1].toElement(lrfWriter.getSourceEncoding()))

        # look for the thumbnail file and get the filename
        tnail = info.find("DocInfo/CThumbnail")
        if tnail is not None:
            lrfWriter.setThumbnailFile(tnail.get("file"))
            # does not work: info.remove(tnail)

        _formatXml(info)

        # fix up the doc info to match the LRF format
        # NB: generates an encoding attribute, which lrs2lrf does not
        xml_info = ElementWriter(
            info,
            header=True,
            sourceEncoding=lrfWriter.getSourceEncoding(),
            spaceBeforeClose=False,
        ).toString()

        xml_info = re.sub(r"<CThumbnail.*?>\n", "", xml_info)
        xml_info = xml_info.replace("SumPage>", "Page>")
        lrfWriter.docInfoXml = xml_info


class TableOfContents(object):
    """
    Provide the tableofcontents contract for validated ebook processing.

    Example:
        Exercise TableOfContents through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the tableofcontents state.

        Example:
            Exercise TableOfContents.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.tocEntries = []

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise TableOfContents.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getMethods operation under explicit file-format and conversion rules.

        Example:
            Exercise TableOfContents.getMethods through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ["addTocEntry"]

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getSettings operation under explicit file-format and conversion rules.

        Example:
            Exercise TableOfContents.getSettings through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []

    def addTocEntry(self: _typing.Self, tocLabel: _typing.Any, textBlock: _typing.Any) -> None:
        """
        Perform the addTocEntry operation under explicit file-format and conversion rules.

        Example:
            Exercise TableOfContents.addTocEntry through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param tocLabel: Value supplied for tocLabel under the utility contract.
        :param textBlock: Value supplied for textBlock under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not isinstance(textBlock, (Canvas, TextBlock, ImageBlock, RuledLine)):
            raise LrsError(
                "TOC destination must be a Canvas, TextBlock, ImageBlock or RuledLine not a " + str(type(textBlock))
            )

        if textBlock.parent is None:
            raise LrsError("TOC text block must be already appended to a page")

        if False and textBlock.parent.parent is None:
            raise LrsError("TOC destination page must be already appended to a book")

        if not hasattr(textBlock.parent, "objId"):
            raise LrsError("TOC destination must be appended to a container with an objID")

        for tl in self.tocEntries:
            if tl.label == tocLabel and tl.textBlock == textBlock:
                return

        self.tocEntries.append(TocLabel(tocLabel, textBlock))
        textBlock.tocLabel = tocLabel

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise TableOfContents.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if len(self.tocEntries) == 0:
            return None

        toc = Element("TOC")
        for t in self.tocEntries:
            toc.append(t.toElement(se))
        return toc

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise TableOfContents.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if len(self.tocEntries) == 0:
            return

        toc = []
        for t in self.tocEntries:
            toc.append((t.textBlock.parent.objId, t.textBlock.objId, t.label))

        lrf_toc = LrfToc(LrsObject.getNextObjId(), toc, lrfWriter.getSourceEncoding())
        lrfWriter.append(lrf_toc)
        lrfWriter.setTocObject(lrf_toc)


class TocLabel(object):
    """
    Provide the toclabel contract for validated ebook processing.

    Example:
        Exercise TocLabel through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, label: _typing.Any, textBlock: _typing.Any) -> None:
        """
        Initialize and validate the toclabel state.

        Example:
            Exercise TocLabel.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param label: Value supplied for label under the utility contract.
        :param textBlock: Value supplied for textBlock under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.label = escape(re.sub(r"&(\S+?);", entity_to_unicode, label))
        self.textBlock = textBlock

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise TocLabel.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ElementWithText(
            "TocLabel",
            self.label,
            refobj=str(self.textBlock.objId),
            refpage=str(self.textBlock.parent.objId),
        )


class BookInfo(object):
    """
    Carry normalized bookinfo data across the conversion pipeline.

    Example:
        Exercise BookInfo through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the bookinfo state.

        Example:
            Exercise BookInfo.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.title = "Untitled"
        self.author = "Anonymous"
        self.bookid = None
        self.pi = None
        self.isbn = None
        self.publisher = None
        self.freetext = "\n\n"
        self.label = None
        self.category = None
        self.classification = None

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise BookInfo.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getMethods operation under explicit file-format and conversion rules.

        Example:
            Exercise BookInfo.getMethods through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getSettings operation under explicit file-format and conversion rules.

        Example:
            Exercise BookInfo.getSettings through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [
            "author",
            "title",
            "bookid",
            "isbn",
            "publisher",
            "freetext",
            "label",
            "category",
            "classification",
        ]

    def _appendISBN(self: _typing.Self, bi: _typing.Any) -> None:
        """
        Perform the appendISBN operation under explicit file-format and conversion rules.

        Example:
            Exercise BookInfo. appendISBN through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param bi: Value supplied for bi under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pi = Element("ProductIdentifier")
        isbn_element = ElementWithText("ISBNPrintable", self.isbn)
        isbn_value_element = ElementWithText("ISBNValue", self.isbn.replace("-", ""))

        pi.append(isbn_element)
        pi.append(isbn_value_element)
        bi.append(pi)

    def toElement(self: _typing.Self, se: _typing.Any, reading: bool = True) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise BookInfo.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :param reading: Value supplied for reading under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        bi = Element("BookInfo")
        bi.append(ElementWithReading("Title", self.title, reading=reading))
        bi.append(ElementWithReading("Author", self.author, reading=reading))
        bi.append(ElementWithText("BookID", self.bookid))
        if self.isbn is not None:
            self._appendISBN(bi)

        if self.publisher is not None:
            bi.append(ElementWithReading("Publisher", self.publisher))

        bi.append(ElementWithReading("Label", self.label, reading=reading))
        bi.append(ElementWithText("Category", self.category))
        bi.append(ElementWithText("Classification", self.classification))
        bi.append(ElementWithText("FreeText", self.freetext))
        return bi


class DocInfo(object):
    """
    Carry normalized docinfo data across the conversion pipeline.

    Example:
        Exercise DocInfo through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the docinfo state.

        Example:
            Exercise DocInfo.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.thumbnail = None
        self.language = "en"
        self.creator = None
        self.creationdate = str(isoformat(date.today()))
        self.producer = "%s v%s" % (__appname__, __version__)
        self.numberofpages = "0"

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise DocInfo.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getMethods operation under explicit file-format and conversion rules.

        Example:
            Exercise DocInfo.getMethods through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getSettings operation under explicit file-format and conversion rules.

        Example:
            Exercise DocInfo.getSettings through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [
            "thumbnail",
            "language",
            "creator",
            "creationdate",
            "producer",
            "numberofpages",
        ]

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise DocInfo.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        docInfo = Element("DocInfo")

        if self.thumbnail is not None:
            docInfo.append(Element("CThumbnail", file=self.thumbnail))

        docInfo.append(ElementWithText("Language", self.language))
        docInfo.append(ElementWithText("Creator", self.creator))
        docInfo.append(ElementWithText("CreationDate", self.creationdate))
        docInfo.append(ElementWithText("Producer", self.producer))
        docInfo.append(ElementWithText("SumPage", str(self.numberofpages)))
        return docInfo


class Main(LrsContainer):
    """
    Provide the main contract for validated ebook processing.

    Example:
        Exercise Main through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the main state.

        Example:
            Exercise Main.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [Page])

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getMethods operation under explicit file-format and conversion rules.

        Example:
            Exercise Main.getMethods through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ["appendPage", "Page"]

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getSettings operation under explicit file-format and conversion rules.

        Example:
            Exercise Main.getSettings through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []

    def Page(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the Page operation under explicit file-format and conversion rules.

        Example:
            Exercise Main.Page through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        p = Page(*args, **kwargs)
        self.append(p)
        return p

    def appendPage(self: _typing.Self, page: _typing.Any) -> None:
        """
        Perform the appendPage operation under explicit file-format and conversion rules.

        Example:
            Exercise Main.appendPage through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param page: Value supplied for page under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.append(page)

    def toElement(self: _typing.Self, sourceEncoding: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Main.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param sourceEncoding: Value supplied for sourceEncoding under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        main = Element(self.__class__.__name__)

        for page in self.contents:
            main.append(page.toElement(sourceEncoding))

        return main

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise Main.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        page_ids = []

        # set this id now so that pages can see it
        page_tree_id = LrsObject.getNextObjId()
        lrfWriter.setPageTreeId(page_tree_id)

        # create a list of all the page object ids while dumping the pages

        for p in self.contents:
            page_ids.append(p.objId)
            p.toLrf(lrfWriter)

        # create a page tree object
        page_tree = LrfObject("PageTree", page_tree_id)
        page_tree.appendLrfTag(LrfTag("PageList", page_ids))

        lrfWriter.append(page_tree)


class Solos(LrsContainer):
    """
    Provide the solos contract for validated ebook processing.

    Example:
        Exercise Solos through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the solos state.

        Example:
            Exercise Solos.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [Solo])

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getMethods operation under explicit file-format and conversion rules.

        Example:
            Exercise Solos.getMethods through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ["appendSolo", "Solo"]

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getSettings operation under explicit file-format and conversion rules.

        Example:
            Exercise Solos.getSettings through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []

    def Solo(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the Solo operation under explicit file-format and conversion rules.

        Example:
            Exercise Solos.Solo through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        p = Solo(*args, **kwargs)
        self.append(p)
        return p

    def appendSolo(self: _typing.Self, solo: _typing.Any) -> None:
        """
        Perform the appendSolo operation under explicit file-format and conversion rules.

        Example:
            Exercise Solos.appendSolo through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param solo: Value supplied for solo under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.append(solo)

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise Solos.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for s in self.contents:
            s.toLrf(lrfWriter)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Solos.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        solos = []
        for s in self.contents:
            solos.append(s.toElement(se))

        if len(solos) == 0:
            return None

        return solos


class Solo(Main):
    """
    Provide the solo contract for validated ebook processing.

    Example:
        Exercise Solo through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    pass


class Template(object):
    """
    Does nothing that I know of.

    Example:
        Exercise Template through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise Template.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getMethods operation under explicit file-format and conversion rules.

        Example:
            Exercise Template.getMethods through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getSettings operation under explicit file-format and conversion rules.

        Example:
            Exercise Template.getSettings through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Template.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        t = Element("Template")
        t.attrib["version"] = "1.0"
        return t

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        # does nothing
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise Template.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass


class StyleDefault(LrsAttributes):
    """
    Supply some defaults for all TextBlocks. The legal values are a subset of what is allowed on a TextBlock -- ruby, emphasis, and waitprop settings.

    Example:
        Exercise StyleDefault through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    defaults = dict(
        rubyalign="start",
        rubyadjust="none",
        rubyoverhang="none",
        empdotsposition="before",
        empdotsfontname="Dutch801 Rm BT Roman",
        empdotscode="0x002e",
        emplineposition="after",
        emplinetype="solid",
        setwaitprop="noreplay",
    )

    alsoAllow = ["refempdotsfont", "rubyAlignAndAdjust"]

    def __init__(self: _typing.Self, **settings: _typing.Any) -> None:
        """
        Initialize and validate the styledefault state.

        Example:
            Exercise StyleDefault.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsAttributes.__init__(self, self.defaults, alsoAllow=self.alsoAllow, **settings)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise StyleDefault.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return Element("SetDefault", self.attrs)


class Style(LrsContainer, Delegator):
    """
    Provide the style contract for validated ebook processing.

    Example:
        Exercise Style through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, styledefault: _typing.Any = StyleDefault()) -> None:
        """
        Initialize and validate the style state.

        Example:
            Exercise Style.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param styledefault: Value supplied for styledefault under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [PageStyle, TextStyle, BlockStyle])
        Delegator.__init__(self, [BookStyle(styledefault=styledefault)])
        self.bookStyle = self.delegates[0]
        self.appendPageStyle = self.appendTextStyle = self.appendBlockStyle = self.append

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise Style.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        LrsContainer.appendReferencedObjects(self, parent)

    def getMethods(self: _typing.Self) -> _typing.Any:
        """
        Perform the getMethods operation under explicit file-format and conversion rules.

        Example:
            Exercise Style.getMethods through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [
            "PageStyle",
            "TextStyle",
            "BlockStyle",
            "appendPageStyle",
            "appendTextStyle",
            "appendBlockStyle",
        ] + self.delegatedMethods

    def getSettings(self: _typing.Self) -> _typing.Any:
        """
        Perform the getSettings operation under explicit file-format and conversion rules.

        Example:
            Exercise Style.getSettings through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [(self.bookStyle, x) for x in self.bookStyle.getSettings()]

    def PageStyle(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the PageStyle operation under explicit file-format and conversion rules.

        Example:
            Exercise Style.PageStyle through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ps = PageStyle(*args, **kwargs)
        self.append(ps)
        return ps

    def TextStyle(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the TextStyle operation under explicit file-format and conversion rules.

        Example:
            Exercise Style.TextStyle through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ts = TextStyle(*args, **kwargs)
        self.append(ts)
        return ts

    def BlockStyle(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the BlockStyle operation under explicit file-format and conversion rules.

        Example:
            Exercise Style.BlockStyle through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        bs = BlockStyle(*args, **kwargs)
        self.append(bs)
        return bs

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Style.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        style = Element("Style")
        style.append(self.bookStyle.toElement(se))

        for content in self.contents:
            style.append(content.toElement(se))

        return style

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise Style.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.bookStyle.toLrf(lrfWriter)

        for s in self.contents:
            s.toLrf(lrfWriter)


class BookStyle(LrsObject, LrsContainer):
    """
    Provide the bookstyle contract for validated ebook processing.

    Example:
        Exercise BookStyle through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, styledefault: _typing.Any = StyleDefault()) -> None:
        """
        Initialize and validate the bookstyle state.

        Example:
            Exercise BookStyle.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param styledefault: Value supplied for styledefault under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsObject.__init__(self, assignId=True)
        LrsContainer.__init__(self, [Font])
        self.styledefault = styledefault
        self.booksetting = BookSetting()
        self.appendFont = self.append

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getSettings operation under explicit file-format and conversion rules.

        Example:
            Exercise BookStyle.getSettings through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ["styledefault", "booksetting"]

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getMethods operation under explicit file-format and conversion rules.

        Example:
            Exercise BookStyle.getMethods through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ["Font", "appendFont"]

    def Font(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> None:
        """
        Perform the Font operation under explicit file-format and conversion rules.

        Example:
            Exercise BookStyle.Font through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        f = Font(*args, **kwargs)
        self.append(f)
        return

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise BookStyle.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_style = self.lrsObjectElement("BookStyle", objlabel="stylelabel", labelDecorate=False)
        book_style.append(self.styledefault.toElement(se))
        book_style.append(self.booksetting.toElement(se))
        for font in self.contents:
            book_style.append(font.toElement(se))
        return book_style

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise BookStyle.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        book_atr = LrfObject("BookAtr", self.objId)
        book_atr.appendLrfTag(LrfTag("ChildPageTree", lrfWriter.getPageTreeId()))
        book_atr.appendTagDict(self.styledefault.attrs)

        self.booksetting.toLrf(lrfWriter)

        lrfWriter.append(book_atr)
        lrfWriter.setRootObject(book_atr)

        for font in self.contents:
            font.toLrf(lrfWriter)


class BookSetting(LrsAttributes):
    """
    Provide the booksetting contract for validated ebook processing.

    Example:
        Exercise BookSetting through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, **settings: _typing.Any) -> None:
        """
        Initialize and validate the booksetting state.

        Example:
            Exercise BookSetting.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        defaults = dict(
            bindingdirection="Lr",
            dpi="1660",
            screenheight="800",
            screenwidth="600",
            colordepth="24",
        )
        LrsAttributes.__init__(self, defaults, **settings)

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise BookSetting.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        a = self.attrs
        lrfWriter.dpi = int(a["dpi"])
        lrfWriter.bindingdirection = BINDING_DIRECTION_ENCODING[a["bindingdirection"]]
        lrfWriter.height = int(a["screenheight"])
        lrfWriter.width = int(a["screenwidth"])
        lrfWriter.colorDepth = int(a["colordepth"])

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise BookSetting.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return Element("BookSetting", self.attrs)


class LrsStyle(LrsObject, LrsAttributes, LrsContainer):
    """
    A mixin class for styles.

    Example:
        Exercise LrsStyle through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, elementName: _typing.Any, defaults: _typing.Any = None, alsoAllow: _typing.Any = None, **overrides: _typing.Any) -> None:
        """
        Initialize and validate the lrsstyle state.

        Example:
            Exercise LrsStyle.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param elementName: Value supplied for elementName under the utility contract.
        :param defaults: Value supplied for defaults under the utility contract.
        :param alsoAllow: Value supplied for alsoAllow under the utility contract.
        :param overrides: Value supplied for overrides under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if defaults is None:
            defaults = {}

        LrsObject.__init__(self)
        LrsAttributes.__init__(self, defaults, alsoAllow=alsoAllow, **overrides)
        LrsContainer.__init__(self, [])
        self.elementName = elementName
        self.objectsAppended = False
        # self.label = "%s.%d" % (elementName, self.objId)
        # self.label = str(self.objId)
        # self.parent = None

    def update(self: _typing.Self, settings: _typing.Any) -> None:
        """
        Perform the update operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsStyle.update through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param settings: Value supplied for settings under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for name, value in settings.items():
            if name not in self.__class__.validSettings:
                raise LrsError("%s not a valid setting for %s" % (name, self.__class__.__name__))
            self.attrs[name] = value

    def getLabel(self: _typing.Self) -> _typing.Any:
        """
        Perform the getLabel operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsStyle.getLabel through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return str(self.objId)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsStyle.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        element = Element(self.elementName, stylelabel=self.getLabel(), objid=str(self.objId))
        element.attrib.update(self.attrs)
        return element

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsStyle.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        obj = LrfObject(self.elementName, self.objId)
        obj.appendTagDict(self.attrs, self.__class__.__name__)
        lrfWriter.append(obj)

    def __eq__(self: _typing.Self, other: _typing.Any) -> bool:
        """
        Perform the eq operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsStyle.  eq   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if hasattr(other, "attrs"):
            return self.__class__ == other.__class__ and self.attrs == other.attrs
        return False


class TextStyle(LrsStyle):
    """
    The text style of a TextBlock. Default is 10 pt. Times Roman.

    Example:
        Exercise TextStyle through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    baseDefaults = dict(
        columnsep="0",
        charspace="0",
        textlinewidth="2",
        align="head",
        linecolor="0x00000000",
        column="1",
        fontsize="100",
        fontwidth="-10",
        fontescapement="0",
        fontorientation="0",
        fontweight="400",
        fontfacename="Dutch801 Rm BT Roman",
        textcolor="0x00000000",
        wordspace="25",
        letterspace="0",
        baselineskip="120",
        linespace="10",
        parindent="0",
        parskip="0",
        textbgcolor="0xFF000000",
    )

    alsoAllow = [
        "empdotscode",
        "empdotsfontname",
        "refempdotsfont",
        "rubyadjust",
        "rubyalign",
        "rubyoverhang",
        "empdotsposition",
        "emplinetype",
        "emplineposition",
    ]

    validSettings = [_ for _ in baseDefaults.keys()] + alsoAllow

    defaults = baseDefaults.copy()

    def __init__(self: _typing.Self, **overrides: _typing.Any) -> None:
        """
        Initialize and validate the textstyle state.

        Example:
            Exercise TextStyle.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param overrides: Value supplied for overrides under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsStyle.__init__(self, "TextStyle", self.defaults, alsoAllow=self.alsoAllow, **overrides)

    def copy(self: _typing.Self) -> _typing.Any:
        """
        Perform the copy operation under explicit file-format and conversion rules.

        Example:
            Exercise TextStyle.copy through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        tb = TextStyle()
        tb.attrs = self.attrs.copy()
        return tb


class BlockStyle(LrsStyle):
    """
    The block style of a TextBlock. Default is an expandable 560 pixel wide area with no space for headers or footers.

    Example:
        Exercise BlockStyle through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    baseDefaults = dict(
        bgimagemode="fix",
        framemode="square",
        blockwidth="560",
        blockheight="100",
        blockrule="horz-adjustable",
        layout="LrTb",
        framewidth="0",
        framecolor="0x00000000",
        topskip="0",
        sidemargin="0",
        footskip="0",
        bgcolor="0xFF000000",
    )

    validSettings = baseDefaults.keys()
    defaults = baseDefaults.copy()

    def __init__(self: _typing.Self, **overrides: _typing.Any) -> None:
        """
        Initialize and validate the blockstyle state.

        Example:
            Exercise BlockStyle.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param overrides: Value supplied for overrides under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsStyle.__init__(self, "BlockStyle", self.defaults, **overrides)

    def copy(self: _typing.Self) -> _typing.Any:
        """
        Perform the copy operation under explicit file-format and conversion rules.

        Example:
            Exercise BlockStyle.copy through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        tb = BlockStyle()
        tb.attrs = self.attrs.copy()
        return tb


class PageStyle(LrsStyle):
    """
    Setting Value Default -------- ----- ------- evensidemargin pixels 20 oddsidemargin pixels 20 topmargin pixels 20

    Example:
        Exercise PageStyle through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    baseDefaults = dict(
        topmargin="20",
        headheight="0",
        headsep="0",
        oddsidemargin="20",
        textheight="747",
        textwidth="575",
        footspace="0",
        evensidemargin="20",
        footheight="0",
        layout="LrTb",
        bgimagemode="fix",
        pageposition="any",
        setwaitprop="noreplay",
        setemptyview="show",
    )

    alsoAllow = [
        "header",
        "evenheader",
        "oddheader",
        "footer",
        "evenfooter",
        "oddfooter",
    ]

    validSettings = [_ for _ in baseDefaults.keys()] + alsoAllow
    defaults = baseDefaults.copy()

    @classmethod
    def translateHeaderAndFooter(selfClass: _typing.Any, parent: _typing.Any, settings: _typing.Any) -> None:
        """
        Perform the translateHeaderAndFooter operation under explicit file-format and conversion rules.

        Example:
            Exercise PageStyle.translateHeaderAndFooter through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :param settings: Value supplied for settings under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        selfClass._fixup(parent, "header", settings)
        selfClass._fixup(parent, "footer", settings)

    @classmethod
    def _fixup(selfClass: _typing.Any, parent: _typing.Any, basename: _typing.Any, settings: _typing.Any) -> None:
        """
        Perform the fixup operation under explicit file-format and conversion rules.

        Example:
            Exercise PageStyle. fixup through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :param basename: Value supplied for basename under the utility contract.
        :param settings: Value supplied for settings under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        evenbase = "even" + basename
        oddbase = "odd" + basename
        if basename in settings:
            baseObj = settings[basename]
            del settings[basename]
            settings[evenbase] = settings[oddbase] = baseObj

        if evenbase in settings:
            evenObj = settings[evenbase]
            del settings[evenbase]
            if evenObj.parent is None:
                parent.append(evenObj)
            settings[evenbase + "id"] = str(evenObj.objId)

        if oddbase in settings:
            oddObj = settings[oddbase]
            del settings[oddbase]
            if oddObj.parent is None:
                parent.append(oddObj)
            settings[oddbase + "id"] = str(oddObj.objId)

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise PageStyle.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.objectsAppended:
            return
        PageStyle.translateHeaderAndFooter(parent, self.attrs)
        self.objectsAppended = True

    def __init__(self: _typing.Self, **settings: _typing.Any) -> None:
        # self.fixHeaderSettings(settings)
        """
        Initialize and validate the pagestyle state.

        Example:
            Exercise PageStyle.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsStyle.__init__(self, "PageStyle", self.defaults, alsoAllow=self.alsoAllow, **settings)


class Page(LrsObject, LrsContainer):
    """
    Pages are added to Books. Pages can be supplied a PageStyle. If they are not, Page.defaultPageStyle will be used.

    Example:
        Exercise Page through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    defaultPageStyle = PageStyle()

    def __init__(self: _typing.Self, pageStyle: _typing.Any = defaultPageStyle, **settings: _typing.Any) -> None:
        """
        Initialize and validate the page state.

        Example:
            Exercise Page.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param pageStyle: Value supplied for pageStyle under the utility contract.
        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [TextBlock, BlockSpace, RuledLine, ImageBlock, Canvas])

        self.pageStyle = pageStyle

        for settingName in settings.keys():
            if settingName not in PageStyle.defaults and settingName not in PageStyle.alsoAllow:
                raise LrsError("setting %s not allowed on Page" % settingName)

        self.settings = settings.copy()

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        PageStyle.translateHeaderAndFooter(parent, self.settings)

        self.pageStyle.appendReferencedObjects(parent)

        if self.pageStyle.parent is None:
            parent.append(self.pageStyle)

        LrsContainer.appendReferencedObjects(self, parent)

    def RuledLine(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the RuledLine operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.RuledLine through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        rl = RuledLine(*args, **kwargs)
        self.append(rl)
        return rl

    def BlockSpace(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the BlockSpace operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.BlockSpace through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        bs = BlockSpace(*args, **kwargs)
        self.append(bs)
        return bs

    def TextBlock(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Create and append a new text block (shortcut).

        Example:
            Exercise Page.TextBlock through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        tb = TextBlock(*args, **kwargs)
        self.append(tb)
        return tb

    def ImageBlock(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Create and append and new Image block (shorthand).

        Example:
            Exercise Page.ImageBlock through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ib = ImageBlock(*args, **kwargs)
        self.append(ib)
        return ib

    def addLrfObject(self: _typing.Self, objId: _typing.Any) -> None:
        """
        Perform the addLrfObject operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.addLrfObject through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param objId: Value supplied for objId under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.stream.appendLrfTag(LrfTag("Link", objId))

    def appendLrfTag(self: _typing.Self, lrfTag: _typing.Any) -> None:
        """
        Perform the appendLrfTag operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.appendLrfTag through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfTag: Value supplied for lrfTag under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.stream.appendLrfTag(lrfTag)

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        # tags:
        # ObjectList
        # Link to pagestyle
        # Parent page tree id
        # stream of tags

        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        p = LrfObject("Page", self.objId)
        lrfWriter.append(p)

        page_content = set()
        self.stream = LrfTagStream(0)
        for content in self.contents:
            content.toLrfContainer(lrfWriter, self)
            if hasattr(content, "getReferencedObjIds"):
                page_content.update(content.getReferencedObjIds())

        # print "page contents:", pageContent
        # ObjectList not needed and causes slowdown in SONY LRF renderer
        # p.appendLrfTag(LrfTag("ObjectList", pageContent))
        p.appendLrfTag(LrfTag("Link", self.pageStyle.objId))
        p.appendLrfTag(LrfTag("ParentPageTree", lrfWriter.getPageTreeId()))
        p.appendTagDict(self.settings)
        p.appendLrfTags(self.stream.getStreamTags(lrfWriter.getSourceEncoding()))

    def toElement(self: _typing.Self, sourceEncoding: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param sourceEncoding: Value supplied for sourceEncoding under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        page = self.lrsObjectElement("Page")
        page.set("pagestyle", self.pageStyle.getLabel())
        page.attrib.update(self.settings)

        for content in self.contents:
            page.append(content.toElement(sourceEncoding))

        return page


class TextBlock(LrsObject, LrsContainer):
    """
    TextBlocks are added to Pages. They hold Paragraphs or CRs.

    Example:
        Exercise TextBlock through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    defaultTextStyle = TextStyle()
    defaultBlockStyle = BlockStyle()

    def __init__(self: _typing.Self, textStyle: _typing.Any = defaultTextStyle, blockStyle: _typing.Any = defaultBlockStyle, **settings: _typing.Any) -> None:
        """
        Create TextBlock.

        Example:
            Exercise TextBlock.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param textStyle: Value supplied for textStyle under the utility contract.
        :param blockStyle: Value supplied for blockStyle under the utility contract.
        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [Paragraph, CR])

        self.textSettings = {}
        self.blockSettings = {}

        for name, value in settings.items():
            if name in TextStyle.validSettings:
                self.textSettings[name] = value
            elif name in BlockStyle.validSettings:
                self.blockSettings[name] = value
            elif name == "toclabel":
                self.tocLabel = value
            else:
                raise LrsError("%s not a valid setting for TextBlock" % name)

        self.textStyle = textStyle
        self.blockStyle = blockStyle

        # create a textStyle with our current text settings (for Span to find)
        self.currentTextStyle = textStyle.copy() if self.textSettings else textStyle
        self.currentTextStyle.attrs.update(self.textSettings)

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise TextBlock.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.textStyle.parent is None:
            parent.append(self.textStyle)

        if self.blockStyle.parent is None:
            parent.append(self.blockStyle)

        LrsContainer.appendReferencedObjects(self, parent)

    def Paragraph(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Create and append a Paragraph to this TextBlock. A CR is automatically inserted after the Paragraph. To avoid this behavior, create the Paragraph and append it to the TextBlock in a separate call.

        Example:
            Exercise TextBlock.Paragraph through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        p = Paragraph(*args, **kwargs)
        self.append(p)
        self.append(CR())
        return p

    def toElement(self: _typing.Self, sourceEncoding: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise TextBlock.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param sourceEncoding: Value supplied for sourceEncoding under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        tb = self.lrsObjectElement("TextBlock", labelName="Block")
        tb.attrib.update(self.textSettings)
        tb.attrib.update(self.blockSettings)
        tb.set("textstyle", self.textStyle.getLabel())
        tb.set("blockstyle", self.blockStyle.getLabel())
        if hasattr(self, "tocLabel"):
            tb.set("toclabel", self.tocLabel)

        for content in self.contents:
            tb.append(content.toElement(sourceEncoding))

        return tb

    def getReferencedObjIds(self: _typing.Self) -> _typing.Any:
        """
        Perform the getReferencedObjIds operation under explicit file-format and conversion rules.

        Example:
            Exercise TextBlock.getReferencedObjIds through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ids = [self.objId, self.extraId, self.blockStyle.objId, self.textStyle.objId]
        for content in self.contents:
            if hasattr(content, "getReferencedObjIds"):
                ids.extend(content.getReferencedObjIds())

        return ids

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise TextBlock.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.toLrfContainer(lrfWriter, lrfWriter)

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        # id really belongs to the outer block
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise TextBlock.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param container: Value supplied for container under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        extraId = LrsObject.getNextObjId()

        b = LrfObject("Block", self.objId)
        b.appendLrfTag(LrfTag("Link", self.blockStyle.objId))
        b.appendLrfTags(LrfTagStream(0, [LrfTag("Link", extraId)]).getStreamTags(lrfWriter.getSourceEncoding()))
        b.appendTagDict(self.blockSettings)
        container.addLrfObject(b.objId)
        lrfWriter.append(b)

        tb = LrfObject("TextBlock", extraId)
        tb.appendLrfTag(LrfTag("Link", self.textStyle.objId))
        tb.appendTagDict(self.textSettings)

        stream = LrfTagStream(STREAM_COMPRESSED)
        for content in self.contents:
            content.toLrfContainer(lrfWriter, stream)

        if lrfWriter.saveStreamTags:  # true only if testing
            tb.saveStreamTags = stream.tags

        tb.appendLrfTags(
            stream.getStreamTags(
                lrfWriter.getSourceEncoding(),
                optimizeTags=lrfWriter.optimizeTags,
                optimizeCompression=lrfWriter.optimizeCompression,
            )
        )
        lrfWriter.append(tb)

        self.extraId = extraId


class Paragraph(LrsContainer):
    """
    Note: <P> alone does not make a paragraph. Only a CR inserted into a text block right after a <P> makes a real paragraph. Two Paragraphs appended in a row act like a single Paragraph.

    Example:
        Exercise Paragraph through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, text: _typing.Any = None) -> None:
        """
        Initialize and validate the paragraph state.

        Example:
            Exercise Paragraph.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [Text, CR, DropCaps, CharButton, LrsSimpleChar1, six_string_types])
        if text is not None:
            if isinstance(text, six_string_types):
                text = Text(text)
            self.append(text)

    def CR(self: _typing.Self) -> _typing.Any:
        # Okay, here's a single autoappender for this common operation
        """
        Perform the CR operation under explicit file-format and conversion rules.

        Example:
            Exercise Paragraph.CR through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        cr = CR()
        self.append(cr)
        return cr

    def getReferencedObjIds(self: _typing.Self) -> _typing.Any:
        """
        Perform the getReferencedObjIds operation under explicit file-format and conversion rules.

        Example:
            Exercise Paragraph.getReferencedObjIds through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ids = []
        for content in self.contents:
            if hasattr(content, "getReferencedObjIds"):
                ids.extend(content.getReferencedObjIds())
        return ids

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, parent: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise Paragraph.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        parent.appendLrfTag(LrfTag("pstart", 0))
        for content in self.contents:
            content.toLrfContainer(lrfWriter, parent)
        parent.appendLrfTag(LrfTag("pend"))

    def toElement(self: _typing.Self, sourceEncoding: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Paragraph.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param sourceEncoding: Value supplied for sourceEncoding under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        p = Element("P")
        appendTextElements(p, self.contents, sourceEncoding)
        return p


class LrsTextTag(LrsContainer):
    """
    Provide the lrstexttag contract for validated ebook processing.

    Example:
        Exercise LrsTextTag through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, text: _typing.Any, validContents: _typing.Any) -> None:
        """
        Initialize and validate the lrstexttag state.

        Example:
            Exercise LrsTextTag.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param text: Text parsed, normalized or rendered.
        :param validContents: Value supplied for validContents under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [Text, six_string_types] + validContents)
        if text is not None:
            self.append(text)

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, parent: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsTextTag.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if hasattr(self, "tagName"):
            tagName = self.tagName
        else:
            tagName = self.__class__.__name__

        parent.appendLrfTag(LrfTag(tagName))

        for content in self.contents:
            content.toLrfContainer(lrfWriter, parent)

        parent.appendLrfTag(LrfTag(tagName + "End"))

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsTextTag.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if hasattr(self, "tagName"):
            tagName = self.tagName
        else:
            tagName = self.__class__.__name__

        p = Element(tagName)
        appendTextElements(p, self.contents, se)
        return p


class LrsSimpleChar1(object):
    """
    Provide the lrssimplechar1 contract for validated ebook processing.

    Example:
        Exercise LrsSimpleChar1 through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def isEmpty(self: _typing.Self) -> bool:
        """
        Perform the isEmpty operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsSimpleChar1.isEmpty through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for content in self.contents:
            if not content.isEmpty():
                return False
        return True

    def hasFollowingContent(self: _typing.Self) -> bool:
        """
        Perform the hasFollowingContent operation under explicit file-format and conversion rules.

        Example:
            Exercise LrsSimpleChar1.hasFollowingContent through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        foundSelf = False
        for content in self.parent.contents:
            if content == self:
                foundSelf = True
            elif foundSelf:
                if not content.isEmpty():
                    return True
        return False


class DropCaps(LrsTextTag):
    """
    Provide the dropcaps contract for validated ebook processing.

    Example:
        Exercise DropCaps through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, line: int = 1) -> None:
        """
        Initialize and validate the dropcaps state.

        Example:
            Exercise DropCaps.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param line: Value supplied for line under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsTextTag.__init__(self, None, [LrsSimpleChar1])
        if int(line) <= 0:
            raise LrsError("A DrawChar must span at least one line.")
        self.line = int(line)

    def isEmpty(self: _typing.Self) -> bool:
        """
        Perform the isEmpty operation under explicit file-format and conversion rules.

        Example:
            Exercise DropCaps.isEmpty through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.text is None or not self.text.strip()

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise DropCaps.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        elem = Element("DrawChar", line=str(self.line))
        appendTextElements(elem, self.contents, se)
        return elem

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, parent: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise DropCaps.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        parent.appendLrfTag(LrfTag("DrawChar", (int(self.line),)))

        for content in self.contents:
            content.toLrfContainer(lrfWriter, parent)

        parent.appendLrfTag(LrfTag("DrawCharEnd"))


class Button(LrsObject, LrsContainer):
    """
    Provide the button contract for validated ebook processing.

    Example:
        Exercise Button through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, **settings: _typing.Any) -> None:
        """
        Initialize and validate the button state.

        Example:
            Exercise Button.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsObject.__init__(self, **settings)
        LrsContainer.__init__(self, [PushButton])

    def findJumpToRefs(self: _typing.Self) -> tuple[_typing.Any, ...]:
        """
        Perform the findJumpToRefs operation under explicit file-format and conversion rules.

        Example:
            Exercise Button.findJumpToRefs through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for sub1 in self.contents:
            if isinstance(sub1, PushButton):
                for sub2 in sub1.contents:
                    if isinstance(sub2, JumpTo):
                        return sub2.textBlock.objId, sub2.textBlock.parent.objId
        raise LrsError("%s has no PushButton or JumpTo subs" % self.__class__.__name__)

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise Button.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        (refobj, refpage) = self.findJumpToRefs()
        # print "Button writing JumpTo refobj=", jumpto.refobj, ", and refpage=", jumpto.refpage
        button = LrfObject("Button", self.objId)
        button.appendLrfTag(LrfTag("buttonflags", 0x10))  # pushbutton
        button.appendLrfTag(LrfTag("PushButtonStart"))
        button.appendLrfTag(LrfTag("buttonactions"))
        button.appendLrfTag(LrfTag("jumpto", (int(refpage), int(refobj))))
        button.append(LrfTag("endbuttonactions"))
        button.appendLrfTag(LrfTag("PushButtonEnd"))
        lrfWriter.append(button)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Button.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        b = self.lrsObjectElement("Button")

        for content in self.contents:
            b.append(content.toElement(se))

        return b


class ButtonBlock(Button):
    """
    Provide the buttonblock contract for validated ebook processing.

    Example:
        Exercise ButtonBlock through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    pass


class PushButton(LrsContainer):
    """
    Provide the pushbutton contract for validated ebook processing.

    Example:
        Exercise PushButton through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, **settings: _typing.Any) -> None:
        """
        Initialize and validate the pushbutton state.

        Example:
            Exercise PushButton.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [JumpTo])

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise PushButton.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        b = Element("PushButton")

        for content in self.contents:
            b.append(content.toElement(se))

        return b


class JumpTo(LrsContainer):
    """
    Provide the jumpto contract for validated ebook processing.

    Example:
        Exercise JumpTo through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, textBlock: _typing.Any) -> None:
        """
        Initialize and validate the jumpto state.

        Example:
            Exercise JumpTo.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param textBlock: Value supplied for textBlock under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [])
        self.textBlock = textBlock

    def setTextBlock(self: _typing.Self, textBlock: _typing.Any) -> None:
        """
        Perform the setTextBlock operation under explicit file-format and conversion rules.

        Example:
            Exercise JumpTo.setTextBlock through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param textBlock: Value supplied for textBlock under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.textBlock = textBlock

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise JumpTo.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return Element(
            "JumpTo",
            refpage=str(self.textBlock.parent.objId),
            refobj=str(self.textBlock.objId),
        )


class Plot(LrsSimpleChar1, LrsContainer):

    """
    Provide the plot contract for validated ebook processing.

    Example:
        Exercise Plot through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    ADJUSTMENT_VALUES = {"center": 1, "baseline": 2, "top": 3, "bottom": 4}

    def __init__(self: _typing.Self, obj: _typing.Any, xsize: int = 0, ysize: int = 0, adjustment: _typing.Any = None) -> None:
        """
        Initialize and validate the plot state.

        Example:
            Exercise Plot.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param xsize: Value supplied for xsize under the utility contract.
        :param ysize: Value supplied for ysize under the utility contract.
        :param adjustment: Value supplied for adjustment under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [])
        if obj is not None:
            self.setObj(obj)
        if xsize < 0 or ysize < 0:
            raise LrsError("Sizes must be positive semi-definite")
        self.xsize = int(xsize)
        self.ysize = int(ysize)
        if adjustment and adjustment not in Plot.ADJUSTMENT_VALUES.keys():
            raise LrsError("adjustment must be one of" + six_unicode(Plot.ADJUSTMENT_VALUES.keys()))
        self.adjustment = adjustment

    def setObj(self: _typing.Self, obj: _typing.Any) -> None:
        """
        Perform the setObj operation under explicit file-format and conversion rules.

        Example:
            Exercise Plot.setObj through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not isinstance(obj, (Image, Button)):
            raise LrsError("Plot elements can only refer to Image or Button elements")
        self.obj = obj

    def getReferencedObjIds(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getReferencedObjIds operation under explicit file-format and conversion rules.

        Example:
            Exercise Plot.getReferencedObjIds through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [self.obj.objId]

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise Plot.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.obj.parent is None:
            parent.append(self.obj)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Plot.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        elem = Element(
            "Plot",
            xsize=str(self.xsize),
            ysize=str(self.ysize),
            refobj=str(self.obj.objId),
        )
        if self.adjustment:
            elem.set("adjustment", self.adjustment)
        return elem

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, parent: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise Plot.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        adj = self.adjustment if self.adjustment else "bottom"
        params = (
            int(self.xsize),
            int(self.ysize),
            int(self.obj.objId),
            Plot.ADJUSTMENT_VALUES[adj],
        )
        parent.appendLrfTag(LrfTag("Plot", params))


class Text(LrsContainer):
    """
    A object that represents raw text. Does not have a toElement.

    Example:
        Exercise Text through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, text: _typing.Any) -> None:
        """
        Initialize and validate the text state.

        Example:
            Exercise Text.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [])
        self.text = text

    def isEmpty(self: _typing.Self) -> bool:
        """
        Perform the isEmpty operation under explicit file-format and conversion rules.

        Example:
            Exercise Text.isEmpty through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return not self.text or not self.text.strip()

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, parent: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise Text.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.text:
            if isinstance(self.text, str):
                parent.appendLrfTag(LrfTag("rawtext", self.text))
            else:
                parent.appendLrfTag(LrfTag("textstring", self.text))


class CR(LrsSimpleChar1, LrsContainer):
    """
    A line break (when appended to a Paragraph) or a paragraph break (when appended to a TextBlock).

    Example:
        Exercise CR through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the cr state.

        Example:
            Exercise CR.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [])

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise CR.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return Element("CR")

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, parent: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise CR.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        parent.appendLrfTag(LrfTag("CR"))


class Italic(LrsSimpleChar1, LrsTextTag):
    """
    Provide the italic contract for validated ebook processing.

    Example:
        Exercise Italic through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, text: _typing.Any = None) -> None:
        """
        Initialize and validate the italic state.

        Example:
            Exercise Italic.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: None; validated state is stored on the receiving object.
        """
        LrsTextTag.__init__(self, text, [LrsSimpleChar1])


class Sub(LrsSimpleChar1, LrsTextTag):
    """
    Provide the sub contract for validated ebook processing.

    Example:
        Exercise Sub through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, text: _typing.Any = None) -> None:
        """
        Initialize and validate the sub state.

        Example:
            Exercise Sub.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: None; validated state is stored on the receiving object.
        """
        LrsTextTag.__init__(self, text, [])


class Sup(LrsSimpleChar1, LrsTextTag):
    """
    Provide the sup contract for validated ebook processing.

    Example:
        Exercise Sup through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, text: _typing.Any = None) -> None:
        """
        Initialize and validate the sup state.

        Example:
            Exercise Sup.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: None; validated state is stored on the receiving object.
        """
        LrsTextTag.__init__(self, text, [])


class NoBR(LrsSimpleChar1, LrsTextTag):
    """
    Provide the nobr contract for validated ebook processing.

    Example:
        Exercise NoBR through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, text: _typing.Any = None) -> None:
        """
        Initialize and validate the nobr state.

        Example:
            Exercise NoBR.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: None; validated state is stored on the receiving object.
        """
        LrsTextTag.__init__(self, text, [LrsSimpleChar1])


class Space(LrsSimpleChar1, LrsContainer):
    """
    Provide the space contract for validated ebook processing.

    Example:
        Exercise Space through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, xsize: int = 0, x: int = 0) -> None:
        """
        Initialize and validate the space state.

        Example:
            Exercise Space.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param xsize: Value supplied for xsize under the utility contract.
        :param x: Value supplied for x under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [])
        if xsize == 0 and x != 0:
            xsize = x
        self.xsize = xsize

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Space.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.xsize == 0:
            return

        return Element("Space", xsize=str(self.xsize))

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise Space.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param container: Value supplied for container under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.xsize != 0:
            container.appendLrfTag(LrfTag("Space", self.xsize))


class Box(LrsSimpleChar1, LrsContainer):
    """
    Draw a box around text. Unfortunately, does not seem to do anything on the PRS-500.

    Example:
        Exercise Box through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, linetype: str = "solid") -> None:
        """
        Initialize and validate the box state.

        Example:
            Exercise Box.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param linetype: Value supplied for linetype under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [Text, six_string_types])
        if linetype not in LINE_TYPE_ENCODING:
            raise LrsError(linetype + " is not a valid line type")
        self.linetype = linetype

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Box.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        e = Element("Box", linetype=self.linetype)
        appendTextElements(e, self.contents, se)
        return e

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise Box.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param container: Value supplied for container under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        container.appendLrfTag(LrfTag("Box", self.linetype))
        for content in self.contents:
            content.toLrfContainer(lrfWriter, container)
        container.appendLrfTag(LrfTag("BoxEnd"))


class Span(LrsSimpleChar1, LrsContainer):
    """
    Provide the span contract for validated ebook processing.

    Example:
        Exercise Span through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self, text: _typing.Any = None, **attrs: _typing.Any) -> None:
        """
        Initialize and validate the span state.

        Example:
            Exercise Span.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param text: Text parsed, normalized or rendered.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [LrsSimpleChar1, Text, six_string_types])
        if text is not None:
            if isinstance(text, six_string_types):
                text = Text(text)
            self.append(text)

        for attrname in attrs.keys():
            if attrname not in TextStyle.defaults and attrname not in TextStyle.alsoAllow:
                raise LrsError("setting %s not allowed on Span" % attrname)
        self.attrs = attrs

    def findCurrentTextStyle(self: _typing.Self) -> _typing.Any:
        """
        Perform the findCurrentTextStyle operation under explicit file-format and conversion rules.

        Example:
            Exercise Span.findCurrentTextStyle through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        parent = self.parent
        while 1:
            if parent is None or hasattr(parent, "currentTextStyle"):
                break
            parent = parent.parent

        if parent is None:
            raise LrsError("no enclosing current TextStyle found")

        return parent.currentTextStyle

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:

        # find the currentTextStyle
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise Span.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param container: Value supplied for container under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        oldTextStyle = self.findCurrentTextStyle()

        # set the attributes we want changed
        for (name, value) in self.attrs.items():
            if name in oldTextStyle.attrs and oldTextStyle.attrs[name] == self.attrs[name]:
                self.attrs.pop(name)
            else:
                container.appendLrfTag(LrfTag(name, value))

        # set a currentTextStyle so nested span can put things back
        oldTextStyle = self.findCurrentTextStyle()
        self.currentTextStyle = oldTextStyle.copy()
        self.currentTextStyle.attrs.update(self.attrs)

        for content in self.contents:
            content.toLrfContainer(lrfWriter, container)

        # put the attributes back the way we found them
        # the attributes persist beyond the next </P>
        # if self.hasFollowingContent():
        for name in self.attrs.keys():
            container.appendLrfTag(LrfTag(name, oldTextStyle.attrs[name]))

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Span.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        element = Element("Span")
        for (key, value) in self.attrs.items():
            element.set(key, str(value))

        appendTextElements(element, self.contents, se)
        return element


class EmpLine(LrsTextTag, LrsSimpleChar1):

    """
    Provide the empline contract for validated ebook processing.

    Example:
        Exercise EmpLine through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    emplinetypes = ["none", "solid", "dotted", "dashed", "double"]
    emplinepositions = ["before", "after"]

    def __init__(self: _typing.Self, text: _typing.Any = None, emplineposition: str = "before", emplinetype: str = "solid") -> None:
        """
        Initialize and validate the empline state.

        Example:
            Exercise EmpLine.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param text: Text parsed, normalized or rendered.
        :param emplineposition: Value supplied for emplineposition under the utility
            contract.
        :param emplinetype: Value supplied for emplinetype under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsTextTag.__init__(self, text, [LrsSimpleChar1])
        if emplineposition not in self.__class__.emplinepositions:
            raise LrsError("emplineposition for an EmpLine must be one of: " + str(self.__class__.emplinepositions))
        if emplinetype not in self.__class__.emplinetypes:
            raise LrsError("emplinetype for an EmpLine must be one of: " + str(self.__class__.emplinetypes))

        self.emplinetype = emplinetype
        self.emplineposition = emplineposition

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, parent: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise EmpLine.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        parent.appendLrfTag(LrfTag(self.__class__.__name__, (self.emplineposition, self.emplinetype)))
        parent.appendLrfTag(LrfTag("emplineposition", self.emplineposition))
        parent.appendLrfTag(LrfTag("emplinetype", self.emplinetype))
        for content in self.contents:
            content.toLrfContainer(lrfWriter, parent)

        parent.appendLrfTag(LrfTag(self.__class__.__name__ + "End"))

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise EmpLine.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        element = Element(self.__class__.__name__)
        element.set("emplineposition", self.emplineposition)
        element.set("emplinetype", self.emplinetype)

        appendTextElements(element, self.contents, se)
        return element


class Bold(Span):
    """
    There is no known "bold" lrf tag. Use Span with a fontweight in LRF, but use the word Bold in the LRS.

    Example:
        Exercise Bold through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, text: _typing.Any = None) -> None:
        """
        Initialize and validate the bold state.

        Example:
            Exercise Bold.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param text: Text parsed, normalized or rendered.
        :return: None; validated state is stored on the receiving object.
        """
        Span.__init__(self, text, fontweight=800)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Bold.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        e = Element("Bold")
        appendTextElements(e, self.contents, se)
        return e


class BlockSpace(LrsContainer):
    """
    Can be appended to a page to move the text point.

    Example:
        Exercise BlockSpace through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, xspace: int = 0, yspace: int = 0, x: int = 0, y: int = 0) -> None:
        """
        Initialize and validate the blockspace state.

        Example:
            Exercise BlockSpace.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param xspace: Value supplied for xspace under the utility contract.
        :param yspace: Value supplied for yspace under the utility contract.
        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [])
        if xspace == 0 and x != 0:
            xspace = x
        if yspace == 0 and y != 0:
            yspace = y
        self.xspace = xspace
        self.yspace = yspace

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise BlockSpace.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param container: Value supplied for container under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.xspace != 0:
            container.appendLrfTag(LrfTag("xspace", self.xspace))
        if self.yspace != 0:
            container.appendLrfTag(LrfTag("yspace", self.yspace))

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise BlockSpace.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        element = Element("BlockSpace")

        if self.xspace != 0:
            element.attrib["xspace"] = str(self.xspace)
        if self.yspace != 0:
            element.attrib["yspace"] = str(self.yspace)

        return element


class CharButton(LrsSimpleChar1, LrsContainer):
    """
    Define the text and target of a CharButton. Must be passed a JumpButton that is the destination of the CharButton.

    Example:
        Exercise CharButton through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, button: _typing.Any, text: _typing.Any = None) -> None:
        """
        Initialize and validate the charbutton state.

        Example:
            Exercise CharButton.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param button: Value supplied for button under the utility contract.
        :param text: Text parsed, normalized or rendered.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [basestring, Text, LrsSimpleChar1])
        self.button = None
        if button != None:
            self.setButton(button)

        if text is not None:
            self.append(text)

    def setButton(self: _typing.Self, button: _typing.Any) -> None:
        """
        Perform the setButton operation under explicit file-format and conversion rules.

        Example:
            Exercise CharButton.setButton through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param button: Value supplied for button under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not isinstance(button, (JumpButton, Button)):
            raise LrsError("CharButton button must be a JumpButton or Button")

        self.button = button

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise CharButton.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.button.parent is None:
            parent.append(self.button)

    def getReferencedObjIds(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getReferencedObjIds operation under explicit file-format and conversion rules.

        Example:
            Exercise CharButton.getReferencedObjIds through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [self.button.objId]

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise CharButton.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param container: Value supplied for container under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        container.appendLrfTag(LrfTag("CharButton", self.button.objId))

        for content in self.contents:
            content.toLrfContainer(lrfWriter, container)

        container.appendLrfTag(LrfTag("CharButtonEnd"))

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise CharButton.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        cb = Element("CharButton", refobj=str(self.button.objId))
        appendTextElements(cb, self.contents, se)
        return cb


class Objects(LrsContainer):
    """
    Provide the objects contract for validated ebook processing.

    Example:
        Exercise Objects through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the objects state.

        Example:
            Exercise Objects.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(
            self,
            [
                JumpButton,
                TextBlock,
                HeaderOrFooter,
                ImageStream,
                Image,
                ImageBlock,
                Button,
                ButtonBlock,
            ],
        )

        self.appendJumpButton = (
            self.appendTextBlock
        ) = (
            self.appendHeader
        ) = self.appendFooter = self.appendImageStream = self.appendImage = self.appendImageBlock = self.append

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getMethods operation under explicit file-format and conversion rules.

        Example:
            Exercise Objects.getMethods through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [
            "JumpButton",
            "appendJumpButton",
            "TextBlock",
            "appendTextBlock",
            "Header",
            "appendHeader",
            "Footer",
            "appendFooter",
            "ImageBlock",
            "ImageStream",
            "appendImageStream",
            "Image",
            "appendImage",
            "appendImageBlock",
        ]

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getSettings operation under explicit file-format and conversion rules.

        Example:
            Exercise Objects.getSettings through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []

    def ImageBlock(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the ImageBlock operation under explicit file-format and conversion rules.

        Example:
            Exercise Objects.ImageBlock through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ib = ImageBlock(*args, **kwargs)
        self.append(ib)
        return ib

    def JumpButton(self: _typing.Self, textBlock: _typing.Any) -> _typing.Any:
        """
        Perform the JumpButton operation under explicit file-format and conversion rules.

        Example:
            Exercise Objects.JumpButton through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param textBlock: Value supplied for textBlock under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        b = JumpButton(textBlock)
        self.append(b)
        return b

    def TextBlock(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the TextBlock operation under explicit file-format and conversion rules.

        Example:
            Exercise Objects.TextBlock through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        tb = TextBlock(*args, **kwargs)
        self.append(tb)
        return tb

    def Header(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the Header operation under explicit file-format and conversion rules.

        Example:
            Exercise Objects.Header through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        h = Header(*args, **kwargs)
        self.append(h)
        return h

    def Footer(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the Footer operation under explicit file-format and conversion rules.

        Example:
            Exercise Objects.Footer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        h = Footer(*args, **kwargs)
        self.append(h)
        return h

    def ImageStream(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the ImageStream operation under explicit file-format and conversion rules.

        Example:
            Exercise Objects.ImageStream through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        i = ImageStream(*args, **kwargs)
        self.append(i)
        return i

    def Image(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the Image operation under explicit file-format and conversion rules.

        Example:
            Exercise Objects.Image through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        i = Image(*args, **kwargs)
        self.append(i)
        return i

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Objects.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        o = Element("Objects")

        for content in self.contents:
            o.append(content.toElement(se))

        return o

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise Objects.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for content in self.contents:
            content.toLrf(lrfWriter)


class JumpButton(LrsObject, LrsContainer):
    """
    The target of a CharButton. Needs a parented TextBlock to jump to. Actually creates several elements in the XML. JumpButtons must be eventually appended to a Book (actually, an Object.)

    Example:
        Exercise JumpButton through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, textBlock: _typing.Any) -> None:
        """
        Initialize and validate the jumpbutton state.

        Example:
            Exercise JumpButton.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param textBlock: Value supplied for textBlock under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [])
        self.textBlock = textBlock

    def setTextBlock(self: _typing.Self, textBlock: _typing.Any) -> None:
        """
        Perform the setTextBlock operation under explicit file-format and conversion rules.

        Example:
            Exercise JumpButton.setTextBlock through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param textBlock: Value supplied for textBlock under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.textBlock = textBlock

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise JumpButton.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        button = LrfObject("Button", self.objId)
        button.appendLrfTag(LrfTag("buttonflags", 0x10))  # pushbutton
        button.appendLrfTag(LrfTag("PushButtonStart"))
        button.appendLrfTag(LrfTag("buttonactions"))
        button.appendLrfTag(LrfTag("jumpto", (self.textBlock.parent.objId, self.textBlock.objId)))
        button.append(LrfTag("endbuttonactions"))
        button.appendLrfTag(LrfTag("PushButtonEnd"))
        lrfWriter.append(button)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise JumpButton.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        b = self.lrsObjectElement("Button")
        pb = SubElement(b, "PushButton")
        SubElement(
            pb,
            "JumpTo",
            refpage=str(self.textBlock.parent.objId),
            refobj=str(self.textBlock.objId),
        )
        return b


class RuledLine(LrsContainer, LrsAttributes, LrsObject):
    """
    A line. Default is 500 pixels long, 2 pixels wide.

    Example:
        Exercise RuledLine through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    defaults = dict(linelength="500", linetype="solid", linewidth="2", linecolor="0x00000000")

    def __init__(self: _typing.Self, **settings: _typing.Any) -> None:
        """
        Initialize and validate the ruledline state.

        Example:
            Exercise RuledLine.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [])
        LrsAttributes.__init__(self, self.defaults, **settings)
        LrsObject.__init__(self)

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise RuledLine.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param container: Value supplied for container under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        a = self.attrs
        container.appendLrfTag(
            LrfTag(
                "RuledLine",
                (a["linelength"], a["linetype"], a["linewidth"], a["linecolor"]),
            )
        )

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise RuledLine.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return Element("RuledLine", self.attrs)


class HeaderOrFooter(LrsObject, LrsContainer, LrsAttributes):
    """
    Creates empty header or footer objects. Append PutObj objects to the header or footer to create the text.

    Example:
        Exercise HeaderOrFooter through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    defaults = dict(
        framemode="square",
        layout="LrTb",
        framewidth="0",
        framecolor="0x00000000",
        bgcolor="0xFF000000",
    )

    def __init__(self: _typing.Self, **settings: _typing.Any) -> None:
        """
        Initialize and validate the headerorfooter state.

        Example:
            Exercise HeaderOrFooter.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [PutObj])
        LrsAttributes.__init__(self, self.defaults, **settings)

    def put_object(self: _typing.Self, obj: _typing.Any, x1: _typing.Any, y1: _typing.Any) -> None:
        """
        Perform the put object operation under explicit file-format and conversion rules.

        Example:
            Exercise HeaderOrFooter.put object through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param x1: Value supplied for x1 under the utility contract.
        :param y1: Value supplied for y1 under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.append(PutObj(obj, x1, y1))

    def PutObj(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Perform the PutObj operation under explicit file-format and conversion rules.

        Example:
            Exercise HeaderOrFooter.PutObj through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        p = PutObj(*args, **kwargs)
        self.append(p)
        return p

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise HeaderOrFooter.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        hd = LrfObject(self.__class__.__name__, self.objId)
        hd.appendTagDict(self.attrs)

        stream = LrfTagStream(0)
        for content in self.contents:
            content.toLrfContainer(lrfWriter, stream)

        hd.appendLrfTags(stream.getStreamTags(lrfWriter.getSourceEncoding()))
        lrfWriter.append(hd)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise HeaderOrFooter.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        name = self.__class__.__name__
        labelName = name.lower() + "label"
        hd = self.lrsObjectElement(name, objlabel=labelName)
        hd.attrib.update(self.attrs)

        for content in self.contents:
            hd.append(content.toElement(se))

        return hd


class Header(HeaderOrFooter):
    """
    Provide the header contract for validated ebook processing.

    Example:
        Exercise Header through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    pass


class Footer(HeaderOrFooter):
    """
    Provide the footer contract for validated ebook processing.

    Example:
        Exercise Footer through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    pass


class Canvas(LrsObject, LrsContainer, LrsAttributes):
    """
    Provide the canvas contract for validated ebook processing.

    Example:
        Exercise Canvas through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    defaults = dict(
        framemode="square",
        layout="LrTb",
        framewidth="0",
        framecolor="0x00000000",
        bgcolor="0xFF000000",
        canvasheight=0,
        canvaswidth=0,
        blockrule="block-adjustable",
    )

    def __init__(self: _typing.Self, width: _typing.Any, height: _typing.Any, **settings: _typing.Any) -> None:
        """
        Initialize and validate the canvas state.

        Example:
            Exercise Canvas.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param width: Value supplied for width under the utility contract.
        :param height: Value supplied for height under the utility contract.
        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [PutObj])
        LrsAttributes.__init__(self, self.defaults, **settings)

        self.settings = self.defaults.copy()
        self.settings.update(settings)
        self.settings["canvasheight"] = int(height)
        self.settings["canvaswidth"] = int(width)

    def put_object(self: _typing.Self, obj: _typing.Any, x1: _typing.Any, y1: _typing.Any) -> None:
        """
        Perform the put object operation under explicit file-format and conversion rules.

        Example:
            Exercise Canvas.put object through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param obj: Value supplied for obj under the utility contract.
        :param x1: Value supplied for x1 under the utility contract.
        :param y1: Value supplied for y1 under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.append(PutObj(obj, x1, y1))

    def toElement(self: _typing.Self, source_encoding: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Canvas.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param source_encoding: Value supplied for source encoding under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        el = self.lrsObjectElement("Canvas", **self.settings)
        for po in self.contents:
            el.append(po.toElement(source_encoding))
        return el

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise Canvas.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.toLrfContainer(lrfWriter, lrfWriter)

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise Canvas.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param container: Value supplied for container under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        c = LrfObject("Canvas", self.objId)
        c.appendTagDict(self.settings)
        stream = LrfTagStream(STREAM_COMPRESSED)
        for content in self.contents:
            content.toLrfContainer(lrfWriter, stream)
        if lrfWriter.saveStreamTags:  # true only if testing
            c.saveStreamTags = stream.tags

        c.appendLrfTags(
            stream.getStreamTags(
                lrfWriter.getSourceEncoding(),
                optimizeTags=lrfWriter.optimizeTags,
                optimizeCompression=lrfWriter.optimizeCompression,
            )
        )
        container.addLrfObject(c.objId)
        lrfWriter.append(c)

    def has_text(self: _typing.Self) -> _typing.Any:
        """
        Return whether has text holds for the supplied ebook data.

        Example:
            Exercise Canvas.has text through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: True when the documented condition holds; otherwise False.
        """
        return bool(self.contents)


class PutObj(LrsContainer):
    """
    PutObj holds other objects that are drawn on a Canvas or Header.

    Example:
        Exercise PutObj through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, content: _typing.Any, x1: int = 0, y1: int = 0) -> None:
        """
        Initialize and validate the putobj state.

        Example:
            Exercise PutObj.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param content: Value supplied for content under the utility contract.
        :param x1: Value supplied for x1 under the utility contract.
        :param y1: Value supplied for y1 under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [TextBlock, ImageBlock])
        self.content = content
        self.x1 = int(x1)
        self.y1 = int(y1)

    def setContent(self: _typing.Self, content: _typing.Any) -> None:
        """
        Perform the setContent operation under explicit file-format and conversion rules.

        Example:
            Exercise PutObj.setContent through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param content: Value supplied for content under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.content = content

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise PutObj.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.content.parent is None:
            parent.append(self.content)

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise PutObj.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param container: Value supplied for container under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        container.appendLrfTag(LrfTag("PutObj", (self.x1, self.y1, self.content.objId)))

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise PutObj.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        el = Element("PutObj", x1=str(self.x1), y1=str(self.y1), refobj=str(self.content.objId))
        return el


class ImageStream(LrsObject, LrsContainer):
    """
    Embed an image file into an Lrf.

    Example:
        Exercise ImageStream through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    VALID_ENCODINGS = ["JPEG", "GIF", "BMP", "PNG"]

    def __init__(self: _typing.Self, file: _typing.Any = None, encoding: _typing.Any = None, comment: _typing.Any = None) -> None:
        """
        Initialize and validate the imagestream state.

        Example:
            Exercise ImageStream.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param file: Value supplied for file under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :param comment: Value supplied for comment under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [])
        _checkExists(file)
        self.filename = file
        self.comment = comment
        # TODO: move encoding from extension to lrf module
        if encoding is None:
            extension = os.path.splitext(file)[1]
            if not extension:
                raise LrsError("file must have extension if encoding is not specified")
            extension = extension[1:].upper()

            if extension == "JPG":
                extension = "JPEG"

            encoding = extension
        else:
            encoding = encoding.upper()

        if encoding not in self.VALID_ENCODINGS:
            raise LrsError("encoding or file extension not JPEG, GIF, BMP, or PNG")

        self.encoding = encoding

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise ImageStream.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        imageFile = file(self.filename, "rb")
        imageData = imageFile.read()
        imageFile.close()

        isObj = LrfObject("ImageStream", self.objId)
        if self.comment is not None:
            isObj.appendLrfTag(LrfTag("comment", self.comment))

        streamFlags = IMAGE_TYPE_ENCODING[self.encoding]
        stream = LrfStreamBase(streamFlags, imageData)
        isObj.appendLrfTags(stream.getStreamTags())
        lrfWriter.append(isObj)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise ImageStream.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        element = self.lrsObjectElement(
            "ImageStream",
            objlabel="imagestreamlabel",
            encoding=self.encoding,
            file=self.filename,
        )
        element.text = self.comment
        return element


class Image(LrsObject, LrsContainer, LrsAttributes):

    """
    Provide the image contract for validated ebook processing.

    Example:
        Exercise Image through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """
    defaults = dict()

    def __init__(self: _typing.Self, refstream: _typing.Any, x0: int = 0, x1: int = 0, y0: int = 0, y1: int = 0, xsize: int = 0, ysize: int = 0, **settings: _typing.Any) -> None:
        """
        Initialize and validate the image state.

        Example:
            Exercise Image.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param refstream: Value supplied for refstream under the utility contract.
        :param x0: Value supplied for x0 under the utility contract.
        :param x1: Value supplied for x1 under the utility contract.
        :param y0: Value supplied for y0 under the utility contract.
        :param y1: Value supplied for y1 under the utility contract.
        :param xsize: Value supplied for xsize under the utility contract.
        :param ysize: Value supplied for ysize under the utility contract.
        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [])
        LrsAttributes.__init__(self, self.defaults, settings)
        self.x0, self.y0, self.x1, self.y1 = int(x0), int(y0), int(x1), int(y1)
        self.xsize, self.ysize = int(xsize), int(ysize)
        self.setRefstream(refstream)

    def setRefstream(self: _typing.Self, refstream: _typing.Any) -> None:
        """
        Perform the setRefstream operation under explicit file-format and conversion rules.

        Example:
            Exercise Image.setRefstream through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param refstream: Value supplied for refstream under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.refstream = refstream

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise Image.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.refstream.parent is None:
            parent.append(self.refstream)

    def getReferencedObjIds(self: _typing.Self) -> list[_typing.Any]:
        """
        Perform the getReferencedObjIds operation under explicit file-format and conversion rules.

        Example:
            Exercise Image.getReferencedObjIds through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return [self.objId, self.refstream.objId]

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Image.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        element = self.lrsObjectElement("Image", **self.attrs)
        element.set("refstream", str(self.refstream.objId))
        for name in ["x0", "y0", "x1", "y1", "xsize", "ysize"]:
            element.set(name, str(getattr(self, name)))
        return element

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise Image.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ib = LrfObject("Image", self.objId)
        ib.appendLrfTag(LrfTag("ImageRect", (self.x0, self.y0, self.x1, self.y1)))
        ib.appendLrfTag(LrfTag("ImageSize", (self.xsize, self.ysize)))
        ib.appendLrfTag(LrfTag("RefObjId", self.refstream.objId))
        lrfWriter.append(ib)


class ImageBlock(LrsObject, LrsContainer, LrsAttributes):
    """
    Create an image on a page.

    Example:
        Exercise ImageBlock through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    # TODO: allow other block attributes

    defaults = BlockStyle.baseDefaults.copy()

    def __init__(
        self: _typing.Self,
        refstream: _typing.Any,
        x0: str = "0",
        y0: str = "0",
        x1: str = "600",
        y1: str = "800",
        xsize: str = "600",
        ysize: str = "800",
        blockStyle: _typing.Any = BlockStyle(blockrule="block-fixed"),
        alttext: _typing.Any = None,
        **settings: _typing.Any
    ) -> None:
        """
        Initialize and validate the imageblock state.

        Example:
            Exercise ImageBlock.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param refstream: Value supplied for refstream under the utility contract.
        :param x0: Value supplied for x0 under the utility contract.
        :param y0: Value supplied for y0 under the utility contract.
        :param x1: Value supplied for x1 under the utility contract.
        :param y1: Value supplied for y1 under the utility contract.
        :param xsize: Value supplied for xsize under the utility contract.
        :param ysize: Value supplied for ysize under the utility contract.
        :param blockStyle: Value supplied for blockStyle under the utility contract.
        :param alttext: Value supplied for alttext under the utility contract.
        :param settings: Value supplied for settings under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [Text, Image])
        LrsAttributes.__init__(self, self.defaults, **settings)
        self.x0, self.y0, self.x1, self.y1 = int(x0), int(y0), int(x1), int(y1)
        self.xsize, self.ysize = int(xsize), int(ysize)
        self.setRefstream(refstream)
        self.blockStyle = blockStyle
        self.alttext = alttext

    def setRefstream(self: _typing.Self, refstream: _typing.Any) -> None:
        """
        Perform the setRefstream operation under explicit file-format and conversion rules.

        Example:
            Exercise ImageBlock.setRefstream through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param refstream: Value supplied for refstream under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.refstream = refstream

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the appendReferencedObjects operation under explicit file-format and conversion rules.

        Example:
            Exercise ImageBlock.appendReferencedObjects through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.refstream.parent is None:
            parent.append(self.refstream)

        if self.blockStyle is not None and self.blockStyle.parent is None:
            parent.append(self.blockStyle)

    def getReferencedObjIds(self: _typing.Self) -> _typing.Any:
        """
        Perform the getReferencedObjIds operation under explicit file-format and conversion rules.

        Example:
            Exercise ImageBlock.getReferencedObjIds through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        objects = [self.objId, self.extraId, self.refstream.objId]
        if self.blockStyle is not None:
            objects.append(self.blockStyle.objId)

        return objects

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise ImageBlock.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.toLrfContainer(lrfWriter, lrfWriter)

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        # id really belongs to the outer block

        """
        Perform the toLrfContainer operation under explicit file-format and conversion rules.

        Example:
            Exercise ImageBlock.toLrfContainer through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :param container: Value supplied for container under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        extraId = LrsObject.getNextObjId()

        b = LrfObject("Block", self.objId)
        if self.blockStyle is not None:
            b.appendLrfTag(LrfTag("Link", self.blockStyle.objId))
        b.appendTagDict(self.attrs)

        b.appendLrfTags(LrfTagStream(0, [LrfTag("Link", extraId)]).getStreamTags(lrfWriter.getSourceEncoding()))
        container.addLrfObject(b.objId)
        lrfWriter.append(b)

        ib = LrfObject("Image", extraId)

        ib.appendLrfTag(LrfTag("ImageRect", (self.x0, self.y0, self.x1, self.y1)))
        ib.appendLrfTag(LrfTag("ImageSize", (self.xsize, self.ysize)))
        ib.appendLrfTag(LrfTag("RefObjId", self.refstream.objId))
        if self.alttext:
            ib.appendLrfTag("Comment", self.alttext)

        lrfWriter.append(ib)
        self.extraId = extraId

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise ImageBlock.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        element = self.lrsObjectElement("ImageBlock", **self.attrs)
        element.set("refstream", str(self.refstream.objId))
        for name in ["x0", "y0", "x1", "y1", "xsize", "ysize"]:
            element.set(name, str(getattr(self, name)))
        element.text = self.alttext
        return element


class Font(LrsContainer):
    """
    Allows a TrueType file to be embedded in an Lrf.

    Example:
        Exercise Font through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py
    """

    def __init__(self: _typing.Self, file: _typing.Any = None, fontname: _typing.Any = None, fontfilename: _typing.Any = None, encoding: _typing.Any = None) -> None:
        """
        Initialize and validate the font state.

        Example:
            Exercise Font.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param file: Value supplied for file under the utility contract.
        :param fontname: Value supplied for fontname under the utility contract.
        :param fontfilename: Value supplied for fontfilename under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        LrsContainer.__init__(self, [])
        try:
            _checkExists(fontfilename)
            self.truefile = fontfilename
        except:
            try:
                _checkExists(file)
                self.truefile = file
            except:
                raise LrsError("neither '%s' nor '%s' exists" % (fontfilename, file))

        self.file = file
        self.fontname = fontname
        self.fontfilename = fontfilename
        self.encoding = encoding

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        """
        Perform the toLrf operation under explicit file-format and conversion rules.

        Example:
            Exercise Font.toLrf through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param lrfWriter: Value supplied for lrfWriter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        font = LrfObject("Font", LrsObject.getNextObjId())
        lrfWriter.registerFontId(font.objId)
        font.appendLrfTag(LrfTag("FontFilename", lrfWriter.toUnicode(self.truefile)))
        font.appendLrfTag(LrfTag("FontFacename", lrfWriter.toUnicode(self.fontname)))

        stream = LrfFileStream(STREAM_FORCE_COMPRESSED, self.truefile)
        font.appendLrfTags(stream.getStreamTags())

        lrfWriter.append(font)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        """
        Perform the toElement operation under explicit file-format and conversion rules.

        Example:
            Exercise Font.toElement through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_output_modernized.py


        :param se: Value supplied for se under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        element = Element(
            "RegistFont",
            encoding="TTF",
            fontname=self.fontname,
            file=self.file,
            fontfilename=self.file,
        )
        return element
