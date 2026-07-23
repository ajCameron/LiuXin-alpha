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
    pass


class ContentError(Exception):
    pass


def _checkExists(filename: _typing.Any) -> None:
    if not os.path.exists(filename):
        raise LrsError("file '%s' not found" % filename)


def _formatXml(root: _typing.Any) -> None:
    """
    A helper to make the LRS output look nicer.
    :param root:
    :return:
    """
    for elem in root.iter():
        if len(elem) > 0 and (not elem.text or not elem.text.strip()):
            elem.text = "\n"
        if not elem.tail or not elem.tail.strip():
            elem.tail = "\n"


def ElementWithText(tag: _typing.Any, text: _typing.Any, **extra: _typing.Any) -> _typing.Any:
    """
    A shorthand function to create Elements with text.
    :param tag:
    :param text:
    :param extra:
    :return:
    """
    e = Element(tag, **extra)
    e.text = text
    return e


def ElementWithReading(tag: _typing.Any, text: _typing.Any, reading: bool = False) -> _typing.Any:
    """
    A helper function that creates reading attributes.
    :param tag:
    :param text:
    :param reading:
    :return:
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
    :param e:
    :param contentsList:
    :param se:
    :return:
    """

    def uconcat(text: _typing.Any, newText: _typing.Any, se: _typing.Any) -> _typing.Any:
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
    """

    def __init__(self: _typing.Self, delegates: _typing.Any) -> None:
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
        for d in self.delegates:
            e = d.toElement(sourceEncoding)
            if e is not None:
                if isinstance(e, list):
                    for e1 in e:
                        element.append(e1)
                else:
                    element.append(e)

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        for d in self.delegates:
            d.appendReferencedObjects(parent)

    def getMethods(self: _typing.Self) -> _typing.Any:
        return self.delegatedMethods

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        return []

    def toLrfDelegates(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        for d in self.delegates:
            d.toLrf(lrfWriter)

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        self.toLrfDelegates(lrfWriter)


class LrsAttributes(object):
    """
    A mixin class to handle default and user supplied attributes.
    """

    def __init__(self: _typing.Self, defaults: _typing.Any, alsoAllow: _typing.Any = None, **settings: _typing.Any) -> None:
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
    """

    def __init__(self: _typing.Self, validChildren: _typing.Any) -> None:
        self.parent = None
        self.contents = []
        self.validChildren = validChildren
        self.must_append = False  #: If True even an empty container is appended by append_to

    def has_text(self: _typing.Self) -> bool:
        """
        Return True iff this container has non whitespace text
        :return:
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
        :param parent:
        :return:
        """
        if self.contents or self.must_append:
            parent.append(self)

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        for c in self.contents:
            c.appendReferencedObjects(parent)

    def setParent(self: _typing.Self, parent: _typing.Any) -> None:
        if self.parent is not None:
            raise LrsError("object already has parent")
        self.parent = parent

    def append(self: _typing.Self, content: _typing.Any, convertText: bool = True) -> _typing.Any:
        """
        Appends valid objects to container.  Can auto-covert text strings to Text objects.
        :param content:
        :param convertText:
        :return:
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
        for child in self.contents:
            if predicate(child):
                yield child
            if hasattr(child, "get_all"):
                for grandchild in child.get_all(predicate):
                    yield grandchild


class LrsObject(object):
    """
    A mixin class for elements that need an object id.
    """

    nextObjId = 0

    @classmethod
    def getNextObjId(selfClass: _typing.Any) -> _typing.Any:
        selfClass.nextObjId += 1
        return selfClass.nextObjId

    def __init__(self: _typing.Self, assignId: bool = False) -> None:
        if assignId:
            self.objId = LrsObject.getNextObjId()
        else:
            self.objId = 0

    def assignId(self: _typing.Self) -> None:
        if self.objId != 0:
            raise LrsError("id already assigned to " + self.__class__.__name__)
        self.objId = LrsObject.getNextObjId()

    def lrsObjectElement(self: _typing.Self, name: _typing.Any, objlabel: str = "objlabel", labelName: _typing.Any = None, labelDecorate: bool = True, **settings: _typing.Any) -> _typing.Any:
        """

        :param name:
        :param objlabel:
        :param labelName:
        :param labelDecorate:
        :param settings:
        :return:
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
    Main class for any lrs or lrf.  All objects must be appended to
    the Book class in some way or another in order to be rendered as
    an LRS or LRF file.

    The following settings are available on the contructor of Book:

    author="book author" or author=("book author", "sort as")
    Author of the book.

    title="book title" or title=("book title", "sort as")
    Title of the book.

    sourceencoding="codec"
    Gives the assumed encoding for all non-unicode strings.


    thumbnail="thumbnail file name"
    A small (80x80?) graphics file with a thumbnail of the book's cover.

    bookid="book id"
    A unique id for the book.

    textstyledefault=<dictionary of settings>
    Sets the default values for all TextStyles.

    pagetstyledefault=<dictionary of settings>
    Sets the default values for all PageStyles.

    blockstyledefault=<dictionary of settings>
    Sets the default values for all BlockStyles.

    booksetting=BookSetting()
    Override the default BookSetting.

    setdefault=StyleDefault()
    Override the default SetDefault.

    There are several other settings -- see the BookInfo class for more.
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
        ot = self.delegates[0].delegates[0].delegates[0].title
        self.delegates[0].delegates[0].delegates[0].title = (title, ot[1])

    def set_author(self: _typing.Self, author: _typing.Any) -> None:
        ot = self.delegates[0].delegates[0].delegates[0].author
        self.delegates[0].delegates[0].delegates[0].author = (author, ot[1])

    def create_text_style(self: _typing.Self, **settings: _typing.Any) -> _typing.Any:
        ans = TextStyle(**self.defaultTextStyle.attrs.copy())
        ans.update(settings)
        return ans

    def create_block_style(self: _typing.Self, **settings: _typing.Any) -> _typing.Any:
        ans = BlockStyle(**self.defaultBlockStyle.attrs.copy())
        ans.update(settings)
        return ans

    def create_page_style(self: _typing.Self, **settings: _typing.Any) -> _typing.Any:
        if not self.allow_new_page:
            raise ContentError
        ans = PageStyle(**self.defaultPageStyle.attrs.copy())
        ans.update(settings)
        return ans

    def create_page(self: _typing.Self, pageStyle: _typing.Any = None, **settings: _typing.Any) -> _typing.Any:
        """
        Return a new L{Page}. The page has not been appended to this book.
        :param pageStyle:
        :param settings:
        :return:
        """
        if not pageStyle:
            pageStyle = self.defaultPageStyle
        return Page(pageStyle=pageStyle, **settings)

    def create_text_block(self: _typing.Self, textStyle: _typing.Any = None, blockStyle: _typing.Any = None, **settings: _typing.Any) -> _typing.Any:
        """
        Return a new L{TextBlock}. The block has not been appended to this book.
        :param textStyle:
        :param blockStyle:
        :param settings:
        :return:
        """
        if not textStyle:
            textStyle = self.defaultTextStyle
        if not blockStyle:
            blockStyle = self.defaultBlockStyle
        return TextBlock(textStyle=textStyle, blockStyle=blockStyle, **settings)

    def pages(self: _typing.Self) -> _typing.Any:
        """
        Return list of Page objects in this book
        :return:
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
        :return:
        """
        for item in self.delegates:
            if isinstance(item, Main):
                temp = list(item.contents)
                temp.reverse()
                for candidate in temp:
                    if isinstance(candidate, Page):
                        return candidate

    def embed_font(self: _typing.Self, file: _typing.Any, facename: _typing.Any) -> None:
        f = Font(file, facename)
        self.append(f)

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        return ["sourceencoding"]

    def append(self: _typing.Self, content: _typing.Any) -> None:
        """
        Find and invoke the correct appender for this content.
        :param content:
        :return:
        """
        className = content.__class__.__name__
        try:
            method = getattr(self, "append" + className)
        except AttributeError:
            raise LrsError("can't append %s to Book" % className)
        method(content)

    def rationalize_font_sizes(self: _typing.Self, base_font_size: int = 10) -> None:
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
        if isinstance(lrsFile, six_string_types):
            lrsFile = codecs.open(lrsFile, "wb", encoding=encoding)
        self.render(lrsFile, outputEncodingName=encoding)
        lrsFile.close()

    def renderLrf(self: _typing.Self, lrfFile: _typing.Any) -> None:
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
        root = Element("BBeBXylog", version="1.0")
        root.append(Element("Property"))
        self.appendDelegates(root, self.sourceencoding)
        return root

    def render(self: _typing.Self, f: _typing.Any, outputEncodingName: str = "UTF-8") -> None:
        """
        Write the book as an LRS to file f.
        :param f:
        :param outputEncodingName:
        :return:
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
    """

    def __init__(self: _typing.Self) -> None:
        Delegator.__init__(self, [Info(), TableOfContents()])

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        bi = Element("BookInformation")
        self.appendDelegates(bi, se)
        return bi


class Info(Delegator):
    """
    Just a container for the BookInfo and DocInfo elements.
    """

    def __init__(self: _typing.Self) -> None:
        self.genreading = DEFAULT_GENREADING
        Delegator.__init__(self, [BookInfo(), DocInfo()])

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        return ["genreading"]  # + self.delegatedSettings

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        info = Element("Info", version="1.1")
        info.append(self.delegates[0].toElement(se, reading="s" in self.genreading))
        info.append(self.delegates[1].toElement(se))
        return info

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        # this info is set in XML form in the LRF
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
    def __init__(self: _typing.Self) -> None:
        self.tocEntries = []

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        pass

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        return ["addTocEntry"]

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        return []

    def addTocEntry(self: _typing.Self, tocLabel: _typing.Any, textBlock: _typing.Any) -> None:
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
        if len(self.tocEntries) == 0:
            return None

        toc = Element("TOC")
        for t in self.tocEntries:
            toc.append(t.toElement(se))
        return toc

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        if len(self.tocEntries) == 0:
            return

        toc = []
        for t in self.tocEntries:
            toc.append((t.textBlock.parent.objId, t.textBlock.objId, t.label))

        lrf_toc = LrfToc(LrsObject.getNextObjId(), toc, lrfWriter.getSourceEncoding())
        lrfWriter.append(lrf_toc)
        lrfWriter.setTocObject(lrf_toc)


class TocLabel(object):
    def __init__(self: _typing.Self, label: _typing.Any, textBlock: _typing.Any) -> None:
        self.label = escape(re.sub(r"&(\S+?);", entity_to_unicode, label))
        self.textBlock = textBlock

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        return ElementWithText(
            "TocLabel",
            self.label,
            refobj=str(self.textBlock.objId),
            refpage=str(self.textBlock.parent.objId),
        )


class BookInfo(object):
    def __init__(self: _typing.Self) -> None:
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
        pass

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        return []

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
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
        pi = Element("ProductIdentifier")
        isbn_element = ElementWithText("ISBNPrintable", self.isbn)
        isbn_value_element = ElementWithText("ISBNValue", self.isbn.replace("-", ""))

        pi.append(isbn_element)
        pi.append(isbn_value_element)
        bi.append(pi)

    def toElement(self: _typing.Self, se: _typing.Any, reading: bool = True) -> _typing.Any:
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
    def __init__(self: _typing.Self) -> None:
        self.thumbnail = None
        self.language = "en"
        self.creator = None
        self.creationdate = str(isoformat(date.today()))
        self.producer = "%s v%s" % (__appname__, __version__)
        self.numberofpages = "0"

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        pass

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        return []

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        return [
            "thumbnail",
            "language",
            "creator",
            "creationdate",
            "producer",
            "numberofpages",
        ]

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
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
    def __init__(self: _typing.Self) -> None:
        LrsContainer.__init__(self, [Page])

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        return ["appendPage", "Page"]

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        return []

    def Page(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        p = Page(*args, **kwargs)
        self.append(p)
        return p

    def appendPage(self: _typing.Self, page: _typing.Any) -> None:
        self.append(page)

    def toElement(self: _typing.Self, sourceEncoding: _typing.Any) -> _typing.Any:
        main = Element(self.__class__.__name__)

        for page in self.contents:
            main.append(page.toElement(sourceEncoding))

        return main

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
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
    def __init__(self: _typing.Self) -> None:
        LrsContainer.__init__(self, [Solo])

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        return ["appendSolo", "Solo"]

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        return []

    def Solo(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        p = Solo(*args, **kwargs)
        self.append(p)
        return p

    def appendSolo(self: _typing.Self, solo: _typing.Any) -> None:
        self.append(solo)

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        for s in self.contents:
            s.toLrf(lrfWriter)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        solos = []
        for s in self.contents:
            solos.append(s.toElement(se))

        if len(solos) == 0:
            return None

        return solos


class Solo(Main):
    pass


class Template(object):
    """
    Does nothing that I know of.
    """

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        pass

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        return []

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        return []

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        t = Element("Template")
        t.attrib["version"] = "1.0"
        return t

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        # does nothing
        pass


class StyleDefault(LrsAttributes):
    """
    Supply some defaults for all TextBlocks.
    The legal values are a subset of what is allowed on a
    TextBlock -- ruby, emphasis, and waitprop settings.
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
        LrsAttributes.__init__(self, self.defaults, alsoAllow=self.alsoAllow, **settings)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        return Element("SetDefault", self.attrs)


class Style(LrsContainer, Delegator):
    def __init__(self: _typing.Self, styledefault: _typing.Any = StyleDefault()) -> None:
        LrsContainer.__init__(self, [PageStyle, TextStyle, BlockStyle])
        Delegator.__init__(self, [BookStyle(styledefault=styledefault)])
        self.bookStyle = self.delegates[0]
        self.appendPageStyle = self.appendTextStyle = self.appendBlockStyle = self.append

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        LrsContainer.appendReferencedObjects(self, parent)

    def getMethods(self: _typing.Self) -> _typing.Any:
        return [
            "PageStyle",
            "TextStyle",
            "BlockStyle",
            "appendPageStyle",
            "appendTextStyle",
            "appendBlockStyle",
        ] + self.delegatedMethods

    def getSettings(self: _typing.Self) -> _typing.Any:
        return [(self.bookStyle, x) for x in self.bookStyle.getSettings()]

    def PageStyle(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        ps = PageStyle(*args, **kwargs)
        self.append(ps)
        return ps

    def TextStyle(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        ts = TextStyle(*args, **kwargs)
        self.append(ts)
        return ts

    def BlockStyle(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        bs = BlockStyle(*args, **kwargs)
        self.append(bs)
        return bs

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        style = Element("Style")
        style.append(self.bookStyle.toElement(se))

        for content in self.contents:
            style.append(content.toElement(se))

        return style

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        self.bookStyle.toLrf(lrfWriter)

        for s in self.contents:
            s.toLrf(lrfWriter)


class BookStyle(LrsObject, LrsContainer):
    def __init__(self: _typing.Self, styledefault: _typing.Any = StyleDefault()) -> None:
        LrsObject.__init__(self, assignId=True)
        LrsContainer.__init__(self, [Font])
        self.styledefault = styledefault
        self.booksetting = BookSetting()
        self.appendFont = self.append

    def getSettings(self: _typing.Self) -> list[_typing.Any]:
        return ["styledefault", "booksetting"]

    def getMethods(self: _typing.Self) -> list[_typing.Any]:
        return ["Font", "appendFont"]

    def Font(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> None:
        f = Font(*args, **kwargs)
        self.append(f)
        return

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        book_style = self.lrsObjectElement("BookStyle", objlabel="stylelabel", labelDecorate=False)
        book_style.append(self.styledefault.toElement(se))
        book_style.append(self.booksetting.toElement(se))
        for font in self.contents:
            book_style.append(font.toElement(se))
        return book_style

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        book_atr = LrfObject("BookAtr", self.objId)
        book_atr.appendLrfTag(LrfTag("ChildPageTree", lrfWriter.getPageTreeId()))
        book_atr.appendTagDict(self.styledefault.attrs)

        self.booksetting.toLrf(lrfWriter)

        lrfWriter.append(book_atr)
        lrfWriter.setRootObject(book_atr)

        for font in self.contents:
            font.toLrf(lrfWriter)


class BookSetting(LrsAttributes):
    def __init__(self: _typing.Self, **settings: _typing.Any) -> None:
        defaults = dict(
            bindingdirection="Lr",
            dpi="1660",
            screenheight="800",
            screenwidth="600",
            colordepth="24",
        )
        LrsAttributes.__init__(self, defaults, **settings)

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        a = self.attrs
        lrfWriter.dpi = int(a["dpi"])
        lrfWriter.bindingdirection = BINDING_DIRECTION_ENCODING[a["bindingdirection"]]
        lrfWriter.height = int(a["screenheight"])
        lrfWriter.width = int(a["screenwidth"])
        lrfWriter.colorDepth = int(a["colordepth"])

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        return Element("BookSetting", self.attrs)


class LrsStyle(LrsObject, LrsAttributes, LrsContainer):
    """
    A mixin class for styles.
    """

    def __init__(self: _typing.Self, elementName: _typing.Any, defaults: _typing.Any = None, alsoAllow: _typing.Any = None, **overrides: _typing.Any) -> None:
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
        for name, value in settings.items():
            if name not in self.__class__.validSettings:
                raise LrsError("%s not a valid setting for %s" % (name, self.__class__.__name__))
            self.attrs[name] = value

    def getLabel(self: _typing.Self) -> _typing.Any:
        return str(self.objId)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        element = Element(self.elementName, stylelabel=self.getLabel(), objid=str(self.objId))
        element.attrib.update(self.attrs)
        return element

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        obj = LrfObject(self.elementName, self.objId)
        obj.appendTagDict(self.attrs, self.__class__.__name__)
        lrfWriter.append(obj)

    def __eq__(self: _typing.Self, other: _typing.Any) -> bool:
        if hasattr(other, "attrs"):
            return self.__class__ == other.__class__ and self.attrs == other.attrs
        return False


class TextStyle(LrsStyle):
    """
    The text style of a TextBlock.  Default is 10 pt. Times Roman.

    Setting         Value                   Default
    --------        -----                   -------
    align           "head","center","foot"  "head" (left aligned)
    baselineskip    points * 10             120 (12 pt. distance between
                                              bottoms of lines)
    fontsize        points * 10             100 (10 pt.)
    fontweight      1 to 1000               400 (normal, 800 is bold)
    fontwidth       points * 10 or -10      -10 (use values from font)
    linespace       points * 10             10 (min space btw. lines?)
    wordspace       points * 10             25 (min space btw. each word)

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
        LrsStyle.__init__(self, "TextStyle", self.defaults, alsoAllow=self.alsoAllow, **overrides)

    def copy(self: _typing.Self) -> _typing.Any:
        tb = TextStyle()
        tb.attrs = self.attrs.copy()
        return tb


class BlockStyle(LrsStyle):
    """
    The block style of a TextBlock.  Default is an expandable 560 pixel
    wide area with no space for headers or footers.

    Setting      Value                  Default
    --------     -----                  -------
    blockwidth   pixels                 560
    sidemargin   pixels                 0
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
        LrsStyle.__init__(self, "BlockStyle", self.defaults, **overrides)

    def copy(self: _typing.Self) -> _typing.Any:
        tb = BlockStyle()
        tb.attrs = self.attrs.copy()
        return tb


class PageStyle(LrsStyle):
    """
    Setting         Value                   Default
    --------        -----                   -------
    evensidemargin  pixels                  20
    oddsidemargin   pixels                  20
    topmargin       pixels                  20
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
        selfClass._fixup(parent, "header", settings)
        selfClass._fixup(parent, "footer", settings)

    @classmethod
    def _fixup(selfClass: _typing.Any, parent: _typing.Any, basename: _typing.Any, settings: _typing.Any) -> None:
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
        if self.objectsAppended:
            return
        PageStyle.translateHeaderAndFooter(parent, self.attrs)
        self.objectsAppended = True

    def __init__(self: _typing.Self, **settings: _typing.Any) -> None:
        # self.fixHeaderSettings(settings)
        LrsStyle.__init__(self, "PageStyle", self.defaults, alsoAllow=self.alsoAllow, **settings)


class Page(LrsObject, LrsContainer):
    """
    Pages are added to Books.  Pages can be supplied a PageStyle.
    If they are not, Page.defaultPageStyle will be used.
    """

    defaultPageStyle = PageStyle()

    def __init__(self: _typing.Self, pageStyle: _typing.Any = defaultPageStyle, **settings: _typing.Any) -> None:
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [TextBlock, BlockSpace, RuledLine, ImageBlock, Canvas])

        self.pageStyle = pageStyle

        for settingName in settings.keys():
            if settingName not in PageStyle.defaults and settingName not in PageStyle.alsoAllow:
                raise LrsError("setting %s not allowed on Page" % settingName)

        self.settings = settings.copy()

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        PageStyle.translateHeaderAndFooter(parent, self.settings)

        self.pageStyle.appendReferencedObjects(parent)

        if self.pageStyle.parent is None:
            parent.append(self.pageStyle)

        LrsContainer.appendReferencedObjects(self, parent)

    def RuledLine(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        rl = RuledLine(*args, **kwargs)
        self.append(rl)
        return rl

    def BlockSpace(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        bs = BlockSpace(*args, **kwargs)
        self.append(bs)
        return bs

    def TextBlock(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Create and append a new text block (shortcut).
        :param args:
        :param kwargs:
        :return:
        """
        tb = TextBlock(*args, **kwargs)
        self.append(tb)
        return tb

    def ImageBlock(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Create and append and new Image block (shorthand).
        :param args:
        :param kwargs:
        :return:
        """
        ib = ImageBlock(*args, **kwargs)
        self.append(ib)
        return ib

    def addLrfObject(self: _typing.Self, objId: _typing.Any) -> None:
        self.stream.appendLrfTag(LrfTag("Link", objId))

    def appendLrfTag(self: _typing.Self, lrfTag: _typing.Any) -> None:
        self.stream.appendLrfTag(lrfTag)

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        # tags:
        # ObjectList
        # Link to pagestyle
        # Parent page tree id
        # stream of tags

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
        page = self.lrsObjectElement("Page")
        page.set("pagestyle", self.pageStyle.getLabel())
        page.attrib.update(self.settings)

        for content in self.contents:
            page.append(content.toElement(sourceEncoding))

        return page


class TextBlock(LrsObject, LrsContainer):
    """
    TextBlocks are added to Pages.  They hold Paragraphs or CRs.

    If a TextBlock is used in a header, it should be appended to
    the Book, not to a specific Page.
    """

    defaultTextStyle = TextStyle()
    defaultBlockStyle = BlockStyle()

    def __init__(self: _typing.Self, textStyle: _typing.Any = defaultTextStyle, blockStyle: _typing.Any = defaultBlockStyle, **settings: _typing.Any) -> None:
        """
        Create TextBlock.
        :param textStyle:
        :param blockStyle:
        :param settings:
        :return:
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
        if self.textStyle.parent is None:
            parent.append(self.textStyle)

        if self.blockStyle.parent is None:
            parent.append(self.blockStyle)

        LrsContainer.appendReferencedObjects(self, parent)

    def Paragraph(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        """
        Create and append a Paragraph to this TextBlock.  A CR is automatically inserted after the Paragraph.
        To avoid this behavior, create the Paragraph and append it to the TextBlock in a separate call.
        :param args:
        :param kwargs:
        :return:
        """
        p = Paragraph(*args, **kwargs)
        self.append(p)
        self.append(CR())
        return p

    def toElement(self: _typing.Self, sourceEncoding: _typing.Any) -> _typing.Any:
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
        ids = [self.objId, self.extraId, self.blockStyle.objId, self.textStyle.objId]
        for content in self.contents:
            if hasattr(content, "getReferencedObjIds"):
                ids.extend(content.getReferencedObjIds())

        return ids

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        self.toLrfContainer(lrfWriter, lrfWriter)

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        # id really belongs to the outer block
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
    Note: <P> alone does not make a paragraph.  Only a CR inserted
    into a text block right after a <P> makes a real paragraph.
    Two Paragraphs appended in a row act like a single Paragraph.

    Also note that there are few autoappenders for Paragraph (and
    the things that can go in it.)  It's less confusing (to me) to use
    explicit .append methods to build up the text stream.
    """

    def __init__(self: _typing.Self, text: _typing.Any = None) -> None:
        LrsContainer.__init__(self, [Text, CR, DropCaps, CharButton, LrsSimpleChar1, six_string_types])
        if text is not None:
            if isinstance(text, six_string_types):
                text = Text(text)
            self.append(text)

    def CR(self: _typing.Self) -> _typing.Any:
        # Okay, here's a single autoappender for this common operation
        cr = CR()
        self.append(cr)
        return cr

    def getReferencedObjIds(self: _typing.Self) -> _typing.Any:
        ids = []
        for content in self.contents:
            if hasattr(content, "getReferencedObjIds"):
                ids.extend(content.getReferencedObjIds())
        return ids

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, parent: _typing.Any) -> None:
        parent.appendLrfTag(LrfTag("pstart", 0))
        for content in self.contents:
            content.toLrfContainer(lrfWriter, parent)
        parent.appendLrfTag(LrfTag("pend"))

    def toElement(self: _typing.Self, sourceEncoding: _typing.Any) -> _typing.Any:
        p = Element("P")
        appendTextElements(p, self.contents, sourceEncoding)
        return p


class LrsTextTag(LrsContainer):
    def __init__(self: _typing.Self, text: _typing.Any, validContents: _typing.Any) -> None:
        LrsContainer.__init__(self, [Text, six_string_types] + validContents)
        if text is not None:
            self.append(text)

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, parent: _typing.Any) -> None:
        if hasattr(self, "tagName"):
            tagName = self.tagName
        else:
            tagName = self.__class__.__name__

        parent.appendLrfTag(LrfTag(tagName))

        for content in self.contents:
            content.toLrfContainer(lrfWriter, parent)

        parent.appendLrfTag(LrfTag(tagName + "End"))

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        if hasattr(self, "tagName"):
            tagName = self.tagName
        else:
            tagName = self.__class__.__name__

        p = Element(tagName)
        appendTextElements(p, self.contents, se)
        return p


class LrsSimpleChar1(object):
    def isEmpty(self: _typing.Self) -> bool:
        for content in self.contents:
            if not content.isEmpty():
                return False
        return True

    def hasFollowingContent(self: _typing.Self) -> bool:
        foundSelf = False
        for content in self.parent.contents:
            if content == self:
                foundSelf = True
            elif foundSelf:
                if not content.isEmpty():
                    return True
        return False


class DropCaps(LrsTextTag):
    def __init__(self: _typing.Self, line: int = 1) -> None:
        LrsTextTag.__init__(self, None, [LrsSimpleChar1])
        if int(line) <= 0:
            raise LrsError("A DrawChar must span at least one line.")
        self.line = int(line)

    def isEmpty(self: _typing.Self) -> bool:
        return self.text is None or not self.text.strip()

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        elem = Element("DrawChar", line=str(self.line))
        appendTextElements(elem, self.contents, se)
        return elem

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, parent: _typing.Any) -> None:
        parent.appendLrfTag(LrfTag("DrawChar", (int(self.line),)))

        for content in self.contents:
            content.toLrfContainer(lrfWriter, parent)

        parent.appendLrfTag(LrfTag("DrawCharEnd"))


class Button(LrsObject, LrsContainer):
    def __init__(self: _typing.Self, **settings: _typing.Any) -> None:
        LrsObject.__init__(self, **settings)
        LrsContainer.__init__(self, [PushButton])

    def findJumpToRefs(self: _typing.Self) -> tuple[_typing.Any, ...]:
        for sub1 in self.contents:
            if isinstance(sub1, PushButton):
                for sub2 in sub1.contents:
                    if isinstance(sub2, JumpTo):
                        return sub2.textBlock.objId, sub2.textBlock.parent.objId
        raise LrsError("%s has no PushButton or JumpTo subs" % self.__class__.__name__)

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
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
        b = self.lrsObjectElement("Button")

        for content in self.contents:
            b.append(content.toElement(se))

        return b


class ButtonBlock(Button):
    pass


class PushButton(LrsContainer):
    def __init__(self: _typing.Self, **settings: _typing.Any) -> None:
        LrsContainer.__init__(self, [JumpTo])

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        b = Element("PushButton")

        for content in self.contents:
            b.append(content.toElement(se))

        return b


class JumpTo(LrsContainer):
    def __init__(self: _typing.Self, textBlock: _typing.Any) -> None:
        LrsContainer.__init__(self, [])
        self.textBlock = textBlock

    def setTextBlock(self: _typing.Self, textBlock: _typing.Any) -> None:
        self.textBlock = textBlock

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        return Element(
            "JumpTo",
            refpage=str(self.textBlock.parent.objId),
            refobj=str(self.textBlock.objId),
        )


class Plot(LrsSimpleChar1, LrsContainer):

    ADJUSTMENT_VALUES = {"center": 1, "baseline": 2, "top": 3, "bottom": 4}

    def __init__(self: _typing.Self, obj: _typing.Any, xsize: int = 0, ysize: int = 0, adjustment: _typing.Any = None) -> None:
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
        if not isinstance(obj, (Image, Button)):
            raise LrsError("Plot elements can only refer to Image or Button elements")
        self.obj = obj

    def getReferencedObjIds(self: _typing.Self) -> list[_typing.Any]:
        return [self.obj.objId]

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        if self.obj.parent is None:
            parent.append(self.obj)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
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
    A object that represents raw text.  Does not have a toElement.
    """

    def __init__(self: _typing.Self, text: _typing.Any) -> None:
        LrsContainer.__init__(self, [])
        self.text = text

    def isEmpty(self: _typing.Self) -> bool:
        return not self.text or not self.text.strip()

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, parent: _typing.Any) -> None:
        if self.text:
            if isinstance(self.text, str):
                parent.appendLrfTag(LrfTag("rawtext", self.text))
            else:
                parent.appendLrfTag(LrfTag("textstring", self.text))


class CR(LrsSimpleChar1, LrsContainer):
    """
    A line break (when appended to a Paragraph) or a paragraph break (when appended to a TextBlock).
    """

    def __init__(self: _typing.Self) -> None:
        LrsContainer.__init__(self, [])

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        return Element("CR")

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, parent: _typing.Any) -> None:
        parent.appendLrfTag(LrfTag("CR"))


class Italic(LrsSimpleChar1, LrsTextTag):
    def __init__(self: _typing.Self, text: _typing.Any = None) -> None:
        LrsTextTag.__init__(self, text, [LrsSimpleChar1])


class Sub(LrsSimpleChar1, LrsTextTag):
    def __init__(self: _typing.Self, text: _typing.Any = None) -> None:
        LrsTextTag.__init__(self, text, [])


class Sup(LrsSimpleChar1, LrsTextTag):
    def __init__(self: _typing.Self, text: _typing.Any = None) -> None:
        LrsTextTag.__init__(self, text, [])


class NoBR(LrsSimpleChar1, LrsTextTag):
    def __init__(self: _typing.Self, text: _typing.Any = None) -> None:
        LrsTextTag.__init__(self, text, [LrsSimpleChar1])


class Space(LrsSimpleChar1, LrsContainer):
    def __init__(self: _typing.Self, xsize: int = 0, x: int = 0) -> None:
        LrsContainer.__init__(self, [])
        if xsize == 0 and x != 0:
            xsize = x
        self.xsize = xsize

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        if self.xsize == 0:
            return

        return Element("Space", xsize=str(self.xsize))

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        if self.xsize != 0:
            container.appendLrfTag(LrfTag("Space", self.xsize))


class Box(LrsSimpleChar1, LrsContainer):
    """
    Draw a box around text.  Unfortunately, does not seem to do anything on the PRS-500.
    """

    def __init__(self: _typing.Self, linetype: str = "solid") -> None:
        LrsContainer.__init__(self, [Text, six_string_types])
        if linetype not in LINE_TYPE_ENCODING:
            raise LrsError(linetype + " is not a valid line type")
        self.linetype = linetype

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        e = Element("Box", linetype=self.linetype)
        appendTextElements(e, self.contents, se)
        return e

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        container.appendLrfTag(LrfTag("Box", self.linetype))
        for content in self.contents:
            content.toLrfContainer(lrfWriter, container)
        container.appendLrfTag(LrfTag("BoxEnd"))


class Span(LrsSimpleChar1, LrsContainer):
    def __init__(self: _typing.Self, text: _typing.Any = None, **attrs: _typing.Any) -> None:
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
        element = Element("Span")
        for (key, value) in self.attrs.items():
            element.set(key, str(value))

        appendTextElements(element, self.contents, se)
        return element


class EmpLine(LrsTextTag, LrsSimpleChar1):

    emplinetypes = ["none", "solid", "dotted", "dashed", "double"]
    emplinepositions = ["before", "after"]

    def __init__(self: _typing.Self, text: _typing.Any = None, emplineposition: str = "before", emplinetype: str = "solid") -> None:
        LrsTextTag.__init__(self, text, [LrsSimpleChar1])
        if emplineposition not in self.__class__.emplinepositions:
            raise LrsError("emplineposition for an EmpLine must be one of: " + str(self.__class__.emplinepositions))
        if emplinetype not in self.__class__.emplinetypes:
            raise LrsError("emplinetype for an EmpLine must be one of: " + str(self.__class__.emplinetypes))

        self.emplinetype = emplinetype
        self.emplineposition = emplineposition

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, parent: _typing.Any) -> None:
        parent.appendLrfTag(LrfTag(self.__class__.__name__, (self.emplineposition, self.emplinetype)))
        parent.appendLrfTag(LrfTag("emplineposition", self.emplineposition))
        parent.appendLrfTag(LrfTag("emplinetype", self.emplinetype))
        for content in self.contents:
            content.toLrfContainer(lrfWriter, parent)

        parent.appendLrfTag(LrfTag(self.__class__.__name__ + "End"))

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        element = Element(self.__class__.__name__)
        element.set("emplineposition", self.emplineposition)
        element.set("emplinetype", self.emplinetype)

        appendTextElements(element, self.contents, se)
        return element


class Bold(Span):
    """
    There is no known "bold" lrf tag. Use Span with a fontweight in LRF,
    but use the word Bold in the LRS.
    """

    def __init__(self: _typing.Self, text: _typing.Any = None) -> None:
        Span.__init__(self, text, fontweight=800)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        e = Element("Bold")
        appendTextElements(e, self.contents, se)
        return e


class BlockSpace(LrsContainer):
    """Can be appended to a page to move the text point."""

    def __init__(self: _typing.Self, xspace: int = 0, yspace: int = 0, x: int = 0, y: int = 0) -> None:
        LrsContainer.__init__(self, [])
        if xspace == 0 and x != 0:
            xspace = x
        if yspace == 0 and y != 0:
            yspace = y
        self.xspace = xspace
        self.yspace = yspace

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        if self.xspace != 0:
            container.appendLrfTag(LrfTag("xspace", self.xspace))
        if self.yspace != 0:
            container.appendLrfTag(LrfTag("yspace", self.yspace))

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        element = Element("BlockSpace")

        if self.xspace != 0:
            element.attrib["xspace"] = str(self.xspace)
        if self.yspace != 0:
            element.attrib["yspace"] = str(self.yspace)

        return element


class CharButton(LrsSimpleChar1, LrsContainer):
    """
    Define the text and target of a CharButton.  Must be passed a
    JumpButton that is the destination of the CharButton.

    Only text or SimpleChars can be appended to the CharButton.
    """

    def __init__(self: _typing.Self, button: _typing.Any, text: _typing.Any = None) -> None:
        LrsContainer.__init__(self, [basestring, Text, LrsSimpleChar1])
        self.button = None
        if button != None:
            self.setButton(button)

        if text is not None:
            self.append(text)

    def setButton(self: _typing.Self, button: _typing.Any) -> None:
        if not isinstance(button, (JumpButton, Button)):
            raise LrsError("CharButton button must be a JumpButton or Button")

        self.button = button

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        if self.button.parent is None:
            parent.append(self.button)

    def getReferencedObjIds(self: _typing.Self) -> list[_typing.Any]:
        return [self.button.objId]

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        container.appendLrfTag(LrfTag("CharButton", self.button.objId))

        for content in self.contents:
            content.toLrfContainer(lrfWriter, container)

        container.appendLrfTag(LrfTag("CharButtonEnd"))

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        cb = Element("CharButton", refobj=str(self.button.objId))
        appendTextElements(cb, self.contents, se)
        return cb


class Objects(LrsContainer):
    def __init__(self: _typing.Self) -> None:
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
        return []

    def ImageBlock(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        ib = ImageBlock(*args, **kwargs)
        self.append(ib)
        return ib

    def JumpButton(self: _typing.Self, textBlock: _typing.Any) -> _typing.Any:
        b = JumpButton(textBlock)
        self.append(b)
        return b

    def TextBlock(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        tb = TextBlock(*args, **kwargs)
        self.append(tb)
        return tb

    def Header(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        h = Header(*args, **kwargs)
        self.append(h)
        return h

    def Footer(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        h = Footer(*args, **kwargs)
        self.append(h)
        return h

    def ImageStream(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        i = ImageStream(*args, **kwargs)
        self.append(i)
        return i

    def Image(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        i = Image(*args, **kwargs)
        self.append(i)
        return i

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        o = Element("Objects")

        for content in self.contents:
            o.append(content.toElement(se))

        return o

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        for content in self.contents:
            content.toLrf(lrfWriter)


class JumpButton(LrsObject, LrsContainer):
    """
    The target of a CharButton.  Needs a parented TextBlock to jump to.
    Actually creates several elements in the XML.  JumpButtons must
    be eventually appended to a Book (actually, an Object.)
    """

    def __init__(self: _typing.Self, textBlock: _typing.Any) -> None:
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [])
        self.textBlock = textBlock

    def setTextBlock(self: _typing.Self, textBlock: _typing.Any) -> None:
        self.textBlock = textBlock

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        button = LrfObject("Button", self.objId)
        button.appendLrfTag(LrfTag("buttonflags", 0x10))  # pushbutton
        button.appendLrfTag(LrfTag("PushButtonStart"))
        button.appendLrfTag(LrfTag("buttonactions"))
        button.appendLrfTag(LrfTag("jumpto", (self.textBlock.parent.objId, self.textBlock.objId)))
        button.append(LrfTag("endbuttonactions"))
        button.appendLrfTag(LrfTag("PushButtonEnd"))
        lrfWriter.append(button)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
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
    A line.  Default is 500 pixels long, 2 pixels wide.
    """

    defaults = dict(linelength="500", linetype="solid", linewidth="2", linecolor="0x00000000")

    def __init__(self: _typing.Self, **settings: _typing.Any) -> None:
        LrsContainer.__init__(self, [])
        LrsAttributes.__init__(self, self.defaults, **settings)
        LrsObject.__init__(self)

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        a = self.attrs
        container.appendLrfTag(
            LrfTag(
                "RuledLine",
                (a["linelength"], a["linetype"], a["linewidth"], a["linecolor"]),
            )
        )

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        return Element("RuledLine", self.attrs)


class HeaderOrFooter(LrsObject, LrsContainer, LrsAttributes):
    """
    Creates empty header or footer objects.  Append PutObj objects to
    the header or footer to create the text.

    Note: it seems that adding multiple PutObjs to a header or footer
          only shows the last one.
    """

    defaults = dict(
        framemode="square",
        layout="LrTb",
        framewidth="0",
        framecolor="0x00000000",
        bgcolor="0xFF000000",
    )

    def __init__(self: _typing.Self, **settings: _typing.Any) -> None:
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [PutObj])
        LrsAttributes.__init__(self, self.defaults, **settings)

    def put_object(self: _typing.Self, obj: _typing.Any, x1: _typing.Any, y1: _typing.Any) -> None:
        self.append(PutObj(obj, x1, y1))

    def PutObj(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
        p = PutObj(*args, **kwargs)
        self.append(p)
        return p

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        hd = LrfObject(self.__class__.__name__, self.objId)
        hd.appendTagDict(self.attrs)

        stream = LrfTagStream(0)
        for content in self.contents:
            content.toLrfContainer(lrfWriter, stream)

        hd.appendLrfTags(stream.getStreamTags(lrfWriter.getSourceEncoding()))
        lrfWriter.append(hd)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        name = self.__class__.__name__
        labelName = name.lower() + "label"
        hd = self.lrsObjectElement(name, objlabel=labelName)
        hd.attrib.update(self.attrs)

        for content in self.contents:
            hd.append(content.toElement(se))

        return hd


class Header(HeaderOrFooter):
    pass


class Footer(HeaderOrFooter):
    pass


class Canvas(LrsObject, LrsContainer, LrsAttributes):
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
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [PutObj])
        LrsAttributes.__init__(self, self.defaults, **settings)

        self.settings = self.defaults.copy()
        self.settings.update(settings)
        self.settings["canvasheight"] = int(height)
        self.settings["canvaswidth"] = int(width)

    def put_object(self: _typing.Self, obj: _typing.Any, x1: _typing.Any, y1: _typing.Any) -> None:
        self.append(PutObj(obj, x1, y1))

    def toElement(self: _typing.Self, source_encoding: _typing.Any) -> _typing.Any:
        el = self.lrsObjectElement("Canvas", **self.settings)
        for po in self.contents:
            el.append(po.toElement(source_encoding))
        return el

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        self.toLrfContainer(lrfWriter, lrfWriter)

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
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
        return bool(self.contents)


class PutObj(LrsContainer):
    """
    PutObj holds other objects that are drawn on a Canvas or Header.
    """

    def __init__(self: _typing.Self, content: _typing.Any, x1: int = 0, y1: int = 0) -> None:
        LrsContainer.__init__(self, [TextBlock, ImageBlock])
        self.content = content
        self.x1 = int(x1)
        self.y1 = int(y1)

    def setContent(self: _typing.Self, content: _typing.Any) -> None:
        self.content = content

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        if self.content.parent is None:
            parent.append(self.content)

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        container.appendLrfTag(LrfTag("PutObj", (self.x1, self.y1, self.content.objId)))

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        el = Element("PutObj", x1=str(self.x1), y1=str(self.y1), refobj=str(self.content.objId))
        return el


class ImageStream(LrsObject, LrsContainer):
    """
    Embed an image file into an Lrf.
    """

    VALID_ENCODINGS = ["JPEG", "GIF", "BMP", "PNG"]

    def __init__(self: _typing.Self, file: _typing.Any = None, encoding: _typing.Any = None, comment: _typing.Any = None) -> None:
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
        element = self.lrsObjectElement(
            "ImageStream",
            objlabel="imagestreamlabel",
            encoding=self.encoding,
            file=self.filename,
        )
        element.text = self.comment
        return element


class Image(LrsObject, LrsContainer, LrsAttributes):

    defaults = dict()

    def __init__(self: _typing.Self, refstream: _typing.Any, x0: int = 0, x1: int = 0, y0: int = 0, y1: int = 0, xsize: int = 0, ysize: int = 0, **settings: _typing.Any) -> None:
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [])
        LrsAttributes.__init__(self, self.defaults, settings)
        self.x0, self.y0, self.x1, self.y1 = int(x0), int(y0), int(x1), int(y1)
        self.xsize, self.ysize = int(xsize), int(ysize)
        self.setRefstream(refstream)

    def setRefstream(self: _typing.Self, refstream: _typing.Any) -> None:
        self.refstream = refstream

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        if self.refstream.parent is None:
            parent.append(self.refstream)

    def getReferencedObjIds(self: _typing.Self) -> list[_typing.Any]:
        return [self.objId, self.refstream.objId]

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        element = self.lrsObjectElement("Image", **self.attrs)
        element.set("refstream", str(self.refstream.objId))
        for name in ["x0", "y0", "x1", "y1", "xsize", "ysize"]:
            element.set(name, str(getattr(self, name)))
        return element

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        ib = LrfObject("Image", self.objId)
        ib.appendLrfTag(LrfTag("ImageRect", (self.x0, self.y0, self.x1, self.y1)))
        ib.appendLrfTag(LrfTag("ImageSize", (self.xsize, self.ysize)))
        ib.appendLrfTag(LrfTag("RefObjId", self.refstream.objId))
        lrfWriter.append(ib)


class ImageBlock(LrsObject, LrsContainer, LrsAttributes):
    """Create an image on a page."""

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
        LrsObject.__init__(self)
        LrsContainer.__init__(self, [Text, Image])
        LrsAttributes.__init__(self, self.defaults, **settings)
        self.x0, self.y0, self.x1, self.y1 = int(x0), int(y0), int(x1), int(y1)
        self.xsize, self.ysize = int(xsize), int(ysize)
        self.setRefstream(refstream)
        self.blockStyle = blockStyle
        self.alttext = alttext

    def setRefstream(self: _typing.Self, refstream: _typing.Any) -> None:
        self.refstream = refstream

    def appendReferencedObjects(self: _typing.Self, parent: _typing.Any) -> None:
        if self.refstream.parent is None:
            parent.append(self.refstream)

        if self.blockStyle is not None and self.blockStyle.parent is None:
            parent.append(self.blockStyle)

    def getReferencedObjIds(self: _typing.Self) -> _typing.Any:
        objects = [self.objId, self.extraId, self.refstream.objId]
        if self.blockStyle is not None:
            objects.append(self.blockStyle.objId)

        return objects

    def toLrf(self: _typing.Self, lrfWriter: _typing.Any) -> None:
        self.toLrfContainer(lrfWriter, lrfWriter)

    def toLrfContainer(self: _typing.Self, lrfWriter: _typing.Any, container: _typing.Any) -> None:
        # id really belongs to the outer block

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
        element = self.lrsObjectElement("ImageBlock", **self.attrs)
        element.set("refstream", str(self.refstream.objId))
        for name in ["x0", "y0", "x1", "y1", "xsize", "ysize"]:
            element.set(name, str(getattr(self, name)))
        element.text = self.alttext
        return element


class Font(LrsContainer):
    """
    Allows a TrueType file to be embedded in an Lrf.
    """

    def __init__(self: _typing.Self, file: _typing.Any = None, fontname: _typing.Any = None, fontfilename: _typing.Any = None, encoding: _typing.Any = None) -> None:
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
        font = LrfObject("Font", LrsObject.getNextObjId())
        lrfWriter.registerFontId(font.objId)
        font.appendLrfTag(LrfTag("FontFilename", lrfWriter.toUnicode(self.truefile)))
        font.appendLrfTag(LrfTag("FontFacename", lrfWriter.toUnicode(self.fontname)))

        stream = LrfFileStream(STREAM_FORCE_COMPRESSED, self.truefile)
        font.appendLrfTags(stream.getStreamTags())

        lrfWriter.append(font)

    def toElement(self: _typing.Self, se: _typing.Any) -> _typing.Any:
        element = Element(
            "RegistFont",
            encoding="TTF",
            fontname=self.fontname,
            file=self.file,
            fontfilename=self.file,
        )
        return element
