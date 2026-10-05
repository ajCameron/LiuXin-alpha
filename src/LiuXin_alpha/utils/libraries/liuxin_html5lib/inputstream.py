"""
Decode HTML byte and character streams with encoding detection and normalized positions.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise inputstream through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

try:
    text_type = unicode
except NameError:
    text_type = str

import codecs
import re

from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import (
    EOF,
    spaceCharacters,
    asciiLetters,
    asciiUppercase,
)
from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import encodings, ReparseException
from LiuXin_alpha.utils.libraries.liuxin_html5lib import utils

from io import StringIO

try:
    from io import BytesIO
except ImportError:
    BytesIO = StringIO

try:
    from io import BufferedIOBase
except ImportError:

    class BufferedIOBase(object):
        """
        Provide the BufferedIOBase utility contract with explicit state and cleanup behavior.

        Example:
            Exercise BufferedIOBase through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py
        """
        pass


# Non-unicode versions of constants for use in the pre-parser
spaceCharactersBytes = frozenset([item.encode("ascii") for item in spaceCharacters])
asciiLettersBytes = frozenset([item.encode("ascii") for item in asciiLetters])
asciiUppercaseBytes = frozenset([item.encode("ascii") for item in asciiUppercase])
spacesAngleBrackets = spaceCharactersBytes | frozenset([b">", b"<"])

invalid_unicode_re = re.compile(
    "[\u0001-\u0008\u000B\u000E-\u001F\u007F-\u009F\uD800-\uDFFF\uFDD0-\uFDEF\uFFFE\uFFFF\U0001FFFE\U0001FFFF\U0002FFFE\U0002FFFF\U0003FFFE\U0003FFFF\U0004FFFE\U0004FFFF\U0005FFFE\U0005FFFF\U0006FFFE\U0006FFFF\U0007FFFE\U0007FFFF\U0008FFFE\U0008FFFF\U0009FFFE\U0009FFFF\U000AFFFE\U000AFFFF\U000BFFFE\U000BFFFF\U000CFFFE\U000CFFFF\U000DFFFE\U000DFFFF\U000EFFFE\U000EFFFF\U000FFFFE\U000FFFFF\U0010FFFE\U0010FFFF]"
)  # noqa

non_bmp_invalid_codepoints = set(
    [
        0x1FFFE,
        0x1FFFF,
        0x2FFFE,
        0x2FFFF,
        0x3FFFE,
        0x3FFFF,
        0x4FFFE,
        0x4FFFF,
        0x5FFFE,
        0x5FFFF,
        0x6FFFE,
        0x6FFFF,
        0x7FFFE,
        0x7FFFF,
        0x8FFFE,
        0x8FFFF,
        0x9FFFE,
        0x9FFFF,
        0xAFFFE,
        0xAFFFF,
        0xBFFFE,
        0xBFFFF,
        0xCFFFE,
        0xCFFFF,
        0xDFFFE,
        0xDFFFF,
        0xEFFFE,
        0xEFFFF,
        0xFFFFE,
        0xFFFFF,
        0x10FFFE,
        0x10FFFF,
    ]
)

ascii_punctuation_re = re.compile("[\u0009-\u000D\u0020-\u002F\u003A-\u0040\u005B-\u0060\u007B-\u007E]")

# Cache for charsUntil()
charsUntilRegEx = {}


class BufferedStream(object):
    """
    Buffering for streams that do not have buffering of their own

    Example:
        Exercise BufferedStream through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """

    def __init__(self, stream):
        """
        Initialize and validate the BufferedStream state.

        Example:
            Exercise BufferedStream.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :return: None; validated state is stored on the receiving object.
        """
        self.stream = stream
        self.buffer = []
        self.position = [-1, 0]  # chunk number, offset

    def tell(self):
        """
        Perform the tell utility operation under explicit compatibility rules.

        Example:
            Exercise BufferedStream.tell through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        pos = 0
        for chunk in self.buffer[: self.position[0]]:
            pos += len(chunk)
        pos += self.position[1]
        return pos

    def seek(self, pos):
        """
        Perform the seek utility operation under explicit compatibility rules.

        Example:
            Exercise BufferedStream.seek through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param pos: Value supplied for pos under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        assert pos <= self._bufferedBytes()
        offset = pos
        i = 0
        while len(self.buffer[i]) < offset:
            offset -= len(self.buffer[i])
            i += 1
        self.position = [i, offset]

    def read(self, bytes):
        """
        Forward the read operation while preserving adapter ownership rules.

        Example:
            Exercise BufferedStream.read through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param bytes: Value supplied for bytes under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not self.buffer:
            return self._readStream(bytes)
        elif self.position[0] == len(self.buffer) and self.position[1] == len(self.buffer[-1]):
            return self._readStream(bytes)
        else:
            return self._readFromBuffer(bytes)

    def _bufferedBytes(self):
        """
        Perform the bufferedBytes utility operation under explicit compatibility rules.

        Example:
            Exercise BufferedStream. bufferedBytes through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return sum([len(item) for item in self.buffer])

    def _readStream(self, bytes):
        """
        Perform the readStream utility operation under explicit compatibility rules.

        Example:
            Exercise BufferedStream. readStream through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param bytes: Value supplied for bytes under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        data = self.stream.read(bytes)
        self.buffer.append(data)
        self.position[0] += 1
        self.position[1] = len(data)
        return data

    def _readFromBuffer(self, bytes):
        """
        Perform the readFromBuffer utility operation under explicit compatibility rules.

        Example:
            Exercise BufferedStream. readFromBuffer through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param bytes: Value supplied for bytes under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        remainingBytes = bytes
        rv = []
        bufferIndex = self.position[0]
        bufferOffset = self.position[1]
        while bufferIndex < len(self.buffer) and remainingBytes != 0:
            assert remainingBytes > 0
            bufferedData = self.buffer[bufferIndex]

            if remainingBytes <= len(bufferedData) - bufferOffset:
                bytesToRead = remainingBytes
                self.position = [bufferIndex, bufferOffset + bytesToRead]
            else:
                bytesToRead = len(bufferedData) - bufferOffset
                self.position = [bufferIndex, len(bufferedData)]
                bufferIndex += 1
            rv.append(bufferedData[bufferOffset : bufferOffset + bytesToRead])
            remainingBytes -= bytesToRead

            bufferOffset = 0

        if remainingBytes:
            rv.append(self._readStream(remainingBytes))

        return b"".join(rv)


def HTMLInputStream(source, encoding=None, parseMeta=True, chardet=True):
    """
    Perform the HTMLInputStream utility operation under explicit compatibility rules.

    Example:
        Exercise HTMLInputStream through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param source: Value supplied for source under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param parseMeta: Value supplied for parseMeta under the utility contract.
    :param chardet: Value supplied for chardet under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if (
        hasattr(source, "unget")
        and hasattr(source, "charsUntil")
        and hasattr(source, "position")
        and hasattr(source, "char")
        and hasattr(source, "reset")
        and hasattr(source, "errors")
    ):
        return source
    if hasattr(source, "read"):
        isUnicode = isinstance(source.read(0), text_type)
    else:
        isUnicode = isinstance(source, text_type)

    if isUnicode:
        if encoding is not None:
            raise TypeError("Cannot explicitly set an encoding with a unicode string")

        return HTMLUnicodeInputStream(source)
    else:
        return HTMLBinaryInputStream(source, encoding, parseMeta, chardet)


class HTMLUnicodeInputStream(object):
    """
    Provides a unicode stream of characters to the HTMLTokenizer.

    Example:
        Exercise HTMLUnicodeInputStream through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """

    _defaultChunkSize = 10240

    def __init__(self, source):
        """
        Initialises the HTMLInputStream.

        Example:
            Exercise HTMLUnicodeInputStream.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param source: Value supplied for source under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """

        # Craziness
        if len("\U0010FFFF") == 1:
            self.reportCharacterErrors = self.characterErrorsUCS4
            self.replaceCharactersRegexp = re.compile("[\uD800-\uDFFF]")
        else:
            self.reportCharacterErrors = self.characterErrorsUCS2
            self.replaceCharactersRegexp = re.compile(
                "([\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF])"
            )

        # List of where new lines occur
        self.newLines = [0]

        self.charEncoding = ("utf-8", "certain")
        self.dataStream = self.openStream(source)

        self.reset()

    def reset(self):
        """
        Perform the reset utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLUnicodeInputStream.reset through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.chunk = ""
        self.chunkSize = 0
        self.chunkOffset = 0
        self.errors = []

        # number of (complete) lines in previous chunks
        self.prevNumLines = 0
        # number of columns in the last line of the previous chunk
        self.prevNumCols = 0

        # Deal with CR LF and surrogates split over chunk boundaries
        self._bufferedCharacter = None

    def openStream(self, source):
        """
        Produces a file object from source.

        Example:
            Exercise HTMLUnicodeInputStream.openStream through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param source: Value supplied for source under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Already a file object
        if hasattr(source, "read"):
            stream = source
        else:
            stream = StringIO(source)

        return stream

    def _position(self, offset):
        """
        Perform the position utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLUnicodeInputStream. position through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param offset: Value supplied for offset under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        chunk = self.chunk
        nLines = chunk.count("\n", 0, offset)
        positionLine = self.prevNumLines + nLines
        lastLinePos = chunk.rfind("\n", 0, offset)
        if lastLinePos == -1:
            positionColumn = self.prevNumCols + offset
        else:
            positionColumn = offset - (lastLinePos + 1)
        return (positionLine, positionColumn)

    def position(self):
        """
        Returns (line, col) of the current position in the stream.

        Example:
            Exercise HTMLUnicodeInputStream.position through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        line, col = self._position(self.chunkOffset)
        return (line + 1, col)

    def char(self):
        """
        Read one character from the stream or queue if available. Return EOF when EOF is reached.

        Example:
            Exercise HTMLUnicodeInputStream.char through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Read a new chunk from the input stream if necessary
        if self.chunkOffset >= self.chunkSize:
            if not self.readChunk():
                return EOF

        chunkOffset = self.chunkOffset
        char = self.chunk[chunkOffset]
        self.chunkOffset = chunkOffset + 1

        return char

    def readChunk(self, chunkSize=None):
        """
        Perform the readChunk utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLUnicodeInputStream.readChunk through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param chunkSize: Value supplied for chunkSize under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if chunkSize is None:
            chunkSize = self._defaultChunkSize

        self.prevNumLines, self.prevNumCols = self._position(self.chunkSize)

        self.chunk = ""
        self.chunkSize = 0
        self.chunkOffset = 0

        data = self.dataStream.read(chunkSize)

        # Deal with CR LF and surrogates broken across chunks
        if self._bufferedCharacter:
            data = self._bufferedCharacter + data
            self._bufferedCharacter = None
        elif not data:
            # We have no more data, bye-bye stream
            return False

        if len(data) > 1:
            lastv = ord(data[-1])
            if lastv == 0x0D or 0xD800 <= lastv <= 0xDBFF:
                self._bufferedCharacter = data[-1]
                data = data[:-1]

        self.reportCharacterErrors(data)

        # Replace invalid characters
        # Note U+0000 is dealt with in the tokenizer
        data = self.replaceCharactersRegexp.sub("\ufffd", data)

        data = data.replace("\r\n", "\n")
        data = data.replace("\r", "\n")

        self.chunk = data
        self.chunkSize = len(data)

        return True

    def characterErrorsUCS4(self, data):
        """
        Perform the characterErrorsUCS4 utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLUnicodeInputStream.characterErrorsUCS4 through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for i in range(len(invalid_unicode_re.findall(data))):
            self.errors.append("invalid-codepoint")

    def characterErrorsUCS2(self, data):
        # Someone picked the wrong compile option
        # You lose
        """
        Perform the characterErrorsUCS2 utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLUnicodeInputStream.characterErrorsUCS2 through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        skip = False
        for match in invalid_unicode_re.finditer(data):
            if skip:
                continue
            codepoint = ord(match.group())
            pos = match.start()
            # Pretty sure there should be endianness issues here
            if utils.isSurrogatePair(data[pos : pos + 2]):
                # We have a surrogate pair!
                char_val = utils.surrogatePairToCodepoint(data[pos : pos + 2])
                if char_val in non_bmp_invalid_codepoints:
                    self.errors.append("invalid-codepoint")
                skip = True
            elif codepoint >= 0xD800 and codepoint <= 0xDFFF and pos == len(data) - 1:
                self.errors.append("invalid-codepoint")
            else:
                skip = False
                self.errors.append("invalid-codepoint")

    def charsUntil(self, characters, opposite=False):
        """
        Returns a string of characters from the stream up to but not including any character in 'characters' or EOF. 'characters' must be a container that supports the 'in' method and iteration over its characters.

        Example:
            Exercise HTMLUnicodeInputStream.charsUntil through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param characters: Value supplied for characters under the utility contract.
        :param opposite: Value supplied for opposite under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        # Use a cache of regexps to find the required characters
        try:
            chars = charsUntilRegEx[(characters, opposite)]
        except KeyError:
            if __debug__:
                for c in characters:
                    assert ord(c) < 128
            regex = "".join(["\\x%02x" % ord(c) for c in characters])
            if not opposite:
                regex = "^%s" % regex
            chars = charsUntilRegEx[(characters, opposite)] = re.compile("[%s]+" % regex)

        rv = []

        while True:
            # Find the longest matching prefix
            m = chars.match(self.chunk, self.chunkOffset)
            if m is None:
                # If nothing matched, and it wasn't because we ran out of chunk,
                # then stop
                if self.chunkOffset != self.chunkSize:
                    break
            else:
                end = m.end()
                # If not the whole chunk matched, return everything
                # up to the part that didn't match
                if end != self.chunkSize:
                    rv.append(self.chunk[self.chunkOffset : end])
                    self.chunkOffset = end
                    break
            # If the whole remainder of the chunk matched,
            # use it all and read the next chunk
            rv.append(self.chunk[self.chunkOffset :])
            if not self.readChunk():
                # Reached EOF
                break

        r = "".join(rv)
        return r

    def unget(self, char):
        # Only one character is allowed to be ungotten at once - it must
        # be consumed again before any further call to unget
        """
        Perform the unget utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLUnicodeInputStream.unget through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param char: Value supplied for char under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if char is not None:
            if self.chunkOffset == 0:
                # unget is called quite rarely, so it's a good idea to do
                # more work here if it saves a bit of work in the frequently
                # called char and charsUntil.
                # So, just prepend the ungotten character onto the current
                # chunk:
                self.chunk = char + self.chunk
                self.chunkSize += 1
            else:
                self.chunkOffset -= 1
                assert self.chunk[self.chunkOffset] == char


class HTMLBinaryInputStream(HTMLUnicodeInputStream):
    """
    Provides a unicode stream of characters to the HTMLTokenizer.

    Example:
        Exercise HTMLBinaryInputStream through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """

    def __init__(self, source, encoding=None, parseMeta=True, chardet=True):
        """
        Initialises the HTMLInputStream.

        Example:
            Exercise HTMLBinaryInputStream.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param source: Value supplied for source under the utility contract.
        :param encoding: Value supplied for encoding under the utility contract.
        :param parseMeta: Value supplied for parseMeta under the utility contract.
        :param chardet: Value supplied for chardet under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        # Raw Stream - for unicode objects this will encode to utf-8 and set
        #              self.charEncoding as appropriate
        self.rawStream = self.openStream(source)

        HTMLUnicodeInputStream.__init__(self, self.rawStream)

        self.charEncoding = (codecName(encoding), "certain")

        # Encoding Information
        # Number of bytes to use when looking for a meta element with
        # encoding information
        self.numBytesMeta = 512
        # Number of bytes to use when using detecting encoding using chardet
        self.numBytesChardet = 100
        # Encoding to use if no other information can be found
        self.defaultEncoding = "windows-1252"

        # Detect encoding iff no explicit "transport level" encoding is supplied
        if self.charEncoding[0] is None:
            self.charEncoding = self.detectEncoding(parseMeta, chardet)

        # Call superclass
        self.reset()

    def reset(self):
        """
        Perform the reset utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLBinaryInputStream.reset through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.dataStream = codecs.getreader(self.charEncoding[0])(self.rawStream, "replace")
        HTMLUnicodeInputStream.reset(self)

    def openStream(self, source):
        """
        Produces a file object from source.

        Example:
            Exercise HTMLBinaryInputStream.openStream through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param source: Value supplied for source under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Already a file object
        if hasattr(source, "read"):
            stream = source
        else:
            stream = BytesIO(source)

        try:
            stream.seek(stream.tell())
        except:
            stream = BufferedStream(stream)

        return stream

    def detectEncoding(self, parseMeta=True, chardet=True):
        # First look for a BOM
        # This will also read past the BOM if present
        """
        Perform the detectEncoding utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLBinaryInputStream.detectEncoding through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param parseMeta: Value supplied for parseMeta under the utility contract.
        :param chardet: Value supplied for chardet under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        encoding = self.detectBOM()
        confidence = "certain"
        # If there is no BOM need to look for meta elements with encoding
        # information
        if encoding is None and parseMeta:
            encoding = self.detectEncodingMeta()
            confidence = "tentative"
        # Guess with chardet, if avaliable
        if encoding is None and chardet:
            confidence = "tentative"
            try:
                try:
                    from charade.universaldetector import UniversalDetector
                except ImportError:
                    from chardet.universaldetector import UniversalDetector
                buffers = []
                detector = UniversalDetector()
                while not detector.done:
                    buffer = self.rawStream.read(self.numBytesChardet)
                    assert isinstance(buffer, bytes)
                    if not buffer:
                        break
                    buffers.append(buffer)
                    detector.feed(buffer)
                detector.close()
                encoding = detector.result["encoding"]
                self.rawStream.seek(0)
            except ImportError:
                pass
        # If all else fails use the default encoding
        if encoding is None:
            confidence = "tentative"
            encoding = self.defaultEncoding

        # Substitute for equivalent encodings:
        encodingSub = {"iso-8859-1": "windows-1252"}

        if encoding.lower() in encodingSub:
            encoding = encodingSub[encoding.lower()]

        return encoding, confidence

    def changeEncoding(self, newEncoding):
        """
        Perform the changeEncoding utility operation under explicit compatibility rules.

        Example:
            Exercise HTMLBinaryInputStream.changeEncoding through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param newEncoding: Value supplied for newEncoding under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        assert self.charEncoding[1] != "certain"
        newEncoding = codecName(newEncoding)
        if newEncoding in ("utf-16", "utf-16-be", "utf-16-le"):
            newEncoding = "utf-8"
        if newEncoding is None:
            return
        elif newEncoding == self.charEncoding[0]:
            self.charEncoding = (self.charEncoding[0], "certain")
        else:
            self.rawStream.seek(0)
            self.reset()
            self.charEncoding = (newEncoding, "certain")
            raise ReparseException("Encoding changed from %s to %s" % (self.charEncoding[0], newEncoding))

    def detectBOM(self):
        """
        Attempts to detect at BOM at the start of the stream. If an encoding can be determined from the BOM return the name of the encoding otherwise return None

        Example:
            Exercise HTMLBinaryInputStream.detectBOM through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        bomDict = {
            codecs.BOM_UTF8: "utf-8",
            codecs.BOM_UTF16_LE: "utf-16-le",
            codecs.BOM_UTF16_BE: "utf-16-be",
            codecs.BOM_UTF32_LE: "utf-32-le",
            codecs.BOM_UTF32_BE: "utf-32-be",
        }

        # Go to beginning of file and read in 4 bytes
        string = self.rawStream.read(4)
        assert isinstance(string, bytes)

        # Try detecting the BOM using bytes from the string
        encoding = bomDict.get(string[:3])  # UTF-8
        seek = 3
        if not encoding:
            # Need to detect UTF-32 before UTF-16
            encoding = bomDict.get(string)  # UTF-32
            seek = 4
            if not encoding:
                encoding = bomDict.get(string[:2])  # UTF-16
                seek = 2

        # Set the read position past the BOM if one was found, otherwise
        # set it to the start of the stream
        self.rawStream.seek(encoding and seek or 0)

        return encoding

    def detectEncodingMeta(self):
        """
        Report the encoding declared by the meta element

        Example:
            Exercise HTMLBinaryInputStream.detectEncodingMeta through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        buffer = self.rawStream.read(self.numBytesMeta)
        assert isinstance(buffer, bytes)
        parser = EncodingParser(buffer)
        self.rawStream.seek(0)
        encoding = parser.getEncoding()

        if encoding in ("utf-16", "utf-16-be", "utf-16-le"):
            encoding = "utf-8"

        return encoding


class EncodingBytes(bytes):
    """
    String-like object with an associated position and various extra methods If the position is ever greater than the string length then an exception is raised

    Example:
        Exercise EncodingBytes through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """

    def __new__(self, value):
        """
        Perform the new utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingBytes.  new   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param value: Value normalized, stored, formatted or returned.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert isinstance(value, bytes)
        return bytes.__new__(self, value.lower())

    def __init__(self, value):
        """
        Initialize and validate the EncodingBytes state.

        Example:
            Exercise EncodingBytes.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param value: Value normalized, stored, formatted or returned.
        :return: None; validated state is stored on the receiving object.
        """
        self._position = -1

    def __iter__(self):
        """
        Expose iter behavior for the compatibility container.

        Example:
            Exercise EncodingBytes.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self

    def __next__(self):
        """
        Perform the next utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingBytes.  next   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        p = self._position = self._position + 1
        if p >= len(self):
            raise StopIteration
        elif p < 0:
            raise TypeError
        return self[p : p + 1]

    def next(self):
        # Py2 compat
        """
        Perform the next utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingBytes.next through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.__next__()

    def previous(self):
        """
        Perform the previous utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingBytes.previous through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        p = self._position
        if p >= len(self):
            raise StopIteration
        elif p < 0:
            raise TypeError
        self._position = p = p - 1
        return self[p : p + 1]

    def setPosition(self, position):
        """
        Perform the setPosition utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingBytes.setPosition through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param position: Value supplied for position under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self._position >= len(self):
            raise StopIteration
        self._position = position

    def getPosition(self):
        """
        Perform the getPosition utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingBytes.getPosition through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._position >= len(self):
            raise StopIteration
        if self._position >= 0:
            return self._position
        else:
            return None

    position = property(getPosition, setPosition)

    def getCurrentByte(self):
        """
        Perform the getCurrentByte utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingBytes.getCurrentByte through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self[self.position : self.position + 1]

    currentByte = property(getCurrentByte)

    def skip(self, chars=spaceCharactersBytes):
        """
        Skip past a list of characters

        Example:
            Exercise EncodingBytes.skip through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param chars: Value supplied for chars under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        p = self.position  # use property for the error-checking
        while p < len(self):
            c = self[p : p + 1]
            if c not in chars:
                self._position = p
                return c
            p += 1
        self._position = p
        return None

    def skipUntil(self, chars):
        """
        Perform the skipUntil utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingBytes.skipUntil through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param chars: Value supplied for chars under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        p = self.position
        while p < len(self):
            c = self[p : p + 1]
            if c in chars:
                self._position = p
                return c
            p += 1
        self._position = p
        return None

    def matchBytes(self, bytes):
        """
        Look for a sequence of bytes at the start of a string. If the bytes are found return True and advance the position to the byte after the match. Otherwise return False and leave the position alone

        Example:
            Exercise EncodingBytes.matchBytes through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param bytes: Value supplied for bytes under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        p = self.position
        data = self[p : p + len(bytes)]
        rv = data.startswith(bytes)
        if rv:
            self.position += len(bytes)
        return rv

    def jumpTo(self, bytes):
        """
        Look for the next sequence of bytes matching a given sequence. If a match is found advance the position to the last byte of the match

        Example:
            Exercise EncodingBytes.jumpTo through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param bytes: Value supplied for bytes under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        newPosition = self[self.position :].find(bytes)
        if newPosition > -1:
            # XXX: This is ugly, but I can't see a nicer way to fix this.
            if self._position == -1:
                self._position = 0
            self._position += newPosition + len(bytes) - 1
            return True
        else:
            raise StopIteration


class EncodingParser(object):
    """
    Mini parser for detecting character encoding from meta elements

    Example:
        Exercise EncodingParser through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """

    def __init__(self, data):
        """
        string - the data to work on for encoding detection

        Example:
            Exercise EncodingParser.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.data = EncodingBytes(data)
        self.encoding = None

    def getEncoding(self):
        """
        Perform the getEncoding utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingParser.getEncoding through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        methodDispatch = (
            (b"<!--", self.handleComment),
            (b"<meta", self.handleMeta),
            (b"</", self.handlePossibleEndTag),
            (b"<!", self.handleOther),
            (b"<?", self.handleOther),
            (b"<", self.handlePossibleStartTag),
        )
        for byte in self.data:
            keepParsing = True
            for key, method in methodDispatch:
                if self.data.matchBytes(key):
                    try:
                        keepParsing = method()
                        break
                    except StopIteration:
                        keepParsing = False
                        break
            if not keepParsing:
                break

        return self.encoding

    def handleComment(self):
        """
        Skip over comments

        Example:
            Exercise EncodingParser.handleComment through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.data.jumpTo(b"-->")

    def handleMeta(self):
        """
        Perform the handleMeta utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingParser.handleMeta through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.data.currentByte not in spaceCharactersBytes:
            # if we have <meta not followed by a space so just keep going
            return True
        # We have a valid meta element we want to search for attributes
        hasPragma = False
        pendingEncoding = None
        while True:
            # Try to find the next attribute after the current position
            attr = self.getAttribute()
            if attr is None:
                return True
            else:
                if attr[0] == b"http-equiv":
                    hasPragma = attr[1] == b"content-type"
                    if hasPragma and pendingEncoding is not None:
                        self.encoding = pendingEncoding
                        return False
                elif attr[0] == b"charset":
                    tentativeEncoding = attr[1]
                    codec = codecName(tentativeEncoding)
                    if codec is not None:
                        self.encoding = codec
                        return False
                elif attr[0] == b"content":
                    contentParser = ContentAttrParser(EncodingBytes(attr[1]))
                    tentativeEncoding = contentParser.parse()
                    if tentativeEncoding is not None:
                        codec = codecName(tentativeEncoding)
                        if codec is not None:
                            if hasPragma:
                                self.encoding = codec
                                return False
                            else:
                                pendingEncoding = codec

    def handlePossibleStartTag(self):
        """
        Perform the handlePossibleStartTag utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingParser.handlePossibleStartTag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.handlePossibleTag(False)

    def handlePossibleEndTag(self):
        """
        Perform the handlePossibleEndTag utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingParser.handlePossibleEndTag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        next(self.data)
        return self.handlePossibleTag(True)

    def handlePossibleTag(self, endTag):
        """
        Perform the handlePossibleTag utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingParser.handlePossibleTag through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param endTag: Value supplied for endTag under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        data = self.data
        if data.currentByte not in asciiLettersBytes:
            # If the next byte is not an ascii letter either ignore this
            # fragment (possible start tag case) or treat it according to
            # handleOther
            if endTag:
                data.previous()
                self.handleOther()
            return True

        c = data.skipUntil(spacesAngleBrackets)
        if c == b"<":
            # return to the first step in the overall "two step" algorithm
            # reprocessing the < byte
            data.previous()
        else:
            # Read all attributes
            attr = self.getAttribute()
            while attr is not None:
                attr = self.getAttribute()
        return True

    def handleOther(self):
        """
        Perform the handleOther utility operation under explicit compatibility rules.

        Example:
            Exercise EncodingParser.handleOther through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.data.jumpTo(b">")

    def getAttribute(self):
        """
        Return a name,value pair for the next attribute in the stream, if one is found, or None

        Example:
            Exercise EncodingParser.getAttribute through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        data = self.data
        # Step 1 (skip chars)
        c = data.skip(spaceCharactersBytes | frozenset([b"/"]))
        assert c is None or len(c) == 1
        # Step 2
        if c in (b">", None):
            return None
        # Step 3
        attrName = []
        attrValue = []
        # Step 4 attribute name
        while True:
            if c == b"=" and attrName:
                break
            elif c in spaceCharactersBytes:
                # Step 6!
                c = data.skip()
                break
            elif c in (b"/", b">"):
                return b"".join(attrName), b""
            elif c in asciiUppercaseBytes:
                attrName.append(c.lower())
            elif c is None:
                return None
            else:
                attrName.append(c)
            # Step 5
            c = next(data)
        # Step 7
        if c != b"=":
            data.previous()
            return b"".join(attrName), b""
        # Step 8
        next(data)
        # Step 9
        c = data.skip()
        # Step 10
        if c in (b"'", b'"'):
            # 10.1
            quoteChar = c
            while True:
                # 10.2
                c = next(data)
                # 10.3
                if c == quoteChar:
                    next(data)
                    return b"".join(attrName), b"".join(attrValue)
                # 10.4
                elif c in asciiUppercaseBytes:
                    attrValue.append(c.lower())
                # 10.5
                else:
                    attrValue.append(c)
        elif c == b">":
            return b"".join(attrName), b""
        elif c in asciiUppercaseBytes:
            attrValue.append(c.lower())
        elif c is None:
            return None
        else:
            attrValue.append(c)
        # Step 11
        while True:
            c = next(data)
            if c in spacesAngleBrackets:
                return b"".join(attrName), b"".join(attrValue)
            elif c in asciiUppercaseBytes:
                attrValue.append(c.lower())
            elif c is None:
                return None
            else:
                attrValue.append(c)


class ContentAttrParser(object):
    """
    Parse or evaluate ContentAttrParser expressions under Calibre template semantics.

    Example:
        Exercise ContentAttrParser through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __init__(self, data):
        """
        Initialize and validate the ContentAttrParser state.

        Example:
            Exercise ContentAttrParser.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        assert isinstance(data, bytes)
        self.data = data

    def parse(self):
        """
        Parse the supplied date text and return its normalized datetime value.

        Example:
            Exercise ContentAttrParser.parse through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            # Check if the attr name is charset
            # otherwise return
            self.data.jumpTo(b"charset")
            self.data.position += 1
            self.data.skip()
            if not self.data.currentByte == b"=":
                # If there is no = sign keep looking for attrs
                return None
            self.data.position += 1
            self.data.skip()
            # Look for an encoding between matching quote marks
            if self.data.currentByte in (b'"', b"'"):
                quoteMark = self.data.currentByte
                self.data.position += 1
                oldPosition = self.data.position
                if self.data.jumpTo(quoteMark):
                    return self.data[oldPosition : self.data.position]
                else:
                    return None
            else:
                # Unquoted value
                oldPosition = self.data.position
                try:
                    self.data.skipUntil(spaceCharactersBytes)
                    return self.data[oldPosition : self.data.position]
                except StopIteration:
                    # Return the whole remaining value
                    return self.data[oldPosition:]
        except StopIteration:
            return None


def codecName(encoding):
    """
    Return the python codec name corresponding to an encoding or None if the string doesn't correspond to a valid encoding.

    Example:
        Exercise codecName through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param encoding: Value supplied for encoding under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(encoding, bytes):
        try:
            encoding = encoding.decode("ascii")
        except UnicodeDecodeError:
            return None
    if encoding:
        canonicalName = ascii_punctuation_re.sub("", encoding).lower()
        return encodings.get(canonicalName, None)
    else:
        return None
