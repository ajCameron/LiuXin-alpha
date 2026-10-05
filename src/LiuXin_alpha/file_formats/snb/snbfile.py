# -*- coding: utf-8 -*-

"""
Read and write SNB archives, metadata and resource trees.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise snbfile through a consuming regression::

        python -m pytest -q tests/file_formats/snb/test_snb_modernized.py
"""
from __future__ import annotations

import typing as _typing

import bz2
import os
import struct
import sys
import zlib
from dataclasses import dataclass
from functools import cmp_to_key
from typing import BinaryIO, Protocol, TypeAlias

from LiuXin_alpha.utils.calibre import guess_type
from LiuXin_alpha.utils.libraries.liuxin_six import six_cmp

__license__ = "GPL 3"
__copyright__ = "2010, Li Fanxi <lifanxi@freemindworld.com>"
__docformat__ = "restructuredtext en"


def _ceil_div(value: int, chunk: int) -> int:
    """
    Perform the ceil div operation under explicit file-format and conversion rules.

    Example:
        Exercise  ceil div through a consuming regression::

            python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


    :param value: Value normalized, stored, formatted or returned.
    :param chunk: Value supplied for chunk under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if value <= 0:
        return 0
    return (value + chunk - 1) // chunk


def _normalize_name(name: str) -> str:
    # SNB stores Unix style paths regardless of host platform.
    """
    Normalize name under the format's safety and compatibility rules.

    Example:
        Exercise  normalize name through a consuming regression::

            python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return str(name).replace("\\", "/").replace(os.sep, "/")


@dataclass
class FileStream:
    """
    Provide the filestream contract for validated ebook processing.

    Example:
        Exercise FileStream through a consuming regression::

            python -m pytest -q tests/file_formats/snb/test_snb_modernized.py
    """
    attr: int = 0
    fileNameOffset: int = 0
    fileSize: int = 0
    fileName: str = ""
    blockIndex: int = 0
    contentOffset: int = 0
    fileBody: bytes = b""

    def IsBinary(self: _typing.Self) -> bool:
        """
        Perform the IsBinary operation under explicit file-format and conversion rules.

        Example:
            Exercise FileStream.IsBinary through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.attr & 0x41000000 != 0x41000000


def compareFileStream(file1: FileStream, file2: FileStream) -> int:
    """
    Perform the compareFileStream operation under explicit file-format and conversion rules.

    Example:
        Exercise compareFileStream through a consuming regression::

            python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


    :param file1: Value supplied for file1 under the utility contract.
    :param file2: Value supplied for file2 under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return six_cmp(file1.fileName, file2.fileName)


@dataclass
class BlockData:
    """
    Provide the blockdata contract for validated ebook processing.

    Example:
        Exercise BlockData through a consuming regression::

            python -m pytest -q tests/file_formats/snb/test_snb_modernized.py
    """
    Offset: int = 0


class _BinaryWriter(Protocol):
    """
    Provide the binarywriter contract for validated ebook processing.

    Example:
        Exercise  BinaryWriter through a consuming regression::

            python -m pytest -q tests/file_formats/snb/test_snb_modernized.py
    """
    def close(self: _typing.Self) -> object:
        """
        Perform the close operation under explicit file-format and conversion rules.

        Example:
            Exercise  BinaryWriter.close through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    def tell(self: _typing.Self) -> int:
        """
        Perform the tell operation under explicit file-format and conversion rules.

        Example:
            Exercise  BinaryWriter.tell through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    def seek(self: _typing.Self, offset: int, whence: int = 0) -> int:
        """
        Perform the seek operation under explicit file-format and conversion rules.

        Example:
            Exercise  BinaryWriter.seek through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param offset: Value supplied for offset under the utility contract.
        :param whence: Value supplied for whence under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    def write(self: _typing.Self, data: bytes) -> int:
        """
        Perform the write operation under explicit file-format and conversion rules.

        Example:
            Exercise  BinaryWriter.write through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...


SNBPath: TypeAlias = str | os.PathLike[str]
SNBOutput: TypeAlias = SNBPath | _BinaryWriter


class SNBFile:
    """
    Provide the snbfile contract for validated ebook processing.

    Example:
        Exercise SNBFile through a consuming regression::

            python -m pytest -q tests/file_formats/snb/test_snb_modernized.py
    """
    BLOCK_SIZE = 0x8000
    HEADER_SIZE = 44

    MAGIC = b"SNBP000B"
    REV80 = 0x00008000
    REVA3 = 0x00A3A3A3
    REVZ1 = 0x00000000
    REVZ2 = 0x00000000

    def __init__(
        self: _typing.Self,
        inputFile: SNBPath | None = None,
    ) -> None:
        """
        Initialize and validate the snbfile state.

        Example:
            Exercise SNBFile.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param inputFile: Value supplied for inputFile under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.files: list[FileStream] = []
        self.blocks: list[BlockData] = []
        self.fileName: str | None = None
        self.magic = b""
        self.rev80 = 0
        self.revA3 = 0
        self.revZ1 = 0
        self.fileCount = 0
        self.vfatSize = 0
        self.vfatCompressed = 0
        self.binStreamSize = 0
        self.plainStreamSizeUncompressed = 0
        self.revZ2 = 0
        self.vfat = b""
        self.tailSize = 0
        self.tailOffset = 0
        self.tailMagic = b""
        self.vTailUncompressed = b""
        self.tailSizeUncompressed = 0
        self.binBlock = 0
        self.plainBlock = 0
        if inputFile is not None:
            self.Open(inputFile)

    @staticmethod
    def _read_c_string(blob: bytes, offset: int) -> bytes:
        """
        Read c string under the format's safety and compatibility rules.

        Example:
            Exercise SNBFile. read c string through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param blob: Value supplied for blob under the utility contract.
        :param offset: Value supplied for offset under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if offset < 0 or offset >= len(blob):
            return b""
        end = blob.find(b"\0", offset)
        if end < 0:
            end = len(blob)
        return blob[offset:end]

    @staticmethod
    def _decode_name(raw: bytes) -> str:
        """
        Perform the decode name operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile. decode name through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param raw: Value supplied for raw under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return raw.decode("utf-8", "replace")

    @staticmethod
    def _encode_name(name: str) -> bytes:
        """
        Perform the encode name operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile. encode name through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return _normalize_name(name).encode("utf-8", "replace")

    def Open(self: _typing.Self, inputFile: SNBPath) -> None:
        """
        Perform the Open operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile.Open through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param inputFile: Value supplied for inputFile under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.fileName = os.fspath(inputFile)
        with open(self.fileName, "rb") as snbFile:
            snbFile.seek(0)
            self.Parse(snbFile)

    def Parse(self: _typing.Self, snbFile: BinaryIO, metaOnly: bool = False) -> None:
        """
        Perform the Parse operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile.Parse through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param snbFile: Value supplied for snbFile under the utility contract.
        :param metaOnly: Value supplied for metaOnly under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.files = []
        self.blocks = []

        vmbr = snbFile.read(self.HEADER_SIZE)
        if len(vmbr) != self.HEADER_SIZE:
            raise ValueError("SNB header is truncated")
        (
            self.magic,
            self.rev80,
            self.revA3,
            self.revZ1,
            self.fileCount,
            self.vfatSize,
            self.vfatCompressed,
            self.binStreamSize,
            self.plainStreamSizeUncompressed,
            self.revZ2,
        ) = struct.unpack(">8siiiiiiiii", vmbr)

        self.vfat = zlib.decompress(snbFile.read(self.vfatCompressed))
        self.ParseFile(self.vfat, self.fileCount)

        snbFile.seek(-16, os.SEEK_END)
        tailblock = snbFile.read(16)
        if len(tailblock) != 16:
            raise ValueError("SNB tail pointer is truncated")
        (self.tailSize, self.tailOffset, self.tailMagic) = struct.unpack(">ii8s", tailblock)
        snbFile.seek(self.tailOffset)
        self.vTailUncompressed = zlib.decompress(snbFile.read(self.tailSize))
        self.tailSizeUncompressed = len(self.vTailUncompressed)
        self.ParseTail(self.vTailUncompressed, self.fileCount)

        if metaOnly:
            return

        binary_stream_offset = self.HEADER_SIZE + self.vfatCompressed
        snbFile.seek(binary_stream_offset)
        binary_stream = snbFile.read(self.binStreamSize)
        plain_stream = self._read_plain_stream(snbFile)

        for f in self.files:
            if f.attr & 0x41000000 == 0x41000000:
                plain_block_index = max(0, f.blockIndex - self.binBlock)
                start = plain_block_index * self.BLOCK_SIZE + f.contentOffset
                f.fileBody = plain_stream[start : start + f.fileSize]
            elif f.attr & 0x01000000 == 0x01000000:
                start = f.blockIndex * self.BLOCK_SIZE + f.contentOffset
                f.fileBody = binary_stream[start : start + f.fileSize]
            else:
                raise ValueError(f"Invalid file entry attr={f.attr!r} name={f.fileName!r}")

    def _read_plain_stream(self: _typing.Self, snbFile: BinaryIO) -> bytes:
        """
        Read plain stream under the format's safety and compatibility rules.

        Example:
            Exercise SNBFile. read plain stream through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param snbFile: Value supplied for snbFile under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.plainBlock <= 0:
            return b""

        chunks = []
        for i in range(self.plainBlock):
            idx = self.binBlock + i
            start = self.blocks[idx].Offset
            end = self.tailOffset if i == self.plainBlock - 1 else self.blocks[idx + 1].Offset
            if end < start:
                raise ValueError("SNB block table is malformed")
            snbFile.seek(start)
            data = snbFile.read(end - start)
            if not data:
                continue
            try:
                chunks.append(bz2.decompress(data))
            except Exception:
                # Compatibility: malformed files can contain raw blocks.
                chunks.append(data)
        return b"".join(chunks)

    def ParseFile(self: _typing.Self, vfat: bytes, fileCount: int) -> None:
        """
        Perform the ParseFile operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile.ParseFile through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param vfat: Value supplied for vfat under the utility contract.
        :param fileCount: Value supplied for fileCount under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        names_blob = vfat[fileCount * 12 :]
        for i in range(fileCount):
            f = FileStream()
            (f.attr, f.fileNameOffset, f.fileSize) = struct.unpack(">iii", vfat[i * 12 : (i + 1) * 12])
            f.fileName = self._decode_name(self._read_c_string(names_blob, f.fileNameOffset))
            self.files.append(f)

    def ParseTail(self: _typing.Self, vtail: bytes, fileCount: int) -> None:
        """
        Perform the ParseTail operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile.ParseTail through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param vtail: Value supplied for vtail under the utility contract.
        :param fileCount: Value supplied for fileCount under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.binBlock = _ceil_div(self.binStreamSize, self.BLOCK_SIZE)
        self.plainBlock = _ceil_div(self.plainStreamSizeUncompressed, self.BLOCK_SIZE)
        for i in range(self.binBlock + self.plainBlock):
            block = BlockData()
            (block.Offset,) = struct.unpack(">i", vtail[i * 4 : (i + 1) * 4])
            self.blocks.append(block)
        rec_start = (self.binBlock + self.plainBlock) * 4
        for i in range(fileCount):
            (self.files[i].blockIndex, self.files[i].contentOffset) = struct.unpack(
                ">ii",
                vtail[rec_start + i * 8 : rec_start + (i + 1) * 8],
            )

    def IsValid(self: _typing.Self) -> bool:
        """
        Perform the IsValid operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile.IsValid through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.magic != SNBFile.MAGIC:
            return False
        if self.rev80 != SNBFile.REV80:
            return False
        if self.revZ1 != SNBFile.REVZ1:
            return False
        if self.revZ2 != SNBFile.REVZ2:
            return False
        if self.vfatSize != len(self.vfat):
            return False
        if self.fileCount != len(self.files):
            return False
        if (self.binBlock + self.plainBlock) * 4 + self.fileCount * 8 != self.tailSizeUncompressed:
            return False
        if self.tailMagic != SNBFile.MAGIC:
            return False
        return True

    def FromDir(self: _typing.Self, tdir: SNBPath) -> None:
        """
        Perform the FromDir operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile.FromDir through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param tdir: Value supplied for tdir under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for root, dirs, files in os.walk(tdir):
            dirs.sort()
            files.sort()
            for name in files:
                _, ext = os.path.splitext(name)
                rel_path = os.path.relpath(os.path.join(root, name), tdir)
                if ext in [".snbf", ".snbc"]:
                    self.AppendPlain(rel_path, tdir)
                else:
                    self.AppendBinary(rel_path, tdir)

    def _append(
        self: _typing.Self,
        fileName: str,
        tdir: SNBPath,
        attr: int,
    ) -> None:
        """
        Perform the append operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile. append through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param fileName: Value supplied for fileName under the utility contract.
        :param tdir: Value supplied for tdir under the utility contract.
        :param attr: Value supplied for attr under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        f = FileStream()
        f.attr = attr
        disk_path = os.path.join(tdir, fileName)
        with open(disk_path, "rb") as src:
            f.fileBody = src.read()
        f.fileSize = len(f.fileBody)
        f.fileName = _normalize_name(fileName)
        self.files.append(f)

    def AppendPlain(
        self: _typing.Self,
        fileName: str,
        tdir: SNBPath,
    ) -> None:
        """
        Perform the AppendPlain operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile.AppendPlain through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param fileName: Value supplied for fileName under the utility contract.
        :param tdir: Value supplied for tdir under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._append(fileName, tdir, 0x41000000)

    def AppendBinary(
        self: _typing.Self,
        fileName: str,
        tdir: SNBPath,
    ) -> None:
        """
        Perform the AppendBinary operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile.AppendBinary through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param fileName: Value supplied for fileName under the utility contract.
        :param tdir: Value supplied for tdir under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._append(fileName, tdir, 0x01000000)

    def GetFileStream(
        self: _typing.Self,
        fileName: str,
    ) -> bytes | None:
        """
        Perform the GetFileStream operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile.GetFileStream through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param fileName: Value supplied for fileName under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        target = _normalize_name(fileName)
        for file in self.files:
            if file.fileName == target:
                return file.fileBody
        return None

    def OutputImageFiles(
        self: _typing.Self,
        path: SNBPath,
    ) -> list[tuple[str, str]]:
        """
        Perform the OutputImageFiles operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile.OutputImageFiles through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        fileNames: list[tuple[str, str]] = []
        for f in self.files:
            fname = os.path.basename(f.fileName)
            _, ext = os.path.splitext(fname)
            if ext.lower() in [".jpeg", ".jpg", ".gif", ".svg", ".png"]:
                with open(os.path.join(path, fname), "wb") as file_obj:
                    file_obj.write(f.fileBody)
                mime_type = guess_type("a" + ext)[0] or "application/octet-stream"
                fileNames.append((fname, mime_type))
        return fileNames

    def Output(self: _typing.Self, outputFile: SNBOutput) -> None:
        # Required by SNB format: entries sorted by filename.
        """
        Perform the Output operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile.Output through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :param outputFile: Value supplied for outputFile under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.files.sort(key=cmp_to_key(compareFileStream))

        if isinstance(outputFile, (str, os.PathLike)):
            output_handle = open(outputFile, "wb")
            close_output = True
        else:
            output_handle = outputFile
            close_output = False

        try:
            vmbrp1 = struct.pack(
                ">8siiii",
                SNBFile.MAGIC,
                SNBFile.REV80,
                SNBFile.REVA3,
                SNBFile.REVZ1,
                len(self.files),
            )

            vfat = bytearray()
            fileNameTable = bytearray()
            plainStream = bytearray()
            binStream = bytearray()
            for f in self.files:
                name = self._encode_name(f.fileName)
                body = bytes(f.fileBody)
                f.fileSize = len(body)
                vfat.extend(struct.pack(">iii", f.attr, len(fileNameTable), f.fileSize))
                fileNameTable.extend(name + b"\0")

                if f.attr & 0x41000000 == 0x41000000:
                    f.contentOffset = len(plainStream)
                    plainStream.extend(body)
                elif f.attr & 0x01000000 == 0x01000000:
                    f.contentOffset = len(binStream)
                    binStream.extend(body)
                else:
                    raise ValueError(f"Unknown file type attr={f.attr!r} name={f.fileName!r}")

            raw_vfat = bytes(vfat) + bytes(fileNameTable)
            vfatCompressed = zlib.compress(raw_vfat)

            vmbrp2 = struct.pack(
                ">iiiii",
                len(raw_vfat),
                len(vfatCompressed),
                len(binStream),
                len(plainStream),
                SNBFile.REVZ2,
            )
            output_handle.write(vmbrp1 + vmbrp2)
            output_handle.write(vfatCompressed)

            binBlockOffset = self.HEADER_SIZE + len(vfatCompressed)
            plainBlockOffset = binBlockOffset + len(binStream)
            binBlock = _ceil_div(len(binStream), self.BLOCK_SIZE)

            tailBlock = bytearray()
            for i in range(binBlock):
                tailBlock.extend(struct.pack(">i", binBlockOffset + i * self.BLOCK_SIZE))

            tailRec = bytearray()
            for f in self.files:
                t = 0 if f.IsBinary() else binBlock
                tailRec.extend(
                    struct.pack(">ii", f.contentOffset // self.BLOCK_SIZE + t, f.contentOffset % self.BLOCK_SIZE)
                )

            output_handle.write(bytes(binStream))

            pos = 0
            offset = 0
            while pos < len(plainStream):
                block = bytes(plainStream[pos : pos + self.BLOCK_SIZE])
                compressed = bz2.compress(block)
                tailBlock.extend(struct.pack(">i", plainBlockOffset + offset))
                output_handle.write(compressed)
                offset += len(compressed)
                pos += self.BLOCK_SIZE

            compressedTail = zlib.compress(bytes(tailBlock) + bytes(tailRec))
            output_handle.write(compressedTail)
            output_handle.write(struct.pack(">ii", len(compressedTail), plainBlockOffset + offset))
            output_handle.write(SNBFile.MAGIC)
        finally:
            if close_output:
                output_handle.close()

    def Dump(self: _typing.Self) -> None:
        """
        Perform the Dump operation under explicit file-format and conversion rules.

        Example:
            Exercise SNBFile.Dump through a consuming regression::

                python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.fileName:
            print("File Name:\t", self.fileName)
        print("File Count:\t", self.fileCount)
        print("VFAT Size(Compressed):\t%d(%d)" % (self.vfatSize, self.vfatCompressed))
        print("Binary Stream Size:\t", self.binStreamSize)
        print("Plain Stream Uncompressed Size:\t", self.plainStreamSizeUncompressed)
        print("Binary Block Count:\t", self.binBlock)
        print("Plain Block Count:\t", self.plainBlock)
        for i in range(self.fileCount):
            print("File ", i)
            f = self.files[i]
            print("File Name: ", f.fileName)
            print("File Attr: ", f.attr)
            print("File Size: ", f.fileSize)
            print("Block Index: ", f.blockIndex)
            print("Content Offset: ", f.contentOffset)


def usage() -> None:
    """
    Perform the usage operation under explicit file-format and conversion rules.

    Example:
        Exercise usage through a consuming regression::

            python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    print("This unit test is for INTERNAL usage only)!")
    print("This unit test accepts two parameters.")
    print("python snbfile.py <INPUTFILE> <DESTFILE>")
    print("The input file will be extracted and written to dest file.")
    print("Meta data of the file will be shown during this process.")


def main() -> int:
    """
    Perform the main operation under explicit file-format and conversion rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/file_formats/snb/test_snb_modernized.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if len(sys.argv) != 3:
        usage()
        sys.exit(0)
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]

    print("Input file: ", inputFile)
    print("Output file: ", outputFile)

    snbFile = SNBFile(inputFile)
    if snbFile.IsValid():
        snbFile.Dump()
        snbFile.Output(outputFile)
    else:
        print("The input file is invalid.")
        return 1
    return 0


if __name__ == "__main__":
    """SNB file unit test"""
    sys.exit(main())
