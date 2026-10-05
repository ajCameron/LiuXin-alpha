#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Locate and invoke unrar-compatible tools while preserving extraction safety.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise unrar through a consuming regression::

        python -m pytest -q tests/utils/decompression/test_archives.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"

import os
import sys
import re
import posixpath
from io import BytesIO

from LiuXin_alpha.utils.calibre import force_unicode
from LiuXin_alpha.constants import filesystem_encoding, isosx


class UNRARError(Exception):
    """
    Report the UNRARError Calibre compatibility failure.

    Example:
        Exercise UNRARError through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """
    pass


class DevNull:
    """
    Provide the DevNull utility contract with explicit state and cleanup behavior.

    Example:
        Exercise DevNull through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """
    def write(self, x):
        """
        Forward the write operation while preserving adapter ownership rules.

        Example:
            Exercise DevNull.write through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param x: Value supplied for x under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass


class RARStream(object):
    """
    Provide the RARStream utility contract with explicit state and cleanup behavior.

    Example:
        Exercise RARStream through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """
    def __init__(self, stream, unrar, get_comment=False):
        """
        Initialize and validate the RARStream state.

        Example:
            Exercise RARStream.  init   through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param unrar: Value supplied for unrar under the utility contract.
        :param get_comment: Value supplied for get comment under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.stream = stream
        self.unrar = unrar
        self._current_cache = None
        try:
            self.r = unrar.RARArchive(
                stream,
                force_unicode(getattr(stream, "name", "<stream>"), filesystem_encoding),
                self,
                get_comment,
            )
        except unrar.UNRARError as e:
            raise UNRARError(type("")(e))
        self.comment = self.r.comment

    def handle_data(self, raw):
        """
        Perform the handle data utility operation under explicit compatibility rules.

        Example:
            Exercise RARStream.handle data through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param raw: Value supplied for raw under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self._current_dest is not None:
            self._current_dest.write(raw)

    def populate_header(self):
        """
        Perform the populate header utility operation under explicit compatibility rules.

        Example:
            Exercise RARStream.populate header through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        c = self._current_cache
        if c["filenamew"] is None:
            c["filenamew"] = self._decode(c["filename"])
        c["filename"] = c.pop("filenamew").replace("\\", "/")

    def _decode(self, raw):
        """
        Perform the decode utility operation under explicit compatibility rules.

        Example:
            Exercise RARStream. decode through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param raw: Value supplied for raw under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for enc in ("utf-8", "utf-16le"):
            try:
                return raw.decode(enc)
            except UnicodeDecodeError:
                pass
        return raw.decode("windows-1252", "replace")

    @property
    def current_item(self):
        """
        Perform the current item utility operation under explicit compatibility rules.

        Example:
            Exercise RARStream.current item through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self._current_cache is None:
            try:
                self._current_cache = self.r.current_item()
            except self.unrar.UNRARError as e:
                raise UNRARError(type("")(e))
            if self._current_cache is None:
                raise EOFError("End of RAR file")
            self.populate_header()
        return self._current_cache

    def process_current_item(self, extract_to=None):
        """
        Perform the process current item utility operation under explicit compatibility rules.

        Example:
            Exercise RARStream.process current item through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param extract_to: Value supplied for extract to under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self._current_cache = None
        self._current_dest = extract_to
        try:
            ans = self.r.process_item(extract_to is not None)
        except self.unrar.UNRARError as e:
            raise UNRARError(type("")(e))
        return ans

    def test(self, print_names=False):
        """
        Perform the test utility operation under explicit compatibility rules.

        Example:
            Exercise RARStream.test through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param print_names: Value supplied for print names under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        null = DevNull()
        while True:
            try:
                h = self.current_item
            except EOFError:
                break
            if print_names:
                print(h["filename"].encode(sys.stdout.encoding))
            self.process_current_item(null)


def RARFile(stream, get_comment=False):
    """
    Perform the RARFile utility operation under explicit compatibility rules.

    Example:
        Exercise RARFile through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param stream: Input or output stream wrapped by the terminal or compatibility
        layer.
    :param get_comment: Value supplied for get comment under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils.plugins import plugins

    unrar, err = plugins["unrar"]
    if not plugins.plugin_okay("unrar"):
        raise RuntimeError("Failed to load unrar module with error: %s" % err)
    return RARStream(stream, unrar, get_comment=get_comment)


class SaveStream(object):
    """
    Provide the SaveStream utility contract with explicit state and cleanup behavior.

    Example:
        Exercise SaveStream through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py
    """
    def __init__(self, stream):
        """
        Initialize and validate the SaveStream state.

        Example:
            Exercise SaveStream.  init   through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :return: None; validated state is stored on the receiving object.
        """
        self.stream = stream

    def __enter__(self):
        """
        Implement the resource's enter lifecycle operation.

        Example:
            Exercise SaveStream.  enter   through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.stream.seek(0)

    def __exit__(self, *args):
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise SaveStream.  exit   through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.stream.seek(0)


def safe_path(base, relpath):
    """
    Perform the safe path utility operation under explicit compatibility rules.

    Example:
        Exercise safe path through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param base: Value supplied for base under the utility contract.
    :param relpath: Value supplied for relpath under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    relpath = str(relpath).replace("\\", "/")
    parts = relpath.split("/")
    if (
        relpath.startswith("/")
        or (len(relpath) > 1 and relpath[1] == ":")
        or ".." in parts
    ):
        return None
    relpath = posixpath.normpath(relpath)
    if relpath in {"", ".", ".."} or relpath.startswith("../"):
        return None

    base = os.path.abspath(base)
    path = os.path.abspath(os.path.join(base, relpath))
    if os.path.normcase(path) == os.path.normcase(base) or not os.path.normcase(path).startswith(
        os.path.normcase(base)
    ):
        return None
    return path


def is_useful(h):
    """
    Return or update whether is useful holds for the compatibility value.

    Example:
        Exercise is useful through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param h: Value supplied for h under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    return not (h["is_label"] or h["is_symlink"] or h["has_password"] or h["is_directory"])


def stream_extract(stream, location):
    """
    Perform the stream extract utility operation under explicit compatibility rules.

    Example:
        Exercise stream extract through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param stream: Input or output stream wrapped by the terminal or compatibility
        layer.
    :param location: Value supplied for location under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    location = os.path.abspath(location)
    if not os.path.exists(location):
        os.makedirs(location)

    with SaveStream(stream):
        f = RARFile(stream)
        while True:
            try:
                h = f.current_item
            except EOFError:
                break
            if not is_useful(h):
                f.process_current_item()  # Skip these
                if h["is_directory"]:
                    try:
                        os.makedirs(safe_path(location, h["filename"]))
                    except:
                        # We ignore create directory errors since we dont
                        # care about missing empty dirs
                        pass
            else:
                path = safe_path(location, h["filename"])
                if path is None:
                    f.process_current_item()
                    continue
                base, fname = os.path.split(path)
                if not os.path.exists(base):
                    os.makedirs(base)
                with open(path, "wb") as dest:
                    f.process_current_item(dest)


def extract(path, location):
    """
    Extract the selected archive members into the destination under the active safety policy.

    Example:
        Exercise extract through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :param location: Value supplied for location under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with open(path, "rb") as stream:
        stream_extract(stream, location)


def names(stream):
    """
    Perform the names utility operation under explicit compatibility rules.

    Example:
        Exercise names through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param stream: Input or output stream wrapped by the terminal or compatibility
        layer.
    :return: An iterator yielding the normalized values described above.
    """
    with SaveStream(stream):
        f = RARFile(stream)
        while True:
            try:
                h = f.current_item
            except EOFError:
                break
            f.process_current_item()
            if is_useful(h):
                yield h["filename"]


def extract_member(stream, match=re.compile(r"\.(jpg|jpeg|gif|png)\s*$", re.I), name=None):
    """
    Perform the extract member utility operation under explicit compatibility rules.

    Example:
        Exercise extract member through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param stream: Input or output stream wrapped by the terminal or compatibility
        layer.
    :param match: Value supplied for match under the utility contract.
    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def is_match(fname):
        """
        Return or update whether is match holds for the compatibility value.

        Example:
            Exercise extract member.is match through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param fname: Value supplied for fname under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        return (name is not None and fname == name) or (match is not None and match.search(fname) is not None)

    with SaveStream(stream):
        f = RARFile(stream)
        while True:
            try:
                h = f.current_item
            except EOFError:
                break
            if not is_useful(h) or not is_match(h["filename"]):
                f.process_current_item()
                continue

            et = BytesIO()
            f.process_current_item(et)
            return h["filename"], et.getvalue()


def extract_first_alphabetically(stream):
    """
    Perform the extract first alphabetically utility operation under explicit compatibility rules.

    Example:
        Exercise extract first alphabetically through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param stream: Input or output stream wrapped by the terminal or compatibility
        layer.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    names_ = sorted([x for x in names(stream) if os.path.splitext(x)[1][1:].lower() in {"png", "jpg", "jpeg", "gif"}])
    return extract_member(stream, name=names_[0], match=None)


# Test normal RAR file {{{
def test_basic():

    """
    Perform the test basic utility operation under explicit compatibility rules.

    Example:
        Exercise test basic through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    stream = BytesIO(
        b"Rar!\x1a\x07\x00\xcf\x90s\x00\x00\r\x00\x00\x00\x00\x00\x00\x00\x14\xe7z\x00\x80#\x00\x17\x00\x00\x00\r\x00\x00\x00\x03\xc2\xb3\x96o\x00\x00\x00\x00\x1d3\x03\x00\x00\x00\x00\x00CMT\x0c\x00\x8b\xec\x8e\xef\x14\xf6\xe6h\x04\x17\xff\xcd\x0f\xffk9b\x11]^\x80\xd3dt \x90+\x00\x14\x00\x00\x00\x08\x00\x00\x00\x03\xf1\x84\x93\\\xb9]yA\x1d3\t\x00\xa4\x81\x00\x001\\sub-one\x00\xc0\x0c\x00\x8f\xec\x89\xfe.JM\x86\x82\x0c_\xfd\xfd\xd7\x11\x1a\xef@\x9eHt \x80'\x00\x0e\x00\x00\x00\x04\x00\x00\x00\x03\x9f\xa8\x17\xf8\xaf]yA\x1d3\x07\x00\xa4\x81\x00\x00one.txt\x00\x08\xbf\x08\xae\xf3\xca\x87\xfeo\xfe\xd2n\x80-Ht \x82:\x00\x18\x00\x00\x00\x10\x00\x00\x00\x03\xa86\x81\xdf\xf9fyA\x1d3\x1a\x00\xa4\x81\x00\x00\xe8\xaf\xb6\xe6\xaf\x94\xe5\xb1\x81.txt\x00\x8bh\xf6\xd4kA\\.\x00txt\x0c\x00\x8b\xec\x8e\xef\x14\xf6\xe2l\x91\x189\xff\xdf\xfe\xc2\xd3:g\x9a\x19F=cYt \x928\x00\x11\x00\x00\x00\x08\x00\x00\x00\x03\x7f\xd6\xb6\x7f\xeafyA\x1d3\x16\x00\xa4\x81\x00\x00F\xc3\xbc\xc3\x9fe.txt\x00\x01\x00F\xfc\xdfe\x00.txt\x00\xc0<D\xfe\xc8\xef\xbc\xd1\x04I?\xfd\xff\xdbF)]\xe8\xb9\xe1t \x90/\x00\x13\x00\x00\x00\x08\x00\x00\x00\x03\x1a$\x932\xc2]yA\x1d3\r\x00\xa4\x81\x00\x002\\sub-two.txt\x00\xc0\x10\x00S\xec\xcb\x7f\x8b\xa5(\x0b\x01\xcb\xef\xdf\xf6t\x89\x97z\x0eft \x90)\x00\r\x00\x00\x00\r\x00\x00\x00\x03c\x89K\xd3\xc8fyA\x140\x07\x00\xff\xa1\x00\x00symlink\x00\xc02/sub-two.txt\xeb\x86t\xe0\x90#\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\xb9]yA\x140\x01\x00\xedA\x00\x001\x00\xc0\xe0Dt\xe0\x90#\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\xc2]yA\x140\x01\x00\xedA\x00\x002\x00\xc0u\xa1t \x80,\x00\r\x00\x00\x00\r\x00\x00\x00\x03T\xea\x04\xca\xe6\x84yA\x140\x0c\x00\xa4\x81\x00\x00uncompresseduncompressed\n\xda\x10t \x900\x00\x0e\x00\x00\x00\x04\x00\x00\x00\x035K.\xa6\x18\x85yA\x1d5\x0e\x00\xa4\x81\x00\x00max-compressed\x00\xc0\x00\x08\xbf\x08\xae\xf2\xcc\x01s\xf8\xff\xec\x96\xe8\xc4={\x00@\x07\x00"
    )  # noqa
    tdata = {
        "1": b"",
        "1/sub-one": b"sub-one\n",
        "2": b"",
        "2/sub-two.txt": b"sub-two\n",
        "F\xfc\xdfe.txt": b"unicode\n",
        "max-compressed": b"max\n",
        "one.txt": b"one\n",
        "symlink": b"2/sub-two.txt",
        "uncompressed": b"uncompressed\n",
        "\u8bf6\u6bd4\u5c41.txt": b"chinese unicode\n",
    }
    f = RARFile(stream, True)
    names = set()
    data = {}
    if f.comment != b"some comment\n":
        raise ValueError("Comment not read: %r != %r" % (f.comment, b"some comment\n"))
    while True:
        try:
            h = f.current_item
        except EOFError:
            break
        isdir = h["is_directory"]
        if isdir and h["filename"] not in {"1", "2"}:
            raise ValueError("Incorrect identification of a directory")
        if h["is_symlink"] and h["filename"] != "symlink":
            raise ValueError("Incorrect identification of a symlink")
        names.add(h["filename"])
        et = BytesIO()
        f.process_current_item(et)
        data[h["filename"]] = et.getvalue()

    if names != {
        "1/sub-one",
        "one.txt",
        "2/sub-two.txt",
        "1",
        "2",
        "诶比屁.txt",
        "Füße.txt",
        "symlink",
        "uncompressed",
        "max-compressed",
    }:
        raise ValueError("Name list does not match")
    if data != tdata:
        raise ValueError("Some data was not read correctly")

    from LiuXin_alpha.utils.mem import memory
    import gc

    del f
    for i in xrange(3):
        gc.collect()

    def get_mem_use(num):
        """
        Return mem use under the documented compatibility and safety rules.

        Example:
            Exercise test basic.get mem use through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param num: Value supplied for num under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        start = memory()
        s = SaveStream(stream)
        for i in xrange(num):
            with s:
                f = RARFile(stream)
                f.test()
        del f, s
        for i in xrange(3):
            gc.collect()
        return memory() - start

    (get_mem_use(20))
    a, b = get_mem_use(10), get_mem_use(110)
    if not isosx and abs(b - a) > 1:
        raise ValueError("Leaked %s MB for %d calls" % (b - a, 100))
    # }}}


def test_rar(path):
    """
    Perform the test rar utility operation under explicit compatibility rules.

    Example:
        Exercise test rar through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with open(path, "rb") as stream:
        f = RARFile(stream)
        f.test(print_names=True)


if __name__ == "__main__":
    test_basic()
