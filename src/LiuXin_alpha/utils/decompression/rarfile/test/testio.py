#! /usr/bin/env python

"""
Exercise RAR stream and member I/O in the bundled compatibility suite.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise testio through a consuming regression::

        python -m pytest -q tests/utils/decompression/test_archives.py
"""
import rarfile, os, os.path, time, sys

try:
    from io import BufferedReader, TextIOWrapper
except ImportError:
    print("no io module")
    sys.exit(0)

    def BufferedReader(x):
        """
        Perform the BufferedReader utility operation under explicit compatibility rules.

        Example:
            Exercise BufferedReader through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param x: Value supplied for x under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return x

    def TextIOWrapper(x):
        """
        Perform the TextIOWrapper utility operation under explicit compatibility rules.

        Example:
            Exercise TextIOWrapper through a consuming regression::

                python -m pytest -q tests/utils/decompression/test_archives.py


        :param x: Value supplied for x under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return x


def test_readline(rf, fn):
    """
    Perform the test readline utility operation under explicit compatibility rules.

    Example:
        Exercise test readline through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param rf: Value supplied for rf under the utility contract.
    :param fn: Value supplied for fn under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    f = rf.open(fn)
    tr = TextIOWrapper(BufferedReader(f))
    while 1:
        ln = tr.readline()
        if not ln:
            break
    tr.close()


def main():
    """
    Perform the main utility operation under explicit compatibility rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    files = ["stest1.txt", "stest2.txt"]
    arc = "files/seektest.rar"

    rf = rarfile.RarFile(arc, crc_check=0)
    for fn in files:
        sys.stdout.write("test/readline: %s .. " % fn)
        sys.stdout.flush()
        test_readline(rf, fn)
        print("ok")


if __name__ == "__main__":
    main()
