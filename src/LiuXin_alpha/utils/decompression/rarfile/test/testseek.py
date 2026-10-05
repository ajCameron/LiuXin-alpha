#! /usr/bin/env python

"""
Exercise seeking behavior for bundled RAR entry streams.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise testseek through a consuming regression::

        python -m pytest -q tests/utils/decompression/test_archives.py
"""
import rarfile, os, os.path, time, sys


def show_fds():
    """
    Perform the show fds utility operation under explicit compatibility rules.

    Example:
        Exercise show fds through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    fdir = "/proc/%d/fd" % os.getpid()
    if os.path.isdir(fdir):
        os.system('printf "fds = "; ls -l %s | wc -l' % fdir)


def do_seek(f, pos, lim):
    """
    Perform the do seek utility operation under explicit compatibility rules.

    Example:
        Exercise do seek through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param f: Value supplied for f under the utility contract.
    :param pos: Value supplied for pos under the utility contract.
    :param lim: Value supplied for lim under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    ofs = pos * 4
    fsize = lim * 4

    if ofs < 0:
        exp = 0
    elif ofs > fsize:
        exp = fsize
    else:
        exp = ofs

    f.seek(ofs)

    got = f.tell()

    if got != exp:
        raise Exception("seek failed (got=%d, exp=%d)" % (got, exp))
    ln = f.read(4)
    if got == fsize and ln:
        raise Exception("unexpected read")
    if not ln and got < fsize:
        raise Exception("unexpected read failure")
    if ln:
        spos = int(ln)
        if spos * 4 != got:
            raise Exception("unexpected pos: spos=%d pos=%d" % (spos, pos))


def test_seek(rf, fn):
    """
    Perform the test seek utility operation under explicit compatibility rules.

    Example:
        Exercise test seek through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param rf: Value supplied for rf under the utility contract.
    :param fn: Value supplied for fn under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    inf = rf.getinfo(fn)
    cnt = int(inf.file_size / 4)
    f = rf.open(fn)

    do_seek(f, int(cnt / 2), cnt)
    do_seek(f, 0, cnt)

    for i in range(int(cnt / 2)):
        do_seek(f, i * 2, cnt)

    for i in range(cnt):
        do_seek(f, i * 2 - int(cnt / 2), cnt)

    for i in range(cnt + 10):
        do_seek(f, cnt - i - 5, cnt)

    f.close()

    print("OK")


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
        sys.stdout.write("test/seek %s .. " % fn)
        sys.stdout.flush()
        test_seek(rf, fn)

    time.sleep(1)
    show_fds()


if __name__ == "__main__":
    main()
