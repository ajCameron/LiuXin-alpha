#! /usr/bin/env python

"""
Exercise corrupt-archive handling in the bundled rarfile compatibility suite.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise testcorrupt through a consuming regression::

        python -m pytest -q tests/utils/decompression/test_archives.py
"""
import rarfile
import sys, os, time
import tempfile


def progress():
    """
    Perform the progress utility operation under explicit compatibility rules.

    Example:
        Exercise progress through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    sys.stdout.write(".")
    sys.stdout.flush()


def try_read(tmpfn):
    # progress()
    """
    Perform the try read utility operation under explicit compatibility rules.

    Example:
        Exercise try read through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param tmpfn: Value supplied for tmpfn under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    try:
        rf = rarfile.RarFile(tmpfn)
        if rf.needs_password():
            rf.setpassword("password")
    except rarfile.Error:
        return
    for fn in rf.namelist():
        try:
            data = rf.read(fn)
            pass
        except rarfile.Error:
            pass


def test_rar(rarfn):
    """
    Perform the test rar utility operation under explicit compatibility rules.

    Example:
        Exercise test rar through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :param rarfn: Value supplied for rarfn under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    data = open(rarfn, "rb").read()

    fd, tmpfn = tempfile.mkstemp(".rar")
    os.close(fd)

    print("testcorrupt 1")
    for n in range(len(data)):
        bad = data[:n]
        f = open(tmpfn, "wb")
        f.write(bad)
        f.close()

        try_read(tmpfn)

    print("testcorrupt 2")
    crap = rarfile.RAR_ID
    for n in range(1, len(data)):
        for i in range(len(crap)):
            c = crap[i : i + 1]
            bad = data[: n - 1] + c + data[n:]
            f = open(tmpfn, "wb")
            f.write(bad)
            f.close()
            try_read(tmpfn)

    os.unlink(tmpfn)


test_rar_list = [
    "files/ctime0.rar",
    "files/ctime1.rar",
    "files/ctime2.rar",
    "files/ctime3.rar",
    "files/ctime4.rar",
    "files/seektest.rar",
    "files/rar15-comment-lock.rar",
    "files/rar15-comment.rar",
    "files/rar202-comment-nopsw.rar",
    "files/rar202-comment-psw.rar",
    "files/rar3-comment-hpsw.rar",
    "files/rar3-comment-plain.rar",
    "files/rar3-comment-psw.rar",
    "files/unicode.rar",
]


def main():
    """
    Perform the main utility operation under explicit compatibility rules.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/utils/decompression/test_archives.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if sys.argv[-1] == "--quick":
        test_rar("files/rar3-comment-plain.rar")
        return
    for rar in test_rar_list:
        print(rar)
        test_rar(rar)


if __name__ == "__main__":
    try:
        main()
    except OSError:
        print("OSError: pid = %d" % os.getpid())
        time.sleep(80000)
