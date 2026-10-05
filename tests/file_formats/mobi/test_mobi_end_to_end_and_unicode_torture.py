"""
Provide test mobi end to end and unicode torture utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test mobi end to end and unicode torture through a consuming regression::

        python -m pytest -q tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pytest


class _Log:
    """
    Provide the log contract for validated ebook processing.

    Example:
        Exercise  Log through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py
    """
    def __call__(self, *args, **kwargs):
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def info(self, *args, **kwargs):
        """
        Perform the info operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.info through a consuming regression::

                python -m pytest -q tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def debug(self, *args, **kwargs):
        """
        Perform the debug operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.debug through a consuming regression::

                python -m pytest -q tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def warning(self, *args, **kwargs):
        """
        Perform the warning operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.warning through a consuming regression::

                python -m pytest -q tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    warn = warning

    def error(self, *args, **kwargs):
        """
        Perform the error operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.error through a consuming regression::

                python -m pytest -q tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def exception(self, *args, **kwargs):
        """
        Perform the exception operation under explicit file-format and conversion rules.

        Example:
            Exercise  Log.exception through a consuming regression::

                python -m pytest -q tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None


def _opts(input_encoding: str = "utf-8") -> SimpleNamespace:
    """
    Perform the opts operation under explicit file-format and conversion rules.

    Example:
        Exercise  opts through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py


    :param input_encoding: Value supplied for input encoding under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return SimpleNamespace(input_encoding=input_encoding, debug_pipeline=False)


def _mobi_paths(md_test_files_by_ext: dict[str, list[Path]]) -> list[Path]:
    """
    Perform the mobi paths operation under explicit file-format and conversion rules.

    Example:
        Exercise  mobi paths through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py


    :param md_test_files_by_ext: Value supplied for md test files by ext under the
        utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    paths = list(md_test_files_by_ext.get("mobi", []))
    if not paths:
        pytest.skip("No .mobi fixtures found in optional LiuXin_alpha_data corpus")
    return paths


def _assert_valid_opf(path: Path) -> None:
    """
    Perform the assert valid opf operation under explicit file-format and conversion rules.

    Example:
        Exercise  assert valid opf through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    assert path.exists(), f"missing output OPF: {path}"
    root = ET.parse(path).getroot()
    assert root.tag.endswith("package")

    rendered = path.read_text(encoding="utf-8", errors="replace")
    assert ".html" in rendered or ".xhtml" in rendered


def test_mobi_input_end_to_end_on_real_fixtures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, md_test_files_by_ext: dict[str, list[Path]]
) -> None:
    """
    Perform the test mobi input end to end on real fixtures operation under explicit file-format and conversion rules.

    Example:
        Exercise test mobi input end to end on real fixtures through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param md_test_files_by_ext: Value supplied for md test files by ext under the
        utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.mobi_input import MOBIInput

    for idx, mobi_path in enumerate(_mobi_paths(md_test_files_by_ext)):
        work = tmp_path / f"mobi_case_{idx}"
        work.mkdir()
        monkeypatch.chdir(work)

        with mobi_path.open("rb") as stream:
            out = MOBIInput(None).convert(stream, _opts(), "mobi", _Log(), {})

        out_path = Path(out) if Path(out).is_absolute() else work / out
        _assert_valid_opf(out_path)


def test_mobi_input_handles_non_utf8_option(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, md_test_files_by_ext: dict[str, list[Path]]
) -> None:
    """
    Perform the test mobi input handles non utf8 option operation under explicit file-format and conversion rules.

    Example:
        Exercise test mobi input handles non utf8 option through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_end_to_end_and_unicode_torture.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param md_test_files_by_ext: Value supplied for md test files by ext under the
        utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.mobi_input import MOBIInput

    mobi_path = _mobi_paths(md_test_files_by_ext)[0]
    work = tmp_path / "mobi_cp1252"
    work.mkdir()
    monkeypatch.chdir(work)

    with mobi_path.open("rb") as stream:
        out = MOBIInput(None).convert(stream, _opts("cp1252"), "mobi", _Log(), {})

    out_path = Path(out) if Path(out).is_absolute() else work / out
    _assert_valid_opf(out_path)
