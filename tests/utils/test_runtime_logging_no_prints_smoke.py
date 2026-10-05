"""
Provide test runtime logging no prints smoke utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test runtime logging no prints smoke through a consuming regression::

        python -m pytest -q tests/utils/test_runtime_logging_no_prints_smoke.py
"""
from __future__ import annotations

import io


def test_runtime_error_paths_use_logging_not_print(capsys) -> None:
    """
    Perform the test runtime error paths use logging not print utility operation under explicit compatibility rules.

    Example:
        Exercise test runtime error paths use logging not print through a consuming regression::

            python -m pytest -q tests/utils/test_runtime_logging_no_prints_smoke.py


    :param capsys: Value supplied for capsys under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.errors import BadInputException, InvalidFolderStoreDriver
    from LiuXin_alpha.metadata.book.json_codec import JsonCodec
    from LiuXin_alpha.utils.config.config_base import OptionSet

    BadInputException("bad-input")
    InvalidFolderStoreDriver("bad-store-driver")

    # Invalid JSON should log a warning and return defaults without printing.
    OptionSet().parse_string("{bad json")

    # Decoding failure should be logged via logger.exception, not printed.
    JsonCodec().decode_from_file(io.StringIO("{bad"), [], object, "")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
