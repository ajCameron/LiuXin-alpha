from __future__ import annotations

import io


def test_runtime_error_paths_use_logging_not_print(capsys) -> None:
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
