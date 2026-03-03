from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def test_override_table_uses_bug_handler_for_missing_list_id() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.rtf2xml.override_table")

    table = mod.OverrideTable(
        list_of_lists=[[{"list-id": [], "list-table-id": "42"}]],
        bug_handler=RuntimeError,
        run_level=4,
    )
    table._OverrideTable__override_list = [{"list-table-id": "42"}]

    with pytest.raises(RuntimeError, match="list-id"):
        table._OverrideTable__parse_override_dict()


def test_list_table_unknown_state_raises_configured_bug_handler() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.rtf2xml.list_table")
    table = mod.ListTable(bug_handler=RuntimeError, run_level=4)
    table._ListTable__state = "unknown-state"

    with pytest.raises(RuntimeError, match="No parser action"):
        table._ListTable__parse_lines("tx<nu<__________<payload\n")


def test_preamble_div_unknown_margin_token_raises_at_high_run_level(tmp_path: Path) -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.rtf2xml.preamble_div")
    src = tmp_path / "in.data"
    src.write_text("", encoding="utf-8")
    preamble = mod.PreambleDiv(in_file=str(src), bug_handler=RuntimeError, run_level=4)
    preamble._PreambleDiv__initiate_values()

    with pytest.raises(RuntimeError, match="Unexpected margin token"):
        preamble._PreambleDiv__margin_func("cw<pa<bogus-code_<nu<1234\n")


def test_parse_rtf2xml_end_to_end_smoke(tmp_path: Path) -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.rtf2xml.ParseRtf")
    src = tmp_path / "sample.rtf"
    out = tmp_path / "sample.xml"
    src.write_bytes(
        b"{\\rtf1\\ansi\\ansicpg1252\\deff0\n"
        b"{\\fonttbl{\\f0\\fnil Times New Roman;}}\n"
        b"\\pard Hello Unicode \\u945? test\\par\n"
        b"}\n"
    )

    parser = mod.ParseRtf(in_file=str(src), out_file=str(out), no_dtd=1, run_level=1)
    exit_code = parser.parse_rtf()

    assert exit_code == 0
    assert out.exists()
    xml = out.read_text("utf-8", "replace")
    assert "<doc" in xml
    assert "Hello Unicode" in xml
