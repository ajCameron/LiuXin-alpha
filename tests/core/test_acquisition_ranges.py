"""Check bounded reads and resource-version continuity without loading full files."""

from types import SimpleNamespace

import pytest

from LiuXin_alpha.core.acquisition_ranges import read_range
from LiuXin_alpha.core.content import acquisition_chunks
from LiuXin_alpha.core.errors import CoreError


def test_large_local_resource_streams_in_bounded_versioned_chunks(tmp_path):
    path = tmp_path / "book.epub"
    expected = b"a" * (2 * 1024 * 1024 + 3)
    path.write_bytes(expected)
    row = {"file_original_path": str(path)}
    calls = []

    def query(name, payload):
        assert name == "acquisition.read-range"
        calls.append(dict(payload))
        options = {
            key: value for key, value in payload.items() if key not in ("kind", "id")
        }
        return {
            "resource": {"id": 1},
            **read_range(None, kind="legacy-file", row=row, store=None, **options),
        }

    chunks = list(acquisition_chunks(query, "legacy-file", 1))
    assert b"".join(content for _, content in chunks) == expected
    assert len(chunks) == 3 and all(
        len(content) <= 1024 * 1024 for _, content in chunks
    )
    assert calls[1]["if_version"] == calls[2]["if_version"]


def test_changed_file_cannot_be_spliced_into_continued_read(tmp_path):
    path = tmp_path / "book.epub"
    path.write_bytes(b"a" * (1024 * 1024 + 1))
    row = {"file_original_path": str(path)}
    first = read_range(None, kind="legacy-file", row=row, store=None)
    path.write_bytes(b"b" * (1024 * 1024 + 1))
    with pytest.raises(CoreError) as error:
        read_range(
            None,
            kind="legacy-file",
            row=row,
            store=None,
            offset=first["next_offset"],
            if_version=first["version"],
        )
    assert error.value.code == "acquisition_changed"


def test_fallback_store_key_cannot_escape_root(tmp_path):
    root = tmp_path / "store"
    root.mkdir()
    (tmp_path / "outside.epub").write_bytes(b"private")
    runtime = SimpleNamespace(
        services=SimpleNamespace(library=SimpleNamespace(storage=object()))
    )
    with pytest.raises(CoreError, match="escapes"):
        read_range(
            runtime,
            kind="legacy-file",
            row={"file_store_id": 1, "file_storage_key": "../outside.epub"},
            store={
                "store_id": 1,
                "store_uuid": "00000000-0000-0000-0000-000000000001",
                "store_name": "Local",
                "store_root_uri": str(root),
                "store_kind": "filesystem",
            },
        )
