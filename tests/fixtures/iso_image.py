"""Small standards-shaped ISO images for dependency-free storage tests."""

from __future__ import annotations

import dataclasses
import pathlib

from collections.abc import Mapping
from typing import Literal


BLOCK_SIZE = 2048


@dataclasses.dataclass(slots=True)
class _Node:
    name: str | bytes | None
    payload: bytes | None = None
    children: dict[str | bytes, "_Node"] = dataclasses.field(default_factory=dict)
    alias: bytes = b""
    primary_lba: int = 0
    primary_blocks: int = 0
    secondary_lba: int = 0
    secondary_blocks: int = 0
    file_lba: int = 0

    @property
    def is_directory(self) -> bool:
        return self.payload is None


def build_joliet_iso(path: pathlib.Path, files: Mapping[str, bytes]) -> pathlib.Path:
    """Build an ISO 9660 image with a Joliet Unicode namespace."""

    return _build_iso(path, files, namespace="joliet")


def build_rock_ridge_iso(
    path: pathlib.Path,
    files: Mapping[bytes, bytes],
) -> pathlib.Path:
    """Build an ISO 9660 image whose Rock Ridge names retain raw bytes."""

    return _build_iso(path, files, namespace="rock-ridge")


def build_iso9660_iso(path: pathlib.Path, files: Mapping[str, bytes]) -> pathlib.Path:
    """Build a primary-volume-only ISO 9660 image."""

    return _build_iso(path, files, namespace="iso9660")


def _build_iso(
    path: pathlib.Path,
    files: Mapping[str, bytes] | Mapping[bytes, bytes],
    *,
    namespace: Literal["joliet", "rock-ridge", "iso9660"],
) -> pathlib.Path:
    root = _tree(files)
    directories = _directories(root)
    regular_files = _files(root)
    for index, node in enumerate(directories[1:], start=1):
        node.alias = f"D{index:04d}".encode("ascii")
    for index, node in enumerate(regular_files, start=1):
        node.alias = f"F{index:04d}.DAT;1".encode("ascii")
    if namespace == "iso9660":
        for node in directories[1:]:
            assert isinstance(node.name, str)
            node.alias = node.name.upper().encode("ascii")
        for node in regular_files:
            assert isinstance(node.name, str)
            node.alias = (node.name.upper() + ";1").encode("ascii")

    layouts = ["primary"]
    if namespace == "joliet":
        layouts.append("secondary")

    table_locations: dict[tuple[str, str], tuple[int, int]] = {}
    next_lba = 19
    for layout in layouts:
        table_size = _path_table_size(directories, layout=layout)
        table_blocks = max(1, _blocks(table_size))
        for byte_order in ("little", "big"):
            table_locations[(layout, byte_order)] = (next_lba, table_size)
            next_lba += table_blocks

    for layout in layouts:
        for node in directories:
            blocks = _directory_blocks(node, layout=layout, rock_ridge=(namespace == "rock-ridge"))
            if layout == "primary":
                node.primary_lba = next_lba
                node.primary_blocks = blocks
            else:
                node.secondary_lba = next_lba
                node.secondary_blocks = blocks
            next_lba += blocks

    for node in regular_files:
        assert node.payload is not None
        node.file_lba = next_lba
        next_lba += _blocks(len(node.payload))
    volume_blocks = max(next_lba + 1, 24)
    image = bytearray(volume_blocks * BLOCK_SIZE)

    primary = _volume_descriptor(
        descriptor_type=1,
        root=root,
        layout="primary",
        volume_blocks=volume_blocks,
        table_locations=table_locations,
    )
    image[16 * BLOCK_SIZE : 17 * BLOCK_SIZE] = primary
    descriptor_lba = 17
    if namespace == "joliet":
        supplementary = _volume_descriptor(
            descriptor_type=2,
            root=root,
            layout="secondary",
            volume_blocks=volume_blocks,
            table_locations=table_locations,
        )
        image[descriptor_lba * BLOCK_SIZE : (descriptor_lba + 1) * BLOCK_SIZE] = supplementary
        descriptor_lba += 1
    terminator = bytearray(BLOCK_SIZE)
    terminator[0] = 255
    terminator[1:6] = b"CD001"
    terminator[6] = 1
    image[descriptor_lba * BLOCK_SIZE : (descriptor_lba + 1) * BLOCK_SIZE] = terminator

    for layout in layouts:
        for byte_order in ("little", "big"):
            lba, _size = table_locations[(layout, byte_order)]
            table = _path_table(directories, layout=layout, byte_order=byte_order)
            image[lba * BLOCK_SIZE : lba * BLOCK_SIZE + len(table)] = table

    for layout in layouts:
        for node in directories:
            payload = _directory_payload(
                node,
                root=root,
                layout=layout,
                rock_ridge=(namespace == "rock-ridge"),
            )
            lba = node.primary_lba if layout == "primary" else node.secondary_lba
            image[lba * BLOCK_SIZE : lba * BLOCK_SIZE + len(payload)] = payload
    for node in regular_files:
        assert node.payload is not None
        start = node.file_lba * BLOCK_SIZE
        image[start : start + len(node.payload)] = node.payload

    path.write_bytes(image)
    return path


def _tree(files: Mapping[str, bytes] | Mapping[bytes, bytes]) -> _Node:
    root = _Node(None)
    expected_type: type[str] | type[bytes] | None = None
    for key, payload in files.items():
        expected_type = expected_type or type(key)
        if type(key) is not expected_type:
            raise TypeError("ISO fixture keys must use one text representation.")
        separator = "/" if isinstance(key, str) else b"/"
        components = key.split(separator)
        if not components or any(component in {"", b""} for component in components):
            raise ValueError("ISO fixture keys must be canonical relative paths.")
        current = root
        for component in components[:-1]:
            current = current.children.setdefault(component, _Node(component))
            if not current.is_directory:
                raise ValueError("ISO fixture path collides with a regular file.")
        filename = components[-1]
        if filename in current.children:
            raise ValueError("ISO fixture contains a duplicate path.")
        current.children[filename] = _Node(filename, payload=bytes(payload))
    return root


def _directories(root: _Node) -> list[_Node]:
    result: list[_Node] = []

    def visit(node: _Node) -> None:
        if not node.is_directory:
            return
        result.append(node)
        for child in node.children.values():
            visit(child)

    visit(root)
    return result


def _files(root: _Node) -> list[_Node]:
    return [
        node
        for directory in _directories(root)
        for node in directory.children.values()
        if not node.is_directory
    ]


def _blocks(length: int) -> int:
    return (length + BLOCK_SIZE - 1) // BLOCK_SIZE


def _both16(value: int) -> bytes:
    return value.to_bytes(2, "little") + value.to_bytes(2, "big")


def _both32(value: int) -> bytes:
    return value.to_bytes(4, "little") + value.to_bytes(4, "big")


def _recording_time() -> bytes:
    return bytes((124, 1, 2, 3, 4, 5, 0))


def _directory_record(
    identifier: bytes,
    *,
    lba: int,
    size: int,
    directory: bool,
    system_use: bytes = b"",
) -> bytes:
    padding = b"\x00" if len(identifier) % 2 == 0 else b""
    length = 33 + len(identifier) + len(padding) + len(system_use)
    if length > 255:
        raise ValueError("ISO fixture directory record exceeds 255 bytes.")
    record = bytearray(length)
    record[0] = length
    record[2:10] = _both32(lba)
    record[10:18] = _both32(size)
    record[18:25] = _recording_time()
    record[25] = 2 if directory else 0
    record[28:32] = _both16(1)
    record[32] = len(identifier)
    record[33 : 33 + len(identifier)] = identifier
    start = 33 + len(identifier) + len(padding)
    record[start:] = system_use
    return bytes(record)


def _identifier(node: _Node, *, layout: str) -> bytes:
    if layout == "primary":
        return node.alias
    assert isinstance(node.name, str)
    text = node.name if node.is_directory else node.name + ";1"
    return text.encode("utf-16-be")


def _rock_ridge_name(node: _Node) -> bytes:
    if isinstance(node.name, bytes):
        return node.name
    assert isinstance(node.name, str)
    return node.name.encode("utf-8")


def _nm_entries(name: bytes) -> bytes:
    chunks = [name[index : index + 240] for index in range(0, len(name), 240)] or [b""]
    entries = []
    for index, chunk in enumerate(chunks):
        flags = 1 if index < len(chunks) - 1 else 0
        entries.append(b"NM" + bytes((5 + len(chunk), 1, flags)) + chunk)
    return b"".join(entries)


def _sp_entry() -> bytes:
    return b"SP" + bytes((7, 1, 190, 239, 0))


def _node_extent(node: _Node, *, layout: str) -> tuple[int, int]:
    if node.is_directory:
        if layout == "primary":
            return node.primary_lba, node.primary_blocks * BLOCK_SIZE
        return node.secondary_lba, node.secondary_blocks * BLOCK_SIZE
    assert node.payload is not None
    return node.file_lba, len(node.payload)


def _directory_record_length(node: _Node, *, layout: str, rock_ridge: bool) -> int:
    identifier = _identifier(node, layout=layout)
    system_use = _nm_entries(_rock_ridge_name(node)) if rock_ridge else b""
    return len(
        _directory_record(
            identifier,
            lba=0,
            size=0,
            directory=node.is_directory,
            system_use=system_use,
        )
    )


def _directory_blocks(node: _Node, *, layout: str, rock_ridge: bool) -> int:
    lengths = [34 + (7 if rock_ridge and node.name is None else 0), 34]
    lengths.extend(
        _directory_record_length(child, layout=layout, rock_ridge=rock_ridge)
        for child in node.children.values()
    )
    position = 0
    for length in lengths:
        remaining = BLOCK_SIZE - position % BLOCK_SIZE
        if length > remaining:
            position += remaining
        position += length
    return max(1, _blocks(position))


def _parent_map(root: _Node) -> dict[int, _Node]:
    parents: dict[int, _Node] = {id(root): root}

    def visit(node: _Node) -> None:
        for child in node.children.values():
            if child.is_directory:
                parents[id(child)] = node
                visit(child)

    visit(root)
    return parents


def _directory_payload(
    node: _Node,
    *,
    root: _Node,
    layout: str,
    rock_ridge: bool,
) -> bytes:
    parents = _parent_map(root)
    parent = parents[id(node)]
    node_lba, node_size = _node_extent(node, layout=layout)
    parent_lba, parent_size = _node_extent(parent, layout=layout)
    records = [
        _directory_record(
            b"\x00",
            lba=node_lba,
            size=node_size,
            directory=True,
            system_use=(_sp_entry() if rock_ridge and node is root else b""),
        ),
        _directory_record(
            b"\x01",
            lba=parent_lba,
            size=parent_size,
            directory=True,
        ),
    ]
    for child in node.children.values():
        child_lba, child_size = _node_extent(child, layout=layout)
        records.append(
            _directory_record(
                _identifier(child, layout=layout),
                lba=child_lba,
                size=child_size,
                directory=child.is_directory,
                system_use=(
                    _nm_entries(_rock_ridge_name(child)) if rock_ridge else b""
                ),
            )
        )
    target_length = (
        node.primary_blocks if layout == "primary" else node.secondary_blocks
    ) * BLOCK_SIZE
    payload = bytearray(target_length)
    position = 0
    for record in records:
        remaining = BLOCK_SIZE - position % BLOCK_SIZE
        if len(record) > remaining:
            position += remaining
        payload[position : position + len(record)] = record
        position += len(record)
    return bytes(payload)


def _path_identifier(node: _Node, *, layout: str) -> bytes:
    if node.name is None:
        return b"\x00"
    return _identifier(node, layout=layout)


def _path_table_size(directories: list[_Node], *, layout: str) -> int:
    total = 0
    for node in directories:
        identifier = _path_identifier(node, layout=layout)
        total += 8 + len(identifier) + (len(identifier) % 2)
    return total


def _path_table(
    directories: list[_Node],
    *,
    layout: str,
    byte_order: Literal["little", "big"],
) -> bytes:
    numbers = {id(node): index for index, node in enumerate(directories, start=1)}
    parents = _parent_map(directories[0])
    result = bytearray()
    for node in directories:
        identifier = _path_identifier(node, layout=layout)
        lba, _size = _node_extent(node, layout=layout)
        parent_number = numbers[id(parents[id(node)])]
        result.extend(bytes((len(identifier), 0)))
        result.extend(lba.to_bytes(4, byte_order))
        result.extend(parent_number.to_bytes(2, byte_order))
        result.extend(identifier)
        if len(identifier) % 2:
            result.append(0)
    return bytes(result)


def _volume_descriptor(
    *,
    descriptor_type: int,
    root: _Node,
    layout: Literal["primary", "secondary"],
    volume_blocks: int,
    table_locations: Mapping[tuple[str, str], tuple[int, int]],
) -> bytes:
    descriptor = bytearray(BLOCK_SIZE)
    descriptor[0] = descriptor_type
    descriptor[1:6] = b"CD001"
    descriptor[6] = 1
    descriptor[8:40] = b"LIUXIN".ljust(32, b" ")
    descriptor[40:72] = b"STORAGE TEST".ljust(32, b" ")
    descriptor[80:88] = _both32(volume_blocks)
    descriptor[120:124] = _both16(1)
    descriptor[124:128] = _both16(1)
    descriptor[128:132] = _both16(BLOCK_SIZE)
    _little_lba, table_size = table_locations[(layout, "little")]
    descriptor[132:140] = _both32(table_size)
    descriptor[140:144] = table_locations[(layout, "little")][0].to_bytes(4, "little")
    descriptor[148:152] = table_locations[(layout, "big")][0].to_bytes(4, "big")
    root_lba, root_size = _node_extent(root, layout=layout)
    root_record = _directory_record(
        b"\x00",
        lba=root_lba,
        size=root_size,
        directory=True,
    )
    descriptor[156 : 156 + len(root_record)] = root_record
    descriptor[881] = 1
    if descriptor_type == 2:
        descriptor[88:91] = b"%/E"
    return bytes(descriptor)


__all__ = ["build_iso9660_iso", "build_joliet_iso", "build_rock_ridge_iso"]
