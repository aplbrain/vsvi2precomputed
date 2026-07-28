import io
import struct

import pytest

from vsvi2precomputed.vsv import (
    VSV2Lookup,
    decode_vast_brick,
    decode_vsv2_record,
    read_index_offsets,
    read_vsv2_header,
)

def test_read_vsv2_header():
    data = bytearray(0x500)
    data[:4] = b"VSV2"
    struct.pack_into("<I", data, 4, 0xC4)
    struct.pack_into("<I", data, 0x30, 3)
    struct.pack_into("<III", data, 0x68, 320, 480, 96)
    struct.pack_into("<III", data, 0x74, 16, 16, 16)
    struct.pack_into("<I", data, 0x8C, 1)
    struct.pack_into("<fff", data, 0x98, 4.0, 4.0, 40.0)
    struct.pack_into("<Q", data, 0xC4, 0x1000)
    struct.pack_into("<Q", data, 0xCC, 0x2000)
    struct.pack_into("<Q", data, 0xD4, 0x3000)

    header = read_vsv2_header(io.BytesIO(data))

    assert header.size == (320, 480, 96)
    assert header.brick_size == (16, 16, 16)
    assert header.bytes_per_voxel == 1
    assert header.voxel_size_nm == pytest.approx((4.0, 4.0, 40.0))
    assert header.mip_count == 3
    assert header.index_offsets == (0x1000, 0x2000, 0x3000)


def test_read_index_offsets():
    stream = io.BytesIO(b"prefix" + struct.pack("<QQQ", 100, 200, 300))

    assert read_index_offsets(stream, 6, 3) == (100, 200, 300)


def test_rejects_non_vsv2():
    with pytest.raises(ValueError, match="not a VSV2"):
        read_vsv2_header(io.BytesIO(bytes(0x390)))


def test_decode_raw_vsv2_record():
    assert decode_vsv2_record(struct.pack("<II4B", 1, 4, 1, 2, 3, 4), 4) == bytes(
        [1, 2, 3, 4]
    )


def test_decode_constant_vsv2_record():
    assert decode_vsv2_record(struct.pack("<IIH", 0x102, 2, 42), 3) == struct.pack(
        "<3H", 42, 42, 42
    )


def test_decode_compressed_vsv2_record():
    raw = bytes(range(16))
    compressed = __import__("zlib").compress(raw)
    record = struct.pack("<III", 0x201, len(compressed), len(raw)) + compressed
    assert decode_vsv2_record(record, 16) == raw


def test_decode_vast_raw_and_constant_bricks():
    raw = bytes(index % 256 for index in range(4096))
    assert decode_vast_brick(b"\0" + raw) == raw
    assert decode_vast_brick(b"\1\x2a") == b"\x2a" * 4096


def test_sparse_extent_lookup(tmp_path):
    data = bytearray(0x40000)
    data[:4] = b"VSV2"
    struct.pack_into("<I", data, 4, 0xC4)
    struct.pack_into("<I", data, 0x30, 1)
    struct.pack_into("<III", data, 0x68, 512, 512, 512)
    struct.pack_into("<III", data, 0x74, 16, 16, 16)
    struct.pack_into("<I", data, 0x8C, 1)
    struct.pack_into("<Q", data, 0xC4, 0x300)
    struct.pack_into("<3Q", data, 0x300, 0x1000, 0x2000, 0x3000)
    struct.pack_into("<Q", data, 0x1000, 0x18000)
    struct.pack_into("<Q", data, 0x1000 + 1 * 8, 0x19000)
    struct.pack_into("<Q", data, 0x1000 + 16 * 8, 0x1A000)
    struct.pack_into("<Q", data, 0x1000 + 256 * 8, 0x1B000)
    source = tmp_path / "sample.vsv"
    source.write_bytes(data)

    lookup = VSV2Lookup(source)

    assert lookup.locate(0, 0, 0) == (0x18000, 0)
    assert lookup.locate(16, 0, 0) == (0x19000, 0)
    assert lookup.locate(0, 16, 0) == (0x1A000, 0)
    assert lookup.locate(1, 17, 0) == (0x1A000, 17)
    assert lookup.locate(0, 0, 1) == (0x18000, 256)
    assert lookup.locate(0, 0, 16) == (0x1B000, 0)


def test_extent_lookup_crosses_hierarchy_levels(tmp_path):
    data = bytearray(0x10000)
    data[:4] = b"VSV2"
    struct.pack_into("<I", data, 4, 0xC4)
    struct.pack_into("<I", data, 0x30, 1)
    struct.pack_into("<III", data, 0x68, 257 * 16, 257 * 16, 257 * 16)
    struct.pack_into("<III", data, 0x74, 16, 16, 16)
    struct.pack_into("<I", data, 0x8C, 1)
    struct.pack_into("<Q", data, 0xC4, 0x300)
    struct.pack_into("<Q", data, 0x300, 0x1000)
    struct.pack_into("<Q", data, 0x300 + 1 * 8, 0x2000)
    struct.pack_into("<Q", data, 0x300 + 16 * 8, 0x3000)
    struct.pack_into("<Q", data, 0x300 + 256 * 8, 0x4000)
    struct.pack_into("<Q", data, 0x1000 + 4095 * 8, 0x8000)
    struct.pack_into("<Q", data, 0x2000, 0x9000)
    struct.pack_into("<Q", data, 0x3000, 0xA000)
    struct.pack_into("<Q", data, 0x4000, 0xB000)
    source = tmp_path / "sample.vsv"
    source.write_bytes(data)

    lookup = VSV2Lookup(source)

    assert lookup.locate(255, 255, 255) == (0x8000, 4095)
    assert lookup.locate(256, 0, 0) == (0x9000, 0)
    assert lookup.locate(0, 256, 0) == (0xA000, 0)
    assert lookup.locate(0, 0, 256) == (0xB000, 0)
