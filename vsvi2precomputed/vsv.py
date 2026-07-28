import struct
import zlib
from dataclasses import dataclass
from typing import BinaryIO

from .vbc_pure import decode_vbc


VSV2_HEADER_SIZE = 0xC4


@dataclass(frozen=True)
class VSV2Header:
    size: tuple[int, int, int]
    brick_size: tuple[int, int, int]
    bytes_per_voxel: int
    voxel_size_nm: tuple[float, float, float]
    mip_count: int
    index_offsets: tuple[int, ...]


class VSV2Lookup:
    """Resolve brick coordinates through VSV2's sparse extent index."""

    def __init__(self, source):
        self.source = str(source)
        with open(source, "rb") as stream:
            stream.seek(0, 2)
            self._file_size = stream.tell()
            header = read_vsv2_header(stream)
            self.size = header.size
            self.bricks = tuple(
                (header.size[i] + header.brick_size[i] - 1) // header.brick_size[i]
                for i in range(3)
            )
            self._descriptor_offset = header.index_offsets[0]

    def locate(self, x, y, z):
        """Locate a brick coordinate's containing page and child slot."""
        bounds = self.bricks
        if any(value < 0 for value in (x, y, z)) or any(
            value >= bounds[i] for i, value in enumerate((x, y, z))
        ):
            raise IndexError((x, y, z))
        # VSV2 stores two base-16 descriptor levels above each 16^3 brick page.
        low = (x & 15, y & 15, z & 15)
        middle = ((x >> 4) & 15, (y >> 4) & 15, (z >> 4) & 15)
        high = (x >> 8, y >> 8, z >> 8)
        child = (low[2] * 16 + low[1]) * 16 + low[0]
        with open(self.source, "rb") as stream:
            top_index = (high[2] * 16 + high[1]) * 16 + high[0]
            stream.seek(self._descriptor_offset + top_index * 8)
            second_level = struct.unpack("<Q", stream.read(8))[0]
            if not second_level:
                return 0, child
            middle_index = (middle[2] * 16 + middle[1]) * 16 + middle[0]
            stream.seek(second_level + middle_index * 8)
            page_offset = struct.unpack("<Q", stream.read(8))[0]
        return page_offset, child


def read_vsv2_header(stream: BinaryIO) -> VSV2Header:
    """Read the common metadata and mip index pointers from a VSV2 file."""
    stream.seek(0)
    data = stream.read(0x390)
    if len(data) < 0x390:
        raise ValueError("VSV2 header is truncated")
    if data[:4] != b"VSV2":
        raise ValueError("Input is not a VSV2 file")

    header_size = struct.unpack_from("<I", data, 4)[0]
    if header_size != VSV2_HEADER_SIZE:
        raise ValueError(f"Unsupported VSV2 header size: {header_size}")

    mip_count = struct.unpack_from("<I", data, 0x30)[0]
    size = struct.unpack_from("<III", data, 0x68)
    brick_size = struct.unpack_from("<III", data, 0x74)
    bytes_per_voxel = struct.unpack_from("<I", data, 0x8C)[0]
    voxel_size_nm = struct.unpack_from("<fff", data, 0x98)

    index_offsets = tuple(
        struct.unpack_from("<Q", data, VSV2_HEADER_SIZE + mip * 8)[0]
        for mip in range(mip_count)
    )
    return VSV2Header(
        size=size,
        brick_size=brick_size,
        bytes_per_voxel=bytes_per_voxel,
        voxel_size_nm=voxel_size_nm,
        mip_count=mip_count,
        index_offsets=index_offsets,
    )


def read_index_offsets(stream: BinaryIO, offset: int, count: int) -> tuple[int, ...]:
    """Read little-endian absolute file offsets from a VSV2 index table."""
    stream.seek(offset)
    data = stream.read(count * 8)
    if len(data) != count * 8:
        raise ValueError("VSV2 index table is truncated")
    return struct.unpack(f"<{count}Q", data)


def parse_brick_page(data: bytes):
    """Return the brick lengths and offset slots from a 40,960-byte VSV2 page."""
    if len(data) != 40960:
        raise ValueError("VSV2 brick page has an invalid size")
    return struct.unpack_from("<4096H", data), struct.unpack_from("<4096Q", data, 8192)


def decode_vsv2_record(data: bytes, count: int) -> bytes:
    """Decode one VSV2 array record into its native little-endian byte layout."""
    if len(data) < 8:
        raise ValueError("VSV2 record is truncated")

    descriptor, payload_size = struct.unpack_from("<II", data)
    element_width = descriptor & 0xFF
    encoding = (descriptor >> 8) & 0xFF
    if element_width not in (1, 2, 3, 4, 6, 8):
        raise ValueError(f"Unsupported VSV2 element width: {element_width}")

    output_size = count * element_width
    if encoding == 0:
        payload = data[8 : 8 + output_size]
        if len(payload) != output_size:
            raise ValueError("VSV2 raw record is truncated")
        return payload
    if encoding == 1:
        value = data[8 : 8 + element_width]
        if len(value) != element_width:
            raise ValueError("VSV2 constant record is truncated")
        return value * count
    if encoding == 2:
        payload = data[12 : 12 + payload_size]
        if len(payload) != payload_size:
            raise ValueError("VSV2 compressed record is truncated")
        result = zlib.decompress(payload)
        if len(result) != output_size:
            raise ValueError("VSV2 decompressed size does not match record metadata")
        return result
    raise ValueError(f"Unsupported VSV2 encoding: {encoding}")


def decode_vast_brick(data: bytes) -> bytes:
    """Decode a byte-pixel VAST brick payload by its compression mode."""
    if not data:
        raise ValueError("VAST brick is empty")
    if data[0] == 0:
        if len(data) != 4097:
            raise ValueError("Uncompressed VAST brick has an invalid size")
        return data[1:]
    if data[0] == 1:
        if len(data) != 2:
            raise ValueError("Constant VAST brick has an invalid size")
        return data[1:] * 4096
    if data[0] == 2:
        return decode_vbc(data)
    if data[0] == 4:
        try:
            result = zlib.decompress(data[1:])
        except zlib.error as error:
            raise ValueError("VAST zlib brick is invalid") from error
        if len(result) != 4096:
            raise ValueError("VAST zlib brick has an invalid decoded size")
        return result
    raise ValueError(f"Unsupported VAST brick compression mode: {data[0]}")
