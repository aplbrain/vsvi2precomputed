from operator import itemgetter


def _morton_to_linear(index: int) -> int:
    x = (index & 1) | ((index >> 1) & 2) | ((index >> 2) & 4) | ((index >> 3) & 8)
    y = ((index >> 1) & 1) | ((index >> 2) & 2) | ((index >> 3) & 4) | ((index >> 4) & 8)
    return y * 16 + x


MORTON_TO_LINEAR = tuple(_morton_to_linear(index) for index in range(256))
LINEAR_TO_MORTON = tuple(MORTON_TO_LINEAR.index(index) for index in range(256))
_DEINTERLEAVE_16_CUBE = itemgetter(
    *(slice_offset + index for slice_offset in range(0, 4096, 256) for index in LINEAR_TO_MORTON)
)


def unpack_values(data: bytes, count: int, bits: int) -> tuple[bytes, int]:
    if not 0 <= bits <= 8:
        raise ValueError(f"unsupported VBC bit depth: {bits}")
    if bits == 0:
        return bytes(count), 0
    result = bytearray(count)
    accumulator = 0
    available = 0
    consumed = 0
    mask = (1 << bits) - 1
    for index in range(count):
        while available < bits:
            if consumed >= len(data):
                raise ValueError("truncated VBC bit stream")
            accumulator |= data[consumed] << available
            consumed += 1
            available += 8
        result[index] = accumulator & mask
        accumulator >>= bits
        available -= bits
    return bytes(result), consumed


def decode_vbc(payload: bytes, count=4096, quantization=0) -> bytes:
    """Decode VAST variable-bitdepth rows in linear voxel order."""
    if not payload or payload[0] != 2:
        raise ValueError("VBC payload must start with mode 2")
    row_size = 1 << payload[1]
    position = 2
    output = bytearray(count)
    output_position = 0
    while output_position < count:
        if position >= len(payload):
            raise ValueError("truncated VBC row header")
        bits = payload[position]
        position += 1
        predictor_count = 1
        if position + predictor_count > len(payload):
            raise ValueError("truncated VBC predictor")
        predictors = payload[position : position + predictor_count]
        position += predictor_count
        values, consumed = unpack_values(payload[position:], min(row_size, count - output_position), bits)
        position += consumed
        for index, value in enumerate(values):
            predictor = predictors[index % predictor_count]
            output[output_position + index] = ((value << quantization) + predictor) & 0xFF
        output_position += len(values)
    if count % 256:
        raise ValueError("VBC output size must contain complete 16x16 slices")
    if count == 4096:
        return bytes(_DEINTERLEAVE_16_CUBE(output))
    result = bytearray(count)
    for slice_offset in range(0, count, 256):
        for linear_index, morton_index in enumerate(LINEAR_TO_MORTON):
            result[slice_offset + linear_index] = output[slice_offset + morton_index]
    return bytes(result)
