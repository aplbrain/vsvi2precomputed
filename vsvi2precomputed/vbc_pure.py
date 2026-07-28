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
    return bytes(output)
