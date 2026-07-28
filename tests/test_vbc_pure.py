from vsvi2precomputed.vbc_pure import decode_vbc, unpack_values


def test_unpack_six_bit_values():
    values = [0, 1, 2, 3, 17, 31, 42, 63]
    accumulator = sum(value << (index * 6) for index, value in enumerate(values))
    packed = accumulator.to_bytes(6, "little")
    assert list(unpack_values(packed, len(values), 6)[0]) == values


def test_synthetic_vbc_rows():
    for row_exponent in range(5, 9):
        row_size = 1 << row_exponent
        payload = bytearray((2, row_exponent))
        expected = bytearray()
        for row in range(4096 // row_size):
            bits = row % 8 + 1
            predictor = row % 251
            values = [index & ((1 << bits) - 1) for index in range(row_size)]
            accumulator = sum(value << (index * bits) for index, value in enumerate(values))
            payload.extend((bits, predictor))
            payload.extend(accumulator.to_bytes((row_size * bits + 7) // 8, "little"))
            expected.extend((value + predictor) & 0xFF for value in values)

        assert decode_vbc(bytes(payload)) == bytes(expected)
