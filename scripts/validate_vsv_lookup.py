#!/usr/bin/env python3
"""Validate the pure VSV2 coordinate lookup against decoded image continuity."""

import argparse
import random
import statistics

import numpy as np

from vsvi2precomputed.vsv import VSV2Lookup, decode_vast_brick, parse_brick_page


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    parser.add_argument("--samples", type=int, default=60)
    parser.add_argument("--seed", type=int, default=20260728)
    args = parser.parse_args()

    lookup = VSV2Lookup(args.source)
    rng = random.Random(args.seed)
    points = []
    pages = {}

    def brick(stream, coordinate):
        page_offset, child = lookup.locate(*coordinate)
        if not page_offset:
            return np.zeros((16, 16, 16), dtype=np.uint8)
        if page_offset not in pages:
            stream.seek(page_offset)
            pages[page_offset] = parse_brick_page(stream.read(40960))
        lengths, offsets = pages[page_offset]
        if not lengths[child]:
            return np.zeros((16, 16, 16), dtype=np.uint8)
        stream.seek(offsets[child])
        data = decode_vast_brick(stream.read(lengths[child]))
        return np.frombuffer(data, dtype=np.uint8).reshape((16, 16, 16), order="F")

    scores = [[], [], []]
    random_scores = []
    with open(args.source, "rb") as stream:
        attempts = 0
        decoded = []
        while len(points) < args.samples and attempts < args.samples * 100:
            attempts += 1
            point = tuple(rng.randrange(1, size - 1) for size in lookup.bricks)
            array = brick(stream, point)
            if np.count_nonzero(array):
                points.append(point)
                decoded.append(array)
        if len(points) != args.samples:
            raise RuntimeError("could not sample enough nonzero bricks")
        for point, array in zip(points, decoded):
            for axis in range(3):
                adjacent = list(point)
                adjacent[axis] += 1
                neighbor = brick(stream, adjacent)
                scores[axis].append(
                    np.abs(
                        array.take(-1, axis=axis).astype(int)
                        - neighbor.take(0, axis=axis).astype(int)
                    ).mean()
                )
        for left, right in zip(decoded[::2], decoded[1::2]):
            random_scores.append(np.abs(left[-1].astype(int) - right[0].astype(int)).mean())

    for axis, values in zip("xyz", scores):
        print(f"{axis}: mean={statistics.mean(values):.4f} median={statistics.median(values):.4f}")
    print(
        "random: "
        f"mean={statistics.mean(random_scores):.4f} "
        f"median={statistics.median(random_scores):.4f}"
    )
    unique = len({array.tobytes() for array in decoded})
    print(f"unique: {unique}/{len(decoded)}")
    if unique < len(decoded) * 0.9:
        raise RuntimeError("decoded sample contains excessive repeated bricks")


if __name__ == "__main__":
    main()
