import argparse

from .converter import convert_vsv


def main():
    parser = argparse.ArgumentParser(description="Convert a local VAST .vsv volume to Neuroglancer precomputed")
    parser.add_argument("input", help="local .vsv file")
    parser.add_argument("output", help="CloudVolume path, e.g. s3://bucket/dataset/image")
    parser.add_argument("--profile", help="AWS profile for the output")
    parser.add_argument("--chunk-size", nargs=3, type=int, default=(512, 512, 16), metavar=("X", "Y", "Z"))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--upload-workers", type=int, default=1)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--checkpoint")
    parser.add_argument("--start", nargs=3, type=int, default=(0, 0, 0), metavar=("X", "Y", "Z"))
    parser.add_argument("--shape", nargs=3, type=int, metavar=("X", "Y", "Z"))
    args = parser.parse_args()
    convert_vsv(
        args.input,
        args.output,
        chunk_size=tuple(args.chunk_size),
        workers=args.workers,
        upload_workers=args.upload_workers,
        retries=args.retries,
        checkpoint=args.checkpoint,
        profile=args.profile,
        start=tuple(args.start),
        shape=None if args.shape is None else tuple(args.shape),
    )


if __name__ == "__main__":
    main()
