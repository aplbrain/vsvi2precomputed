import vsvi2precomputed as vp
import argparse
import warnings
import os
warnings.filterwarnings("ignore")


def main():

    parser = argparse.ArgumentParser(
                    prog='vsvi2precomputed',
                    description='Convert an s3 dataset from vsvi format to precomputed format',
    )
    parser.add_argument('-i', '--input_vsvi', help="Path to 3 or location of input dataset .vsvi file")
    parser.add_argument('-o', '--output_dir', help="Path to location for converted output. S3 bucket is supported")
    parser.add_argument('--profile', default="default", help="AWS CLI profile name to use. Defaults to default")

    args = parser.parse_args()
    os.environ["AWS_PROFILE"] = args.profile

    if args.input_vsvi[:5] == "s3://":
        vsvi_data = vp.fetch_s3_vsvi(args.input_vsvi)
    else:
        vsvi_data = vp.read_local_vsvi(args.input_vsvi)

    if args.input_vsvi[-5:] != ".vsvi":
        raise ValueError("Input must be a vsvi file")

    if args.output_dir[-1:] != "/":
        raise ValueError("Output must be a directory path ending in /")

    vsvi_root_path, vsvi_filename = os.path.split(args.input_vsvi)

    if args.output_dir[:5] == "s3://":
        precomputed_path = args.output_dir
    else:
        precomputed_path = "file://" + args.output_dir

    vp.create_precomputed_info(vsvi_data, precomputed_path)
    vp.convert_precomputed_tiles(vsvi_root_path, vsvi_data, precomputed_path)

    print("Done")

if __name__ == "__main__":
    main()
