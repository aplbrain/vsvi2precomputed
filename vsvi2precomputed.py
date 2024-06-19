import vsvi2precomputed as vp
import boto3
import argparse
import warnings
import os
warnings.filterwarnings("ignore")


def main():

    parser = argparse.ArgumentParser(
                    prog='vsvi2precomputed',
                    description='Convert an s3 dataset from vsvi format to precomputed format',
    )
    parser.add_argument('-i', '--input_vsvi', help="Path to s3 or location of input dataset .vsvi file")
    parser.add_argument('-o', '--output_dir', help="Path to s3 location for converted output")
    parser.add_argument('--profile', default="default", help="AWS CLI profile name to use. Defaults to default")

    args = parser.parse_args()
    os.environ["AWS_PROFILE"] = args.profile

    if args.input_vsvi[:5] == "s3://":
        vsvi_data = vp.fetch_s3_vsvi(args.input_vsvi)
    else:
        vsvi_data = vp.read_local_vsvi(args.input_vsvi)

    if args.output_dir[:5] != "s3://":
        raise ValueError("Output path must begin with s3://")

    if args.input_vsvi[-5:] != ".vsvi":
        raise ValueError("Input must be a vsvi file")

    if args.output_dir[-1:] != "/":
        raise ValueError("Output must be a directory path ending in /")

    precomputed_cloud_path = args.output_dir

    vsvi_root_path, vsvi_filename = os.path.split(args.input_vsvi)
    vsvi_cloud_path = args.input_vsvi

    vsvi_data = vp.fetch_s3_vsvi(vsvi_cloud_path)
    info = vp.create_precomputed_info(vsvi_data, precomputed_cloud_path)
    vp.upload_precomputed_tiles_to_bucket(vsvi_root_path, vsvi_data, precomputed_cloud_path)
    print("Done")

if __name__ == "__main__":
    main()
