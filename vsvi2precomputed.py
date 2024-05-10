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
    parser.add_argument('-i', '--input_vsvi', help="Path to s3 location of input dataset .vsvi file")
    parser.add_argument('-o', '--output_dir', help="Path to s3 location for converted output")

    args = parser.parse_args()
    # TODO: add some validation like for trailing slash on output dir

    precomputed_cloud_path = args.output_dir

    vsvi_root_path, vsvi_filename = os.path.split(args.input_vsvi)
    vsvi_cloud_path = args.input_vsvi

    vsvi_data = vp.parse_vsvi(vsvi_cloud_path)
    info = vp.create_precomputed_info(vsvi_data, precomputed_cloud_path)
    vp.upload_tiles_to_precomputed(vsvi_root_path, vsvi_data, precomputed_cloud_path)
    print("Done")

if __name__ == "__main__":
    main()
