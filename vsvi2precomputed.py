import vsvi2precomputed as vp
import argparse
import warnings
import os
warnings.filterwarnings("ignore")

'''
Script to convert VAST formatted data to precomputed format for visualization in Neuroglancer and compatibility with Seung Lab ecosystem

Usage:
python vsvi2precomputed.py -i <input-dir> -o <output-dir> --profile <aws-profile-name>

Input parameters:
-i / --input_vsvi : Path to VSVI file. Can be on filesystem or in S3 bucket. Examples: s3://ec2-alignment-workspace/smartEM_stitched_not_aligned/fuse_18x18_em.vsvi, ./data/input_files
-o / --output_dir : Path to location for converted output. Can be on filesystem or in S3 bucket. Examples: s3://mambo-datalake/connects49a/vsvi2precomputed/windows-test/, ./data/output_files
--profile : AWS CLI profile name. Defaults to "default". Can be set up with command "aws configure"
'''


def main():

    # Parse command line arguments
    parser = argparse.ArgumentParser(
                    prog='vsvi2precomputed',
                    description='Convert an s3 dataset from vsvi format to precomputed format',
    )
    parser.add_argument('-i', '--input_vsvi', help="Path to S3 bucket or location of input dataset .vsvi file")
    parser.add_argument('-o', '--output_dir', help="Path to location for converted output. S3 bucket is supported")
    parser.add_argument('--profile', default="default", help="AWS CLI profile name to use. Defaults to default")
    args = parser.parse_args()

    # Set aws profile to the one given in command line args
    os.environ["AWS_PROFILE"] = args.profile

    # Check command line args for errors
    if args.input_vsvi[-5:] != ".vsvi":
        raise ValueError("Input must be a vsvi file")
    if args.output_dir[-1:] != "/":
        raise ValueError("Output must be a directory path ending in /")

    # Read VSVI metadata into a variable
    if args.input_vsvi[:5] == "s3://":
        vsvi_data = vp.fetch_s3_vsvi(args.input_vsvi)
    else:
        vsvi_data = vp.read_local_vsvi(args.input_vsvi)

    # Separate directory of VSVI file, this should be where the image files are
    vsvi_root_path, vsvi_filename = os.path.split(args.input_vsvi)

    # Add file:// to output dir if local filesystem; this is for cloudvolume
    if args.output_dir[:5] == "s3://":
        precomputed_path = args.output_dir
    else:
        precomputed_path = "file://" + args.output_dir

    # Convert
    vp.convert_precomputed_tiles(vsvi_root_path, vsvi_data, precomputed_path)

    print("Done")

if __name__ == "__main__":
    main()
