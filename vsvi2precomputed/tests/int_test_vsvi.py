import vsvi2precomputed as vp
import boto3


import warnings
warnings.filterwarnings("ignore")

def main():
    session = boto3.Session(profile_name='smartem', region_name='us-east-1')
    s3_client = session.client('s3')
    precomputed_cloud_path = "s3://mambo-datalake/connects49a/vsvi2precomputed/inttest/"

    vsvi_root_path = "s3://smart-em-datasets/datasets/Neha_1mm_wafer/"
    vsvi_cloud_path = vsvi_root_path + "Neha_1mm_ROI1.vsvi"

    vsvi_data = vp.parse_vsvi(vsvi_cloud_path, s3_client)
    info = vp.create_precomputed_info(vsvi_data, precomputed_cloud_path)
    vp.upload_tiles_to_precomputed(vsvi_root_path, vsvi_data, precomputed_cloud_path, s3_client)
    print("Done")

if __name__ == "__main__":
    main()