import json
import os
from cloudvolume import CloudVolume
import boto3

def parse_vsvi(vsvi_dataset_path, aws_profile_name="default", region='us-east-1'):
    if vsvi_dataset_path.startswith('s3://'):
        bucket, key = vsvi_dataset_path.replace('s3://', '').split('/', 1)
        session = boto3.Session(profile_name=aws_profile_name, region_name=region)
        s3 = session.resource('s3')
        vsvi_data = json.load(s3.Object(Bucket=bucket, key=key).get())
    else:
        with open(vsvi_dataset_path, 'r') as file:
            vsvi_data = json.load(file)
    
    return vsvi_data

def create_precomputed_info(vsvi_data, cloudpath):
    info = {
        "data_type": "uint8" if vsvi_data["SourceBytesPerPixel"] == 1 else "uint16",
        "num_channels": 1,
        "scales": [{
            "chunk_sizes": [[vsvi_data["SourceTileSizeX"], vsvi_data["SourceTileSizeY"], 1]],
            "encoding": "raw",
            "key": "mip0",
            "resolution": [vsvi_data["TargetVoxelSizeXnm"], vsvi_data["TargetVoxelSizeYnm"], vsvi_data["TargetVoxelSizeZnm"]],
            "size": [vsvi_data["TargetDataSizeX"], vsvi_data["TargetDataSizeY"], vsvi_data["TargetDataSizeZ"]],
            "voxel_offset": [vsvi_data["OffsetX"], vsvi_data["OffsetY"], vsvi_data["OffsetZ"]]
        }],
        "type": "image"
    }
    
    vol = CloudVolume(cloudpath, info=info)
    vol.commit_info()


def upload_tiles_to_precomputed(vsvi_data, cloudpath, source_dir):
    vol = CloudVolume(cloudpath, mip=0, parallel=True)

    # Example loop 
    for s in range(vsvi_data["SourceMinS"], vsvi_data["SourceMaxS"] + 1):
        for r in range(vsvi_data["SourceMinR"], vsvi_data["SourceMaxR"] + 1):
            for c in range(vsvi_data["SourceMinC"], vsvi_data["SourceMaxC"] + 1):
                # Construct file path based on VSVI parameters
                # Note: You might need to adjust the file path pattern based on the actual file naming
                file_name = vsvi_data["SourceFileNameTemplate"].replace("%04d", "{:04d}").format(s, r, c)
                file_path = os.path.join(source_dir, file_name)
                if os.path.exists(file_path):
                    # Load the image and upload it to the specified location
                    # You will need to add the logic to read the PNG file and upload it.
                    pass


