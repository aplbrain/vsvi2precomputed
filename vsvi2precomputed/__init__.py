import json
import os
import io

import boto3
import numpy as np
from PIL import Image
from cloudvolume import CloudVolume
from joblib import Parallel, delayed
from tqdm import tqdm



def parse_vsvi(vsvi_dataset_path):
    session = boto3.Session(profile_name='default', region_name='us-east-1')
    s3_client = session.client('s3')
    if vsvi_dataset_path.startswith("s3://"):
        bucket, key = vsvi_dataset_path.replace("s3://", "").split("/", 1)
        json_data = _get_object_data(bucket, key).decode("utf-8")
    else:
        with open(vsvi_dataset_path, "r") as file:
            json_data = file.read()

    vsvi_data = json.loads(json_data.replace("\\", "/"))
    return vsvi_data


def create_precomputed_info(vsvi_data, cloudpath):
    info = CloudVolume.create_new_info(
        num_channels=1,
        layer_type="image",
        data_type="uint8" if vsvi_data["SourceBytesPerPixel"] == 1 else "uint16",
        encoding="raw",
        resolution=[
            vsvi_data["TargetVoxelSizeXnm"],
            vsvi_data["TargetVoxelSizeYnm"],
            vsvi_data["TargetVoxelSizeZnm"],
        ],
        voxel_offset=[vsvi_data["OffsetX"], vsvi_data["OffsetY"], vsvi_data["OffsetZ"]],
        chunk_size=[vsvi_data["SourceTileSizeX"], vsvi_data["SourceTileSizeY"], 1],
        # volume_size=[
        #     vsvi_data["TargetDataSizeX"] - vsvi_data["SourceMinC"],
        #     vsvi_data["TargetDataSizeY"] - vsvi_data["SourceMinR"],
        #     vsvi_data["TargetDataSizeZ"] - vsvi_data["SourceMinS"],
        # ],
        volume_size=[
            vsvi_data["TargetDataSizeX"],
            vsvi_data["TargetDataSizeY"],
            vsvi_data["TargetDataSizeZ"],
        ],
    )

    vol = CloudVolume(cloudpath, info=info)
    vol.commit_info()
    return info


def upload_tiles_to_precomputed(vsvi_root_path, vsvi_data, cloudpath):

    bucket, base_prefix = vsvi_root_path.replace("s3://", "").split("/", 1)
    source_prefix = vsvi_data.get("SourceFileNameTemplate").split("/")[1]
    prefix = os.path.join(base_prefix, source_prefix)

    vol = CloudVolume(cloudpath, mip=0, parallel=False, fill_missing=True, non_aligned_writes=True)
    # for key in tqdm(_list_objects(bucket, prefix, s3_client)):
    #     _upload_tile_to_precomputed(vol, bucket, key, vsvi_data, s3_client)

    Parallel(n_jobs=-1)(delayed(_upload_tile_to_precomputed)(vol, bucket, key, vsvi_data) for key in tqdm(_list_objects(bucket, prefix)))

### Utility Functions ###


def _upload_tile_to_precomputed(vol, bucket, key, vsvi_data):
    filename = key.split("/")[-1]
    z, y, x = _parse_filename(filename)
    z = z - vsvi_data["SourceMinS"]
    y = y - vsvi_data["SourceMinR"]
    x = x - vsvi_data["SourceMinC"]

    dx, dy = vsvi_data["SourceTileSizeX"], vsvi_data["SourceTileSizeY"]
    dz = 1
    x_start, y_start, z_start = x * dx, y * dy, z
    # x_stop = min(x_start + dx, vsvi_data["TargetDataSizeX"])
    # y_stop = min(y_start + dy, vsvi_data["TargetDataSizeY"])
    z_stop = z_start + 1

    image_data = Image.open(io.BytesIO(_get_object_data(bucket, key)))
    w, h = image_data.size
    try:
        vol[x_start : x_start + w, y_start : y_start + h, z_start : z_stop] = (
            np.expand_dims(np.asarray(image_data).T, 2)
        )
    except Exception as error:
        print(f"Key: {key}")
        raise error

def _get_object_data(bucket, key):
    session = boto3.Session(profile_name='default', region_name='us-east-1')
    s3_client = session.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def _list_objects(bucket, prefix, start_at=0):
    session = boto3.Session(profile_name='default', region_name='us-east-1')
    s3_client = session.client('s3')
    paginator = s3_client.get_paginator("list_objects_v2")
    num_obj = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for object in page["Contents"]:
            num_obj += 1
            if num_obj >= start_at:
                yield object["Key"]
            


def _parse_filename(filename):
    # 0001_W01_Sec001_tr10-tc16.png
    parts = filename.replace(".png", "").split("_")
    tr, tc = parts[-1].split("-")
    z = int(parts[0])
    x = int(tc[2:])
    y = int(tr[2:])
    return z, y, x
