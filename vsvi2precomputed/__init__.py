import json
import os
import io
import re
import pathlib

import boto3
import botocore
import numpy as np
from PIL import Image
from cloudvolume import CloudVolume
from joblib import Parallel, delayed
from tqdm import tqdm

############
# Core API #
############

def fetch_s3_vsvi(vsvi_dataset_path):
    '''
    Given an S3 path to a VSVI file, return a dict containing the file contents.
    Inputs: str
    Outputs: dict
    '''
    if vsvi_dataset_path.startswith("s3://"):
        bucket, key = vsvi_dataset_path.replace("s3://", "").split("/", 1)
        try:
            json_data = _get_object_data(bucket, key).decode("utf-8")
        except botocore.exceptions.ClientError as error:
            raise ValueError("File not found in S3. Check credentials and that the path is correct.") from error

    vsvi_data = json.loads(json_data.replace("\\", "/"))
    return vsvi_data

def read_local_vsvi(vsvi_dataset_path):
    '''
    Given a local path to a VSVI file, return a JSON object containing the file contents.
    Inputs: str
    Outputs: dict
    '''
    with open(vsvi_dataset_path, "r") as file:
        json_data = file.read()
    vsvi_data = json.loads(json_data.replace("\\", "/"))
    return vsvi_data

def create_precomputed_info(vsvi_data, path):
    '''
    Create a precomputed info file.
    Inputs: dict output of fetch_s3_vsvi() or read_local_vsvi()
            str path, local path must be prepended with "file://", S3 path must be prepended with "s3://"
    Outputs: no output, but cloudvolume info file will be created at path input
    '''
    # Prepend "file://" if path is local and does not already have it
    if path[:5] != "s3://" and path[:7] != "file://":
        path = "file://" + path
        
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

    vol = CloudVolume(path, info=info)
    vol.commit_info()
    return info


def convert_precomputed_tiles(vsvi_root_path, vsvi_data, output_path):
    '''
    Read all files in vsvi_root_path, output them in precomputed format to cloudpath
    Inputs: str path where vast files are located
            dict output of fetch_s3_vsvi() or read_local_vsvi()
            str path for output files
    Outputs: new files located at output_path
    '''
    input_bucket, base_prefix = vsvi_root_path.replace("s3://", "").split("/", 1)

    source_prefix = vsvi_data.get("SourceFileNameTemplate").split("/")[1]
    prefix = "/".join([base_prefix, source_prefix])

    # Prepend "file://" if path is local and does not already have it
    if output_path[:5] != "s3://" and output_path[:7] != "file://":
        output_path = "file://" + output_path
    vol = CloudVolume(output_path, mip=0, parallel=False, fill_missing=True, non_aligned_writes=True)

    if input_bucket:
        Parallel(n_jobs=-1)(delayed(_convert_tile)(vol, input_bucket, key, vsvi_data) for key in tqdm(_list_objects_cloud(input_bucket, prefix)))
        # TODO: add log that counts number of objects copied
    else:
        search_dir = os.path.join(vsvi_root_path, source_prefix)
        Parallel(n_jobs=-1)(delayed(_convert_tile)(vol, key, vsvi_data) for key in tqdm(_list_objects_local(search_dir)))
        # TODO: add log that counts number of objects copied

#####################
# Utility Functions #
#####################

def _convert_tile(vol, filepath, vsvi_data, input_bucket=None):
    '''
    Convert a single VSVI tile to a precomputed tile.
    Inputs: CloudVolume object representing location where output will be stored
            Filepath of VSVI tile as string or pathlib object. Should correspond to "Key" of S3 object or full path of local file
            Dict output of fetch_s3_vsvi() or read_local_vsvi()
            (optional) Str name of S3 bucket
    Outputs: single new file added to CloudVolume location
    '''
    filename = pathlib.Path(filepath)
    if filename.suffix != ".txt":
        z, y, x = _parse_filename(filename, vsvi_data["SourceFileNameTemplate"])
        z = z - vsvi_data["SourceMinS"]
        y = y - vsvi_data["SourceMinR"]
        x = x - vsvi_data["SourceMinC"]

        dx, dy = vsvi_data["SourceTileSizeX"], vsvi_data["SourceTileSizeY"]
        dz = 1
        x_start, y_start, z_start = x * dx, y * dy, z
        # x_stop = min(x_start + dx, vsvi_data["TargetDataSizeX"])
        # y_stop = min(y_start + dy, vsvi_data["TargetDataSizeY"])
        z_stop = z_start + 1

        if input_bucket:
            image_data = Image.open(io.BytesIO(_get_object_data(input_bucket, filepath)))
        else:
            image_data = Image.open(filepath)
        w, h = image_data.size

        try:
            vol[x_start : x_start + w, y_start : y_start + h, z_start : z_stop] = (
                np.expand_dims(np.asarray(image_data).T, 2)
            )
        except Exception as error:
            print(f"Key: {filepath}")
            raise error


def _get_object_data(bucket, key):
    '''
    Get object from S3 bucket
    Inputs: str bucket name, str object key
    Outputs: file contents as binary
    '''
    session = boto3.Session()
    s3_client = session.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def _list_objects_cloud(bucket, prefix, start_at=0):
    '''
    Get all objects in an S3 path
    Inputs: str bucket name, str object prefix, page to start at
    Outputs: generator yielding all object keys in bucket + prefix path
    '''
    session = boto3.Session()
    s3_client = session.client('s3')
    paginator = s3_client.get_paginator("list_objects_v2")
    num_obj = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for object in page["Contents"]:
            num_obj += 1
            if num_obj >= start_at:
                yield object["Key"]


def _list_objects_local(dir):
    '''
    Get relative paths to all files in a directory, recursive
    Inputs: str or Pathlib object directory name
    Outputs: generator yielding all paths to files
    '''
    path = pathlib.Path(dir)
    for file in path.rglob("*"):
        if os.path.splitext(file)[1]:
            yield file


def _parse_filename(filename, template):
    '''
    Get section (z), row (y), and column (x) numbers for a tile from tile filename. Format showing positions of z, y, x within string is given in template param.
    Inputs: filename to parse (str or pathlib object) example: section_001_tr10-tc16.png
            "SourceFileNameTemplate" field from vsvi file (str) example: section_%05d_tr%d-tc%d.png
    Outputs: z, y, x
    '''
    filename = pathlib.Path(filename)

    # Template for where z, y, x are in the string is given in vsvi file, but
    #   may not have same path type as filesystem code is running on
    template_string = pathlib.PurePath(template)
    if template_string.name == template:
        if os.name == "nt":
            template_string = pathlib.PurePosixPath(template)
        elif os.name == "posix":
            template_string = pathlib.PureWindowsPath(template)
    
    # Replace %d in template with regex indicator for digits
    regex_template = re.sub('%[0-9]*d', '([0-9]+)', template_string.name)
    # Parse digits out of input string
    integer_matches = re.search(regex_template, filename.name)
    z = int(integer_matches.group(1))
    y = int(integer_matches.group(2))
    x = int(integer_matches.group(3))

    return z, y, x
