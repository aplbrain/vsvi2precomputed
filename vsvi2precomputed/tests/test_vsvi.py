import pytest
import boto3
from cloudvolume import CloudVolume
import vsvi2precomputed as vp


@pytest.fixture
def vsvi_cloud_path():
    return "s3://smart-em-datasets/datasets/Neha_1mm_wafer/Neha_1mm_ROI1.vsvi"


@pytest.fixture
def vsvi_mip0_path():
    return "s3://smart-em-datasets/datasets/Neha_1mm_wafer/mip0/"


@pytest.fixture
def precomputed_cloud_path():
    return "s3://mambo-datalake/connects49a/vsvi2precomputed/test/"


@pytest.fixture
def s3_client():
    session = boto3.Session(profile_name="smartem", region_name="us-east-1")
    return session.client("s3")


@pytest.fixture
def expected_vsvi_data():
    return {
        "Comment": "Neha_1mm_ROI1",
        "ServerType": "imagetiles",
        "SourceFileNameTemplate": "./mip0/%04d_*/%04d_*_tr%d-tc%d.png",
        "SourceParamSequence": "ssrc",
        "SourceMinS": 1,
        "SourceMaxS": 94,
        "SourceMinR": 1,
        "SourceMaxR": 36,
        "SourceMinC": 1,
        "SourceMaxC": 47,
        "MipMapFileNameTemplate": "./mip%d/slice_%04d/r%02d/%04d_tr%d-tc%d.png",
        "MipMapParamSequence": "msrsrc",
        "SourceMinM": 1,
        "SourceMaxM": 8,
        "SourceTileSizeX": 2048,
        "SourceTileSizeY": 2048,
        "SourceBytesPerPixel": 1,
        "MissingImagePolicy": "white",
        "TargetDataSizeX": 95567,
        "TargetDataSizeY": 73231,
        "TargetDataSizeZ": 94,
        "OffsetX": 0,
        "OffsetY": 0,
        "OffsetZ": 0,
        "OffsetMip": 0,
        "TargetVoxelSizeXnm": 4,
        "TargetVoxelSizeYnm": 4,
        "TargetVoxelSizeZnm": 30,
        "TargetLayerName": "Neha_1mm_ROI1",
    }


@pytest.fixture
def expected_precomputed_info():
    return {
        "num_channels": 1,
        "type": "image",
        "data_type": "uint8",
        "scales": [
            {
                "encoding": "raw",
                "chunk_sizes": [[2048, 2048, 1]],
                "resolution": [4, 4, 30],
                "voxel_offset": [0, 0, 0],
                "size": [95567, 73231, 94],
                "key": "4_4_30",
            }
        ],
    }


@pytest.fixture
def example_filename():
    return "0001_W01_Sec001_tr10-tc16.png"


@pytest.fixture
def example_s3_uri():
    return "s3://smart-em-datasets/datasets/Neha_1mm_wafer/mip0/0001_W01_Sec001/0001_W01_Sec001_tr10-tc16.png"


### TESTS ###


def test_parse_vsvi_cloud(vsvi_cloud_path, expected_vsvi_data, s3_client):
    vsvi_data = vp.parse_vsvi(vsvi_cloud_path, s3_client)
    assert vsvi_data == expected_vsvi_data


def test_create_precomputed_info(
    vsvi_cloud_path, precomputed_cloud_path, expected_precomputed_info, s3_client
):
    vsvi_data = vp.parse_vsvi(vsvi_cloud_path, s3_client)
    info = vp.create_precomputed_info(vsvi_data, precomputed_cloud_path)
    print(info)
    assert info == expected_precomputed_info


def test_get_objects(vsvi_mip0_path, s3_client):
    bucket, prefix = vsvi_mip0_path.replace("s3://", "").split("/", 1)
    i = 0
    for key in vp._list_objects(bucket, prefix, s3_client):
        i += 1
        if i > 100:
            break


def test_parse_filename(example_filename):
    z, y, x = vp._parse_filename(example_filename)
    assert (z, y, x) == (1, 16, 10)


def test_upload_tile(
    precomputed_cloud_path, example_s3_uri, vsvi_cloud_path, s3_client
):
    bucket, key = example_s3_uri.replace("s3://", "").split("/", 1)
    vsvi_data = vp.parse_vsvi(vsvi_cloud_path, s3_client)
    vol = CloudVolume(precomputed_cloud_path, mip=0, parallel=True, fill_missing=True)
    vp._upload_tile_to_precomputed(vol, bucket, key, vsvi_data, s3_client)
