import pytest
import os
from cloudvolume import CloudVolume
from .. import vsvi2precomputed as vp

@pytest.fixture(scope="session", autouse=True)
def set_env(request):
    os.environ["AWS_PROFILE"] = request.config.getoption("--profile")

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
def source_file_name_template():
    return "./mip0/%04d_*/%04d_*_tr%d-tc%d.png"


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


@pytest.fixture
def example_s3_uri_border():
    return "s3://smart-em-datasets/datasets/Neha_1mm_wafer/mip0/0026_W01_Sec026/0026_W01_Sec026_tr22-tc47.png"


### TESTS ###


def test_fetch_s3_vsvi(vsvi_cloud_path, expected_vsvi_data):
    vsvi_data = vp.fetch_s3_vsvi(vsvi_cloud_path)
    assert vsvi_data == expected_vsvi_data

def test_create_precomputed_info(
    vsvi_cloud_path, precomputed_cloud_path, expected_precomputed_info):
    vsvi_data = vp.fetch_s3_vsvi(vsvi_cloud_path)
    info = vp.create_precomputed_info(vsvi_data, precomputed_cloud_path)
    assert info == expected_precomputed_info


def test_get_objects(vsvi_mip0_path):
    bucket, prefix = vsvi_mip0_path.replace("s3://", "").split("/", 1)
    i = 0
    for key in vp._list_objects_cloud(bucket, prefix):
        i += 1
        if i > 100:
            break


def test_parse_filename(example_filename, source_file_name_template):
    z, y, x = vp._parse_filename(example_filename, source_file_name_template)
    assert (z, y, x) == (1, 10, 16)


def test_upload_tile(
    precomputed_cloud_path, example_s3_uri, vsvi_cloud_path
):
    bucket, key = example_s3_uri.replace("s3://", "").split("/", 1)
    vsvi_data = vp.fetch_s3_vsvi(vsvi_cloud_path)
    vol = CloudVolume(precomputed_cloud_path, mip=0, parallel=False, fill_missing=True)
    vp._convert_tile(vol, key, vsvi_data, input_bucket=bucket)


def test_upload_tile_border(
    precomputed_cloud_path, example_s3_uri_border, vsvi_cloud_path
):
    bucket, key = example_s3_uri_border.replace("s3://", "").split("/", 1)
    vsvi_data = vp.fetch_s3_vsvi(vsvi_cloud_path)
    vol = CloudVolume(precomputed_cloud_path, mip=0, parallel=False, fill_missing=True)
    vp._convert_tile(vol, key, vsvi_data, input_bucket=bucket)
