# vsvi2precomputed
![Logo](logo.png)
Package for converting VSVI (used in VAST) image datasets to precomputed volumes. Supports conversion of local and AWS S3 datasets.

Requirements:
* Python
* [uv](https://docs.astral.sh/uv/)
* AWS CLI (if using S3)

## Usage

### VSV volumes

Convert a local `.vsv` file directly to Neuroglancer precomputed:

```bash
uv run vsv2precomputed data/volume.vsv s3://bucket/dataset/image \
  --profile my-profile \
  --chunk-size 512 512 16 \
  --workers 4 \
  --upload-workers 4 \
  --checkpoint volume.checkpoint.jsonl
```

The checkpoint is appended after each successful aligned chunk write. Running
the same command again skips completed chunks. Network operations use exponential
backoff; adjust attempts with `--retries`. Use `--start X Y Z --shape X Y Z` for
an aligned subset. `--workers` controls decoding processes and
`--upload-workers` controls concurrent chunk uploads.

VBC decompression and segmented index lookup are implemented in pure Python.
Input is currently limited to uint8 VSV2 volumes with
`16 x 16 x 16` internal bricks.

### VSVI volumes

Convert a cloud dataset and store in new cloud path:
```
uv sync
uv run python vsvi2precomputed.py -i s3://path/to/config.vsvi -o s3://path/to/output/dir/
```
Don't forget the trailing slash on the output dir.

Convert a local dataset and upload to the cloud:
```
uv run python vsvi2precomputed.py --i path/to/config.vsvi --o s3://path/to/output/dir/
```

Convert a cloud dataset and upload to the cloud:
```
uv run python vsvi2precomputed.py --i s3://path/to/config.vsvi --o path/to/output/dir/
```

Convert a dataset locally:
```
uv run python vsvi2precomputed.py --i path/to/config.vsvi --o path/to/output/dir/
```

Optional Arguments

| Argument  | Description          | Default |
|-----------|----------------------|---------|
| --profile | AWS CLI profile name | default |

## Tests
```
uv sync
uv run pytest
```
To use an non-default AWS CLI profile:
```
uv run pytest --profile <profile-name>
```

## About VSVI and precomputed formats

VSVI format is native to the [VAST](https://lichtman.rc.fas.harvard.edu/vast/) ecosystem. Precomputed format is native to the [Neuroglancer](https://github.com/google/neuroglancer)/[CloudVolume](https://github.com/seung-lab/cloud-volume) ecosystem.

To view converted data in Neuroglancer:
* Navigate to neuroglancer.bossdb.io.
* Add a new layer using the Data Source URL input box on the top right. 
  * S3: The Data Source URL will be the S3 URI of the directory containing the info file, prepended with `precomputed://`. Example: `precomputed://s3://mambo-datalake/connects49a/vsvi2precomputed/local_aligned/`. 
  * Local: You will need to serve the data first. Navigate to the directory containing the info file, then open a terminal and run the following code. The Data Source URL will then follow the format `precomputed://localhost:<port>/`.
  ```
  from cloudvolume import CloudVolume
  cv = CloudVolume("file://.")
  cv.viewer()
  ```
  * Click the yellow "Create as image layer" button at the bottom right.

## Acknowledgements

We thank the Visual Computing Group at Harvard for building the VAST software. https://www.frontiersin.org/journals/neural-circuits/articles/10.3389/fncir.2018.00088/full 

---
Copyright (c) 2024 The Johns Hopkins University Applied Physics Laboratory LLC.
