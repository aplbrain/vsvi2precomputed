# vsvi2precomputed
![Logo](logo.png)
Package for converting VSVI (used in VAST) image datasets to precomputed volumes. Supports conversion of local and AWS S3 datasets.

Requirements:
* Python
* AWS CLI (if using S3)

## Usage

Convert a cloud dataset and store in new cloud path:
```
pip install -r requirements.txt
python vsvi2precomputed.py -i s3://path/to/config.vsvi -o s3://path/to/output/dir/
```
Don't forget the trailing slash on the output dir.

Convert a local dataset and upload to the cloud:
```
python vsvi2precomputed.py --i path/to/config.vsvi --o s3://path/to/output/dir/
```

Convert a cloud dataset and upload to the cloud:
```
python vsvi2precomputed.py --i s3://path/to/config.vsvi --o path/to/output/dir/
```

Convert a dataset locally:
```
python vsvi2precomputed.py --i path/to/config.vsvi --o path/to/output/dir/
```

Optional Arguments

| Argument  | Description          | Default |
|-----------|----------------------|---------|
| --profile | AWS CLI profile name | default |

## Tests
```
pip install pytest
pytest
```
To use an non-default AWS CLI profile:
```
pytest --profile <profile-name>
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

We thank the Visual Computing Group at Harvard for originally building the VAST software. https://www.frontiersin.org/journals/neural-circuits/articles/10.3389/fncir.2018.00088/full 