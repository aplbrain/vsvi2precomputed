# vsvi2precomputed
![Logo](logo.png)
Package for converting VSVI (used in VAST) image datasets to precomputed volumes.

Requirements:
* Python
* AWS CLI (for now)

## Usage
```
pip install -r requirements.txt
python vsvi2precomputed.py -i s3://path/to/info.vsvi -o s3://path/to/output/dir/
```
Don't forget the trailing slash on the output dir.

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