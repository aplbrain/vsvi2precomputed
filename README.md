# vsvi2precomputeds
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
Currently uses `default` aws profile. Need to add optional command line param.
