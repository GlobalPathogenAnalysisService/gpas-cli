



A command line interface and Python library for the Global Pathogen Analysis System. Currently a CLI skeleton.



## Install (development)

```
# From a Python >= 3.10 venv
git clone https://github.com/GenomePathogenAnalysisService/gpas
cd gpas
pip install -e .
pytest
```



## CLI 

```
% gpas -h
usage: gpas [-h] [--version] {upload,validate,download} ...

positional arguments:
  {upload,validate,download}
    upload              Upload reads to the GPAS platform
    validate            Validate an upload CSV. Validates tags remotely if supplied with an auth token
    download            Download analytical outputs from the GPAS platform for an uploaded batch or list of samples

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
```

### `gpas upload`

```
% gpas upload -h
usage: gpas upload [-h] --upload-csv UPLOAD_CSV --token TOKEN [--working-dir WORKING_DIR] [--environment {dev,staging,prod}]
                   [--mapping-prefix MAPPING_PREFIX] [--threads THREADS] [--dry-run]

Upload reads to the GPAS platform

options:
  -h, --help            show this help message and exit
  --upload-csv UPLOAD_CSV
                        Path of upload csv
  --token TOKEN         Path of auth token. Available from GPAS Portal
  --working-dir WORKING_DIR
                        Path of directory in which to generate intermediate files
                        (default: /tmp)
  --environment {dev,staging,prod}
                        GPAS environment to use
                        (default: dev)
  --mapping-prefix MAPPING_PREFIX
                        Filename prefix for mapping CSV
                        (default: )
  --threads THREADS     Number of decontamination tasks to execute in parallel. 0 = auto
                        (default: 0)
  --dry-run             Skip final upload step
                        (default: False)
```



### `gpas validate`

```
% gpas validate -h
usage: gpas validate [-h] --upload-csv UPLOAD_CSV [--token TOKEN] [--environment {dev,staging,prod}]

Validate an upload CSV. Validates tags remotely if supplied with an auth token

options:
  -h, --help            show this help message and exit
  --upload-csv UPLOAD_CSV
                        Path of upload CSV
  --token TOKEN         Path of auth token. Available from GPAS Portal
                        (default: None)
  --environment {dev,staging,prod}
                        GPAS environment to use
                        (default: dev)
```



### `gpas download`

```
% gpas download -h
usage: gpas download [-h] --token TOKEN [--mapping-csv MAPPING_CSV] [--guids GUIDS] [--environment {dev,staging,prod}] [--outputs OUTPUTS]
                     [--output-dir OUTPUT_DIR] [--rename]

Download analytical outputs from the GPAS platform for an uploaded batch or list of samples

options:
  -h, --help            show this help message and exit
  --token TOKEN         Path of auth token (available from GPAS Portal)
  --mapping-csv MAPPING_CSV
                        Path of mapping CSV generated at upload time
                        (default: None)
  --guids GUIDS         Comma-separated list of GPAS sample guids
                        (default: None)
  --environment {dev,staging,prod}
                        GPAS environment to use
                        (default: dev)
  --outputs OUTPUTS     Comma separated list of outputs to download (e.g. json,fasta,bam,vcf)
                        (default: json)
  --output-dir OUTPUT_DIR
                        Path of output directory
                        (default: None)
  --rename              Rename outputs using original sample names (requires --mapping-csv)
                        (default: False)
```

