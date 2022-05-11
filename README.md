 ![Tests](https://github.com/GenomePathogenAnalysisService/gpas-cli/actions/workflows/test.yml/badge.svg)



A **currently experimental** unified command line interface and Python API for the Global Pathogen Analysis System



**Progress**

| Subcommand        | CLI | Python API |
| ----------------- | ------- | ---------- |
| `gpas upload` | ☑️ (wraps gpas-uploader) |  |
| `gpas download` | ✅ (async refactor) |  |
| `gpas validate` | ☑️ (wraps- gpas-uploader) |  |
| `gpas status` | ✅ (async refactor) |  |



## Install

###  With `conda`


```
git clone https://github.com/GenomePathogenAnalysisService/gpas-cli.git
conda env create -f environment.yml
```

### Development

```
conda create -n gpas-cli-dev python=3.10 read-it-and-keep samtools pytest black pre-commit
conda activate gpas-cli-dev
git clone https://github.com/GenomePathogenAnalysisService/gpas-uploader
pip install -e ./gpas-uploader
git clone https://github.com/GenomePathogenAnalysisService/gpas-cli
pip install -e ./gpas-cli

# Test
cd gpas-uploader && pytest
cd ../gpas-cli && pytest

# Online tests
cd ../gpas-cli && pytest --online
```



## Command line interface

```
% gpas -h
usage: gpas [-h] [--version] {upload,validate,download,status} ...

positional arguments:
  {upload,validate,download,status}
    upload              Validate, decontaminate and upload reads to the GPAS platform
    validate            Validate an upload CSV. Validates tags remotely if supplied with an authentication token
    download            Download analytical outputs from the GPAS platform for an uploaded batch or list of samples
    status              Check the status of samples submitted to the GPAS platform

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
```


### `gpas validate`

```
% gpas validate -h
usage: gpas validate [-h] [--token TOKEN] [--environment {development,staging,production}] [--json] upload_csv

Validate an upload CSV. Validates tags remotely if supplied with an authentication token

positional arguments:
  upload_csv            Path of upload CSV

options:
  -h, --help            show this help message and exit
  --token TOKEN         Path of auth token available from GPAS Portal
                        (default: None)
  --environment {development,staging,production}
                        GPAS environment to use
                        (default: development)
  --json                Emit JSON to stdout
                        (default: False)
```


### `gpas upload`
```
% gpas upload -h
usage: gpas upload [-h] [--working-dir WORKING_DIR] [--environment {development,staging,production}] [--mapping-prefix MAPPING_PREFIX] [--threads THREADS]
                   [--dry-run] [--json]
                   upload_csv token

Validate, decontaminate and upload reads to the GPAS platform

positional arguments:
  upload_csv            Path of upload csv
  token                 Path of auth token available from GPAS Portal

options:
  -h, --help            show this help message and exit
  --working-dir WORKING_DIR
                        Path of directory in which to generate intermediate files
                        (default: /tmp)
  --environment {development,staging,production}
                        GPAS environment to use
                        (default: development)
  --mapping-prefix MAPPING_PREFIX
                        Filename prefix for mapping CSV
                        (default: mapping)
  --threads THREADS     Number of decontamination tasks to execute in parallel. 0 = auto
                        (default: 0)
  --dry-run             Skip final upload step
                        (default: False)
  --json                Emit JSON to stdout
                        (default: False)

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

### `gpas status`
```
% gpas status -h
usage: gpas status [-h] [--mapping-csv MAPPING_CSV] [--guids GUIDS] [--environment {development,staging,production}] [--format {csv,json}] token

Check the status of samples submitted to the GPAS platform

positional arguments:
  token                 Path of auth token available from GPAS Portal

options:
  -h, --help            show this help message and exit
  --mapping-csv MAPPING_CSV
                        Path of mapping CSV generated at upload time
                        (default: None)
  --guids GUIDS         Comma-separated list of GPAS sample guids
                        (default: None)
  --environment {development,staging,production}
                        GPAS environment to use
                        (default: development)
  --format {csv,json}   Output format
                        (default: csv)
```
