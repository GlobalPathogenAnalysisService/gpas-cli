 ![Tests](https://github.com/GenomePathogenAnalysisService/gpas-cli/actions/workflows/test.yml/badge.svg) [![PyPI version](https://badge.fury.io/py/gpas.svg)](https://badge.fury.io/py/gpas)

A **currently experimental** standalone command line and Python API client for the Global Pathogen Analysis System. Tested on Linux and MacOS.

**Progress**

| Subcommand        | CLI | Python API |
| ----------------- | ------- | ---------- |
| `gpas upload` | ☑️ (wraps gpas-uploader) |  |
| `gpas download` | ✅ | ✅ `download_async()` |
| `gpas validate` | ☑️ (wraps gpas-uploader) |  |
| `gpas status` | ✅ | ✅ `get_status_async()` |



## Install

###  With `conda` (recommended)


```
conda env create -f environment.yml  # Installs from main branch
conda activate gpas-cli
```

### With `pip`

Install Samtools and [read-it-and-keep](https://github.com/GenomePathogenAnalysisService/read-it-and-keep) manually

```
pip install gpas gpas-uploader
```

## Authentication

Most gpas-cli actions require a valid API token (`token.json`). This can be saved using the 'Get API token' button on the upload page of the GPAS portal.

## Usage (command line)

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

Validate an `upload_csv`

```
gpas validate nanopore-fastq.csv
gpas validate --token token.json nanopore-fastq.csv  # Validates tags
```

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

Decontaminate upload reads specified in `upload_csv` to the GPAS platform

```
gpas upload --environment production nanopore-fastq.csv token.json
gpas upload --environment production nanopore-fastq.csv token.json --dry-run  # Do not upload
```

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

Download json, fasta, vcf and/or bam outputs from the GPAS platform by passing either a `mapping_csv` generated at upload time, or a comma-separated list of guids

```
gpas download --rename --mapping-csv example.mapping.csv token.json  # Download and rename fastas
gpas download --guids 6e024eb1-432c-4b1b-8f57-3911fe87555f --file-types json,vcf token.json  # vcf and json
```

```
% gpas download -h
usage: gpas download [-h] [--mapping-csv MAPPING_CSV] [--guids GUIDS] [--environment {development,staging,production}] [--file-types FILE_TYPES]
                     [--out-dir OUT_DIR] [--rename]
                     token

Download analytical outputs from the GPAS platform for an uploaded batch or list of samples

positional arguments:
  token                 Path of auth token (available from GPAS Portal)

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
  --file-types FILE_TYPES
                        Comma separated list of outputs to download (json,fasta,bam,vcf)
                        (default: fasta)
  --out-dir OUT_DIR     Path of output directory
                        (default: /Users/bede/Research/Git/gpas-cli)
  --rename              Rename outputs using local sample names (requires --mapping-csv)
                        (default: False)
```

### `gpas status`

Check the processing status of one or more samples by passing either a `mapping_csv` generated at upload time, or a comma-separated list of guids

```
gpas status --mapping-csv example.mapping.csv token.json  # prints table to stdout
gpas status --guids 6e024eb1-432c-4b1b-8f57-3911fe87555f --format json token.json  # Prints json to stdout
```

```
% gpas status -h
usage: gpas status [-h] [--mapping-csv MAPPING_CSV] [--guids GUIDS] [--environment {development,staging,production}] [--format {table,csv,json}] [--rename]
                   [--raw]
                   token

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
  --format {table,csv,json}
                        Output format
                        (default: table)
  --rename              Use local sample names (requires --mapping-csv)
                        (default: False)
  --raw                 Emit raw response
                        (default: False)
```



## Development and testing

```
conda create -n gpas-cli-dev python=3.10 read-it-and-keep samtools pytest black pre-commit mypy
conda activate gpas-cli-dev
git clone https://github.com/GenomePathogenAnalysisService/gpas-uploader
pip install -e ./gpas-uploader
git clone https://github.com/GenomePathogenAnalysisService/gpas-cli
pip install -e ./gpas-cli

# Test
cd gpas-uploader && pytest
cd ../gpas-cli && pytest

# Online tests, requires valid token
cd ../gpas-cli && pytest --online
```
