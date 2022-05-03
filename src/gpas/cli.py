from enum import Enum
from pathlib import Path

import defopt

import gpas.util


GPAS_ENVIRONMENTS = Enum('Environment', dict(DEV='development', STAGE='staging', PROD='production'))
OUTPUT_TYPES = Enum('OutputType', dict(JSON='json', FASTA='fasta', BAM='bam', VCF='vcf'))

OUTPUT_TYPES = ('json', 'fasta', 'bam', 'vcf')

def upload(
    upload_csv: Path,
    token: Path,
    *,
    working_dir: Path = Path('/tmp'),
    environment: GPAS_ENVIRONMENTS = GPAS_ENVIRONMENTS.DEV,
    mapping_prefix: str = '',
    threads: int = 0):
    '''
    Upload reads to the GPAS platform

    :arg upload_csv: Path of upload csv 
    :arg token: Path of auth token available from GPAS Portal
    :arg working_dir: Path of directory in which to generate intermediate files
    :arg environment: GPAS environment to use
    :arg mapping_prefix: Filename prefix for mapping CSV
    :arg threads: Number of decontamination tasks to execute in parallel. 0 = auto
    :arg dry_run: Skip final upload step
    '''
    pass


def validate(
    upload_csv: Path,
    *,
    token: Path = None,
    environment: GPAS_ENVIRONMENTS = GPAS_ENVIRONMENTS.DEV):
    '''
    Validate an upload CSV. Validates tags remotely if supplied with an auth token

    :arg upload_csv: Path of upload CSV 
    :arg token: Path of auth token. Available from GPAS Portal
    :arg environment: GPAS environment to use
    '''
    if gpas.util.validate(upload_csv):
        print('Validation passed')
    else:
        print('Validation failed')


def download(
    *,
    token: Path,
    mapping_csv: Path = None,
    guids: str = None,
    environment: GPAS_ENVIRONMENTS = GPAS_ENVIRONMENTS.DEV,
    outputs: str = 'json',
    output_dir: Path = None,
    rename: bool = False):
    '''
    Download analytical outputs from the GPAS platform for an uploaded batch or list of samples

    :arg token: Path of auth token (available from GPAS Portal)
    :arg mapping_csv: Path of mapping CSV generated at upload time
    :arg guids: Comma-separated list of GPAS sample guids
    :arg environment: GPAS environment to use
    :arg outputs: Comma separated list of outputs to download (e.g. json,fasta,bam,vcf)
    :arg output_dir: Path of output directory
    :arg rename: Rename outputs using original sample names (requires --mapping-csv)
    '''
    guids_fmt = guids.strip(',').split(',') if guids else None
    outputs_fmt = outputs.strip(',').split(',') if outputs else None


    print(guids_fmt)
    print(outputs_fmt)


    if mapping_csv and not mapping_csv.is_file():
        raise RuntimeError('Mapping CSV not found: ' + str(mapping_csv.resolve()))

    unexpected_types = set(outputs_fmt) - set(OUTPUT_TYPES)
    if unexpected_types:
        raise RuntimeError('Unexpected outputs: ' + str(unexpected_types))


def status(
    *,
    mapping_csv: Path = None,
    guids: str = None,
    environment: GPAS_ENVIRONMENTS = GPAS_ENVIRONMENTS.DEV):
    guids_fmt = guids.strip(',').split(',') if guids else None

    if mapping_csv:
        print(f'Checking status of mappings {mapping_csv}')
    elif guids:
        print(f'Checking status of guids {guids}')
    else:
        raise RuntimeError('Neither a mapping csv nor guids were specified')  



def main():
    defopt.run({
        'upload': upload,
        'validate': validate,
        'download': download,
        'status': status
    },
    no_negated_flags=True,
    strict_kwonly=False,
    short={})
