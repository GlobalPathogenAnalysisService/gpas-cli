import json
import logging

from pathlib import Path

import defopt
import pandas as pd

from gpas import lib
from gpas.misc import run, FILE_TYPES, DISPLAY_FORMATS, ENVIRONMENTS, DEFAULT_ENVIRONMENT

import gpas_uploader


logger = logging.getLogger()
logger.setLevel(logging.WARNING)


def upload(
    upload_csv: Path,
    token: Path,
    *,
    working_dir: Path = Path('/tmp'),
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    mapping_prefix: str = 'mapping',
    threads: int = 0,
    dry_run: bool = False,
    json: bool = False):
    '''
    Validate, decontaminate and upload reads to the GPAS platform

    :arg upload_csv: Path of upload csv 
    :arg token: Path of auth token available from GPAS Portal
    :arg working_dir: Path of directory in which to generate intermediate files
    :arg environment: GPAS environment to use
    :arg mapping_prefix: Filename prefix for mapping CSV
    :arg threads: Number of decontamination tasks to execute in parallel. 0 = auto
    :arg dry_run: Skip final upload step
    :arg json: Emit JSON to stdout
    '''
    if not upload_csv.is_file():
        raise RuntimeError(f'Upload CSV not found: {upload_csv}')
    if not token.is_file():
        raise RuntimeError(f'Authentication token not found: {token}')

    flags_fmt = ' '.join([
        '--json' if json else '',
        '--parallel' if threads == 0 or threads > 1 else ''])

    if dry_run:
        cmd = f'gpas-upload --environment {environment.value} --token {token} {flags_fmt} decontaminate {upload_csv} --dir {working_dir}'
    else:
        cmd = f'gpas-upload --environment {environment.value} --token {token} {flags_fmt} submit {upload_csv} --dir {working_dir} --output_csv {mapping_prefix}.csv'
    
    run_cmd = run(cmd)
    if run_cmd.returncode == 0:
        logging.info(f'Upload successful. Command: {cmd}')
        stdout = run_cmd.stdout.strip()
        print(stdout)
    else:
        logging.info(f'Upload failed with exit code {run_cmd.returncode}. Command: {cmd}')



def validate(
    upload_csv: Path,
    *,
    token: Path = None,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    json: bool = False):
    '''
    Validate an upload CSV. Validates tags remotely if supplied with an authentication token

    :arg upload_csv: Path of upload CSV 
    :arg token: Path of auth token available from GPAS Portal
    :arg environment: GPAS environment to use
    :arg json: Emit JSON to stdout
    '''
    if not upload_csv.is_file():
        raise RuntimeError(f'Upload CSV not found: {upload_csv}')

    json_flag = '--json' if json else ''
    if token:
        cmd = f'gpas-upload --environment {environment.value} --token {token} {json_flag} validate {upload_csv}'
    else:
        cmd = f'gpas-upload --environment {environment.value} {json_flag} validate {upload_csv}'

    logging.info(f'Validate command: {cmd}')

    run_cmd = run(cmd)
    if run_cmd.returncode == 0:
        stdout = run_cmd.stdout.strip()
        print(stdout)
    else:
        raise RuntimeError(f'{run_cmd.stdout} {run_cmd.stderr}')
 

def download(
    token: Path,
    *,
    mapping_csv: Path = None,
    guids: str = None,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    file_types: str = 'fasta',
    out_dir: Path = '',
    rename: bool = False):
    '''
    Download analytical outputs from the GPAS platform for an uploaded batch or list of samples

    :arg token: Path of auth token (available from GPAS Portal)
    :arg mapping_csv: Path of mapping CSV generated at upload time
    :arg guids: Comma-separated list of GPAS sample guids
    :arg environment: GPAS environment to use
    :arg file_types: Comma separated list of outputs to download (json,fasta,bam,vcf)
    :arg out_dir: Path of output directory
    :arg rename: Rename outputs using original sample names (requires --mapping-csv)
    '''
    # gpas-upload --json --token token.json --environment dev download example.mapping.csv --file_types json fasta --rename
    file_types_fmt = set(file_types.strip(',').split(','))
    unrecognised_file_types = file_types_fmt - {t.name for t in FILE_TYPES}
    if unrecognised_file_types:
        raise RuntimeError(f'Invalid file type(s): {unrecognised_file_types}')
    logging.info(f'Retrieving file types {file_types_fmt}')
    if mapping_csv:
        logging.info(f'Using samples in {mapping_csv}')
        batch = gpas_uploader.DownloadBatch(
            mapping_csv=mapping_csv, token_file=token, environment=environment.value, output_json=False)
        batch.get_status()  # Necessary for some reason
        for file_type in file_types_fmt:
            batch.download(file_type, out_dir, rename)
    elif guids:
        logging.info(f'Using samples in {guids}')
        guids_fmt = guids.strip(',').split(',') if guids else None
        raise NotImplementedError()
    else:
        raise RuntimeError('Neither a mapping csv nor guids were specified')


def status(
    token: Path,
    *,
    mapping_csv: Path = None,
    guids: str = None,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    format: DISPLAY_FORMATS = DISPLAY_FORMATS.table,
    raw: bool = False):
    '''
    Check the status of samples submitted to the GPAS platform

    :arg token: Path of auth token available from GPAS Portal
    :arg mapping_csv: Path of mapping CSV generated at upload time
    :arg guids: Comma-separated list of GPAS sample guids
    :arg environment: GPAS environment to use
    :arg format: Output format
    :arg raw: Emit raw response
    '''
    auth = lib.parse_token(token)
    if mapping_csv:
        logging.info(f'Using samples in {mapping_csv}')
        # batch = gpas_uploader.DownloadBatch(mapping_csv, token, environment.value, False)
        # records = batch.get_status()  # get_status() modified to return dict
        guids_fmt = lib.parse_mapping(mapping_csv)
        records = lib.fetch_status(guids_fmt, auth['access_token'], environment, raw)
    elif guids:
        logging.info(f'Using samples {guids}')
        guids_fmt = guids.strip(',').split(',') if guids else None
        records = lib.fetch_status(guids_fmt, auth['access_token'], environment, raw)
    else:
        raise RuntimeError('Neither a mapping csv nor guids were specified')

    if raw or format.value == 'json':
        records_fmt = json.dumps(records)
    elif format.value == 'table':
        records_fmt = pd.DataFrame(records).to_string(index=False)
    elif format.value == 'csv':
        records_fmt = pd.DataFrame(records).to_csv(index=False).strip()

    print(records_fmt)


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
