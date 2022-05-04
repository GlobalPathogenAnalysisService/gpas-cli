import logging

from enum import Enum
from pathlib import Path

import defopt

from gpas import util


logging.basicConfig(level=logging.INFO)

OUTPUT_TYPES = Enum('OutputType', dict(json='json', fasta='fasta', bam='bam', vcf='vcf'))
ENVIRONMENTS = Enum('Environment', dict(development='dev', staging='staging', production='prod'))
DEFAULT_ENVIRONMENT = ENVIRONMENTS.development


def upload(
    upload_csv: Path,
    token: Path,
    *,
    working_dir: Path = Path('/tmp'),
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    threads: int = 0,
    dry_run: bool = False,
    json: bool = False):
    '''
    Validate, decontaminate and upload reads to the GPAS platform

    :arg upload_csv: Path of upload csv 
    :arg token: Path of auth token available from GPAS Portal
    :arg working_dir: Path of directory in which to generate intermediate files
    :arg environment: GPAS environment to use
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
    passed_msg = '--> All samples have been successfully decontaminated'

    if dry_run:
        cmd = f'gpas-upload --environment {environment.value} --token {token} {flags_fmt} decontaminate {upload_csv} --dir {working_dir}'
    else:
        cmd = f'gpas-upload --environment {environment.value} --token {token} {flags_fmt} submit {upload_csv} --dir {working_dir}'
    
    logging.info(f'Upload command: {cmd}')
    run_cmd = util.run(cmd)
    if run_cmd.returncode == 0:
        logging.info(f'Upload successful')
        stdout = run_cmd.stdout.strip()
        if passed_msg in stdout:
            print(f'Validation passed: {upload_csv}')
        else:
            print(stdout)  # JSON
    else:
        logging.info(f'Upload failed with exit code {run_cmd.returncode}). Command: {cmd}')



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
    passed_msg = '--> All preliminary checks pass and this upload CSV can be passed to the GPAS upload app'
    if token:
        cmd = f'gpas-upload --environment {environment.value} --token {token} {json_flag} validate {upload_csv}'
    else:
        cmd = f'gpas-upload --environment {environment.value} {json_flag} validate {upload_csv}'

    logging.info(f'Validate command: {cmd}')

    run_cmd = util.run(cmd)
    if run_cmd.returncode == 0:
        stdout = run_cmd.stdout.strip()
        if passed_msg in stdout:
            print(f'Validation passed: {upload_csv}')
        else:
            print(stdout)  # JSON
    else:
        raise RuntimeError(f'{run_cmd.stdout} {run_cmd.stderr}')
 

def download(
    *,
    token: Path,
    mapping_csv: Path = None,
    guids: str = None,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
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
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT):
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
