import sys
import json
import logging

from pathlib import Path

import defopt
import requests

from gpas.misc import run, FORMATS, ENVIRONMENTS, DEFAULT_ENVIRONMENT, ENDPOINTS

import gpas_uploader



logging.basicConfig(level=logging.INFO)



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
    
    logging.info(f'Upload command: {cmd}')
    run_cmd = run(cmd)
    if run_cmd.returncode == 0:
        logging.info(f'Upload successful')
        stdout = run_cmd.stdout.strip()
        print(stdout)
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
    token: Path,
    *,
    mapping_csv: Path = None,
    guids: str = None,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    format: FORMATS = FORMATS.csv):
    '''
    Check the status of samples submitted to the GPAS platform

    :arg token: Path of auth token available from GPAS Portal
    :arg mapping_csv: Path of mapping CSV generated at upload time
    :arg guids: Comma-separated list of GPAS sample guids
    :arg environment: GPAS environment to use
    :arg format: Output format
    '''
        
    if mapping_csv:
        logging.info(f'Checking status of samples in {mapping_csv}')
        batch = gpas_uploader.DownloadBatch(mapping_csv='demo_sample_names.csv', token_file='token.json', environment='dev', output_json=True)
        batch.get_status()

    elif guids:
        logging.info(f'Checking status of samples in {guids}')
        guids_fmt = guids.strip(',').split(',') if guids else None
        token_contents = json.loads(token.read_text())
        headers = {'Authorization': 'Bearer ' + token_contents['access_token'], 'Content-Type': 'application/json'}
        endpoint = ENDPOINTS[environment.value]['HOST'] + ENDPOINTS[environment.value]['API_PATH'] + 'get_sample_detail/'
        records = []
        for guid in guids_fmt:
            r = requests.get(url=endpoint+guid, headers=headers)    
            if r.ok:
                records.append(dict(sample=r.json()[0].get('name'), status=r.json()[0].get('status')))
            else:
                print(f'{guid}, status:{r.status_code}', file=sys.stderr)
        if format == FORMATS.csv:
            print('sample', 'status', 'pangolin.lineage', sep=',')
            for r in records:
                print(r[0]['name'], r[0]['status'], r[0]['analysis'][0]['lineageDescription'], sep=',')
        elif format == FORMATS.json:
            print(json.dumps(records))
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
