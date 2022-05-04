import json
import subprocess

import pytest


data_dir = 'tests/test-data'

def run(cmd, cwd='./'):  # Helper for CLI testing
    return subprocess.run(cmd,
                          cwd=data_dir,
                          shell=True,
                          check=True,
                          text=True,
                          capture_output=True)


def test_gpas_uploader_validate():
    run_cmd = run(f'gpas-upload --environment dev --json validate nanopore-fastq.csv')
    assert '{"sample": "unpaired6", "files": ["nanopore-fastq/unpaired6.fastq.gz' in run_cmd.stdout


def test_validate():
    run_cmd = run(f'gpas validate --json --token token.json nanopore-fastq.csv')
    assert '{"sample": "unpaired6", "files": ["nanopore-fastq/unpaired6.fastq.gz' in run_cmd.stdout

def test_version():
    run_cmd = run('gpas --version')


# Requires a valid 'token.json' inside test-data. Runs on dev


@pytest.mark.online
def test_gpas_uploader_validate_online():
    run_cmd = run(f'gpas-upload --environment dev --token token.json --json validate nanopore-fastq.csv')
    assert '{"sample": "unpaired6", "files": ["nanopore-fastq/unpaired6.fastq.gz' in run_cmd.stdout

def test_validate_token_online():
    run_cmd = run(f'gpas validate --json --token token.json nanopore-fastq.csv')
    assert '{"sample": "unpaired6", "files": ["nanopore-fastq/unpaired6.fastq.gz' in run_cmd.stdout

@pytest.mark.online
def test_upload_dry_run_online():
    run_cmd = run(f'gpas upload nanopore-fastq.csv token.json --dry-run')
    assert 'Upload successful' in run_cmd.stderr  # Logging INFO
    run('rm -f sample_names* mapping*')

@pytest.mark.online
def test_upload_dry_run_json_online():
    run_cmd = run(f'gpas upload nanopore-fastq.csv token.json --dry-run --json')
    assert '{"submission": {"status": "completed"' in run_cmd.stdout
    run('rm -f sample_names* mapping*')

@pytest.mark.online
def test_status_guids_json_online():
    run_cmd = run(f'gpas status --guids 6e024eb1-432c-4b1b-8f57-3911fe87555f,2ddbd7d4-9979-4960-8c17-e7b92f0bf413,8daadc7d-8d58-46a6-efb4-9ddefc1e4669 --format json token.json')
    assert '{"sample": "6e024eb1-432c-4b1b-8f57-3911fe87555f", "status": "Unreleased"}' in run_cmd.stdout

@pytest.mark.online
def test_status_mapping_csv_json_online():
    run_cmd = run(f'gpas status --mapping-csv demo_sample_names.csv --format json token.json')
    assert '{"sample": "6e024eb1-432c-4b1b-8f57-3911fe87555f", "status": "Unreleased"}' in run_cmd.stdout
    # JSON format currently deviates between guid and mapping CSV options