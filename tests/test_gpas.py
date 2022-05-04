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


def test_validate():
    run(f'gpas-upload --environment dev --json validate nanopore-fastq.csv')

def test_version():
    run_cmd = run('gpas --version')


# REQUIRE A VALID AUTH TOKEN inside online/


@pytest.mark.online
def test_validate_online():
    run(f'gpas-upload --environment dev --token token.json --json validate nanopore-fastq.csv')

@pytest.mark.online
def test_upload_dry_run_online():
    run_cmd = run(f'gpas upload nanopore-fastq.csv token.json --dry-run')
    assert 'Validation passed' in run_cmd.stdout
    run('rm sample_names*')

@pytest.mark.online
def test_upload_dry_run_json_online():
    run_cmd = run(f'gpas upload nanopore-fastq.csv token.json --dry-run --json')
    assert '{"submission": {"status": "completed"' in run_cmd.stdout
    run('rm sample_names*')
