import json
import subprocess

from pathlib import Path

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
    assert '{"sample": "unpaired6", "files": ["reads/nanopore-fastq/unpaired6.fastq.gz' in run_cmd.stdout

def test_validate():
    run_cmd = run(f'gpas validate --json --token token.json nanopore-fastq.csv')
    assert '{"sample": "unpaired6", "files": ["reads/nanopore-fastq/unpaired6.fastq.gz' in run_cmd.stdout

def test_version():
    run_cmd = run('gpas --version')

# def test_exception_file_types():
#     with pytest.raises(RuntimeError) as e:
#         run_cmd = run(f'gpas download --rename --mapping-csv example.mapping.csv --file-types fastaa,vcf,club token.json')
#     assert 'Invalid file type(s)' in str(run_cmd.stderr)


# Requires a valid 'token.json' inside test-data. Runs on dev


@pytest.mark.online
def test_gpas_uploader_validate_online():
    run_cmd = run(f'gpas-upload --environment dev --token token.json --json validate nanopore-fastq.csv')
    assert '{"sample": "unpaired6", "files": ["reads/nanopore-fastq/unpaired6.fastq.gz' in run_cmd.stdout

def test_validate_token_online():
    run_cmd = run(f'gpas validate --json --token token.json nanopore-fastq.csv')
    assert '{"sample": "unpaired6", "files": ["reads/nanopore-fastq/unpaired6.fastq.gz' in run_cmd.stdout

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
    run_cmd = run(f'gpas status --mapping-csv example.mapping.csv --format json token.json')
    assert '{"sample": "6e024eb1-432c-4b1b-8f57-3911fe87555f", "status": "Unreleased"}' in run_cmd.stdout

@pytest.mark.online
def test_status_mapping_csv_csv_online():
    run_cmd = run(f'gpas status --mapping-csv example.mapping.csv --format csv token.json')
    assert '6e024eb1-432c-4b1b-8f57-3911fe87555f,Unreleased' in run_cmd.stdout

@pytest.mark.online
def test_status_mapping_csv_table_online():
    run_cmd = run(f'gpas status --mapping-csv example.mapping.csv --format table token.json')
    assert '6e024eb1-432c-4b1b-8f57-3911fe87555f Unreleased' in run_cmd.stdout

@pytest.mark.online
def test_gpas_uploader_download_mapping_csv_online():
    run_cmd = run(f'gpas-upload --json --token token.json --environment dev download example.mapping.csv --file_types fasta')
    assert Path(f'{data_dir}/2ddbd7d4-9979-4960-8c17-e7b92f0bf413.fasta.gz').is_file()
    run('rm -f 2ddbd7d4-9979-4960-8c17-e7b92f0bf413.fasta.gz 6e024eb1-432c-4b1b-8f57-3911fe87555f.fasta.gz')

@pytest.mark.online
def test_gpas_uploader_download_mapping_csv_online():
    run_cmd = run(f'gpas download --mapping-csv example.mapping.csv token.json')
    assert Path(f'{data_dir}/2ddbd7d4-9979-4960-8c17-e7b92f0bf413.fasta.gz').is_file()
    run('rm -f 2ddbd7d4-9979-4960-8c17-e7b92f0bf413.fasta.gz 6e024eb1-432c-4b1b-8f57-3911fe87555f.fasta.gz')

@pytest.mark.online
def test_download_mapping_csv_online():
    run_cmd = run(f'gpas download --mapping-csv example.mapping.csv token.json')
    assert Path(f'{data_dir}/2ddbd7d4-9979-4960-8c17-e7b92f0bf413.fasta.gz').is_file()
    run('rm -f 2ddbd7d4-9979-4960-8c17-e7b92f0bf413.fasta.gz 6e024eb1-432c-4b1b-8f57-3911fe87555f.fasta.gz')

@pytest.mark.online
def test_download_mapping_csv_rename_online():
    run_cmd = run(f'gpas download --rename --mapping-csv example.mapping.csv --file-types vcf token.json')
    assert Path(f'{data_dir}/test1.vcf').is_file()
    run('rm -f test*.vcf')

