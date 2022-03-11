import subprocess

import pytest


data_dir = 'tests/test-data'


def run(cmd, cwd='./'):  # Helper for CLI testing
    return subprocess.run(cmd,
                          cwd=data_dir,
                          shell=True,
                          check=True,
                          universal_newlines=True,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)


def test_validate():
    run_cmd = run('gpas-cli validate nanopore-fastq-upload.csv')
    assert 'Validation passed' in run_cmd.stdout

def test_version():
    run_cmd = run('gpas-cli version')
