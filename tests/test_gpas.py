import subprocess

from pathlib import Path


data_dir = "tests/test-data"


def run(cmd, cwd="./"):  # Helper for CLI testing
    return subprocess.run(
        cmd, cwd=data_dir, shell=True, check=True, text=True, capture_output=True
    )


def test_gpas_uploader_validate():
    run_cmd = run(f"gpas-upload --environment dev --json validate nanopore-fastq.csv")
    assert (
        '{"sample": "unpaired6", "files": ["reads/nanopore-fastq/unpaired6.fastq.gz'
        in run_cmd.stdout
    )


def test_version():
    run_cmd = run("gpas --version")
