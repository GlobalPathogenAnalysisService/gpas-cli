import json
import asyncio
import subprocess

from pathlib import Path

import pytest

from gpas import lib

from gpas.misc import ENVIRONMENTS


data_dir = "tests/test-data"


def run(cmd, cwd="./"):  # Helper for CLI testing
    return subprocess.run(
        cmd, cwd=data_dir, shell=True, check=True, text=True, capture_output=True
    )


# Requires a valid 'token.json' inside test-data. Runs on dev


# CLI tests


@pytest.mark.online
def test_validate_online():
    run_cmd = run(f"gpas validate --json --token token.json nanopore-fastq.csv")
    assert (
        '{"sample": "unpaired6", "files": ["reads/nanopore-fastq/unpaired6.fastq.gz'
        in run_cmd.stdout
    )


@pytest.mark.online
def test_gpas_uploader_validate_online():
    run_cmd = run(
        f"gpas-upload --environment dev --token token.json --json validate nanopore-fastq.csv"
    )
    assert (
        '{"sample": "unpaired6", "files": ["reads/nanopore-fastq/unpaired6.fastq.gz'
        in run_cmd.stdout
    )


@pytest.mark.online
def test_validate_token_online():
    run_cmd = run(f"gpas validate --json --token token.json nanopore-fastq.csv")
    assert (
        '{"sample": "unpaired6", "files": ["reads/nanopore-fastq/unpaired6.fastq.gz'
        in run_cmd.stdout
    )


@pytest.mark.online
def test_upload_dry_run_online():
    run_cmd = run(f"gpas upload nanopore-fastq.csv token.json --dry-run")
    assert "successfully decontaminated" in run_cmd.stdout
    run("rm -f sample_names* mapping*")


@pytest.mark.online
def test_upload_dry_run_json_online():
    run_cmd = run(f"gpas upload --dry-run --json nanopore-fastq.csv token.json")
    assert '"file": "reads/nanopore-fastq/unpaired5.fastq.gz"' in run_cmd.stdout
    run("rm -f sample_names* mapping*")


@pytest.mark.online
def test_status_guids_json_online():
    run_cmd = run(
        f"gpas status --guids 6e024eb1-432c-4b1b-8f57-3911fe87555f,2ddbd7d4-9979-4960-8c17-e7b92f0bf413,8daadc7d-8d58-46a6-efb4-9ddefc1e4669 --format json token.json"
    )
    assert (
        '{"sample": "6e024eb1-432c-4b1b-8f57-3911fe87555f", "status": "Unreleased"}'
        in run_cmd.stdout
    )


@pytest.mark.online
def test_status_mapping_csv_json_online():
    run_cmd = run(
        f"gpas status --mapping-csv example.mapping.csv --format json token.json"
    )
    assert (
        '{"sample": "6e024eb1-432c-4b1b-8f57-3911fe87555f", "status": "Unreleased"}'
        in run_cmd.stdout
    )


@pytest.mark.online
def test_status_mapping_csv_csv_online():
    run_cmd = run(
        f"gpas status --mapping-csv example.mapping.csv --format csv token.json"
    )
    assert "6e024eb1-432c-4b1b-8f57-3911fe87555f,Unreleased" in run_cmd.stdout


@pytest.mark.online
def test_status_mapping_csv_table_online():
    run_cmd = run(
        f"gpas status --mapping-csv example.mapping.csv --format table token.json"
    )
    assert "6e024eb1-432c-4b1b-8f57-3911fe87555f Unreleased" in run_cmd.stdout


@pytest.mark.online
def test_status_mapping_csv_json_rename_online():
    run_cmd = run(
        f"gpas status --mapping-csv example.mapping.csv token.json --format json --rename"
    )
    assert '{"sample": "test4_uploaded", "status": "Uploaded"}' in run_cmd.stdout


@pytest.mark.online
def test_gpas_uploader_download_mapping_csv_online():
    run_cmd = run(
        f"gpas-upload --json --token token.json --environment dev download example.mapping.csv --file_types fasta"
    )
    assert Path(f"{data_dir}/2ddbd7d4-9979-4960-8c17-e7b92f0bf413.fasta.gz").is_file()
    run(
        "rm -f 2ddbd7d4-9979-4960-8c17-e7b92f0bf413.fasta.gz 6e024eb1-432c-4b1b-8f57-3911fe87555f.fasta.gz"
    )


@pytest.mark.online
def test_gpas_uploader_download_mapping_csv_online():
    run_cmd = run(f"gpas download --mapping-csv example.mapping.csv token.json")
    assert Path(f"{data_dir}/6e024eb1-432c-4b1b-8f57-3911fe87555f.fasta.gz").is_file()
    run("rm -f *.fasta.gz")


@pytest.mark.online
def test_download_mapping_csv_online():
    run_cmd = run(f"gpas download --mapping-csv example.mapping.csv token.json")
    assert Path(f"{data_dir}/6e024eb1-432c-4b1b-8f57-3911fe87555f.fasta.gz").is_file()
    run("rm -f *.fasta.gz")


@pytest.mark.online
def test_download_mapping_csv_rename_online():
    run_cmd = run(
        f"gpas download --rename --mapping-csv example.mapping.csv --file-types vcf token.json"
    )
    assert Path(f"{data_dir}/test1.vcf").is_file()
    run("rm -f test*.vcf")


@pytest.mark.online
def test_download_guid_rename_without_mapping():
    run_cmd = run(
        f"gpas download --guids 6e024eb1-432c-4b1b-8f57-3911fe87555f --file-types vcf --rename token.json"
    )
    assert "Samples not renamed" in run_cmd.stderr
    assert Path(f"{data_dir}/6e024eb1-432c-4b1b-8f57-3911fe87555f.vcf").is_file()
    run("rm -f 6e024eb1-432c-4b1b-8f57-3911fe87555f.vcf")


# API tests


@pytest.mark.online
def test_download_guid_api_online():
    auth = lib.parse_token(Path(data_dir) / Path("token.json"))
    asyncio.run(
        lib.async_download(
            guids=["6e024eb1-432c-4b1b-8f57-3911fe87555f"],
            file_types=["vcf"],
            access_token=auth["access_token"],
            out_dir=data_dir,
        )
    )
    assert (Path(data_dir) / Path("6e024eb1-432c-4b1b-8f57-3911fe87555f.vcf")).is_file()
    run("rm -f 6e024eb1-432c-4b1b-8f57-3911fe87555f.vcf")


# Run with pytest --online tests/test_gpas_online.py::test_status_mapping_api_online
@pytest.mark.online
def test_status_mapping_api_online():
    access_token = lib.parse_token(Path(data_dir) / Path("token.json"))["access_token"]
    records = asyncio.run(
        lib.get_status(
            access_token=access_token,
            mapping_csv=Path(data_dir) / Path("example.mapping.csv"),
            environment=ENVIRONMENTS.development,
            rename=False,
        )
    )
    assert records  # Smoke test
    passed = False
    for r in records:
        if (
            r["sample"] == "8daadc7d-8d58-46a6-efb4-9ddefc1e4669"
            and r["status"] == "Uploaded"
        ):
            passed = True
    if not passed:
        raise RuntimeError("Expected dict not found")


# Run with pytest --online tests/test_gpas_online.py::test_status_mapping_rename_api_online
@pytest.mark.online
def test_status_mapping_rename_api_online():
    access_token = lib.parse_token(Path(data_dir) / Path("token.json"))["access_token"]
    records = asyncio.run(
        lib.get_status(
            access_token=access_token,
            mapping_csv=Path(data_dir) / Path("example.mapping.csv"),
            environment=ENVIRONMENTS.development,
            rename=True,
        )
    )
    assert records  # Smoke test
    passed = False
    for r in records:
        if r["sample"] == "test4_uploaded" and r["status"] == "Uploaded":
            passed = True
    if not passed:
        raise RuntimeError("Expected dict not found")
