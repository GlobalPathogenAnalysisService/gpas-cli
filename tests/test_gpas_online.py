import asyncio
import gzip
import json
import subprocess
from pathlib import Path

import pytest
from gpas import lib, validation
from gpas.misc import ENVIRONMENTS

data_dir = "tests/test-data"


auth = lib.parse_token(Path(data_dir) / Path("token.json"))
_, _, allowed_tags = lib.fetch_user_details(auth["access_token"], ENVIRONMENTS.dev)


def run(cmd, cwd="./"):  # Helper for CLI testing
    return subprocess.run(
        cmd, cwd=data_dir, shell=True, check=True, text=True, capture_output=True
    )


# Requires a valid 'token.json' inside test-data. Runs on dev


def test_gpas_uploader_validate():
    run_cmd = run(
        f"gpas-upload --environment dev --token token.json --json validate nanopore-fastq.csv"
    )
    assert (
        '{"sample": "unpaired6", "files": ["reads/nanopore-fastq/unpaired6.fastq.gz'
        in run_cmd.stdout
    )


def test_dry_run():
    run_cmd = run(f"gpas upload-old nanopore-fastq.csv token.json --dry-run")
    assert "successfully decontaminated" in run_cmd.stdout
    run("rm -f sample_names* mapping*")


def test_dry_run_json():
    run_cmd = run(f"gpas upload-old --dry-run --json nanopore-fastq.csv token.json")
    assert '"file": "reads/nanopore-fastq/unpaired5.fastq.gz"' in run_cmd.stdout
    run("rm -f sample_names* mapping*")


def test_status_guids_csv():
    run_cmd = run(
        f"gpas status --guids 6e024eb1-432c-4b1b-8f57-3911fe87555f,2ddbd7d4-9979-4960-8c17-e7b92f0bf413,8daadc7d-8d58-46a6-efb4-9ddefc1e4669 --format csv token.json"
    )
    assert "6e024eb1-432c-4b1b-8f57-3911fe87555f,Unreleased" in run_cmd.stdout


def test_status_mapping_csv_json():
    run_cmd = run(
        f"gpas status --mapping-csv example.mapping.csv --format json token.json"
    )
    assert {
        "sample": "6e024eb1-432c-4b1b-8f57-3911fe87555f",
        "status": "Unreleased",
    } in json.loads(run_cmd.stdout)


def test_status_mapping_csv_csv():
    run_cmd = run(
        f"gpas status --mapping-csv example.mapping.csv --format csv token.json"
    )
    assert "6e024eb1-432c-4b1b-8f57-3911fe87555f,Unreleased" in run_cmd.stdout


def test_status_mapping_csv_table():
    run_cmd = run(
        f"gpas status --mapping-csv example.mapping.csv --format table token.json"
    )
    assert "6e024eb1-432c-4b1b-8f57-3911fe87555f Unreleased" in run_cmd.stdout


def test_status_mapping_csv_rename():
    run_cmd = run(
        f"gpas status --mapping-csv example.mapping.csv token.json --format csv --rename"
    )
    assert "test4_uploaded,Uploaded" in run_cmd.stdout


def test_gpas_uploader_download_mapping_csv():
    run_cmd = run(f"gpas download --mapping-csv example.mapping.csv token.json")
    assert Path(f"{data_dir}/6e024eb1-432c-4b1b-8f57-3911fe87555f.fasta.gz").is_file()
    run("rm -f *.fasta.gz")


def test_download_mapping_csv():
    run_cmd = run(f"gpas download --mapping-csv example.mapping.csv token.json")
    assert Path(f"{data_dir}/6e024eb1-432c-4b1b-8f57-3911fe87555f.fasta.gz").is_file()
    run("rm -f *.fasta.gz")


def test_download_mapping_csv_rename():
    run_cmd = run(
        f"gpas download --rename --mapping-csv example.mapping.csv --file-types vcf token.json"
    )
    assert Path(f"{data_dir}/test1.vcf").is_file()
    run("rm -f test*.vcf")


def test_download_guid_rename_without_mapping():
    run_cmd = run(
        f"gpas download --guids 6e024eb1-432c-4b1b-8f57-3911fe87555f --file-types vcf --rename token.json"
    )
    assert "Cannot rename outputs without mapping CSV" in run_cmd.stderr
    assert Path(f"{data_dir}/6e024eb1-432c-4b1b-8f57-3911fe87555f.vcf").is_file()
    run("rm -f 6e024eb1-432c-4b1b-8f57-3911fe87555f.vcf")


def test_download_guid_api():
    auth = lib.parse_token(Path(data_dir) / Path("token.json"))
    asyncio.run(
        lib.download_async(
            guids=["6e024eb1-432c-4b1b-8f57-3911fe87555f"],
            file_types=["vcf"],
            access_token=auth["access_token"],
            out_dir=data_dir,
        )
    )
    assert (Path(data_dir) / Path("6e024eb1-432c-4b1b-8f57-3911fe87555f.vcf")).is_file()
    run("rm -f 6e024eb1-432c-4b1b-8f57-3911fe87555f.vcf")


# Run with pytest --online tests/test_gpas_online.py::test_status_mapping_api_online
def test_status_mapping_api():
    access_token = lib.parse_token(Path(data_dir) / Path("token.json"))["access_token"]
    guids_names = lib.parse_mapping_csv(Path(data_dir) / Path("example.mapping.csv"))
    records = asyncio.run(
        lib.fetch_status_async(
            access_token=access_token,
            guids=guids_names.keys(),
            environment=ENVIRONMENTS.dev,
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


# pytest -s --online tests/test_gpas_online.py::test_status_sync_mapping
def test_status_sync_mapping():
    access_token = lib.parse_token(Path(data_dir) / Path("token.json"))["access_token"]
    guids_names = lib.parse_mapping_csv(Path(data_dir) / Path("example.mapping.csv"))
    records = lib.fetch_status(
        access_token=access_token,
        guids=guids_names.keys(),
        environment=ENVIRONMENTS.dev,
    )
    print(records)
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


# pytest -s --online tests/test_gpas_online.py::test_status_sync_mapping_rename
def test_status_sync_mapping_rename():
    access_token = lib.parse_token(Path(data_dir) / Path("token.json"))["access_token"]
    guids_names = lib.parse_mapping_csv(Path(data_dir) / Path("example.mapping.csv"))
    records = lib.fetch_status(
        access_token=access_token,
        guids=guids_names,
        environment=ENVIRONMENTS.dev,
    )
    print(records)
    assert records  # Smoke test
    passed = False
    for r in records:
        if r["sample"] == "test4_uploaded" and r["status"] == "Uploaded":
            passed = True
    if not passed:
        raise RuntimeError("Expected dict not found")


# Run with pytest --online tests/test_gpas_online.py::test_status_mapping_rename_api_online
def test_status_mapping_rename_api():
    access_token = lib.parse_token(Path(data_dir) / Path("token.json"))["access_token"]
    guids_names = lib.parse_mapping_csv(Path(data_dir) / Path("example.mapping.csv"))
    records = asyncio.run(
        lib.fetch_status_async(
            access_token=access_token,
            guids=guids_names,
            environment=ENVIRONMENTS.dev,
        )
    )
    assert records  # Smoke test
    passed = False
    for r in records:
        if r["sample"] == "test4_uploaded" and r["status"] == "Uploaded":
            passed = True
    if not passed:
        raise RuntimeError("Expected dict not found")


def test_gpas_uploader_download_mapping_rename_fasta():
    run_cmd = run(
        f"gpas download --mapping-csv example.mapping.csv --rename token.json"
    )
    with gzip.open(Path(f"{data_dir}/test1.fasta.gz"), "rt") as fh:
        assert "cdbc4af8-a75c-42ce-8fe2-8dba2ab5e839|test1" in fh.read()
    run("rm -f *.fasta.gz")


def test_check_auth_success():
    auth = lib.parse_token(Path(data_dir) / Path("token.json"))
    lib.fetch_user_details(
        access_token=auth["access_token"], environment=ENVIRONMENTS.dev
    )


def test_gpas_uploader_download_mapping_rename_fasta():
    run_cmd = run(
        f"gpas download --mapping-csv example.mapping.csv --rename token.json"
    )
    with gzip.open(Path(f"{data_dir}/test1.fasta.gz"), "rt") as fh:
        assert "cdbc4af8-a75c-42ce-8fe2-8dba2ab5e839|test1" in fh.read()
    run("rm -f *.fasta.gz")


def test_gpas_validate():
    run_cmd = run(f"gpas validate --token token.json large-nanopore-fastq.csv")
    assert '"status": "success"' in run_cmd.stdout


def test_validate_fail_wrong_tags():
    auth = lib.parse_token(Path(data_dir) / Path("token.json"))
    _, _, allowed_tags = lib.fetch_user_details(auth["access_token"], ENVIRONMENTS.dev)
    with pytest.raises(validation.ValidationError) as e:
        _, message = validation.validate(
            upload_csv=Path(data_dir) / Path("broken") / Path("wrong-tags.csv"),
            allowed_tags=allowed_tags,
        )
    assert e.value.errors == [
        {"error": "tag(s) {'heffalump'} are invalid for this organisation"}
    ]


def test_validate_fail_wrong_tags_cli():
    with pytest.raises(subprocess.CalledProcessError):
        run_cmd = run(
            f"gpas validate --token tests/test-data/token.json tests/test-data/broken/wrong-tags.csv"
        )


def test_validate_fail_no_tags():
    auth = lib.parse_token(Path(data_dir) / Path("token.json"))
    _, _, allowed_tags = lib.fetch_user_details(auth["access_token"], ENVIRONMENTS.dev)
    with pytest.raises(validation.ValidationError) as e:
        _, message = validation.validate(
            Path(data_dir) / Path("broken") / Path("no-tags.csv"),
            allowed_tags=allowed_tags,
        )
    assert e.value.errors == [
        {"sample_name": "cDNA-VOC-1-v4-1", "error": "tags cannot be empty"}
    ]


def test_validate_fail_no_tags_colon():
    auth = lib.parse_token(Path(data_dir) / Path("token.json"))
    _, _, allowed_tags = lib.fetch_user_details(auth["access_token"], ENVIRONMENTS.dev)
    with pytest.raises(validation.ValidationError) as e:
        _, message = validation.validate(
            Path(data_dir) / Path("broken") / Path("no-tags-colon.csv"),
            allowed_tags=allowed_tags,
        )
    assert e.value.errors == [
        {"sample_name": "cDNA-VOC-1-v4-1", "error": "tags cannot be empty"}
    ]


def test_auth_broken_token():
    auth = lib.parse_token(Path(data_dir) / Path("token.json"))
    _, _, allowed_tags = lib.fetch_user_details(auth["access_token"], ENVIRONMENTS.dev)
    with pytest.raises(SystemExit):
        broken_auth = lib.parse_token(
            Path(data_dir) / Path("broken") / Path("broken-token.json")
        )
        _, _, allowed_tags = lib.fetch_user_details(
            broken_auth["access_token"], ENVIRONMENTS.dev
        )


def test_ont_bam_dry():
    run_cmd = run(f"gpas upload --token token.json large-nanopore-bam.csv --dry-run")
    assert "INFO: Finished converting 1 samples" in run_cmd.stderr
    assert "INFO: Finished decontaminating 1 samples" in run_cmd.stderr


# # def test_validate():
#     run_cmd = run(f"gpas validate-old --json --token token.json nanopore-fastq.csv")
#     assert (
#         '{"sample": "unpaired6", "files": ["reads/nanopore-fastq/unpaired6.fastq.gz'
#         in run_cmd.stdout
#     )


# # def test_validate_token():
#     run_cmd = run(f"gpas validate-old --json --token token.json nanopore-fastq.csv")
#     assert (
#         '{"sample": "unpaired6", "files": ["reads/nanopore-fastq/unpaired6.fastq.gz'
#         in run_cmd.stdout
#     )


# # def test_gpas_uploader_download_mapping_csv_online_fasta():
#     run_cmd = run(
#         f"gpas-upload --json --token token.json --environment dev download example.mapping.csv --file_types fasta"
#     )
#     assert Path(f"{data_dir}/6e024eb1-432c-4b1b-8f57-3911fe87555f.fasta.gz").is_file()
#     run(
#         "rm -f 6e024eb1-432c-4b1b-8f57-3911fe87555f.fasta.gz 657a8b5a-652f-f07c-bd39-287279306a75.fasta.gz cdbc4af8-a75c-42ce-8fe2-8dba2ab5e839.fasta.gz"
#     )
