import subprocess
from pathlib import Path

import pandas as pd
import pytest
from gpas import lib
from gpas.misc import ENVIRONMENTS

data_dir = "tests/test-data"


def run(cmd, cwd="./"):  # Helper for CLI testing
    return subprocess.run(
        cmd, cwd=data_dir, shell=True, check=True, text=True, capture_output=True
    )


auth = lib.parse_token(Path(data_dir) / Path("token.json"))
auth_result = lib.fetch_user_details(auth["access_token"], ENVIRONMENTS.dev)
_, _, permitted_tags, _ = lib.parse_user_details(auth_result)


def test_upload_ont_bam_dry():
    run_cmd = run(
        f"gpas upload --environment dev --token token.json large-nanopore-bam.csv --dry-run"
    )
    assert "INFO: Finished converting 1 sample(s)" in run_cmd.stderr
    assert "INFO: Finished decontaminating 1 sample(s)" in run_cmd.stderr
    batch_guid = str(next(Path(data_dir).glob("*.mapping.csv"))).partition(".")[0]
    run(f"rm -f {batch_guid}.mapping.csv")


def test_upload_ont_bam_dry_json_and_mapping_csv():
    run_cmd = run(
        f"gpas upload --environment dev --token token.json large-nanopore-bam.csv --dry-run --json-messages"
    )
    assert "bam_conversion" in run_cmd.stdout
    assert "COVID_locost_2_barcode10" in run_cmd.stdout
    assert "finished" in run_cmd.stdout
    assert "decontamination" in run_cmd.stdout

    # Check all mapping CSV fields except for server side names
    batch_guid = str(next(Path(data_dir).glob("*.mapping.csv"))).partition(".")[0]
    records = pd.read_csv(Path(f"{batch_guid}.mapping.csv"), dtype=str).to_dict(
        "records"
    )
    assert records[0]["local_batch"] == "run1"
    assert records[0]["local_run_number"] == "run1.1"
    assert records[0]["local_sample_name"] == "COVID_locost_2_barcode10"
    assert records[0]["gpas_run_number"] == "1"
    run(f"rm -f {batch_guid}.mapping.csv")


def test_upload_dry_working_dir():
    run_cmd = run(
        f"gpas upload --environment dev --token token.json large-illumina-bam.csv --dry-run --working-dir temp"
    )
    assert (data_dir / Path("temp") / Path("cDNA-VOC-1-v4-1_2.fastq.gz")).exists()
    run(f"rm -rf temp *.mapping.csv")


def test_upload_action_level_messages():
    run_cmd = run(
        f"gpas upload --environment dev --token token.json --dry-run --json-messages --processes 1 large-illumina-bam.csv"
    )
    assert (
        """{
    "progress": {
        "action": "bam_conversion",
        "status": "started"
    }
}
{
    "progress": {
        "action": "bam_conversion",
        "status": "started",
        "sample": "cDNA-VOC-1-v4-1"
    }
}
{
    "progress": {
        "action": "bam_conversion",
        "status": "finished",
        "sample": "cDNA-VOC-1-v4-1"
    }
}
{
    "progress": {
        "action": "bam_conversion",
        "status": "finished"
    }
}
{
    "progress": {
        "action": "decontamination",
        "status": "started"
    }
}
{
    "progress": {
        "action": "decontamination",
        "status": "started",
        "sample": "cDNA-VOC-1-v4-1"
    }
}
{
    "progress": {
        "action": "decontamination",
        "status": "finished",
        "sample": "cDNA-VOC-1-v4-1"
    }
}
{
    "progress": {
        "action": "decontamination",
        "status": "finished"
    }
}"""
        in run_cmd.stdout
    )
    run(f"rm -rf *.mapping.csv")


def test_upload_empty():
    run_cmd = run(
        f"gpas upload --environment dev --token token.json empty-fastq.csv --dry-run"
    )
    assert "INFO: Finished decontaminating 1 sample(s)" in run_cmd.stderr
    batch_guid = run_cmd.stderr.partition("saved to ")[2].partition(".mapping.csv")[0]
    run(f"rm -f {batch_guid}.mapping.csv")


def test_upload_empty_paired():
    run_cmd = run(
        f"gpas upload --environment dev --token token.json empty-paired-fastq.csv --dry-run"
    )
    assert "INFO: Finished decontaminating 1 sample(s)" in run_cmd.stderr
    batch_guid = run_cmd.stderr.partition("saved to ")[2].partition(".mapping.csv")[0]
    run(f"rm -f {batch_guid}.mapping.csv")


def test_upload_empty_after_decontamination():
    run_cmd = run(
        f"gpas upload --environment dev --token token.json empty-after-decontamination-fastq.csv --dry-run --connections 1"
    )
    assert "INFO: Finished decontaminating 1 sample(s)" in run_cmd.stderr
    batch_guid = run_cmd.stderr.partition("saved to ")[2].partition(".mapping.csv")[0]
    run(f"rm -f {batch_guid}.mapping.csv")


def test_upload_empty_after_decontamination_paired():
    run_cmd = run(
        f"gpas upload --environment dev --token token.json empty-after-decontamination-paired-fastq.csv --dry-run --connections 2"
    )
    assert "INFO: Finished decontaminating 1 sample(s)" in run_cmd.stderr
    batch_guid = run_cmd.stderr.partition("saved to ")[2].partition(".mapping.csv")[0]
    run(f"rm -f {batch_guid}.mapping.csv")
