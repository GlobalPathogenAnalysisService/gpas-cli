import subprocess
from pathlib import Path

import pytest
from gpas import lib
from gpas.misc import ENVIRONMENTS

data_dir = "tests/test-data"


auth = lib.parse_token(Path(data_dir) / Path("token.json"))
_, _, allowed_tags = lib.fetch_user_details(auth["access_token"], ENVIRONMENTS.dev)


def run(cmd, cwd="./"):  # Helper for CLI testing
    return subprocess.run(
        cmd, cwd=data_dir, shell=True, check=True, text=True, capture_output=True
    )


def test_upload_ont_bam_dry():
    run_cmd = run(f"gpas upload --token token.json large-nanopore-bam.csv --dry-run")
    assert "INFO: Finished converting 1 samples" in run_cmd.stderr
    assert "INFO: Finished decontaminating 1 samples" in run_cmd.stderr


def test_upload_ont_bam_dry_json():
    run_cmd = run(
        f"gpas upload --token token.json large-nanopore-bam.csv --dry-run --json-messages"
    )
    assert "bam_conversion" in run_cmd.stdout
    assert "COVID_locost_2_barcode10" in run_cmd.stdout
    assert "finished" in run_cmd.stdout
    assert "decontamination" in run_cmd.stdout
