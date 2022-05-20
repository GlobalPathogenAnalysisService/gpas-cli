import subprocess

from pathlib import Path

from gpas import lib


data_dir = "tests/test-data"


def run(cmd, cwd="./"):  # Helper for CLI testing
    return subprocess.run(
        cmd, cwd=data_dir, shell=True, check=True, text=True, capture_output=True
    )


# def test_gpas_uploader_validate():
#     run_cmd = run(f"gpas-upload --environment dev --json validate nanopore-fastq.csv")
#     assert (
#         '{"sample": "unpaired6", "files": ["reads/nanopore-fastq/unpaired6.fastq.gz'
#         in run_cmd.stdout
#     )


def test_version():
    run_cmd = run("gpas --version")


def test_validate_ok():
    valid, message = lib.validate(Path(data_dir) / Path("large-illumina-fastq.csv"))
    assert valid and message == {
        "validation": {
            "status": "completed",
            "samples": [
                {
                    "sample": "cDNA-VOC-1-v4-1",
                    "files": [
                        "reads/large-illumina-fastq_1.fastq.gz",
                        "reads/large-illumina-fastq_2.fastq.gz",
                    ],
                }
            ],
        }
    }


def test_validate_fail_no_tags():
    valid, message = lib.validate(
        Path(data_dir) / Path("broken") / Path("large-illumina-no-tags-fastq.csv")
    )
    assert not valid and message == {
        "validation": {
            "status": "failure",
            "samples": [
                {"sample_name": "cDNA-VOC-1-v4-1", "error": "tags cannot be empty"}
            ],
        }
    }


def test_validate_fail_dupe_tags():
    valid, message = lib.validate(
        Path(data_dir) / Path("broken") / Path("large-illumina-dupe-tags-fastq.csv")
    )
    assert not valid and message == {
        "validation": {
            "status": "failure",
            "samples": [
                {"sample_name": "cDNA-VOC-1-v4-1", "error": "tags cannot be repeated"}
            ],
        }
    }
