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
    valid, schema, message = lib.validate(
        Path(data_dir) / Path("large-illumina-fastq.csv")
    )
    assert valid and message == {
        "validation": {
            "status": "success",
            "schema": "PairedFastqSchema",
            "samples": [
                {
                    "sample_name": "cDNA-VOC-1-v4-1",
                    "files": [
                        "reads/large-illumina-fastq_1.fastq.gz",
                        "reads/large-illumina-fastq_2.fastq.gz",
                    ],
                }
            ],
        }
    }


def test_validate_fail_no_tags():
    valid, schema, message = lib.validate(
        Path(data_dir) / Path("broken") / Path("large-illumina-no-tags-fastq.csv")
    )
    assert not valid and message == {
        "validation": {
            "status": "failure",
            "schema": "PairedFastqSchema",
            "errors": [
                {"sample_name": "cDNA-VOC-1-v4-1", "error": "tags cannot be empty"}
            ],
        }
    }


def test_validate_fail_dupe_tags():
    valid, schema, message = lib.validate(
        Path(data_dir) / Path("broken") / Path("large-illumina-dupe-tags-fastq.csv")
    )
    assert not valid and message == {
        "validation": {
            "status": "failure",
            "schema": "PairedFastqSchema",
            "errors": [
                {"sample_name": "cDNA-VOC-1-v4-1", "error": "tags cannot be repeated"}
            ],
        }
    }


def test_validate_fail_missing_files():
    valid, schema, message = lib.validate(
        Path(data_dir) / Path("broken") / Path("broken-path.csv")
    )
    assert not valid and message == {
        "validation": {
            "status": "failure",
            "schema": "PairedFastqSchema",
            "errors": [
                {
                    "sample_name": "cDNA-VOC-1-v4-1",
                    "error": "fastq1 file does not exist",
                },
                {
                    "sample_name": "cDNA-VOC-1-v4-1",
                    "error": "fastq2 file does not exist",
                },
            ],
        }
    }


def test_validate_fail_different_platforms():
    valid, schema, message = lib.validate(
        Path(data_dir) / Path("broken") / Path("different-platforms.csv")
    )
    assert not valid and message == {
        "validation": {
            "status": "failure",
            "schema": "PairedFastqSchema",
            "errors": [
                {
                    "error": "instrument_platform must be the same for all samples in a submission"
                }
            ],
        }
    }


def test_validate_fail_country_region():
    valid, schema, message = lib.validate(
        Path(data_dir) / Path("broken") / Path("invalid-country-region.csv")
    )
    assert not valid and message == {
        "validation": {
            "status": "failure",
            "schema": "PairedFastqSchema",
            "errors": [
                {
                    "sample_name": "cDNA-VOC-1-v4-1",
                    "error": "US is not a valid ISO-3166-1 country",
                },
                {
                    "sample_name": "cDNA-VOC-1-v4-1",
                    "error": "Bretagn is not a valid ISO-3166-2 region",
                },
            ],
        }
    }


def test_decontamination():
    lib.Batch(Path(data_dir) / Path("large-nanopore-fastq.csv")).decontaminate()
    lib.Batch(Path(data_dir) / Path("large-illumina-fastq.csv")).decontaminate()
    lib.Batch(Path(data_dir) / Path("large-nanopore-bam.csv")).decontaminate()
    lib.Batch(Path(data_dir) / Path("large-illumina-bam.csv")).decontaminate()


def test_decontamination_stats():
    stdout = """Input reads file 1	5034
Input reads file 2	5034
Kept reads 1	5006
Kept reads 2	5006

"""
    assert lib.parse_decontamination_stats(stdout) == {
        "in": 10068,
        "out": 10012,
        "delta": 56,
        "fraction": 0.0056,
    }
