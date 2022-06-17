import subprocess
from pathlib import Path

import pytest
from gpas import data_dir, lib, misc, validation

data_dir = "tests/test-data"


# def test_gpas_uploader_validate():
#     run_cmd = run(f"gpas-upload --environment dev --json validate nanopore-fastq.csv")
#     assert (
#         '{"sample": "unpaired6", "files": ["reads/nanopore-fastq/unpaired6.fastq.gz'
#         in run_cmd.stdout
#     )


def run(cmd, cwd="./"):  # Helper for CLI testing
    return subprocess.run(
        cmd, cwd=data_dir, shell=True, check=True, text=True, capture_output=True
    )


def test_version():
    run_cmd = run("gpas --version")


def test_static_assets_exist():
    misc.parse_countries_subdivisions()
    assert misc.get_reference_path("SARS-CoV-2").exists()


def test_validate_ok():
    df, message = validation.validate(Path(data_dir) / Path("large-illumina-fastq.csv"))
    # assert message == {
    #     "validation": {
    #         "status": "success",
    #         # "schema": "PairedFastqSchema",
    #         "samples": [
    #             {
    #                 "sample_name": "cDNA-VOC-1-v4-1",
    #                 "files": [
    #                     "/Users/bede/Research/Git/gpas-cli/tests/test-data/reads/large-illumina-fastq_1.fastq.gz",
    #                     "/Users/bede/Research/Git/gpas-cli/tests/test-data/reads/large-illumina-fastq_2.fastq.gz",
    #                 ],
    #             }
    #         ],
    #     }
    # }
    # Can't do above because of absolute paths
    assert message["validation"]["status"] == "success"
    assert message["validation"]["samples"][0]["sample_name"] == "cDNA-VOC-1-v4-1"
    assert message["validation"]["samples"][0]["files"][0].endswith(
        "gpas-cli/tests/test-data/reads/large-illumina-fastq_1.fastq.gz"
    )
    assert message["validation"]["samples"][0]["files"][1].endswith(
        "gpas-cli/tests/test-data/reads/large-illumina-fastq_2.fastq.gz"
    )


def test_validate_new_cli():
    run_cmd = run("gpas validate broken/impossible-country-region.csv --json-messages")
    assert (
        """{
    "validation": {
        "status": "failure",
        "errors": [
            {
                "error": "One or more regions are not valid ISO-3166-2 subdivisions for the specified country"
            }
        ]
    }
}"""
        in run_cmd.stdout
    )
    assert run_cmd.stdout != 0


def test_validate_fail_no_tags():
    with pytest.raises(validation.ValidationError) as e:
        _, message = validation.validate(
            Path(data_dir) / Path("broken") / Path("no-tags.csv")
        )
    assert e.value.errors == [
        {"sample_name": "cDNA-VOC-1-v4-1", "error": "tags cannot be empty"}
    ]
    assert e.value.report == {
        "validation": {
            "status": "failure",
            "errors": [
                {"sample_name": "cDNA-VOC-1-v4-1", "error": "tags cannot be empty"}
            ],
        }
    }


def test_validate_fail_no_tags_colon():
    with pytest.raises(validation.ValidationError) as e:
        _, message = validation.validate(
            Path(data_dir) / Path("broken") / Path("no-tags-colon.csv")
        )
    assert e.value.errors == [
        {"sample_name": "cDNA-VOC-1-v4-1", "error": "tags cannot be empty"}
    ]


def test_validate_fail_dupe_tags():
    with pytest.raises(validation.ValidationError) as e:
        _, message = validation.validate(
            Path(data_dir) / Path("broken") / Path("dupe-tags.csv")
        )
    assert e.value.errors == [
        {"sample_name": "cDNA-VOC-1-v4-1", "error": "tags cannot be repeated"}
    ]


def test_validate_wrong_tags():
    """Should pass since tag validation does not happen offline"""
    run_cmd = run(f"gpas validate broken/wrong-tags.csv")
    assert "Validation successful" in run_cmd.stderr
    assert run_cmd.stdout == ""


def test_validate_fail_missing_files():
    # valid, schema, message = validation.validate(
    #     Path(data_dir) / Path("broken") / Path("broken-path.csv")
    # )
    # assert not valid and message == {
    #     "validation": {
    #         "status": "failure",
    #         "schema": "PairedFastqSchema",
    #         "errors": [
    #             {
    #                 "sample_name": "cDNA-VOC-1-v4-1",
    #                 "error": "fastq1 file does not exist",
    #             },
    #             {
    #                 "sample_name": "cDNA-VOC-1-v4-1",
    #                 "error": "fastq2 file does not exist",
    #             },
    #         ],
    #     }
    # }
    with pytest.raises(validation.ValidationError) as e:
        _, message = validation.validate(
            Path(data_dir) / Path("broken") / Path("broken-path.csv")
        )
    assert e.value.errors == [
        {
            "sample_name": "cDNA-VOC-1-v4-1",
            "error": "fastq1 file does not exist",
        },
        {
            "sample_name": "cDNA-VOC-1-v4-1",
            "error": "fastq2 file does not exist",
        },
    ]


def test_validate_fail_different_platforms():
    # valid, schema, message = validation.validate(
    #     Path(data_dir) / Path("broken") / Path("different-platforms.csv")
    # )
    # assert not valid and message == {
    #     "validation": {
    #         "status": "failure",
    #         "schema": "PairedFastqSchema",
    #         "errors": [
    #             {
    #                 "error": "instrument_platform must be the same for all samples in a submission"
    #             }
    #         ],
    #     }
    # }
    with pytest.raises(validation.ValidationError) as e:
        _, message = validation.validate(
            Path(data_dir) / Path("broken") / Path("different-platforms.csv")
        )
    assert e.value.errors == [
        {
            "error": "instrument_platform must be the same for all samples in a submission"
        }
    ]


def test_validate_fail_country_region():
    # valid, schema, message = validation.validate(
    #     Path(data_dir) / Path("broken") / Path("invalid-country-region.csv")
    # )
    # assert not valid and message == {
    #     "validation": {
    #         "status": "failure",
    #         "schema": "PairedFastqSchema",
    #         "errors": [
    #             {
    #                 "sample_name": "cDNA-VOC-1-v4-1",
    #                 "error": "US is not a valid ISO-3166-1 country",
    #             },
    #             {
    #                 "sample_name": "cDNA-VOC-1-v4-1",
    #                 "error": "Bretagn is not a valid ISO-3166-2 region",
    #             },
    #         ],
    #     }
    # }
    with pytest.raises(validation.ValidationError) as e:
        _, message = validation.validate(
            Path(data_dir) / Path("broken") / Path("invalid-country-region.csv")
        )
    assert e.value.errors == [
        {
            "error": "One or more regions are not valid ISO-3166-2 subdivisions for the specified country"
        },
        {
            "sample_name": "cDNA-VOC-1-v4-1",
            "error": "US is not a valid ISO-3166-1 alpha-3 country code",
        },
        {
            "sample_name": "cDNA-VOC-1-v4-1",
            "error": "Bretagn is not a valid ISO-3166-2 subdivision name",
        },
    ]


def test_validate_empty_region():
    """Empty region should be fine"""
    _, message = validation.validate(Path(data_dir) / Path("empty-region.csv"))


def test_validate_fail_select_schema():
    with pytest.raises(validation.ValidationError) as e:
        _, message = validation.validate(
            Path(data_dir) / Path("broken") / Path("no-schema.csv")
        )
    assert e.value.errors == [
        {
            "error": "could not infer schema from available columns. For FASTQ use 'fastq', for paired-end FASTQ use 'fastq1' and 'fastq2', and for BAM use 'bam'"
        }
    ]


def test_validate_fail_wrong_fastq_suffixes():
    with pytest.raises(validation.ValidationError) as e:
        _, message = validation.validate(
            Path(data_dir) / Path("broken") / Path("wrong-fastq-suffixes.csv")
        )
    assert e.value.errors == [
        {
            "sample_name": "cDNA-VOC-1-v4-1-x",
            "error": "fastq1 must end with .fastq.gz, _1.fastq.gz, _2.fastq.gz or .bam as appropriate",
        },
        {
            "sample_name": "cDNA-VOC-1-v4-1-x",
            "error": "fastq2 must end with .fastq.gz, _1.fastq.gz, _2.fastq.gz or .bam as appropriate",
        },
    ]


def test_validate_fail_wrong_instrument():
    with pytest.raises(validation.ValidationError) as e:
        _, message = validation.validate(
            Path(data_dir) / Path("broken") / Path("wrong-instrument.csv")
        )
    assert e.value.errors[0]["error"].startswith(
        "instrument_platform value 'Illuminati' is not in set"
    )


def test_decontamination():
    lib.Batch(Path(data_dir) / Path("large-nanopore-fastq.csv"))._decontaminate()
    lib.Batch(Path(data_dir) / Path("large-illumina-fastq.csv"))._decontaminate()
    lib.Batch(Path(data_dir) / Path("large-nanopore-bam.csv"))._decontaminate()
    lib.Batch(Path(data_dir) / Path("large-illumina-bam.csv"))._decontaminate()


def test_decontamination_stats():
    stdout = """Input reads file 1	5034
Input reads file 2	5034
Kept reads 1	5006
Kept reads 2	5006

"""
    assert lib.parse_decontamination_stats(stdout) == {
        "in": 10068,
        "out": 10012,
        "fraction": 0.0056,
    }


def test_upload_no_token():
    """When run without a token, upload should quit (exit code 0) after decontamination"""
    run_cmd = run("gpas upload large-nanopore-fastq.csv")
    assert "COVID_locost_2_barcode10.reads.fastq.gz" in run_cmd.stderr
