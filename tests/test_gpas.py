import subprocess

from pathlib import Path

import pytest

from gpas import lib, validation, misc, data_dir


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


def test_static_assets_exist():
    misc.parse_countries_subdivisions()
    assert misc.get_reference_path("SARS-CoV-2").exists()


def test_validate_ok():
    df, message = lib.validate(Path(data_dir) / Path("large-illumina-fastq.csv"))
    assert message == {
        "validation": {
            "status": "success",
            # "schema": "PairedFastqSchema",
            "samples": [
                {
                    "sample_name": "cDNA-VOC-1-v4-1",
                    "files": [
                        "/Users/bede/Research/Git/gpas-cli/tests/test-data/reads/large-illumina-fastq_1.fastq.gz",
                        "/Users/bede/Research/Git/gpas-cli/tests/test-data/reads/large-illumina-fastq_2.fastq.gz",
                    ],
                }
            ],
        }
    }


def test_validate_new_cli():
    run_cmd = run(
        "gpas validate broken/impossible-country-region.csv --machine-readable"
    )
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


def test_validate_fail_no_tags():
    with pytest.raises(validation.ValidationError) as e:
        _, message = lib.validate(
            Path(data_dir) / Path("broken") / Path("large-illumina-no-tags-fastq.csv")
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


def test_validate_fail_dupe_tags():
    with pytest.raises(validation.ValidationError) as e:
        _, message = lib.validate(
            Path(data_dir) / Path("broken") / Path("large-illumina-dupe-tags-fastq.csv")
        )
    assert e.value.errors == [
        {"sample_name": "cDNA-VOC-1-v4-1", "error": "tags cannot be repeated"}
    ]


def test_validate_fail_missing_files():
    # valid, schema, message = lib.validate(
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
        _, message = lib.validate(
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
    # valid, schema, message = lib.validate(
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
        _, message = lib.validate(
            Path(data_dir) / Path("broken") / Path("different-platforms.csv")
        )
    assert e.value.errors == [
        {
            "error": "instrument_platform must be the same for all samples in a submission"
        }
    ]


def test_validate_fail_country_region():
    # valid, schema, message = lib.validate(
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
        _, message = lib.validate(
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


def test_validate_fail_select_schema():
    with pytest.raises(validation.ValidationError) as e:
        _, message = lib.validate(
            Path(data_dir) / Path("broken") / Path("no-schema.csv")
        )
    assert e.value.errors == [
        {
            "error": "Failed inferring schema from available columns. For single FASTQ use 'fastq', for paired-end FASTQ use 'fastq1' and 'fastq2', and for BAM submissions use 'bam'"
        }
    ]


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
