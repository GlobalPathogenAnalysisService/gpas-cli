import json
import asyncio
import logging

from pathlib import Path

import defopt

import pandas as pd

from gpas import lib
from gpas import validation
from gpas.misc import (
    run,
    FORMATS,
    DEFAULT_FORMAT,
    ENVIRONMENTS,
    DEFAULT_ENVIRONMENT,
    GOOD_STATUSES,
)


logger = logging.getLogger()
logging.basicConfig(format="%(levelname)s: %(message)s")
logger.setLevel(logging.INFO)


def status(
    token: Path,
    *,
    mapping_csv: Path | None = None,
    guids: str = "",
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    format: FORMATS = DEFAULT_FORMAT,
    rename: bool = False,
    raw: bool = False,
):
    """
    Check the status of samples submitted to the GPAS platform

    :arg token: Path of auth token available from GPAS Portal
    :arg mapping_csv: Path of mapping CSV generated at upload time
    :arg guids: Comma-separated list of GPAS sample guids
    :arg environment: GPAS environment to use
    :arg format: Output format
    :arg rename: Use local sample names (requires --mapping-csv)
    :arg raw: Emit raw response
    """
    auth = lib.parse_token(token)
    if mapping_csv:
        guids_ = lib.parse_mapping_csv(mapping_csv)  # dict
        if not rename:
            guids_ = guids_.keys()  # list
    elif guids:
        if rename:
            logging.warning("Cannot rename outputs without mapping CSV")
        guids_ = guids.strip(",").split(",")  # list
    else:
        raise RuntimeError("Provide either a mapping CSV or a list of guids")

    records = asyncio.run(
        lib.fetch_status_async(
            access_token=auth["access_token"],
            guids=guids_,
            environment=environment,
            raw=raw,
        )
    )

    if raw or format.value == "json":
        records_fmt = json.dumps(records, indent=4)
    elif format.value == "table":
        records_fmt = pd.DataFrame(records).to_string(index=False)
    elif format.value == "csv":
        records_fmt = pd.DataFrame(records).to_csv(index=False).strip()
    else:
        raise RuntimeError("Unknown output format")

    print(records_fmt)


def download(
    token: Path,
    mapping_csv: Path | None = None,
    guids: str = "",
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    file_types: str = "fasta",
    out_dir: Path = Path.cwd(),
    rename: bool = False,
):
    """
    Download analytical outputs from the GPAS platform for given a mapping csv or list of guids

    :arg token: Path of auth token (available from GPAS Portal)
    :arg mapping_csv: Path of mapping CSV generated at upload time
    :arg guids: Comma-separated list of GPAS sample guids
    :arg environment: GPAS environment to use
    :arg file_types: Comma separated list of outputs to download (json,fasta,bam,vcf)
    :arg out_dir: Path of output directory
    :arg rename: Rename outputs using local sample names (requires --mapping-csv)
    """
    # gpas-upload --json --token token.json --environment dev download example.mapping.csv --file_types json fasta --rename
    file_types_fmt = file_types.strip(",").split(",")
    auth = lib.parse_token(token)
    if mapping_csv:
        guids_ = lib.parse_mapping_csv(mapping_csv)  # dict
        if not rename:
            guids_ = guids_.keys()  # list
    elif guids:
        if rename:
            logging.warning("Cannot rename outputs without mapping CSV")
        guids_ = guids.strip(",").split(",")  # list
    else:
        raise RuntimeError("Provide either a mapping CSV or a list of guids")

    records = asyncio.run(
        lib.fetch_status_async(
            access_token=auth["access_token"],
            guids=guids_.keys() if type(guids_) is dict else guids_,
            environment=environment,
            raw=False,
        )
    )
    downloadable_guids = [
        r.get("sample") for r in records if r.get("status") in GOOD_STATUSES
    ]
    if rename and mapping_csv:
        downloadable_guids = {g: guids_[g] for g in downloadable_guids}

    asyncio.run(
        lib.download_async(
            access_token=auth["access_token"],
            guids=downloadable_guids,
            file_types=file_types_fmt,
            out_dir=out_dir,
            environment=environment,
        )
    )


def validate(
    upload_csv: Path,
    *,
    token: Path | None = None,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    machine_readable: bool = False,
):
    """
    Validate an upload CSV. Validates tags remotely if supplied with an authentication token

    :arg upload_csv: Path of upload CSV
    :arg token: Path of auth token available from GPAS Portal
    :arg environment: GPAS environment to use
    :arg json: Emit JSON to stdout
    """

    try:
        _, message = validation.validate(upload_csv)
        print(json.dumps(message, indent=4))
    except validation.ValidationError as e:
        if machine_readable:
            print(json.dumps(e.report, indent=4))
        else:
            raise e


def upload(
    upload_csv: Path,
    *,
    token: Path | None = None,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    machine_readable: bool = False,
):
    batch = lib.Batch(upload_csv, token=token, environment=environment)
    batch.upload()


def upload_old(
    upload_csv: Path,
    token: Path,
    *,
    working_dir: Path = Path("/tmp"),
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    mapping_prefix: str = "mapping",
    threads: int = 0,
    dry_run: bool = False,
    json: bool = False,
):
    """
    Validate, decontaminate and upload reads to the GPAS platform

    :arg upload_csv: Path of upload csv
    :arg token: Path of auth token available from GPAS Portal
    :arg working_dir: Path of directory in which to generate intermediate files
    :arg environment: GPAS environment to use
    :arg mapping_prefix: Filename prefix for mapping CSV
    :arg threads: Number of decontamination tasks to execute in parallel. 0 = auto
    :arg dry_run: Skip final upload step
    :arg json: Emit JSON to stdout
    """
    if not upload_csv.is_file():
        raise RuntimeError(f"Upload CSV not found: {upload_csv}")
    if not token.is_file():
        raise RuntimeError(f"Authentication token not found: {token}")

    flags_fmt = " ".join(
        ["--json" if json else "", "--parallel" if threads == 0 or threads > 1 else ""]
    )

    if dry_run:
        cmd = f"gpas-upload --environment {environment.value} --token {token} {flags_fmt} decontaminate {upload_csv} --dir {working_dir}"
    else:
        cmd = f"gpas-upload --environment {environment.value} --token {token} {flags_fmt} submit {upload_csv} --dir {working_dir} --output_csv {mapping_prefix}.csv"

    run_cmd = run(cmd)
    if run_cmd.returncode == 0:
        logger.info(f"Upload successful. Command: {cmd}")
        stdout = run_cmd.stdout.strip()
        print(stdout)
    else:
        logger.info(
            f"Upload failed with exit code {run_cmd.returncode}. Command: {cmd}"
        )


def main():
    defopt.run(
        {
            "status": status,
            "download": download,
            "validate": validate,
            "upload": upload,
            "upload-old": upload_old,
        },
        no_negated_flags=True,
        strict_kwonly=False,
        short={},
    )
