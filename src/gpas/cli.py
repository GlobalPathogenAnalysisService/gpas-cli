import json
import asyncio
import logging

from pathlib import Path

import defopt

import pandas as pd

from gpas import lib
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
logger.setLevel(logging.WARNING)


def upload(
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
        logging.info(f"Upload successful. Command: {cmd}")
        stdout = run_cmd.stdout.strip()
        print(stdout)
    else:
        logging.info(
            f"Upload failed with exit code {run_cmd.returncode}. Command: {cmd}"
        )


def validate(
    upload_csv: Path,
    *,
    token: Path = None,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    json: bool = False,
):
    """
    Validate an upload CSV. Validates tags remotely if supplied with an authentication token

    :arg upload_csv: Path of upload CSV
    :arg token: Path of auth token available from GPAS Portal
    :arg environment: GPAS environment to use
    :arg json: Emit JSON to stdout
    """
    if not upload_csv.is_file():
        raise RuntimeError(f"Upload CSV not found: {upload_csv}")

    json_flag = "--json" if json else ""
    if token:
        cmd = f"gpas-upload --environment {environment.value} --token {token} {json_flag} validate {upload_csv}"
    else:
        cmd = f"gpas-upload --environment {environment.value} {json_flag} validate {upload_csv}"

    logging.info(f"Validate command: {cmd}")

    run_cmd = run(cmd)
    if run_cmd.returncode == 0:
        stdout = run_cmd.stdout.strip()
        print(stdout)
    else:
        raise RuntimeError(f"{run_cmd.stdout} {run_cmd.stderr}")


def download(
    token: Path,
    mapping_csv: Path = None,
    guids: str = None,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    file_types: str = "fasta",
    out_dir: Path = Path.cwd(),
    rename: bool = False,
):
    """
    Download analytical outputs from the GPAS platform for an uploaded batch or list of samples

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
        logging.info(f"Using samples in {mapping_csv}")
        mapping_df = lib.parse_mapping(mapping_csv)
        guids_ = mapping_df["gpas_sample_name"].tolist()
    elif guids:
        logging.info(f"Using samples {guids}")
        guids_ = guids.strip(",").split(",") if guids else None
    else:
        raise RuntimeError("Neither a mapping csv nor guids were specified")

    if rename:
        if mapping_csv and "local_sample_name" in mapping_df.columns:
            guids_names = mapping_df.set_index("gpas_sample_name")[
                "local_sample_name"
            ].to_dict()
        else:
            guids_names = None
            logging.warning(
                "Samples not renamed since a valid mapping csv was not specified"
            )
    else:
        guids_names = None

    status_records = asyncio.run(
        lib.get_status_async(
            auth["access_token"],
            mapping_csv,
            guids_,
            environment,
        )
    )

    downloadable_guids = [
        r.get("sample") for r in status_records if r.get("status") in GOOD_STATUSES
    ]

    asyncio.run(
        lib.download_async(
            downloadable_guids,
            file_types_fmt,
            auth["access_token"],
            environment,
            out_dir,
            guids_names,
        )
    )


def status(
    token: Path,
    *,
    mapping_csv: Path = None,
    guids: str = None,
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
    guids_ = guids.strip(",").split(",") if guids else []
    records = asyncio.run(
        lib.get_status_async(
            auth["access_token"],
            mapping_csv,
            guids_,
            environment,
            rename,
            raw,
        )
    )

    if raw or format.value == "json":
        records_fmt = json.dumps(records)
    elif format.value == "table":
        records_fmt = pd.DataFrame(records).to_string(index=False)
    elif format.value == "csv":
        records_fmt = pd.DataFrame(records).to_csv(index=False).strip()

    print(records_fmt)


def validate_new(
    upload_csv: Path,
    *,
    token: Path = None,
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
    valid, details = lib.validate(upload_csv)
    print(json.dumps(details, indent=4))


def main():
    defopt.run(
        {
            "upload": upload,
            "validate": validate,
            "download": download,
            "status": status,
            "validate-new": validate_new,
        },
        no_negated_flags=True,
        strict_kwonly=False,
        short={},
    )
