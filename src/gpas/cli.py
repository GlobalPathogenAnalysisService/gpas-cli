import asyncio
import json
import logging
from pathlib import Path

import pandas as pd

from gpas.misc import jsonify_exceptions

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

from gpas import lib, validation
from gpas.lib import logging
from gpas.misc import (
    DEFAULT_ENVIRONMENT,
    DEFAULT_FORMAT,
    ENVIRONMENTS,
    FORMATS,
    GOOD_STATUSES,
)


def validate(
    upload_csv: Path,
    *,
    token: Path | None = None,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    json_messages: bool = False,
):
    if token:
        auth = lib.parse_token(token)
        _, _, allowed_tags = lib.fetch_user_details(auth["access_token"], environment)
    else:
        allowed_tags = []
    df, schema_name = validation.validate(upload_csv, allowed_tags)
    message = validation.build_validation_message(df, schema_name)
    if json_messages:
        print(json.dumps(message, indent=4))


def validate_wrapper(
    upload_csv: Path,
    *,
    token: Path | None = None,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    json_messages: bool = False,
):
    """
    Validate an upload CSV. Validates tags remotely if supplied with an authentication token

    :arg upload_csv: Path of upload CSV
    :arg token: Path of auth token available from GPAS Portal
    :arg environment: GPAS environment to use
    :arg json_messages: Emit JSON to stdout
    """
    jsonify_exceptions(
        validate,
        upload_csv=upload_csv,
        token=token,
        environment=environment,
        json_messages=json_messages,
    )


def upload(
    upload_csv: Path,
    *,
    token: Path | None = None,
    working_dir: Path = Path("/tmp"),
    out_dir: Path = Path(),
    processes: int = 0,
    dry_run: bool = False,
    debug: bool = False,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    json_messages: bool = False,
):
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    batch = lib.Batch(
        upload_csv,
        token=token,
        working_dir=working_dir,
        out_dir=out_dir,
        processes=processes,
        environment=environment,
        json_messages=json_messages,
    )
    batch.upload(dry_run=dry_run)


def upload_wrapper(
    upload_csv: Path,
    token: Path | None = None,
    working_dir: Path = Path("/tmp"),
    out_dir: Path = Path(),
    processes: int = 0,
    dry_run: bool = False,
    debug: bool = False,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    json_messages: bool = False,
):
    """
    Validate, decontaminate and upload reads to the GPAS platform

    :arg upload_csv: Path of upload csv
    :arg token: Path of auth token available from GPAS Portal
    :arg working_dir: Path of directory in which to make intermediate files
    :arg out_dir: Path of directory in which to save mapping CSV
    :arg processes: Number of tasks to execute in parallel. 0 = auto
    :arg dry_run: Exit before submitting files
    :arg debug: Emit verbose debug messages
    :arg environment: GPAS environment to use
    :arg json_messages: Emit JSON to stdout
    """
    jsonify_exceptions(
        upload,
        upload_csv=upload_csv,
        token=token,
        working_dir=working_dir,
        out_dir=out_dir,
        processes=processes,
        dry_run=dry_run,
        debug=debug,
        environment=environment,
        json_messages=json_messages,
    )


def status(
    token: Path,
    *,
    mapping_csv: Path | None = None,
    guids: str = "",
    format: FORMATS = DEFAULT_FORMAT,
    rename: bool = False,
    raw: bool = False,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
):
    """
    Check the status of samples submitted to the GPAS platform

    :arg token: Path of auth token available from GPAS Portal
    :arg mapping_csv: Path of mapping CSV generated at upload time
    :arg guids: Comma-separated list of GPAS sample guids
    :arg format: Output format
    :arg rename: Use local sample names (requires --mapping-csv)
    :arg raw: Emit raw response
    :arg environment: GPAS environment to use
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
            raw=raw,
            environment=environment,
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
    file_types: str = "fasta",
    out_dir: Path = Path.cwd(),
    rename: bool = False,
    debug: bool = False,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
):
    """
    Download analytical outputs from the GPAS platform for given a mapping csv or list of guids

    :arg token: Path of auth token (available from GPAS Portal)
    :arg mapping_csv: Path of mapping CSV generated at upload time
    :arg guids: Comma-separated list of GPAS sample guids
    :arg file_types: Comma separated list of outputs to download (json,fasta,bam,vcf)
    :arg out_dir: Path of output directory
    :arg rename: Rename outputs using local sample names (requires --mapping-csv)
    :arg debug: Emit verbose debug messages
    :arg environment: GPAS environment to use
    """
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
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
            warn=True,
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


def main():
    import defopt

    defopt.run(
        {
            "validate": validate_wrapper,
            "upload": upload_wrapper,
            "status": status,
            "download": download,
        },
        no_negated_flags=True,
        strict_kwonly=False,
        short={},
    )


if __name__ == "__main__":
    main()
