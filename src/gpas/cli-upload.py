import argparse
import logging
from enum import Enum
from pathlib import Path

from gpas import lib
from gpas.lib import logging
from gpas.misc import DEFAULT_ENVIRONMENT, ENVIRONMENTS, jsonify_exceptions

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


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


def main():
    """Workaround for defopt problems with PyInstaller"""
    ENVIRONMENTS = Enum("Environment", dict(dev="dev", staging="staging", prod="prod"))
    parser = argparse.ArgumentParser(
        description="Alternative entry point for gpas upload",
    )
    parser.add_argument(
        "upload_csv", type=Path, metavar="upload-csv", help="path of upload csv"
    )
    parser.add_argument(
        "--token", type=Path, help="path of auth token available from GPAS Portal"
    )
    parser.add_argument(
        "--working-dir",
        type=Path,
        default=Path("/tmp"),
        help="path of directory in which to make intermediate files",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(),
        help="path of directory in which to save mapping CSV",
    )
    parser.add_argument(
        "--processes", type=int, help="number of tasks to execute in parallel. 0 = auto"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="exit before submitting files"
    )
    parser.add_argument(
        "--debug", action="store_true", help="emit verbose debug messages to stderr"
    )
    parser.add_argument(
        "--environment", type=str, default="prod", help="GPAS environment to use"
    )
    parser.add_argument(
        "--json-messages", action="store_true", help="emit JSON to stdout"
    )
    args, _ = parser.parse_known_args()
    environment = getattr(ENVIRONMENTS, args.environment)

    upload_wrapper(
        upload_csv=args.upload_csv,
        token=args.token,
        working_dir=args.working_dir,
        out_dir=args.out_dir,
        processes=args.processes,
        dry_run=args.dry_run,
        debug=args.debug,
        environment=environment,
        json_messages=args.json_messages,
    )


if __name__ == "__main__":
    main()
