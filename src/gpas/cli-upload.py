import argparse
from enum import Enum
from pathlib import Path

from gpas import cli


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
    # parser.add_argument('--processes', type=int, help='number of tasks to execute in parallel. 0 = auto')
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

    cli.upload_wrapper(
        upload_csv=args.upload_csv,
        token=args.token,
        working_dir=args.working_dir,
        out_dir=args.out_dir,
        processes=1,
        dry_run=args.dry_run,
        debug=args.debug,
        environment=environment,
        json_messages=args.json_messages,
    )


if __name__ == "__main__":
    main()
