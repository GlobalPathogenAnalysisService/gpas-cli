import datetime
import hashlib
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import traceback
from dataclasses import dataclass
from enum import Enum
from functools import partial
from multiprocessing.pool import ThreadPool
from pathlib import Path

import httpx
import pandas as pd
import tqdm
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from tqdm.contrib.logging import logging_redirect_tqdm

from gpas import data_dir, validation

FORMATS = Enum("Formats", {"table": "table", "csv": "csv", "json": "json"})
DEFAULT_FORMAT = FORMATS.table
ENVIRONMENTS = Enum("Environment", {"dev": "dev", "staging": "staging", "prod": "prod"})
DEFAULT_ENVIRONMENT = ENVIRONMENTS.prod
FILE_TYPES = Enum(
    "FileType", {"json": "json", "fasta": "fasta", "bam": "bam", "vcf": "vcf"}
)
GOOD_STATUSES = {"Unreleased", "Released"}


class AuthenticationError(Exception):
    pass


class DecontaminationError(Exception):
    pass


class SubmissionError(Exception):
    pass


class SubprocessError(Exception):
    pass


ENVIRONMENTS_URLS = {
    "dev": {
        "HOST": "https://portal.dev.gpas.ox.ac.uk",
        "API": "https://portal.dev.gpas.ox.ac.uk/ords/gpas_pub/gpasapi",
        "ORDS": "https://portal.dev.gpas.ox.ac.uk/ords/grep/electron",
    },
    "staging": {
        "HOST": "https://portal.staging.gpas.ox.ac.uk",
        "API": "https://portal.staging.gpas.ox.ac.uk/ords/gpas_pub/gpasapi",
        "ORDS": "https://portal.staging.gpas.ox.ac.uk/ords/grep/electron",
    },
    "prod": {
        "HOST": "https://portal.gpas.ox.ac.uk",
        "API": "https://portal.gpas.ox.ac.uk/ords/gpas_pub/gpasapi",
        "ORDS": "https://portal.gpas.ox.ac.uk/ords/grep/electron",
    },
}


@dataclass
class LoggedShellCommand:
    name: str
    action: str
    cmd: str


@dataclass
class SampleUpload:
    name: str
    path1: Path
    path2: str | None
    url1: str
    url2: str | None


def get_value_traceback(e: Exception) -> tuple[str, str, list]:
    """Return 3-tuple of exception, message, and traceback"""
    e_type, e_value, e_traceback = sys.exc_info()
    e_t = str(e_type)
    e_v = repr(e_value)
    e_tb = traceback.format_tb(e_traceback)
    return e_t, e_v, e_tb


def get_value_traceback_fmt(e: Exception) -> tuple[str, str, str]:
    """Return 3-tuple of exception, message, and traceback as strings"""
    e_type, e_value, e_traceback = sys.exc_info()
    e_t = str(e_type)
    e_v = repr(e_value)
    e_tb = "\n".join(traceback.format_tb(e_traceback))
    return e_t, e_v, e_tb


def jsonify_exceptions(function, **kwargs):
    """Catch exceptions and print JSON"""

    def jsonify(obj, generic=False) -> None:
        if generic:
            output = json.dumps({"error": repr(obj)}, indent=4)
        else:
            output = json.dumps(obj, indent=4)
        print(str(output), flush=True)

    if kwargs["json_messages"]:
        try:
            return function(**kwargs)
        except validation.ValidationError as e:
            jsonify(e.report)
        except Exception as e:
            e_t, e_v, e_tb = get_value_traceback(e)
            jsonify({"exception": e_v, "traceback": e_tb})
    else:
        return function(**kwargs)


def run(cmd: str) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)


def print_json(data):
    print(json.dumps(data, indent=4), flush=True)


def print_progress_message_json(action: str, status: str, sample: str = ""):
    message = {
        "progress": {
            "action": action,
            "status": status,
        }
    }
    if sample:
        message["progress"]["sample"] = sample
    print_json(message)


def run_logged(
    command: LoggedShellCommand, json_messages: bool = False
) -> subprocess.CompletedProcess:
    if json_messages:
        print_progress_message_json(
            action=command.action, status="started", sample=command.name
        )
    process = subprocess.run(command.cmd, shell=True, text=True, capture_output=True)
    logging.debug(
        f"Executed command {process.args} {process.stderr=}"
        f" {process.stdout=} {process.returncode=}"
    )
    if process.returncode != 0:
        raise SubprocessError(
            f"Failed to execute command {process.args} {process.stderr=}"
            f" {process.stdout=} {process.returncode=}"
        )
    if json_messages:
        print_progress_message_json(
            action=command.action, status="finished", sample=command.name
        )
    return process


def run_parallel_logged(
    commands: list[LoggedShellCommand],
    processes: int,
    participle: str = "processing",
    json_messages: bool = False,
) -> dict[str, subprocess.CompletedProcess]:
    if json_messages:
        print_progress_message_json(action=commands[0].action, status="started")
    names = [c.name for c in commands]
    cmds = [c.cmd for c in commands]
    logging.debug(f"Started {participle.lower()} {len(cmds)} sample(s) \n{cmds=}")
    with ThreadPool(processes) as pool:
        results = {
            n: c
            for n, c in zip(
                names,
                tqdm.tqdm(
                    pool.imap_unordered(
                        partial(run_logged, json_messages=json_messages),
                        commands,
                    ),
                    total=len(cmds),
                    desc=f"{participle} {len(cmds)} sample(s)",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
                    leave=False,
                ),
            )
        }
    if json_messages:
        print_progress_message_json(action=commands[0].action, status="finished")
    else:
        logging.info(f"Finished {participle.lower()} {len(commands)} sample(s)")
    return results


class set_directory(object):
    """
    Context manager for temporarily changing the current working directory
    """

    def __init__(self, path: Path):
        self.path = path
        self.origin = Path().absolute()

    def __enter__(self):
        os.chdir(self.path)

    def __exit__(self, *exc):
        os.chdir(self.origin)


def resolve_paths(df: pd.DataFrame) -> pd.DataFrame:
    """
    Read CSV and resolve relative paths
    """
    resolve = lambda x: Path(x).resolve()
    if "fastq" in df.columns:
        df["fastq"] = df["fastq"].apply(resolve)
    if "fastq1" in df.columns:
        df["fastq1"] = df["fastq1"].apply(resolve)
    if "fastq2" in df.columns:
        df["fastq2"] = df["fastq2"].apply(resolve)
    if "bam" in df.columns:
        df["bam"] = df["bam"].apply(resolve)
    return df


def get_binary_path(filename: str) -> str:
    env_var = f"GPAS_{filename.upper()}_PATH"
    if os.getenv(env_var) and Path(os.environ[env_var]).exists():  # Environment var
        path = Path(os.environ[env_var]).resolve()
        logging.debug(f"get_binary_path(): Environment variable mode {path=}")
    elif hasattr(sys, "_MEIPASS"):  # PyInstaller onefile
        if platform.system() == "Windows":
            path = (Path(sys.executable).parent / (filename + ".exe")).resolve()
        else:
            path = (Path(sys.executable).parent / filename).resolve()
        logging.debug(f"get_binary_path(): PyInstaller mode {path=}")
    elif shutil.which(filename):  # $PATH
        path = Path(shutil.which(filename)).resolve()
        logging.debug(f"get_binary_path(): $PATH mode {path=}")
    else:
        raise FileNotFoundError(f"Could not find {filename} binary")
    return str(path)


def hash_file(file_path: Path):
    md5 = hashlib.md5()
    with open(file_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(4096), b""):
            md5.update(chunk)
    return md5.hexdigest()


def hash_string(string: str):
    return hashlib.md5(string.encode()).hexdigest()


def get_data_path():
    env_var = "GPAS_DATA_PATH"
    if os.environ.get(env_var) and Path(os.environ[env_var]).exists():
        path = Path(os.environ[env_var]).resolve()
    elif data_dir.exists():
        path = data_dir
    elif (Path(__file__).parents[1] / "data").exists():
        path = Path(__file__).parents[1] / "data"
    else:
        logging.debug(f"{__name__=} {__file__=} {data_dir=}")
        raise FileNotFoundError(f"Could not find data directory")
    return path.resolve()


def get_reference_path(organism):
    prefix = get_data_path() / "refs"
    organisms_paths = {"SARS-CoV-2": "MN908947_no_polyA.fasta"}
    return Path(prefix / organisms_paths[organism]).resolve()


@retry(
    retry=retry_if_exception_type(httpx.HTTPError),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    stop=stop_after_attempt(5),
)
def upload_sample(upload: SampleUpload, headers: dict, json_messages: bool) -> None:
    if json_messages:
        print_progress_message_json(
            action="upload", status="started", sample=upload.name
        )
    with open(upload.path1, "rb") as fh:
        r = httpx.put(url=upload.url1, content=fh, headers=headers)
        r.raise_for_status()
    if upload.path2 and upload.url2:
        with open(upload.path2, "rb") as fh:
            r = httpx.put(url=upload.url2, content=fh, headers=headers)
            r.raise_for_status()
    logging.debug(f"Uploaded sample {upload.name}")
    if json_messages:
        print_progress_message_json(
            action="upload", status="finished", sample=upload.name
        )


def oracle_timestamp() -> str:
    current_time = (
        datetime.datetime.now(datetime.timezone.utc)
        .astimezone()
        .isoformat(timespec="milliseconds")
    )
    tz_start_index = len(current_time) - 6
    return current_time[:tz_start_index] + "Z" + current_time[tz_start_index:]


def number_runs(samples_run_names: dict[str, str]) -> dict[str, str]:
    run_names = list(sorted(set(filter(None, samples_run_names.values()))))
    run_names_numbers = {r: str(i) for i, r in enumerate(run_names, start=1)}
    samples_run_numbers = {
        run_name: run_names_numbers.get(run_number, "")
        for run_name, run_number in samples_run_names.items()
    }
    return samples_run_numbers
