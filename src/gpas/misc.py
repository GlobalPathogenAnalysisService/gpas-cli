import hashlib
import json
import logging
from multiprocessing.dummy import Pool
import os
import shutil
import subprocess
import sys
import traceback
from dataclasses import dataclass
from functools import partial
from enum import Enum
from pathlib import Path

import pandas as pd
import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

import gpas
from gpas import validation


FORMATS = Enum("Formats", dict(table="table", csv="csv", json="json"))
DEFAULT_FORMAT = FORMATS.table
ENVIRONMENTS = Enum("Environment", dict(dev="dev", staging="staging", prod="prod"))
DEFAULT_ENVIRONMENT = ENVIRONMENTS.prod
FILE_TYPES = Enum("FileType", dict(json="json", fasta="fasta", bam="bam", vcf="vcf"))
GOOD_STATUSES = {"Unreleased", "Released"}


class AuthenticationError(Exception):
    pass


class DecontaminationError(Exception):
    pass


class SubmissionError(Exception):
    pass


ENDPOINTS = {
    "dev": {
        "HOST": "https://portal.dev.gpas.ox.ac.uk/",
        "API_PATH": "/ords/gpas_pub/gpasapi/",
        "ORDS_PATH": "/ords/grep/electron/",
        "DASHBOARD_PATH": "/ords/r/gpas/gpas-portal/lineages-voc/",
        "NAME": "DEV",
    },
    "prod": {
        "HOST": "https://portal.gpas.ox.ac.uk/",
        "API_PATH": "ords/gpas_pub/gpasapi/",
        "ORDS_PATH": "ords/grep/electron/",
        "DASHBOARD_PATH": "ords/gpas/r/gpas-portal/lineages-voc/",
        "NAME": "PROD",
    },
    "staging": {
        "HOST": "https://portal.staging.gpas.ox.ac.uk/",
        "API_PATH": "ords/gpasuat/gpas_pub/gpasapi/",
        "ORDS_PATH": "ords/gpasuat/grep/electron/",
        "DASHBOARD_PATH": "ords/gpas/r/gpas-portal/lineages-voc/",
        "NAME": "STAGE",
    },
}


@dataclass
class LoggedShellCommand:
    name: str
    action: str
    cmd: str


def get_value_traceback(e: Exception) -> tuple[str, str, list]:
    e_type, e_value, e_traceback = sys.exc_info()
    e_t = str(e_type)
    e_v = repr(e_value)
    e_tb = traceback.format_tb(e_traceback)
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
    print(json.dumps(message, indent=4), flush=True)


# def print_progress_message(LoggedShellCommand, status: str):
#     message = {
#         "progress": {
#             "action": action,
#             "status": status,
#         }
#     }
#     print(json.dumps(message, indent=4), flush=True)


def run_logged(
    command: LoggedShellCommand, json_messages: bool = False
) -> subprocess.CompletedProcess:
    if json_messages:
        # logging.basicConfig(format="%(message)s", level=logging.INFO)
        print_progress_message_json(
            action=command.action, status="started", sample=command.name
        )
    process = subprocess.run(
        command.cmd, shell=True, check=True, text=True, capture_output=True
    )
    if json_messages:
        print_progress_message_json(
            action=command.action, status="finished", sample=command.name
        )
    # else:
    #     with logging_redirect_tqdm():
    #         logging.info(f"Finished {command.action} for {command.name}")

    return process


def run_parallel_logged(
    commands: list[LoggedShellCommand],
    processes: int,
    participle: str = "processing",
    json_messages: bool = False,
) -> dict[str, subprocess.CompletedProcess]:
    processes = 1 if sys.platform == "win32" else processes
    if json_messages:
        print_progress_message_json(action=commands[0].action, status="started")
    if processes == 1:
        results = {}
        for c in commands:
            results[c.name] = run_logged(command=c, json_messages=json_messages)
            logging.debug(f"{c.cmd=}")
    else:
        names = [c.name for c in commands]
        cmds = [c.cmd for c in commands]
        logging.debug(f"Started {participle.lower()} {len(cmds)} sample(s) \n{cmds=}")
        with Pool(processes) as pool:
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
        with logging_redirect_tqdm():
            logging.info(f"Finished {participle.lower()} {len(commands)} sample(s)")
    return results


def check_unicode(data):
    """Returns a Unicode object on success or None on failure"""
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


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
    if os.getenv(env_var) and Path(os.environ[env_var]).exists():
        path = Path(os.environ[env_var]).resolve()
    elif hasattr(sys, "_MEIPASS"):  # PyInstaller onefile
        path = Path(sys.executable).parent
        print("Using", str(path), "!")
    elif (Path(__file__).parents[0] / filename).exists():
        path = (Path(__file__).parents[0] / filename).resolve()
    elif (Path(__file__).parents[1] / filename).exists():
        path = (Path(__file__).parents[1] / filename).resolve()
    elif (Path(__file__).parents[2] / filename).exists():
        path = (Path(__file__).parents[2] / filename).resolve()
    elif shutil.which(filename):  # $PATH
        path = Path(shutil.which(filename)).resolve()
    else:
        raise FileNotFoundError(f"Could not find {filename} binary")
    logging.debug(f"{filename=} {path=}")
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
    elif gpas.data_dir.exists():
        path = gpas.data_dir
    elif (Path(__file__).parents[1] / "data").exists():
        path = Path(__file__).parents[1] / "data"
    else:
        print(f"{__name__=} {__file__=} {gpas.data_dir=}")
        raise FileNotFoundError(f"Could not find data directory")
    return path.resolve()


def get_reference_path(organism):
    prefix = get_data_path() / "refs"
    organisms_paths = {"SARS-CoV-2": "MN908947_no_polyA.fasta"}
    return Path(prefix / organisms_paths[organism]).resolve()
