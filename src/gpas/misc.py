import os
import hashlib
import subprocess

from enum import Enum
from pathlib import Path

import pandas as pd


FORMATS = Enum("Formats", dict(table="table", csv="csv", json="json"))
DEFAULT_FORMAT = FORMATS.table
ENVIRONMENTS = Enum("Environment", dict(dev="dev", staging="staging", prod="prod"))
DEFAULT_ENVIRONMENT = ENVIRONMENTS.dev
FILE_TYPES = Enum("FileType", dict(json="json", fasta="fasta", bam="bam", vcf="vcf"))
GOOD_STATUSES = {"Unreleased", "Released"}

ENDPOINTS = {
    "dev": {
        "HOST": "https://portal.dev.gpas.ox.ac.uk/",
        "API_PATH": "ords/gpasdevpdb1/gpas_pub/gpasapi/",
        "ORDS_PATH": "ords/gpasdevpdb1/grep/electron/",
        "DASHBOARD_PATH": "ords/gpasdevpdb1/gpas/r/gpas-portal/lineages-voc/",
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


def run(cmd):
    return subprocess.run(cmd, shell=True, text=True, capture_output=True)


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


def hash_file(file_path: Path):
    md5 = hashlib.md5()
    with open(file_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(4096), b""):
            md5.update(chunk)
    return md5.hexdigest()
