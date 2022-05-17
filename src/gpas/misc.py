import subprocess

from enum import Enum

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
        "NAME": "",
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
