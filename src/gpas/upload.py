import logging
import datetime
from random import sample

import pandas as pd

from pathlib import Path

from gpas import validation

from gpas.misc import (
    ENVIRONMENTS,
    DEFAULT_ENVIRONMENT,
    FILE_TYPES,
    ENDPOINTS,
    GOOD_STATUSES,
)


class Sample:
    def __init__(
        *,
        sample_name,
        fastq=None,
        fastq1=None,
        fastq2=None,
        bam=None,
        control,
        collection_date,
        tags,
        country,
        region,
        district=None,
        specimen_organism,
        host,
        instrument_platform,
        primer_scheme
    ):
        self.sample_name = sample_name
        self.fastq = fastq
        self.fastq1 = fastq1
        self.fastq2 = fastq2
        self.bam = bam
        self.control = control
        self.collection_date = collection_date
        self.tags = tags
        self.country = country
        self.region = region
        self.district = district
        self.specimen_organism = specimen_organism
        self.host = host
        self.instrument_platform = instrument_platform
        self.primer_scheme = primer_scheme


class Batch:
    def __init__(
        self, upload_csv, token=None, environment=DEFAULT_ENVIRONMENT, threads=0
    ):
        self.upload_csv = upload_csv
        self.token = token
        self.environment = environment
        self.threads = threads

        validation.validate(upload_csv)
