import os
import logging
import datetime
from contextlib import contextmanager

import pandas as pd
import pandera as pa
import pycountry

from pathlib import Path

import pandera.extensions as extensions

from pandera.typing import Index, Series


VALID_INSTRUMENTS = {"Illumina", "Nanopore"}
VALID_CONTROLS = {"positive", "negative"}


class ValidationError(Exception):
    pass


@contextmanager
def set_directory(path: Path):
    """Sets the cwd within the context

    Args:
        path (Path): The path to the cwd

    Yields:
        None
    """

    origin = Path().absolute()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(origin)


@extensions.register_check_method()
def region_is_valid(df):
    """
    Validate the region field using ISO-3166-2 (pycountry).

    Returns
    -------
    bool
        True if all regions are ok, False otherwise
    """

    def validate_region(row):
        result = pycountry.countries.get(alpha_3=row.country)

        if result is None:
            return False
        elif pd.isna(row.region) or row.region is None:
            return True
        else:
            region_lookup = [
                i.name for i in pycountry.subdivisions.get(country_code=result.alpha_2)
            ]
            return row.region in region_lookup

    df["valid_region"] = df.apply(validate_region, axis=1)

    return df["valid_region"].all()


class BaseSchema(pa.SchemaModel):
    """
    Validate generic GPAS upload CSVs.
    """

    # validate that batch is alphanumeric only
    batch: Series[str] = pa.Field(
        str_matches=r"^[A-Za-z0-9._-]+$", coerce=True, nullable=False
    )

    # validate run_number is alphanumeric but can also be null
    run_number: Series[str] = pa.Field(
        str_matches=r"^[A-Za-z0-9._-]+$", nullable=True, coerce=True
    )

    # validate sample name is alphanumeric and insist it is unique
    sample_name: Index[str] = pa.Field(
        str_matches=r"^[A-Za-z0-9._-]+$", unique=True, coerce=True, nullable=False
    )

    # insist that control is one of positive, negative or null
    control: Series[str] = pa.Field(
        nullable=True, isin=["positive", "negative"], coerce=True
    )

    # validate that the collection is in the ISO format, is no earlier than 01-Jan-2019 and no later than today
    collection_date: Series[pa.DateTime] = pa.Field(
        gt="2019-01-01", le=str(datetime.date.today()), coerce=True, nullable=False
    )

    # insist that the country is one of the entries in the specified lookup table
    country: Series[str] = pa.Field(
        isin=[i.alpha_3 for i in pycountry.countries], coerce=True, nullable=False
    )

    region: Series[str] = pa.Field(
        nullable=True, isin=[i.name for i in list(pycountry.subdivisions)], coerce=True
    )

    district: Series[str] = pa.Field(
        str_matches=r"^[\sA-Za-z0-9:_-]+$", nullable=True, coerce=True
    )

    # insist that the tags is alphanumeric, including : as it is the delimiter
    tags: Series[str] = pa.Field(
        nullable=False,
        str_matches=r"^[A-Za-z0-9:_-]+$",
        coerce=True,
    )

    # at present host can only be human
    host: Series[str] = pa.Field(isin=["human"], coerce=True, nullable=False)

    # at present specimen_organism can only be SARS-CoV-2
    specimen_organism: Series[str] = pa.Field(
        isin=["SARS-CoV-2"], coerce=True, nullable=False
    )

    # at present primer_schema can only be auto
    primer_scheme: Series[str] = pa.Field(isin=["auto"], coerce=True, nullable=False)

    # insist that instrument_platform can only be Illumina or Nanopore
    instrument_platform: Series[str] = pa.Field(
        isin=VALID_INSTRUMENTS, coerce=True, nullable=False
    )

    @pa.check("collection_date")
    def check_collection_date(cls, a):
        """
        Check that collection_date is only the date and does not include time
        e.g. "2022-03-01" will pass but "2022-03-01 10:20:32" will fail
        """
        return (a.dt.floor("d") == a).all()

    @pa.check("instrument_platform")
    def check_unique_instrument_platform(cls, field):
        """
        Check that only one instrument_platform is specified in the upload CSV
        """
        return len(field.unique()) == 1

    @pa.check(tags, element_wise=True)
    def tags_are_unique(cls, value: str) -> bool:
        valid = True
        if value and not pd.isna(value):
            value = value.strip(":")
            if len(set(value.split(":"))) != len(list(value.split(":"))):
                valid = False
        return valid

    class Config:
        strict = False
        coerce = True
        region_is_valid = ()


class SingleFastqSchema(BaseSchema):
    """
    Validate GPAS upload CSVs specifying unpaired reads (e.g. Nanopore).
    """

    # validate that the fastq file is alphanumeric and unique
    fastq: Series[str] = pa.Field(
        unique=True,
        str_matches=r"^[A-Za-z0-9/._-]+$",
        str_endswith=".fastq.gz",
        coerce=True,
        nullable=False,
    )

    @pa.check(fastq, element_wise=True)
    def check_path(cls, path: str) -> bool:
        return Path(path).exists()


class PairedFastqSchema(BaseSchema):
    """
    Validate GPAS upload CSVs specifying paired reads (e.g Illumina).
    """

    # gpas_batch: Series[str] = pa.Field(str_matches=r'^[A-Za-z0-9]')
    # gpas_run_number: Series[int] = pa.Field(nullable=True, ge=0)
    # gpas_sample_name: Index[str] = pa.Field(str_matches=r'^[A-Za-z0-9]')

    # validate that the fastq1 file is alphanumeric and unique
    fastq1: Series[str] = pa.Field(
        # unique=True,  # Joint uniqueness specified in Config
        str_matches=r"^[A-Za-z0-9/._-]+$",
        str_endswith="_1.fastq.gz",
        coerce=True,
        nullable=False,
    )

    # validate that the fastq2 file is alphanumeric and unique
    fastq2: Series[str] = pa.Field(
        # unique=True,
        str_matches=r"^[A-Za-z0-9/._-]+$",
        str_endswith="_2.fastq.gz",
        coerce=True,
        nullable=False,
    )

    @pa.check(fastq1, element_wise=True)
    def check_path_fastq1(cls, path: str) -> bool:
        return Path(path).exists()

    @pa.check(fastq2, element_wise=True)
    def check_path_fastq2(cls, path: str) -> bool:
        return Path(path).exists()

    class Config:
        unique = ["fastq1", "fastq2"]


class BamSchema(BaseSchema):
    """
    Validate GPAS upload CSVs specifying BAM files.
    """

    # Check filename is alphanumeric and unique
    bam: Series[str] = pa.Field(
        unique=True,
        str_matches=r"^[A-Za-z0-9/._-]+$",
        str_endswith=".bam",
        coerce=True,
        nullable=False,
        # is_file=True
    )

    @pa.check(bam, element_wise=True)
    def check_path(cls, path: str) -> bool:
        return Path(path).exists()

    class Config:
        region_is_valid = ()
        name = "BAMCheckSchema"
        strict = True
        coerce = True


def get_valid_samples(df: pd.DataFrame) -> list[dict]:
    samples = []
    for row in df.reset_index().itertuples():
        if row.instrument_platform == "Illumina":
            samples.append(
                {"sample_name": row.sample_name, "files": [row.fastq1, row.fastq2]}
            )
        else:
            samples.append({"sample_name": row.sample_name, "files": [row.fastq]})
    return samples


def remove_nones_in_ld(ld: list[dict]) -> list[dict]:
    """Remove None-valued keys from a list of dicts"""
    return [{k: v for k, v in d.items() if v} for d in ld]


def parse_validation_errors(errors):
    """Parse errors arising during Pandera SchemaModel validation

    Parameters
    ----------
    err: pa.errors.SchemaErrors

    Returns
    -------
    pandas.DataFrame(columns=['sample_name', 'error_message'])
    """
    failure_cases = errors.failure_cases.rename(columns={"index": "sample_name"})
    failure_cases["error"] = failure_cases.apply(parse_validation_error, axis=1)
    print(failure_cases.to_dict("records"))
    failures = failure_cases[["sample_name", "error"]].to_dict("records")
    return remove_nones_in_ld(failures)


def parse_validation_error(row):
    """
    Generate palatable errors from pandera output
    """
    # print(str(row), "\n")
    if row.check == "column_in_schema":
        return "unexpected column " + row.failure_case
    if row.check == "column_in_dataframe":
        return "column " + row.failure_case
    elif row.check == "region_is_valid":
        return "specified regions are not valid ISO-3166-2 regions for the specified country"
    elif row.check == "instrument_is_valid":
        return f"instrument_platform can only contain one of {', '.join(VALID_INSTRUMENTS)}"
    elif row.check == "not_nullable":
        return row.column + " cannot be empty"
    elif row.check == "field_uniqueness":
        return row.column + " must be unique"
    elif row.check == "multiple_fields_uniqueness":
        return "fastq1 and fastq2 must be jointly unique"
    elif "str_matches" in row.check:
        allowed_chars = row.check.split("[")[1].split("]")[0]
        if row.schema_context == "Column":
            return row.column + " can only contain characters (" + allowed_chars + ")"
        elif row.schema_context == "Index":
            return "sample_name can only contain characters (" + allowed_chars + ")"
    elif row.column == "country" and row.check[:4] == "isin":
        return row.failure_case + " is not a valid ISO-3166-1 country"
    elif row.column == "region" and row.check[:4] == "isin":
        return row.failure_case + " is not a valid ISO-3166-2 region"
    elif row.column == "control" and row.check[:4] == "isin":
        return (
            row.failure_case
            + f" in the control field is not valid: field must be either empty or contain the one of the keywords {', '.join(VALID_CONTROLS)}"
        )
    elif row.column == "host" and row.check[:4] == "isin":
        return row.column + " can only contain the keyword human"
    elif row.column == "specimen_organism" and row.check[:4] == "isin":
        return row.column + " can only contain the keyword SARS-CoV-2"
    elif row.column == "primer_scheme" and row.check[:4] == "isin":
        return row.column + " can only contain the keyword auto"

    elif row.column == "instrument_platform" and "isin" in row.check:
        return (
            f"{row.column} value '{row.failure_case}' is not in set {VALID_INSTRUMENTS}"
        )
    elif row.column == "instrument_platform":
        return row.column + " must be the same for all samples in a submission"
    elif row.column == "collection_date":
        if row.sample_name is None:
            return (
                row.column + " must be in form YYYY-MM-DD and cannot include the time"
            )
        if row.check[:4] == "less":
            return row.column + " cannot be in the future"
        if row.check[:7] == "greater":
            return row.column + " cannot be before 2019-01-01"
    elif row.column is None:
        return "problem"
    elif row.check == "tags_are_unique":
        return row.column + " cannot be repeated"
    elif row.check.startswith("check_path"):
        return row.column + " file does not exist"
    else:
        return "problem in " + row.column + " field"
    if row.check.startswith("str_endswith"):
        return (
            row.column
            + " must end with .fastq.gz, _1.fastq.gz, _2.fastq.gz or .bam as appropriate"
        )


def pick_schema(df: pd.DataFrame) -> str:
    """Choose appropriate validation schema and the presence of required columns"""
    columns = set(df.columns)
    if "bam" in columns and not {"fastq", "fastq1", "fastq2"} & columns:
        schema = BamSchema
    elif "fastq" in columns and not {"fastq1", "fastq2", "bam"} & columns:
        schema = SingleFastqSchema
    elif {"fastq1", "fastq2"} < columns and not {"fastq", "bam"} & columns:
        schema = PairedFastqSchema
    else:
        raise (
            ValidationError(
                "Error inferring schema from available columns. For single-end FASTQ "
                "use 'fastq', for paired-end FASTQ use 'fastq1' and 'fastq2', and "
                "for BAM submissions use 'bam'"
            )
        )
    return schema


def validate(upload_csv: Path) -> tuple[bool, dict]:
    """
    Validate an upload CSV. Returns tuple of validity (bool) and a message (dict)
    """
    df = pd.read_csv(upload_csv, encoding="utf-8", index_col="sample_name")
    valid = False

    try:
        schema = pick_schema(df)
        with set_directory(upload_csv.parent):  # Enable file path validation
            schema.validate(df, lazy=True)
        valid = True
        records = get_valid_samples(df)
        message = {"validation": {"status": "completed", "samples": records}}
    except ValidationError as e:  # Failure to pick_schema()
        message = {
            "validation": {
                "status": "failure",
                "errors": [{"error": str(e)}],
            }
        }
    except pa.errors.SchemaErrors as e:  # Validation errorS, because lazy=True
        records = parse_validation_errors(e)
        message = {"validation": {"status": "failure", "errors": records}}

    return (valid, message)
