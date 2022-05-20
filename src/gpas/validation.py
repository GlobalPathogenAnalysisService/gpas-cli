import datetime

import pandas as pd
import pandera as pa
import pycountry

from pathlib import Path

import pandera.extensions as extensions

from pandera.typing import Index, Series
from pandera import Check


@extensions.register_check_method(check_type="element_wise")
def tags_are_unique(field):
    valid = True
    if (
        field
        and not pd.isna(field)
        and len(set(field.split(":"))) != len(list(field.split(":")))
    ):
        valid = False
    return valid


class UploadSchema(pa.SchemaModel):
    """
    Validate generic GPAS upload CSVs.

    Built off to validate specific cases.
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
        nullable=True, isin=["positive", "negative", None], coerce=True
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
        tags_are_unique=True,
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
        isin=["Illumina", "Nanopore"], coerce=True, nullable=False
    )

    # custom method that checks that the collection_date is only the date and does not include the time
    # e.g. "2022-03-01" will pass but "2022-03-01 10:20:32" will fail
    @pa.check("collection_date")
    def check_collection_date(cls, a):
        return (a.dt.floor("d") == a).all()

    # custom method to check that one, and only one, instrument_platform is specified in a single upload CSV
    @pa.check("instrument_platform")
    def check_unique_instrument_platform(cls, a):
        return len(a.unique()) == 1

    class Config:
        name = "UploadSchema"
        strict = False
        coerce = True


##################################################


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


@extensions.register_check_method()
def instrument_is_valid(df):

    if "fastq" in df.columns:
        instrument = "Nanopore"
    elif "fastq2" in df.columns:
        instrument = "Illumina"

    return (df["instrument_platform"] == instrument).all()


class IlluminaFASTQCheckSchema(UploadSchema):
    """
    Validate GPAS upload CSVs specifying paired reads (e.g Illumina).
    """

    # gpas_batch: Series[str] = pa.Field(str_matches=r'^[A-Za-z0-9]')
    # gpas_run_number: Series[int] = pa.Field(nullable=True, ge=0)
    # gpas_sample_name: Index[str] = pa.Field(str_matches=r'^[A-Za-z0-9]')

    # validate that the fastq1 file is alphanumeric and unique
    fastq1: Series[str] = pa.Field(
        unique=True,
        str_matches=r"^[A-Za-z0-9/._-]+$",
        str_endswith="_1.fastq.gz",
        coerce=True,
        nullable=False,
    )

    # validate that the fastq2 file is alphanumeric and unique
    fastq2: Series[str] = pa.Field(
        unique=True,
        str_matches=r"^[A-Za-z0-9/._-]+$",
        str_endswith="_2.fastq.gz",
        coerce=True,
        nullable=False,
    )

    class Config:
        region_is_valid = ()
        instrument_is_valid = ()
        name = "IlluminaFASTQCheckSchema"
        strict = True
        coerce = True


class NanoporeFASTQCheckSchema(UploadSchema):
    """
    Validate GPAS upload CSVs specifying unpaired reads (e.g. Nanopore).
    """

    # gpas_batch: Series[str] = pa.Field(str_matches=r'^[A-Za-z0-9]')
    # gpas_run_number: Series[int] = pa.Field(nullable=True, ge=0)
    # gpas_sample_name: Index[str] = pa.Field(str_matches=r'^[A-Za-z0-9]')

    # validate that the fastq file is alphanumeric and unique
    fastq: Series[str] = pa.Field(
        unique=True,
        str_matches=r"^[A-Za-z0-9/._-]+$",
        str_endswith=".fastq.gz",
        coerce=True,
        nullable=False,
    )

    class Config:
        region_is_valid = ()
        instrument_is_valid = ()
        name = "NanoporeFASTQCheckSchema"
        strict = True
        coerce = True


class BAMCheckSchema(UploadSchema):
    """
    Validate GPAS upload CSVs specifying BAM files.
    """

    # gpas_batch: Series[str] = pa.Field(str_matches=r'^[A-Za-z0-9]')
    # gpas_run_number: Series[int] = pa.Field(nullable=True, ge=0)
    # gpas_sample_name: Index[str] = pa.Field(str_matches=r'^[A-Za-z0-9]')

    # validate that the bam file is alphanumeric and unique
    bam: Series[str] = pa.Field(
        unique=True,
        str_matches=r"^[A-Za-z0-9/._-]+$",
        str_endswith=".bam",
        coerce=True,
        nullable=False,
    )

    # insist that the path to the bam exists
    # @pa.check('bam_path')
    # def check_bam_file_exists(cls, a, error='bam file does not exist'):
    #     return all(a.map(os.path.isfile))

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
                {"sample": row.sample_name, "files": [row.fastq1, row.fastq2]}
            )
        else:
            samples.append({"sample": row.sample_name, "files": [row.fastq]})
    return samples


def parse_validation_errors(errors):
    """Parse errors arising during Pandera SchemaModel validation

    Parameters
    ----------
    err: pa.errors.SchemaErrors

    Returns
    -------
    pandas.DataFrame(columns=['sample_name', 'error_message'])
    """
    failures = errors.failure_cases.rename(columns={"index": "sample_name"})
    failures["error"] = failures.apply(parse_validation_error, axis=1)
    return failures[["sample_name", "error"]].to_dict("records")


def parse_validation_error(row):
    """
    Generate palatable errors from pandera output
    """
    if row.check == "column_in_schema":
        return "unexpected column " + row.failure_case + " found in upload CSV"
    if row.check == "column_in_dataframe":
        return "column " + row.failure_case + " missing from upload CSV"
    elif row.check == "region_is_valid":
        return "specified regions are not valid ISO-3166-2 regions for the specified country"
    elif row.check == "instrument_is_valid":
        return "FASTQ file columns and instrument_platform are inconsistent"
    elif row.check == "not_nullable":
        print(row)
        return row.column + " cannot be empty"
    elif row.check == "field_uniqueness":
        return row.column + " must be unique in the upload CSV"
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
            + " in the control field is not valid: field must be either empty or contain the one of the keywords positive or negative"
        )
    elif row.column == "host" and row.check[:4] == "isin":
        return row.column + " can only contain the keyword human"
    elif row.column == "specimen_organism" and row.check[:4] == "isin":
        return row.column + " can only contain the keyword SARS-CoV-2"
    elif row.column == "primer_scheme" and row.check[:4] == "isin":
        return row.column + " can only contain the keyword auto"
    elif row.column == "instrument_platform":
        if row.sample_name is None:
            return row.column + " must be unique"
        if row.check[:4] == "isin":
            return (
                row.column
                + " can only contain one of the keywords Illumina or Nanopore"
            )
    elif row.column == "collection_date":
        if row.sample_name is None:
            return (
                row.column + " must be in form YYYY-MM-DD and cannot include the time"
            )
        if row.check[:4] == "less":
            return row.column + " cannot be in the future"
        if row.check[:7] == "greater":
            return row.column + " cannot be before 2019-01-01"
    elif row.column in ["fastq1", "fastq2", "fastq"]:
        if row.check == "field_uniqueness":
            return row.column + " must be unique in the upload CSV"
    elif row.column is None:
        return "problem"
    elif row.check == "tags_are_unique":
        print(row)
        print(row.failure_case)
        return row.column + " cannot be repeated"
    else:
        return "problem in " + row.column + " field"


def validate(upload_csv: Path) -> tuple[bool, dict]:
    """
    Validate an upload CSV. Returns tuple of validity (bool) and a message (dict)
    """
    df = pd.read_csv(upload_csv, encoding="utf-8", index_col="sample_name")
    try:
        UploadSchema.validate(df, lazy=True)
        valid = True
        records = get_valid_samples(df)
        message = {"validation": {"status": "completed", "samples": records}}
    except pa.errors.SchemaErrors as errors:
        valid = False
        records = parse_validation_errors(errors)
        message = {"validation": {"status": "failure", "samples": records}}
    return (valid, message)
