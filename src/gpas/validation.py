import datetime
import json
import logging
import os
import re
from pathlib import Path

import pandas as pd
import pandera as pa
import pandera.extensions as extensions
from pandera.typing import Index, Series

import gpas


def parse_countries_subdivisions():
    """Parse static JSON generated by Pycountry to avoid dependency"""
    path = gpas.data_dir / Path("countries") / Path("countries_subdivisions.json")
    if not path.exists():
        path = (
            Path(__file__).parent.parent
            / Path("data")
            / Path("countries")
            / Path("countries_subdivisions.json")
        )
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


CONTROLS = {"positive", "negative"}
HOSTS = {"human"}
INSTRUMENTS = {"Illumina", "Nanopore"}
ORGANISMS = {"SARS-CoV-2"}
PRIMER_SCHEMES = {"auto"}
COUNTRIES_SUBDIVISIONS = parse_countries_subdivisions()
COUNTRIES_ALPHA_3 = COUNTRIES_SUBDIVISIONS.keys()
REGIONS = {i for l in COUNTRIES_SUBDIVISIONS.values() for i in l}


class ValidationError(Exception):
    def __init__(self, errors):
        self.errors = errors
        self.errors_df = pd.DataFrame(errors, columns=["sample_name", "error"]).fillna(
            value=""
        )
        self.report = {
            "validation": {
                "status": "failure",
                "errors": errors,
            }
        }
        super().__init__(self._message())

    def _message(self):
        message = f"Failed to validate upload CSV ({len(self.errors)} errors):\n"
        message += self.errors_df.to_string(index=False, justify="left")
        return message


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


class BaseSchema(pa.SchemaModel):
    """
    Validate generic GPAS upload CSVs
    """

    # validate that batch is alphanumeric only
    batch: Series[str] = pa.Field(str_matches=r"^[A-Za-z0-9._-]+$", nullable=True)

    # validate run_number is alphanumeric but can also be null
    run_number: Series[str] = pa.Field(
        str_matches=r"^[A-Za-z0-9._-]+$",
        nullable=True,
    )

    # validate sample name is alphanumeric and insist it is unique
    sample_name: Index[str] = pa.Field(
        str_matches=r"^[A-Za-z0-9._-]+$",
        unique=True,
        nullable=False,
    )

    # insist that control is one of positive, negative or null
    control: Series[str] = pa.Field(
        nullable=True,
        isin=CONTROLS,
    )

    # validate that the collection is in the ISO format, is no earlier than 01-Jan-2019 and no later than today
    collection_date: Series[str] = pa.Field(
        nullable=False,
    )

    # insist that the tags is alphanumeric, including : as it is the delimiter
    tags: Series[str] = pa.Field(
        nullable=False,
        str_matches=r"^[A-Za-z0-9:_-]+$",
    )

    # insist that the country is one of the entries in the specified lookup table
    country: Series[str] = pa.Field(isin=COUNTRIES_ALPHA_3, nullable=False)

    region: Series[str] = pa.Field(nullable=True, isin=REGIONS)

    district: Series[str] = pa.Field(
        str_matches=r"^[\sA-Za-z0-9:_-]+$",
        nullable=True,
    )

    # at present specimen_organism can only be SARS-CoV-2
    specimen_organism: Series[str] = pa.Field(isin=ORGANISMS, nullable=False)

    # at present host can only be human
    host: Series[str] = pa.Field(isin=HOSTS, nullable=False)

    # insist that instrument_platform can only be Illumina or Nanopore
    instrument_platform: Series[str] = pa.Field(isin=INSTRUMENTS, nullable=False)

    # at present primer_schema can only be auto
    primer_scheme: Series[str] = pa.Field(isin=PRIMER_SCHEMES, nullable=False)

    @pa.check(collection_date, element_wise=True)
    def check_collection_date(cls, value: str) -> bool:
        """Check the date is parseable and within allowed range"""
        valid = True
        try:
            date = pd.to_datetime(value, format="%Y-%m-%d")
            is_parseable = date.strftime("%Y-%m-%d") == value
        except Exception:
            is_parseable = False
        try:
            allowed_range_start = datetime.datetime.strptime("2019-01-01", "%Y-%m-%d")
            allowed_range_end = datetime.datetime.today()
            is_within_range = allowed_range_start < date < allowed_range_end
        except Exception:
            is_within_range = False
        return valid if is_parseable and is_within_range else False

    @pa.check(instrument_platform)
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

    @pa.check(tags, element_wise=True)
    def tags_are_present(cls, value: str) -> bool:
        """Catch colon-only case"""
        return bool(str(value).strip(":"))

    @pa.dataframe_check(ignore_na=False)
    def region_is_valid(cls, df: pd.DataFrame) -> Series[bool]:
        """
        Validate the region field using ISO 3166-2
        """

        def validate_region(row):
            try:
                region = row["region"] if "region" in row else ""
                if (
                    region
                    and not pd.isna(region)
                    and region not in COUNTRIES_SUBDIVISIONS.get(row["country"], {})
                ):
                    valid = False
                else:
                    valid = True
            except KeyError:
                valid = False
            return valid

        return df.apply(validate_region, axis=1)

    class Config:
        coerce = True


class FastqSchema(BaseSchema):
    """
    Validate GPAS upload CSVs specifying unpaired reads (e.g. Nanopore).
    """

    # validate that the fastq file is alphanumeric and unique
    fastq: Series[str] = pa.Field(
        unique=True,
        str_matches=r"^[A-Za-z0-9 /._-]+$",
        str_endswith=".fastq.gz",
        nullable=False,
    )

    @pa.check(fastq, element_wise=True)
    def check_path_fastq(cls, path: str) -> bool:
        if path and pd.notna(path):
            return Path(path).exists()


class PairedFastqSchema(BaseSchema):
    """
    Validate GPAS upload CSVs specifying paired reads (e.g Illumina).
    """

    fastq1: Series[str] = pa.Field(
        unique=True,  # Joint uniqueness specified in Config
        str_matches=r"^[A-Za-z0-9 /._-]+$",
        str_endswith=".fastq.gz",
        nullable=False,
    )

    fastq2: Series[str] = pa.Field(
        unique=True,  # Joint uniqueness specified in Config
        str_matches=r"^[A-Za-z0-9 /._-]+$",
        str_endswith=".fastq.gz",
        nullable=False,
    )

    @pa.check(fastq1, element_wise=True)
    def check_path_fastq1(cls, path: str) -> bool:
        """Check that fastq1 exists"""
        if path and pd.notna(path):
            return Path(path).exists()

    @pa.check(fastq2, element_wise=True)
    def check_path_fastq2(cls, path: str) -> bool:
        """Check that fastq2 exists"""
        if path and pd.notna(path):
            return Path(path).exists()

    @pa.dataframe_check(ignore_na=False)
    def fastq1_does_not_equal_fastq2(cls, df: pd.DataFrame) -> Series[bool]:
        """Check that fastq1 and fastq2 are not the same file"""
        return df["fastq1"] != df["fastq2"]

    class Config:
        unique = ["fastq1", "fastq2"]


class BamSchema(BaseSchema):
    """
    Validate GPAS upload CSVs specifying BAM files.
    """

    bam: Series[str] = pa.Field(
        unique=True,
        str_matches=r"^[A-Za-z0-9 /._-]+$",
        str_endswith=".bam",
        nullable=False,
    )

    @pa.check(bam, element_wise=True)  # Check that bam exists
    def check_path_bam(cls, path: str) -> bool:
        if path and pd.notna(path):
            return Path(path).exists()


class PairedBamSchema(BamSchema):
    pass


def get_valid_samples(df: pd.DataFrame, schema_name: str) -> list[dict]:
    samples = []
    for row in df.reset_index().itertuples():
        if schema_name == "FastqSchema":
            samples.append({"sample_name": row.sample_name, "files": [row.fastq]})
        elif schema_name == "PairedFastqSchema":
            samples.append(
                {"sample_name": row.sample_name, "files": [row.fastq1, row.fastq2]}
            )
        elif schema_name in {"BamSchema", "PairedBamSchema"}:
            samples.append({"sample_name": row.sample_name, "files": [row.bam]})
        else:
            raise ValidationError([{"error": "Unexpected schema"}])
    return samples


def remove_ints(ld: list[dict]):
    """Eliminate autogenerated integer sample names for empty samples"""
    return [{k: v for k, v in d.items() if type(v) != int} for d in ld]


def remove_nones_duplicates_empties_ints_from_ld(ld: list[dict]) -> list[dict]:
    """Remove None-valued keys, duplicated, and empty dicts from a list of dicts"""
    ld = (
        pd.DataFrame(ld, columns=["sample_name", "error"])
        .fillna(value="")
        .sort_values(["sample_name", "error"])
        .drop_duplicates()
        .to_dict("records")
    )
    return list(filter(None, [{k: v for k, v in d.items() if v} for d in ld]))


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
    failures = failure_cases[["sample_name", "error"]].to_dict("records")
    return remove_nones_duplicates_empties_ints_from_ld(remove_ints(failures))


def parse_validation_error(row):
    """
    Generate palatable errors from pandera output
    """
    # print(str(row), "\n")
    if row.check == "column_in_schema":
        return "unexpected column " + row.failure_case
    if row.check == "column_in_dataframe":
        return "missing column " + row.failure_case
    elif row.check == "region_is_valid":
        return "invalid region (ISO 3166-2 subdivision) for specified country"
    elif row.check == "instrument_is_valid" or (
        row.column == "instrument_platform" and "isin" in row.check
    ):
        return (
            f"instrument_platform can only contain one of {list(sorted(INSTRUMENTS))}"
        )
    elif row.check == "not_nullable":
        if row.schema_context == "Index":
            return "sample_name cannot be empty"
        else:
            return row.column + " cannot be empty"
    elif row.check == "field_uniqueness":
        if row.schema_context == "Index":
            return "sample_name must be unique"
        else:
            return row.column + " must be unique"
    elif row.check == "multiple_fields_uniqueness":
        return "fastq1 and fastq2 must be jointly unique"
    elif row.check == "check_collection_date":
        return (
            row.column
            + " must be in format YYYY-MM-DD between 2019-01-01 and "
            + datetime.date.today().isoformat()
        )
    elif row.check == "fastq1_does_not_equal_fastq2":
        return "fastq1 and fastq2 cannot be the same"
    elif "str_matches" in row.check:
        allowed_chars = row.check.split("[")[1].split("]")[0]
        if row.schema_context == "Column":
            if row.column == "district" and row.failure_case == False:
                return None  # empty-name.csv causes problems for district
            else:
                return (
                    row.column + " can only contain characters (" + allowed_chars + ")"
                )
        elif row.schema_context == "Index":
            return "sample_name can only contain characters (" + allowed_chars + ")"
    elif row.column == "country" and row.check[:4] == "isin":
        return row.failure_case + " is not a valid ISO 3166-1 alpha-3 country code"
    elif row.column == "region" and row.check[:4] == "isin":
        return row.failure_case + " is not a valid ISO 3166-2 subdivision name"
    elif row.column == "control" and row.check[:4] == "isin":
        if row.failure_case == False:  # empty-name.csv causes problems for controls
            return None
        else:
            return (
                row.failure_case
                + f" in the control field is not valid; field must be either empty or contain one of the keywords {', '.join(sorted(CONTROLS))}"
            )
    elif row.column == "host" and row.check[:4] == "isin":
        return row.column + " can only contain the keyword human"
    elif row.column == "specimen_organism" and row.check[:4] == "isin":
        return row.column + " can only contain the keyword SARS-CoV-2"
    elif row.column == "primer_scheme" and row.check[:4] == "isin":
        return row.column + " can only contain the keyword auto"
    elif row.column == "instrument_platform":
        return row.column + " must be the same for all samples in a submission"
    elif row.column is None:
        return "problem"
    elif row.check == "tags_are_unique":
        return row.column + " cannot be repeated"
    elif row.check == "tags_are_present":
        return row.column + " cannot be empty"
    elif row.check.startswith("check_path"):
        return row.column + " file does not exist"
    elif row.check.startswith("str_endswith"):
        return row.column + " must end with .fastq.gz or .bam as appropriate"
    else:
        return "problem in " + row.column + " field"


def select_schema(df: pd.DataFrame) -> pa.SchemaModel:
    """Choose appropriate validation schema and the presence of required columns"""
    columns = set(df.columns)
    try:
        if "bam" in columns and not {"fastq", "fastq1", "fastq2"} & columns:
            if "Illumina" in df["instrument_platform"].tolist():
                schema = PairedBamSchema
            else:
                schema = BamSchema
        elif (
            "fastq" in columns
            and not {"fastq1", "fastq2", "bam"} & columns
            and "Nanopore" in df["instrument_platform"].tolist()
        ):
            schema = FastqSchema
        elif (
            {"fastq1", "fastq2"} < columns
            and not {"fastq", "bam"} & columns
            and "Illumina" in df["instrument_platform"].tolist()
        ):
            schema = PairedFastqSchema
        else:
            raise RuntimeError()
    except:
        raise ValidationError(
            [
                {
                    "error": "could not infer upload CSV schema."
                    " For Nanopore samples, column 'instrument_platform' must be"
                    " 'Nanopore', and either column 'fastq' or column 'bam' must be"
                    " valid paths."
                    " For Illumina samples, column 'instrument_platform' must be"
                    " 'Illumina' and either columns 'bam' or 'fastq1' and 'fastq2' must"
                    " be valid paths."
                }
            ]
        )
    return schema


def resolve_paths(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resolve relative paths to files inside dataframe
    """
    resolve = lambda x: str(Path(x).resolve())
    if "fastq" in df.columns:
        df["fastq"] = df["fastq"].apply(resolve)
    if "fastq1" in df.columns:
        df["fastq1"] = df["fastq1"].apply(resolve)
    if "fastq2" in df.columns:
        df["fastq2"] = df["fastq2"].apply(resolve)
    if "bam" in df.columns:
        df["bam"] = df["bam"].apply(resolve)
    return df


def validate_tags(df, permitted_tags):
    """
    Validate tags in upload csv
    """
    invalid_tags = set()
    for value in df["tags"].tolist():  # Get tags for each record
        if value and not pd.isna(value):
            tags = set(filter(None, value.strip(" :").split(":")))  # Get non-empty tags
            for tag in tags:
                if tag not in permitted_tags:
                    invalid_tags.add(tag)

    if invalid_tags:
        raise ValidationError(
            [{"error": f"tag(s) {invalid_tags} are unauthorised for this user"}]
        )


def validate_upload_csv_path(upload_csv) -> bool:
    return bool(re.match(r"^[A-Za-z0-9\\\s:./_-]+$", str(upload_csv)))


def validate(
    upload_csv: Path, permitted_tags: list[str] = []
) -> tuple[pd.DataFrame, pa.SchemaModel]:
    """
    Validate a dataframe. Returns success message or throws ValidationError
    """
    if not validate_upload_csv_path(upload_csv):
        raise ValidationError(
            [{"error": "upload csv path contains illegal characters"}]
        )
    try:
        df = pd.read_csv(
            upload_csv,
            encoding="utf-8",
            index_col="sample_name",
            dtype=str,
        )
    except Exception as e:
        raise ValidationError(
            [{"error": f"failed to parse upload CSV ({str(e)})"}]
        ) from None

    schema = select_schema(df)
    if permitted_tags:  # Only validate if we have tags
        validate_tags(df, permitted_tags)
    try:
        with set_directory(Path(upload_csv).parent):
            df = resolve_paths(schema.validate(df, lazy=True))
    except pa.errors.SchemaErrors as e:  # Validation errorS, because lazy=True
        raise ValidationError(parse_validation_errors(e)) from None
    logging.info("Validation successful")
    return df, schema


def build_validation_message(df: pd.DataFrame, schema_name: str) -> dict:
    records = get_valid_samples(df, schema_name)
    message = {
        "validation": {
            "status": "success",
            "samples": records,
        }
    }
    return message
