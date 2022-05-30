import gzip
import json
import asyncio
import logging

from pathlib import Path

import httpx
import requests

import pandas as pd
import pandera as pa

from tqdm import tqdm

from gpas.misc import (
    run,
    ENVIRONMENTS,
    DEFAULT_ENVIRONMENT,
    FILE_TYPES,
    ENDPOINTS,
    GOOD_STATUSES,
)

from gpas import validation


def parse_token(token: Path) -> dict:
    return json.loads(token.read_text())


def parse_mapping(mapping_csv: Path = None) -> pd.DataFrame:
    df = pd.read_csv(mapping_csv)
    expected_columns = {
        "local_batch",
        "local_run_number",
        "local_sample_name",
        "gpas_batch",
        "gpas_run_number",
        "gpas_sample_name",
    }
    if not expected_columns.issubset(set(df.columns)):
        raise RuntimeError(f"One or more expected columns missing from mapping CSV")
    return df


def update_fasta_header(path: Path, guid: str, name: str):
    """Update the header line of a gzipped fasta file in place"""
    with gzip.open(path, "rt") as fh:
        contents = fh.read()
    if guid in contents:
        with gzip.open(path, "wt") as fh:
            fh.write(contents.replace(guid, f"{guid}|{name}"))
    else:
        logging.warning(f"Could not rename {guid} inside {name}.fasta.gz")


async def get_status_async(
    access_token: str,
    mapping_csv: Path = None,
    guids: list[str] = [],
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    rename: bool = False,
    raw: bool = False,
) -> list[dict]:
    """Returns a list of dicts of containing status records"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    endpoint = (
        ENDPOINTS[environment.value]["HOST"]
        + ENDPOINTS[environment.value]["API_PATH"]
        + "get_sample_detail"
    )

    if mapping_csv:
        logging.info(f"Using samples in {mapping_csv}")
        mapping_df = parse_mapping(mapping_csv)
        guids = mapping_df["gpas_sample_name"].tolist()
    elif guids:
        logging.info(f"Using list of guids")
    else:
        raise RuntimeError("Neither a mapping csv nor guids were specified")

    transport = httpx.AsyncHTTPTransport(retries=2)
    async with httpx.AsyncClient(transport=transport) as client:
        guids_urls = {guid: f"{endpoint}/{guid}" for guid in guids}
        tasks = [
            get_status_single_async(client, guid, url, headers)
            for guid, url in guids_urls.items()
        ]
        records = [
            await f
            for f in tqdm(
                asyncio.as_completed(tasks),
                desc=f"Querying status for {len(guids)} samples",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
                total=len(tasks),
            )
        ]

    if rename:
        if mapping_csv and "local_sample_name" in mapping_df.columns:
            guids_names = mapping_df.set_index("gpas_sample_name")[
                "local_sample_name"
            ].to_dict()
            records = pd.DataFrame(records).replace(guids_names).to_dict("records")
        else:
            logging.warning(
                "Samples were not renamed because a valid mapping csv was not specified"
            )

    return records


async def get_status_single_async(client, guid, url, headers):
    r = await client.get(url=url, headers=headers)
    if r.status_code == httpx.codes.ok:
        r_json = r.json()[0]
        status = r_json.get("status")
        result = dict(sample=guid, status=status)
        if status not in GOOD_STATUSES:
            logging.warning(f"Skipping {guid} (status {status})")
    else:
        result = dict(sample=guid, status="UNKNOWN")
        logging.warning(f"HTTP {r.status_code} ({guid})")
        if r.status_code == 401:
            raise RuntimeError(
                f"Authorisation failed (HTTP {r.status_code}). Invalid token?"
            )
    return result


async def download_async(
    guids: list,
    file_types: list[str],
    access_token: str,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    out_dir: Path = Path.cwd(),
    guids_names=None,
):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    endpoint = (
        ENDPOINTS[environment.value]["HOST"]
        + ENDPOINTS[environment.value]["API_PATH"]
        + "get_output"
    )
    unrecognised_file_types = set(file_types) - {t.name for t in FILE_TYPES}
    if unrecognised_file_types:
        raise RuntimeError(f"Invalid file type(s): {unrecognised_file_types}")
    logging.info(f"Fetching file types {file_types}")
    transport = httpx.AsyncHTTPTransport(retries=5)
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    async with httpx.AsyncClient(transport=transport, limits=limits) as client:
        guids_types_urls = {}
        for guid in guids:
            for file_type in file_types:
                guids_types_urls[(guid, file_type)] = f"{endpoint}/{guid}/{file_type}"
        tasks = [
            download_single_async(
                client,
                guid,
                file_type,
                url,
                headers,
                out_dir,
                guids_names[guid] if guids_names else None,
            )
            for (guid, file_type), url in guids_types_urls.items()
        ]
        return [
            await f
            for f in tqdm(
                asyncio.as_completed(tasks),
                desc=f"Downloading {len(tasks)} files for {len(guids)} samples",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
                total=len(tasks),
            )
        ]


async def download_single_async(
    client, guid, file_type, url, headers, out_dir, name=None
):
    file_types_extensions = {
        "json": "json",
        "fasta": "fasta.gz",
        "bam": "bam",
        "vcf": "vcf",
    }
    prefix = name if name else guid
    r = await client.get(url=url, headers=headers)
    if r.status_code == httpx.codes.ok:
        with open(
            Path(out_dir) / Path(f"{prefix}.{file_types_extensions[file_type]}"), "wb"
        ) as fh:
            fh.write(r.content)
        if name and file_type == "fasta":
            update_fasta_header(
                Path(f"{prefix}.{file_types_extensions[file_type]}"), guid, name
            )
    else:
        result = dict(sample=guid, status="UNKNOWN")
        logging.warning(f"Skipping {guid}.{file_type} (HTTP {r.status_code})")


def validate(upload_csv: Path):
    return validation.validate(upload_csv)


# def get_status(
#     guids: list,
#     access_token: str,
#     environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
#     raw: bool = False,
# ) -> list:
#     """Returns a list of dicts of containing status records"""
#     headers = {
#         "Authorization": f"Bearer {access_token}",
#         "Content-Type": "application/json",
#     }
#     endpoint = (
#         ENDPOINTS[environment.value]["HOST"]
#         + ENDPOINTS[environment.value]["API_PATH"]
#         + "get_sample_detail/"
#     )
#     """
#     Return a list of dictionaries given a list of guids
#     """
#     records = []
#     for guid in tqdm(guids):
#         r = requests.get(url=endpoint + guid, headers=headers)
#         if r.ok:
#             if raw:
#                 records.append(r.json())
#             else:
#                 records.append(
#                     dict(
#                         sample=r.json()[0].get("name"), status=r.json()[0].get("status")
#                     )
#                 )
#         else:
#             logging.warning(f"{guid} (error {r.status_code})")
#     return records


class Sample:
    def __init__(
        self,
        batch,
        run_number,
        sample_name,
        control,
        collection_date,
        tags,
        country,
        region,
        specimen_organism,
        host,
        instrument_platform,
        primer_scheme,
        schema,
        district=None,
        fastq=None,
        fastq1=None,
        fastq2=None,
        bam=None,
    ):
        self.batch = batch
        self.run_number = run_number
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
        self.schema = schema
        self.is_paired = True if self.instrument_platform == "Illumina" else False
        self.ref_path = self.get_reference_path()

    def get_reference_path(self):
        prefix = Path(__file__).parent.parent.parent / Path("ref")
        organisms_paths = {"SARS-CoV-2": "MN908947_no_polyA.fasta"}
        return prefix / organisms_paths[self.specimen_organism]

    def decontaminate(self, schema: str, working_dir: Path = Path()):
        # return "TEST"
        if "Bam" in schema:
            _convert_bam(self.bam, paired=self.is_paired)

        if not self.is_paired:
            self._read_it_and_keep(reads1=self.fastq, tech="ont")
        else:
            self._read_it_and_keep(
                reads1=self.fastq1,
                reads2=self.fastq2,
                tech="illumina",
                working_dir=working_dir,
            )

    def _convert_bam(self, bam, working_dir, paired=False):
        stem = bam.with_suffix("")

        if not self.is_paired:
            cmd = f"samtools fastq -0 {working_dir / Path(stem + '.fastq.gz')} {working_dir / Path(row['bam'])}"
        else:
            cmd = f"samtools sort {bam} | samtools fastq -N -1 {working_dir / Path(stem + '_1.fastq.gz')} -2 {working_dir / Path(stem + '_1.fastq.gz')}"
        run_cmd = run(cmd)
        print(run_cmd.returncode)

    def _read_it_and_keep(self, reads1, tech, working_dir, reads2=None):
        stem = reads1.removesuffix(".fastq.gz")
        prefix = working_dir / Path(stem)
        if not reads2:
            cmd = f"readItAndKeep --tech ont --enumerate_names --ref_fasta {self.ref_path} --reads1 {reads1} --outprefix {working_dir / 'test'}"
        else:
            cmd = f"readItAndKeep --tech illumina --enumerate_names --ref_fasta {self.ref_path} --reads1 {reads1} --reads2 {reads2} --outprefix {working_dir / 'test'}"
        print(working_dir / "test")
        print(cmd)
        run_cmd = run(cmd)
        print(run_cmd.returncode)


class Batch:
    """
    Validation on initialisation
    """

    def __init__(
        self,
        upload_csv: Path,
        token: Path = None,
        environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
        working_dir: Path = Path("/tmp"),
        threads: int = 0,
    ):
        self.upload_csv = upload_csv
        self.token = token
        self.environment = environment
        self.working_dir = working_dir
        self.threads = threads
        (
            self.validation_result,
            self.schema,
            self.validation_message,
        ) = validation.validate(upload_csv)
        self.df = pd.read_csv(upload_csv, encoding="utf-8").fillna("")
        self.samples = [
            Sample(**r, schema=self.schema) for r in self.df.to_dict("records")
        ]

    def decontaminate(self):
        return list(
            map(lambda s: s.decontaminate(self.schema, self.working_dir), self.samples)
        )
