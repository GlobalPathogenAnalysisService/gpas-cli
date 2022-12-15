import asyncio
import datetime
import gzip
import json
import logging
import multiprocessing
import os
import platform
import shutil
import sys
from collections import defaultdict
from functools import partial
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import tqdm
from tenacity import (
    before_sleep,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
    wait_exponential,
)
from tqdm.contrib.concurrent import process_map
from tqdm.contrib.logging import logging_redirect_tqdm

from gpas import __version__, misc
from gpas.misc import (
    DEFAULT_ENVIRONMENT,
    ENVIRONMENTS,
    ENVIRONMENTS_URLS,
    FILE_TYPES,
    GOOD_STATUSES,
)
from gpas.validation import build_validation_message, validate

logger = logging.getLogger(__name__)


def parse_token(token: Path) -> dict:
    return json.loads(Path(token).read_text())


def parse_mapping_csv(mapping_csv: Path) -> dict:
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
    return df.set_index("gpas_sample_name")["local_sample_name"].to_dict()


def fetch_user_details(access_token, environment: ENVIRONMENTS) -> dict:
    """Test API authentication and fetch response from userOrgDtls endpoint"""
    endpoint = f"{ENVIRONMENTS_URLS[environment.value]['ORDS']}/userOrgDtls"
    try:
        logging.debug(f"Fetching user details {endpoint=}")
        r = httpx.get(endpoint, headers={"Authorization": f"Bearer {access_token}"})
        if not r.is_success:
            r.raise_for_status()
        result = r.json()
        logging.debug(f"{result=}")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 400 and "API access" in e.response.json().get(
            "message"
        ):
            raise misc.AuthenticationError(
                f"Authentication failed. User lacks API permissions"
            ) from None
        elif e.response.status_code == 401:
            raise misc.AuthenticationError(
                f"Authentication failed. Check access token and environment"
            ) from None
        else:
            raise e from None
    # Query PyPI instead of GitHub? GitHub has hopelessly low unauthenticated rate limits
    # try:
    #     r = httpx.get(
    #         "https://api.github.com/repos/GlobalPathogenAnalysisService/gpas-cli/releases/latest"
    #     )
    #     tag = r.json().get("tag_name")
    #     if tag and tag != __version__:
    #         logging.warning(
    #             f"Installed gpas-cli version ({__version__}) differs from the latest release ({tag})"
    #         )
    # except:
    #     pass
    return result


def parse_user_details(result: dict) -> tuple:
    """Parse response from userOrgDtls endpoint"""
    result = result.get("userOrgDtl", {})[0]
    user = result.get("userName")
    organisation = result.get("organisation")
    permitted_tags = [
        d.get("tagName") for d in result.get("tags", {}) if d.get("tagAccessYN") == "Y"
    ]
    logging.debug(f"{permitted_tags=}")
    date_mask = result.get("maskCollectionDate")  # Expects {"N", "MONTH", "WEEK"}
    return user, organisation, permitted_tags, date_mask


def update_fasta_header(path: Path, guid: str, name: str):
    """Update the header line of a gzipped fasta file in place"""
    with gzip.open(path, "rt") as fh:
        contents = fh.read()
    if guid in contents:
        with gzip.open(path, "wt") as fh:
            fh.write(contents.replace(guid, f"{guid}|{name}"))
    else:
        logging.warning(f"Could not rename {guid} inside {name}.fasta.gz")


async def fetch_status_async(
    access_token: str,
    guids: list | dict,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
) -> list[dict]:
    """Returns a list of dicts of containing status records"""
    fetch_user_details(access_token, environment)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    endpoint = f"{ENVIRONMENTS_URLS[environment.value]['API']}/get_sample_detail"
    limits = httpx.Limits(
        max_keepalive_connections=10, max_connections=20, keepalive_expiry=10
    )
    transport = httpx.AsyncHTTPTransport(limits=limits, retries=0)
    async with httpx.AsyncClient(transport=transport, timeout=30) as client:
        guids_urls = {guid: f"{endpoint}/{guid}" for guid in guids}
        tasks = [
            fetch_status_single_async(client, guid, url, headers)
            for guid, url in guids_urls.items()
        ]
        records = [
            await f
            # for f in asyncio.as_completed(tasks)
            for f in tqdm.tqdm(
                asyncio.as_completed(tasks),
                desc=f"Querying status for {len(guids)} sample(s)",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
                total=len(tasks),
            )
        ]

    if type(guids) is dict:
        logging.debug("Renaming")
        records = pd.DataFrame(records).replace(guids).to_dict("records")

    return records


@retry(
    retry=retry_if_exception_type(httpx.HTTPError),
    wait=wait_exponential(multiplier=1, min=1, max=16),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep.before_sleep_log(logger, 10),
)
async def fetch_status_single_async(client, guid, url, headers):
    logging.debug(f"fetch_status_single_async(): {url=}")
    r = await client.get(url=url, headers=headers)
    logging.debug(f"fetch_status_single_async(): {r.status_code=} {r.json()=}")
    if r.status_code == httpx.codes.OK:
        r_json = r.json()[0]
        status = r_json.get("status")
        result = dict(sample=guid, status=status)
        if status not in GOOD_STATUSES:
            with logging_redirect_tqdm():
                logging.info(f"{guid} has status {status}")
    elif r.status_code == 400 and "API access" in r.json().get("message"):
        raise misc.AuthenticationError(
            f"Authentication failed (HTTP {r.status_code}). User lacks API permissions"
        )
    elif r.status_code == 401:
        raise misc.AuthenticationError(
            f"Authentication failed (HTTP {r.status_code}). Invalid token?"
        )
    elif r.json().get("message") == "Sample not found.":
        status = "UNKNOWN"
        with logging_redirect_tqdm():
            logging.info(f"{guid} has status {status}")
        result = dict(sample=guid, status=status)
    elif r.json().get("message") == "You do not have access to this sample.":
        status = "UNAUTHORISED"
        with logging_redirect_tqdm():
            logging.info(f"{guid} has status {status}")
        result = dict(sample=guid, status=status)
    else:
        r.raise_for_status()  # Raises retryable httpx.HTTPError

    return result


async def download_async(
    access_token: str,
    guids: list | dict,
    file_types: list[str] = ["fasta"],
    out_dir: Path = Path.cwd(),
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    endpoint = f"{ENVIRONMENTS_URLS[environment.value]['API']}/get_output"

    unrecognised_file_types = set(file_types) - {t.name for t in FILE_TYPES}
    if unrecognised_file_types:
        raise RuntimeError(f"Invalid file type(s): {unrecognised_file_types}")
    with logging_redirect_tqdm():
        logging.info(f"Fetching file types {file_types}")

    limits = httpx.Limits(
        max_keepalive_connections=5, max_connections=10, keepalive_expiry=10
    )
    transport = httpx.AsyncHTTPTransport(limits=limits, retries=0)
    async with httpx.AsyncClient(transport=transport, timeout=120) as client:
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
                guids[guid] if type(guids) is dict else None,
            )
            for (guid, file_type), url in guids_types_urls.items()
        ]
        return [
            await f
            for f in tqdm.tqdm(
                asyncio.as_completed(tasks),
                desc=f"Downloading {len(tasks)} files for {len(guids)} sample(s)",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
                total=len(tasks),
            )
        ]


@retry(
    retry=retry_if_exception_type(httpx.HTTPError),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    stop=stop_after_attempt(4),
    before_sleep=before_sleep.before_sleep_log(logger, 10),
)
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
    Path(out_dir).mkdir(parents=False, exist_ok=True)
    if r.status_code == httpx.codes.OK:
        logging.debug(
            Path(out_dir) / Path(f"{prefix}.{file_types_extensions[file_type]}")
        )
        with open(
            Path(out_dir) / Path(f"{prefix}.{file_types_extensions[file_type]}"), "wb"
        ) as fh:
            fh.write(r.content)
        if name and file_type == "fasta":
            update_fasta_header(
                Path(f"{prefix}.{file_types_extensions[file_type]}"), guid, name
            )
    elif r.status_code == 400 and "API access" in r.json().get("message"):
        raise misc.AuthenticationError(
            f"Bad request (HTTP {r.status_code}). User lacks API permissions"
        )
    elif r.status_code == 401:
        raise misc.AuthenticationError(
            f"Authorisation failed (HTTP {r.status_code}). Invalid token?"
        )
    else:
        r.raise_for_status()  # Raises retryable httpx.HTTPError


def fetch_status(
    access_token: str,
    guids: list | dict,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
) -> list[dict]:
    """
    Return a list of dictionaries given a list of guids
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    endpoint = f"{ENVIRONMENTS_URLS[environment.value]['API']}/get_sample_detail"
    records = []
    for guid in tqdm.tqdm(guids):
        r = httpx.get(url=f"{endpoint}/{guid}", headers=headers)
        if r.is_success:
            if type(guids) == "dict":
                sample = guids[sample]
            else:  # list
                sample = r.json()[0].get("name")
            records.append({"sample": sample, "status": r.json()[0].get("status")})
        else:
            if type(guids) == "dict":
                sample = guids[sample]
            else:  # list
                sample = r.json()[0].get("name")
            records.append({"sample": sample, "status": "Unknown"})
            with logging_redirect_tqdm():
                logging.warning(f"{guid} (error {r.status_code})")
        if type(guids) is dict:
            records = pd.DataFrame(records).replace(guids).to_dict("records")

    return records


class Sample:
    """
    Represent a single sample
    """

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
        schema_name,
        working_dir,
        samtools_path,
        decontaminator_path,
        district=None,
        fastq=None,
        fastq1=None,
        fastq2=None,
        bam=None,
    ):
        self.batch = batch
        self.run_number = run_number
        self.gpas_run_number = None
        self.sample_name = sample_name
        self.fastq = fastq
        self.fastq1 = fastq1
        self.fastq2 = fastq2
        self.bam = bam
        self.control = control
        self.collection_date = collection_date
        self.tags = tags.split(":")
        self.country = country
        self.region = region
        self.district = district
        self.specimen_organism = specimen_organism
        self.host = host
        self.instrument_platform = instrument_platform
        self.primer_scheme = primer_scheme
        self.schema_name = schema_name
        self.paired = True if self.schema_name.startswith("Paired") else False
        self.decontamination_ref_path = self.get_decontamination_ref_path()
        self.working_dir = Path(working_dir)
        self.working_dir.mkdir(parents=False, exist_ok=True)
        logging.debug(f"{working_dir=}")
        self.guid = None
        self.mapping_path = None
        self.samtools_path = samtools_path
        self.decontaminator_path = decontaminator_path
        self.decontamination_stats = None

    def get_decontamination_ref_path(self):
        organisms_decontamination_references = {"SARS-CoV-2": "MN908947_no_polyA.fasta"}
        ref = organisms_decontamination_references[self.specimen_organism]
        return misc.get_data_path() / Path("refs") / Path(ref)

    def _get_decontaminate_cmd(self):
        if self.specimen_organism == "SARS-CoV-2":
            cmd = self._get_riak_cmd()
        else:
            raise misc.DecontaminationError("Invalid organism")

        command = misc.LoggedShellCommand(
            name=self.sample_name,
            action="decontamination",
            cmd=cmd,
        )
        return command

    def _get_convert_bam_cmd(self, paired=False) -> misc.LoggedShellCommand:
        prefix = Path(self.working_dir) / Path(self.sample_name)
        if not self.paired:
            cmd = f'"{self.samtools_path}" fastq -0 "{prefix}.fastq.gz" "{self.bam}"'
            self.fastq = self.working_dir / Path(self.sample_name + ".fastq.gz")
        else:
            cmd = (
                f'"{self.samtools_path}" sort -n "{self.bam}" |'
                f' "{self.samtools_path}" fastq -N'
                f' -1 "{prefix.parent / (prefix.name + "_1.fastq.gz")}"'
                f' -2 "{prefix.parent / (prefix.name + "_2.fastq.gz")}"'
                f' -s "{prefix.parent / (prefix.name + "_s.fastq.gz")}"'
            )
            self.fastq1 = self.working_dir / Path(self.sample_name + "_1.fastq.gz")
            self.fastq2 = self.working_dir / Path(self.sample_name + "_2.fastq.gz")

        command = misc.LoggedShellCommand(
            name=self.sample_name,
            action="bam_conversion",
            cmd=cmd,
        )

        return command

    def _get_riak_cmd(self) -> str:
        if not self.fastq2:
            cmd = (
                f'"{self.decontaminator_path}" --tech ont --enumerate_names'
                f' --ref_fasta "{self.decontamination_ref_path}"'
                f' --reads1 "{self.fastq}"'
                f' --outprefix "{self.working_dir / self.sample_name}"'
            )
        else:
            cmd = (
                f'"{self.decontaminator_path}" --tech illumina --enumerate_names'
                f' --ref_fasta "{self.decontamination_ref_path}"'
                f' --reads1 "{self.fastq1}"'
                f' --reads2 "{self.fastq2}"'
                f' --outprefix "{self.working_dir / self.sample_name}"'
            )
        self.clean_fastq = (
            self.working_dir / Path(self.sample_name + ".reads.fastq.gz")
            if self.fastq
            else None
        )
        self.clean_fastq1 = (
            self.working_dir / Path(self.sample_name + ".reads_1.fastq.gz")
            if self.fastq1
            else None
        )
        self.clean_fastq2 = (
            self.working_dir / Path(self.sample_name + ".reads_2.fastq.gz")
            if self.fastq2
            else None
        )
        return cmd

    def _hash_fastq(self):
        self.md5 = misc.hash_file(str(self.fastq))

    def _hash_fastqs(self):
        self.md5_1 = misc.hash_file(str(self.fastq1))
        self.md5_2 = misc.hash_file(str(self.fastq2))

    def _build_mapping_record(self) -> dict[str, Any]:
        return {
            "local_batch": self.batch,
            "local_run_number": self.run_number,
            "local_sample_name": self.sample_name,
            "gpas_run_number": self.gpas_run_number,
            "gpas_sample_name": self.guid,
        }


class Batch:
    """
    Represent a batch of samples
    """

    def __init__(
        self,
        upload_csv: Path,
        token: Path | None = None,
        working_dir: Path = Path("/tmp"),
        out_dir: Path = Path(),
        processes: int = 0,
        connections: int = 10,
        environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
        json_messages: bool = False,
        save_reads: bool = False,
        user_agent_name: str = "",
        user_agent_version: str = "",
    ):
        self.upload_csv = upload_csv
        self.token = parse_token(token) if token else None
        self.environment = environment
        self.working_dir = working_dir
        self.out_dir = out_dir
        out_dir.mkdir(parents=False, exist_ok=True)
        self.processes = processes if processes else int(multiprocessing.cpu_count())
        self.connections = connections
        self.json = {"validation": "", "decontamination": "", "submission": ""}
        self.json_messages = json_messages
        self.samtools_path = misc.get_binary_path("samtools")
        self.decontaminator_path = misc.get_binary_path("readItAndKeep")
        self.save_reads = save_reads
        self.user_agent_name = user_agent_name
        self.user_agent_version = user_agent_version
        self.client_info = {
            "name": "gpas-cli",
            "version": __version__,
            "platform": platform.system(),
            "python_version": platform.python_version(),
        }
        if self.user_agent_name or self.user_agent_version:
            self.client_info["user_agent"] = {
                "name": self.user_agent_name,
                "version": self.user_agent_version,
            }
        if self.token:
            auth_result = fetch_user_details(
                self.token["access_token"], self.environment
            )
            (
                self.user,
                self.organisation,
                self.permitted_tags,
                self.date_mask,
            ) = parse_user_details(auth_result)
            self.headers = {
                "Authorization": f"Bearer {self.token['access_token']}",
                "Content-Type": "application/json",
                "charset": "utf-8",
            }

        else:
            self.user = None
            self.organisation = None
            self.permitted_tags = []
            self.headers = {}
            self.date_mask = None
        logging.debug(f"{self.upload_csv=}")
        self.df, self.schema = validate(self.upload_csv, self.permitted_tags)
        self.schema_name = self.schema.__schema__.name
        self.schema_fields = set(self.schema.to_schema().columns.keys()) | {
            "sample_name"
        }
        self.validation_json = build_validation_message(self.df, self.schema_name)
        batch_attrs = {
            "schema_name": self.schema_name,
            "working_dir": self.working_dir,
            "samtools_path": self.samtools_path,
            "decontaminator_path": self.decontaminator_path,
        }
        self.samples = [  # Pass only schematised fields to the Sample constructor
            Sample(
                **dict((k, r[k]) for k in self.schema_fields if k in r), **batch_attrs
            )
            for r in self.df.fillna("").reset_index().to_dict("records")
        ]
        self.paired = self.samples[0].paired

        self.uploaded_on = misc.oracle_timestamp()

        self._number_runs()
        self.uploaded = False
        if self.json_messages:
            misc.print_json(self.validation_json)

    def _get_convert_bam_cmds(self) -> list[misc.LoggedShellCommand]:
        return [s._get_convert_bam_cmd() for s in self.samples]

    def _get_decontaminate_cmds(self) -> list[misc.LoggedShellCommand]:
        return [s._get_decontaminate_cmd() for s in self.samples]

    def _save_reads(self) -> None:
        save_dir = self.out_dir / "decontaminated-reads"
        save_dir.mkdir(parents=False, exist_ok=True)
        for s in self.samples:
            if s.clean_fastq:
                shutil.copy2(s.clean_fastq, save_dir)
            if s.clean_fastq1:
                shutil.copy2(s.clean_fastq1, save_dir)
            if s.clean_fastq2:
                shutil.copy2(s.clean_fastq2, save_dir)
        logging.info(f"Saved decontaminated reads to {save_dir.resolve()}")

    def _decontaminate(self) -> None:
        """
        Convert BAM files to FASTQ files, if necessary, and then decontaminate
        """
        if "Bam" in self.schema_name:  # Conversion necessary
            misc.run_parallel_logged(
                self._get_convert_bam_cmds(),
                participle="Converting",
                json_messages=self.json_messages,
                processes=self.processes,
            )
        samples_runs = misc.run_parallel_logged(
            self._get_decontaminate_cmds(),
            participle="Decontaminating",
            json_messages=self.json_messages,
            processes=self.processes,
        )
        self._parse_decontamination_stats(samples_runs)
        if self.save_reads:
            self._save_reads()

    def _parse_decontamination_stats(self, samples_runs) -> None:
        samples_decontamination_stats = {
            s: parse_decontamination_stats(r.stdout) for s, r in samples_runs.items()
        }
        for s in self.samples:
            s.decontamination_stats = samples_decontamination_stats.get(s.sample_name)

    def _hash_fastqs(self):
        if not self.paired:
            list(map(lambda s: s._hash_fastq(), self.samples))
        else:
            list(map(lambda s: s._hash_fastqs(), self.samples))

    def _get_sample_attrs(self, attr) -> dict[str, Any]:
        return {s.sample_name: getattr(s, attr) for s in self.samples}

    def _set_samples(self, name, value):
        for s in self.samples:
            setattr(s, name, value)

    def _fetch_guids(self):
        md5_attr = "md5" if not self.paired else "md5_1"
        checksums = list(self._get_sample_attrs(md5_attr).values())
        payload = {
            "batch": {
                "organisation": self.organisation,
                "uploadedOn": self.uploaded_on,
                "uploadedBy": self.user,
                "samples": checksums,
            }
        }
        logging.debug(f"_fetch_guids(): {payload=}")
        endpoint = (
            f"{ENVIRONMENTS_URLS[self.environment.value]['ORDS']}/createSampleGuids"
        )
        logging.debug(f"Fetching guids; {endpoint=}")
        r = httpx.post(url=endpoint, data=json.dumps(payload), headers=self.headers)
        if not r.is_success:
            r.raise_for_status()
        result = r.json()
        logging.debug(f"{result=}")
        self.batch_guid = result["batch"]["guid"]
        guids_hashes = {s["guid"]: s["hash"] for s in result["batch"]["samples"]}
        logging.debug(f"{guids_hashes=}")

        # Used hash-keyed dict of lists to tolerate samples with same hash
        hashes_guids = defaultdict(list)
        for g, h in guids_hashes.items():
            hashes_guids[h].append(g)

        # Assign each sample a guid from the pool associated with each hash
        for sample in self.samples:
            sample.guid = hashes_guids[getattr(sample, md5_attr)].pop()

    def _rename_fastqs(self):
        """Rename decontaminated fastqs using server-side guids"""
        for s in self.samples:
            # Workaround for ReadItAndKeep making fastas when encountering empty fastqs
            if s.clean_fastq and not s.clean_fastq.exists():
                s.clean_fastq = s.working_dir / Path(s.sample_name + ".reads.fasta.gz")
            if s.clean_fastq1 and not s.clean_fastq1.exists():
                s.clean_fastq1 = s.working_dir / Path(
                    s.sample_name + ".reads_1.fasta.gz"
                )
            if s.clean_fastq2 and not s.clean_fastq2.exists():
                s.clean_fastq2 = s.working_dir / Path(
                    s.sample_name + ".reads_2.fasta.gz"
                )

            s.clean_fastq = (
                s.clean_fastq.rename(s.working_dir / Path(s.guid + ".reads.fastq.gz"))
                if s.clean_fastq
                else None
            )
            s.clean_fastq1 = (
                s.clean_fastq1.rename(
                    s.working_dir / Path(s.guid + ".reads_1.fastq.gz")
                )
                if s.clean_fastq1
                else None
            )
            s.clean_fastq2 = (
                s.clean_fastq2.rename(
                    s.working_dir / Path(s.guid + ".reads_2.fastq.gz")
                )
                if s.clean_fastq2
                else None
            )

    def _build_mapping_csv(self):
        records = [  # Collects attrs from Sample, except for gpas_batch (Batch)
            {**s._build_mapping_record(), "gpas_batch": self.batch_guid}
            for s in self.samples
        ]
        df = pd.DataFrame(
            records,
            columns=[
                "local_batch",
                "local_run_number",
                "local_sample_name",
                "gpas_batch",
                "gpas_run_number",
                "gpas_sample_name",
            ],
        )
        arbitrary_fields = [c for c in self.df.columns if c not in self.schema_fields]
        combined_df = pd.merge(
            df,
            self.df[[*arbitrary_fields]],
            left_on="local_sample_name",
            right_index=True,
        )
        target_path = self.out_dir / Path(self.batch_guid + ".mapping.csv")
        combined_df.to_csv(target_path, index=False)
        self.mapping_path = target_path
        logging.info(f"Saved mapping CSV to {self.mapping_path}")

    def _fetch_par(self):
        """Private method that calls ORDS to get a Pre-Authenticated Request.

        The PAR url is used to upload data to the Organisation's input bucket in OCI

        Returns
        -------
        par: str
        """
        endpoint = f"{ENVIRONMENTS_URLS[self.environment.value]['ORDS']}/pars"
        logging.debug(f"Fetching PAR; {endpoint=} {self.headers=}")
        r = httpx.get(url=endpoint, headers=self.headers)
        if not r.is_success:
            r.raise_for_status()
        result = json.loads(r.content)
        logging.debug(f"{result=}")
        if result.get("status") == "error":
            raise RuntimeError("Problem fetching PAR")
        logging.debug(f"{result}, {r.status_code}")
        self.par = result["par"]
        self.bucket = self.par.split("/")[-3]
        self.batch_url = self.par + self.batch_guid + "/"

    def _get_uploads(self) -> list[misc.SampleUpload]:
        """Return local file path and destination URL for the file to be uploaded"""
        uploads = []
        for s in self.samples:
            url_prefix = self.batch_url + s.guid
            if not s.paired:
                uploads.append(
                    misc.SampleUpload(
                        name=s.sample_name,
                        path1=s.clean_fastq,
                        path2=None,
                        url1=f"{url_prefix}.reads.fastq.gz",
                        url2=None,
                    )
                )
            else:
                uploads.append(
                    misc.SampleUpload(
                        name=s.sample_name,
                        path1=s.clean_fastq1,
                        path2=s.clean_fastq2,
                        url1=f"{url_prefix}.reads_1.fastq.gz",
                        url2=f"{url_prefix}.reads_2.fastq.gz",
                    )
                )
        return uploads

    def _upload_samples(self) -> None:
        if self.json_messages:
            misc.print_progress_message_json(action="upload", status="started")
        uploads = self._get_uploads()
        if self.connections == 1:
            logging.debug("Single upload process")
            for upload in uploads:
                misc.upload_sample(
                    upload=upload,
                    headers=self.headers,
                    json_messages=self.json_messages,
                )
        else:
            process_map(
                partial(
                    misc.upload_sample,
                    headers=self.headers,
                    json_messages=self.json_messages,
                ),
                uploads,
                max_workers=self.connections,
                desc=f"Uploading {len(uploads)} sample(s)",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
                leave=False,
            )
        self.uploaded = True
        if self.json_messages:
            misc.print_progress_message_json(action="upload", status="finished")
        else:
            logging.info(f"Finished uploading {len(uploads)} sample(s)")

    def _finalise_submission(self):
        @retry(
            retry=retry_if_exception_type(httpx.HTTPError),
            wait=wait_fixed(60),
            stop=stop_after_attempt(2),
            before_sleep=before_sleep.before_sleep_log(logger, 10),
        )
        def post_submission(submission: dict, headers: dict):
            """Submit sample metadata to batches endpoint"""
            endpoint = f"{ENVIRONMENTS_URLS[self.environment.value]['ORDS']}/batches"
            logging.debug(f"post_submission(): {json.dumps(self.submission, indent=4)}")
            r = httpx.post(
                url=endpoint,
                data=json.dumps(submission, ensure_ascii=False).encode("utf-8"),
                headers=headers,
                timeout=80,
            )
            logging.debug(f"post_submission(): {r.text=}")
            r.raise_for_status()
            if r.json().get("status") != "success":
                raise misc.SubmissionError(r.json().get("errorMsg"))

        @retry(
            retry=retry_if_exception_type(httpx.HTTPError),
            wait=wait_fixed(10),
            stop=stop_after_attempt(2),
            before_sleep=before_sleep.before_sleep_log(logger, 10),
        )
        def put_done_mark(batch_guid: str, headers: dict):
            """Put upload done marker"""
            url = self.par + batch_guid + "/upload_done.txt"
            logging.debug(f"put_done_mark(): {url=}")
            r = httpx.put(url=url, headers=self.headers)
            logging.debug(f"put_done_mark(): {r.text=}")
            r.raise_for_status()

        if not self.uploaded:
            raise RuntimeError("Reads not uploaded")

        self.submission["batch"]["uploader"]["upload_finish"] = misc.oracle_timestamp()
        post_submission(self.submission, self.headers)
        put_done_mark(self.batch_guid, self.headers)
        logging.info(f"Submitted batch {self.batch_guid}")
        success_message = {
            "submission": {
                "status": "success",
                "batch": self.batch_guid,
                "samples": [s.sample_name for s in self.samples],
            }
        }
        if self.json_messages:
            misc.print_json(success_message)

    def _submit(self):
        self._fetch_par()
        self._upload_samples()
        self._prepare_submission()
        self._finalise_submission()

    def _prepare_submission(self):
        """Prepare the JSON payload for the GPAS Upload app

        Returns
        -------
            dict : JSON payload to pass to GPAS Electron upload app via STDOUT
        """

        samples = []
        for s in self.samples:
            if self.date_mask in {"WEEK", "MONTH"}:
                dt = datetime.datetime.strptime(s.collection_date, "%Y-%m-%d")
                if self.date_mask == "MONTH":
                    s.collection_date = dt.replace(day=1).strftime("%Y-%m-%d")
                elif self.date_mask == "WEEK":
                    td = datetime.timedelta(days=dt.weekday())
                    s.collection_date = (dt - td).strftime("%Y-%m-%d")
                logging.info(
                    f"Masked collection dates to start of {self.date_mask.lower()}"
                )
            sample = {
                "name": s.guid,
                "run_number": s.gpas_run_number,
                "tags": s.tags,
                "control": s.control,
                "collection_date": s.collection_date,
                "country": s.country,
                "region": s.region,
                "district": s.district,
                "specimen": s.specimen_organism,
                "host": s.host,
                "instrument": {"platform": s.instrument_platform},
                "primer_scheme": s.primer_scheme,
                "decontamination_stats": s.decontamination_stats,
            }
            if self.paired:
                sample["pe_reads"] = {
                    "r1_uri": str(s.clean_fastq1),
                    "r1_md5": s.md5_1,
                    "r2_uri": str(s.clean_fastq2),
                    "r2_md5": s.md5_2,
                }
                logging.debug(f"{s.clean_fastq1=}, {s.clean_fastq2=}")
            else:
                sample["se_reads"] = {"uri": str(s.clean_fastq), "md5": s.md5}
            samples.append(sample)

        self.submission = {
            "status": "completed",
            "batch": {
                "file_name": self.batch_guid,
                "bucket_name": self.bucket,
                "uploaded_on": self.uploaded_on,
                "uploaded_by": self.user,
                "organisation": self.organisation,
                "run_numbers": self.run_numbers,
                "samples": samples,
                "uploader": {
                    **self.client_info,
                    "upload_start": self.uploaded_on,
                    "upload_finish": None,  # Set in _finalise_submission()
                },
            },
        }

    def post_exception(self, exception: Exception):
        """Report exceptions occurring during Batch.upload()"""
        try:
            if "PYTEST_CURRENT_TEST" in os.environ:  # Disable reporting under pytest
                return
            e_t, e_v, e_tb = misc.get_value_traceback_fmt(exception)
            endpoint = (
                f"{ENVIRONMENTS_URLS[self.environment.value]['ORDS']}/logUploaderError"
            )
            payload = {
                "exception": {
                    "class": e_t,
                    "message": e_v,
                    "traceback": e_tb,
                    "timestamp": misc.oracle_timestamp(),
                },
                "uploader": self.client_info,
            }
            logging.debug(f"Exception payload {payload=}")
            r = httpx.post(url=endpoint, json=payload, headers=self.headers)
            logging.debug(f"Exception submission {r.is_success=}")
        except Exception:
            pass

    def upload(self, dry_run: bool = False, save_reads: bool = False) -> None:
        try:
            logging.info(
                f"Using {self.processes} process(es), {self.connections} connection(s)"
            )
            self._decontaminate()
            if not self.headers:
                logging.warning("No token provided, quitting")
                sys.exit()
            self._hash_fastqs()
            self._fetch_guids()
            self._build_mapping_csv()
            self._rename_fastqs()
            if dry_run:
                logging.info("Skipped submission")
            else:
                self._submit()
        except Exception as e:
            self.post_exception(e)
            raise e

    def _number_runs(self) -> None:
        """Enumerate unique values of run_number for submission"""
        samples_runs = self._get_sample_attrs("run_number")
        logging.debug(f"{samples_runs=}")
        samples_run_numbers = misc.number_runs(samples_runs)
        logging.debug(f"{samples_run_numbers=}")
        for s in self.samples:
            s.gpas_run_number = samples_run_numbers[s.sample_name]
        self.run_numbers = list(filter(None, samples_run_numbers.values()))
        logging.debug(f"{self.run_numbers=}")


def parse_decontamination_stats(stdout: str) -> dict:
    """
    Parse read-it-and-keep kept and discarded read counts
    """
    lines = stdout.strip().splitlines()
    counts = [int(l.rpartition("\t")[2]) for l in lines]
    count_in = counts[0] + counts[1]
    count_out = counts[2] + counts[3]
    delta = count_in - count_out
    assert delta >= 0
    try:
        fraction = round(delta / count_in, 4)
    except ArithmeticError:  # ZeroDivisionError
        fraction = 0
    return {
        "in": count_in,
        "out": count_out,
        "fraction": fraction,
    }
