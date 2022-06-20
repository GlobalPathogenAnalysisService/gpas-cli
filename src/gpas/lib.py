import asyncio
import datetime
import gzip
import json
import logging
import multiprocessing
import sys
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import requests
import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from gpas import data_dir, misc, validation
from gpas.misc import (
    DEFAULT_ENVIRONMENT,
    ENDPOINTS,
    ENVIRONMENTS,
    FILE_TYPES,
    GOOD_STATUSES,
)
from gpas.validation import build_validation_message, validate


class AuthenticationError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


def parse_token(token: Path) -> dict:
    return json.loads(token.read_text())


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


def fetch_user_details(access_token, environment: ENVIRONMENTS):
    """Test API authentication and return user details, otherwise exit"""
    endpoint = (
        ENDPOINTS[environment.value]["HOST"]
        + ENDPOINTS[environment.value]["ORDS_PATH"]
        + "userOrgDtls"
    )
    try:
        logging.debug(f"Fetching user details {endpoint=}")
        r = requests.get(endpoint, headers={"Authorization": f"Bearer {access_token}"})
        if not r.ok:
            r.raise_for_status()
        result = r.json().get("userOrgDtl", {})[0]
        logging.debug(f"{result=}")
        user = result.get("userName")
        organisation = result.get("organisation")
        allowed_tags = [d.get("tagName") for d in result.get("tags", {})]
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        if status_code == 401:
            raise AuthenticationError(
                f"Authentication failed, check access token and environment"
            ) from None
        else:
            raise e from None
    return user, organisation, allowed_tags


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
    warn: bool = False,
    raw: bool = False,
) -> list[dict]:
    """Returns a list of dicts of containing status records"""
    fetch_user_details(access_token, environment)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    endpoint = (
        ENDPOINTS[environment.value]["HOST"]
        + ENDPOINTS[environment.value]["API_PATH"]
        + "get_sample_detail"
    )

    limits = httpx.Limits(
        max_keepalive_connections=10, max_connections=20, keepalive_expiry=10
    )
    transport = httpx.AsyncHTTPTransport(limits=limits, retries=5)
    async with httpx.AsyncClient(transport=transport, timeout=30) as client:
        guids_urls = {guid: f"{endpoint}/{guid}" for guid in guids}
        tasks = [
            fetch_status_single_async(client, guid, url, headers, warn)
            for guid, url in guids_urls.items()
        ]
        records = [
            await f
            for f in tqdm.tqdm(
                asyncio.as_completed(tasks),
                desc=f"Querying status for {len(guids)} samples",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
                total=len(tasks),
            )
        ]

    if type(guids) is dict:
        logging.debug("Renaming")
        records = pd.DataFrame(records).replace(guids).to_dict("records")

    return records


async def fetch_status_single_async(
    client, guid, url, headers, warn=False, n_retries=5
):
    r = await client.get(url=url, headers=headers)
    if r.status_code == httpx.codes.ok:
        r_json = r.json()[0]
        status = r_json.get("status")
        result = dict(sample=guid, status=status)
        if warn and status not in GOOD_STATUSES:
            with logging_redirect_tqdm():
                logging.warning(f"Skipping {guid} (status {status})")
    else:
        result = dict(sample=guid, status="Unknown")
        with logging_redirect_tqdm():
            logging.warning(f"HTTP {r.status_code} ({guid})")
        if r.status_code == 401:
            raise RuntimeError(
                f"Authorisation failed (HTTP {r.status_code}). Invalid token?"
            )
    return result


async def download_async(
    access_token: str,
    guids: list | dict,
    file_types: list[str] = ["fasta"],
    out_dir: Path = Path.cwd(),
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    raw: bool = False,
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
    with logging_redirect_tqdm():
        logging.info(f"Fetching file types {file_types}")

    limits = httpx.Limits(
        max_keepalive_connections=8, max_connections=16, keepalive_expiry=100
    )
    transport = httpx.AsyncHTTPTransport(limits=limits, retries=5)
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
                desc=f"Downloading {len(tasks)} files for {len(guids)} samples",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
                total=len(tasks),
            )
        ]


async def download_single_async(
    client, guid, file_type, url, headers, out_dir, name=None, retries=5
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
    if r.status_code == httpx.codes.ok:
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
    else:
        result = dict(sample=guid, status="Unknown")
        with logging_redirect_tqdm():
            logging.warning(f"Skipping {guid}.{file_type} (HTTP {r.status_code})")


def fetch_status(
    access_token: str,
    guids: list | dict,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    raw: bool = False,
) -> list[dict]:
    """
    Return a list of dictionaries given a list of guids
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    endpoint = (
        ENDPOINTS[environment.value]["HOST"]
        + ENDPOINTS[environment.value]["API_PATH"]
        + "get_sample_detail/"
    )
    records = []
    for guid in tqdm.tqdm(guids):
        r = requests.get(url=endpoint + guid, headers=headers)
        if r.ok:
            if raw:
                records.append(r.json())
            else:
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
        self.uploaded = False
        self.guid = None
        self.mapping_path = None

    def get_decontamination_ref_path(self):
        organisms_decontamination_references = {"SARS-CoV-2": "MN908947_no_polyA.fasta"}
        ref = organisms_decontamination_references[self.specimen_organism]
        return data_dir / Path("refs") / Path(ref)

    def _get_decontaminate_cmd(self):
        if self.specimen_organism == "SARS-CoV-2":
            cmd = self._get_riak_cmd()
        else:
            raise misc.DecontaminationError("Invalid organism")

        before_msg = {
            "progress": {
                "action": "decontamination",
                "status": "started",
                "sample": self.sample_name,
            }
        }
        after_msg = {
            "progress": {
                "action": "decontamination",
                "status": "finished",
                "sample": self.sample_name,
            }
        }
        command = misc.LoggedShellCommand(
            name=self.sample_name, cmd=cmd, before_msg=before_msg, after_msg=after_msg
        )
        return command

    def _get_convert_bam_cmd(self, paired=False) -> str:
        prefix = Path(self.working_dir) / Path(self.sample_name)
        if not self.paired:
            cmd = f"samtools fastq -0 {prefix.with_suffix('.fastq.gz')} {self.bam}"
            self.fastq = self.working_dir / Path(self.sample_name + ".fastq.gz")
        else:
            cmd = (
                f"samtools sort {self.bam} |"
                f" samtools fastq -N"
                f" -1 {prefix.parent / (prefix.name + '_1.fastq.gz')}"
                f" -2 {prefix.parent / (prefix.name + '_2.fastq.gz')}"
            )
            self.fastq1 = self.working_dir / Path(self.sample_name + "_1.fastq.gz")
            self.fastq2 = self.working_dir / Path(self.sample_name + "_2.fastq.gz")

        before_msg = {
            "progress": {
                "action": "bam_conversion",
                "status": "started",
                "sample_name": self.sample_name,
            }
        }
        after_msg = {
            "progress": {
                "action": "bam_conversion",
                "status": "finished",
                "sample_name": self.sample_name,
            }
        }
        command = misc.LoggedShellCommand(
            name=self.sample_name, cmd=cmd, before_msg=before_msg, after_msg=after_msg
        )

        return command

    def _get_riak_cmd(self) -> str:
        if not self.fastq2:
            cmd = (
                f"readItAndKeep --tech ont --enumerate_names"
                f" --ref_fasta {self.decontamination_ref_path}"
                f" --reads1 {self.fastq}"
                f" --outprefix {self.working_dir / self.sample_name}"
            )
        else:
            cmd = (
                f"readItAndKeep --tech illumina --enumerate_names"
                f" --ref_fasta {self.decontamination_ref_path}"
                f" --reads1 {self.fastq1}"
                f" --reads2 {self.fastq2}"
                f" --outprefix {self.working_dir / self.sample_name}"
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

    def _upload_reads(self, batch_url, headers, json_messages):
        """Upload an unpaired FASTQ file to the Organisation's input bucket in OCI"""
        before_msg = {
            "progress": {
                "action": "upload",
                "status": "started",
                "sample_name": self.sample_name,
            }
        }
        after_msg = {
            "progress": {
                "action": "upload",
                "status": "finished",
                "sample_name": self.sample_name,
            }
        }
        url_prefix = batch_url + self.guid
        if not self.uploaded:
            if json_messages:
                print(json.dumps(before_msg, indent=4))
            if not self.paired:
                with open(self.clean_fastq, "rb") as fh:
                    r = requests.put(
                        url=f"{url_prefix}.reads.fastq.gz", data=fh, headers=headers
                    )
            else:
                with open(self.clean_fastq1, "rb") as fh:
                    r = requests.put(
                        f"{url_prefix}.reads_1.fastq.gz", data=fh, headers=headers
                    )
                with open(self.clean_fastq2, "rb") as fh:
                    r = requests.put(
                        f"{url_prefix}.reads_2.fastq.gz", data=fh, headers=headers
                    )
            if json_messages:
                print(json.dumps(after_msg, indent=4))
        self.uploaded = True


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
        environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
        json_messages: bool = False,
    ):
        self.upload_csv = upload_csv
        self.token = parse_token(token) if token else None
        self.environment = environment
        self.working_dir = working_dir
        self.out_dir = out_dir
        self.processes = (
            processes if processes else int(multiprocessing.cpu_count() / 2)
        )
        self.json = {"validation": "", "decontamination": "", "submission": ""}
        self.json_messages = json_messages
        if self.token:
            (
                self.user,
                self.organisation,
                self.permitted_tags,
            ) = fetch_user_details(self.token["access_token"], self.environment)
            self.headers = {
                "Authorization": f"Bearer {self.token['access_token']}",
                "Content-Type": "application/json",
            }

        else:
            self.user = None
            self.organisation = None
            self.permitted_tags = []
            self.headers = None
        self.df, self.schema_name = validate(self.upload_csv, self.permitted_tags)
        self.validation_json = build_validation_message(self.df, self.schema_name)
        self.errors = {"decontamination": [], "submission": []}
        batch_attrs = {
            "schema_name": self.schema_name,
            "working_dir": self.working_dir,
        }
        self.samples = [
            Sample(**r, **batch_attrs)
            for r in self.df.fillna("").reset_index().to_dict("records")
        ]
        self.paired = self.samples[0].paired

        currentTime = (
            datetime.datetime.now(datetime.timezone.utc)
            .astimezone()
            .isoformat(timespec="milliseconds")
        )
        tzStartIndex = len(currentTime) - 6
        self.uploaded_on = currentTime[:tzStartIndex] + "Z" + currentTime[tzStartIndex:]

        self._number_runs()

        if self.json_messages:
            print(json.dumps(self.validation_json, indent=4))

    def _get_convert_bam_cmds(self) -> list[misc.LoggedShellCommand]:
        return [s._get_convert_bam_cmd() for s in self.samples]

    def _get_decontaminate_cmds(self) -> list[misc.LoggedShellCommand]:
        return [s._get_decontaminate_cmd() for s in self.samples]

    def _decontaminate(self):
        if "Bam" in self.schema_name:  # Conversion necessary
            misc.run_parallel_logged(
                self._get_convert_bam_cmds(),
                participle="Converting",
                json_messages=self.json_messages,
            )
        misc.run_parallel_logged(
            self._get_decontaminate_cmds(),
            participle="Decontaminating",
            json_messages=self.json_messages,
        )

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
        if not self.headers:
            logging.warning("No token provided, quitting after decontamination")
            for s in self.samples:
                logging.info(
                    f"{s.sample_name}: {s.clean_fastq}"
                ) if s.clean_fastq else None
                logging.info(
                    f"{s.sample_name}: {s.clean_fastq1} {s.clean_fastq2}"
                ) if s.clean_fastq1 else None
            sys.exit()
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
        endpoint = (
            ENDPOINTS[self.environment.value]["HOST"]
            + ENDPOINTS[self.environment.value]["ORDS_PATH"]
            + "createSampleGuids"
        )
        logging.debug(f"Fetching guids; {endpoint=}")
        r = requests.post(url=endpoint, data=json.dumps(payload), headers=self.headers)
        if not r.ok:
            r.raise_for_status()
        result = r.json()
        logging.debug(f"{result=}")
        self.batch_guid = result["batch"]["guid"]
        hashes_guids = {s["hash"]: s["guid"] for s in result["batch"]["samples"]}
        for sample in self.samples:
            sample.guid = hashes_guids[getattr(sample, md5_attr)]

    def _rename_fastqs(self):
        """Rename decontaminated fastqs using server-side guids"""
        for s in self.samples:
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
            # print(type(s.fastq))
            # print(s.fastq)
            # print(s.clean_fastq)

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
        Path(self.out_dir).mkdir(parents=False, exist_ok=True)
        target_path = self.out_dir / Path(self.batch_guid + ".mapping.csv")
        df.to_csv(target_path, index=False)
        self.mapping_path = target_path
        logging.info(f"Saved mapping CSV to {self.mapping_path}")

    def _fetch_par(self):
        """Private method that calls ORDS to get a Pre-Authenticated Request.

        The PAR url is used to upload data to the Organisation's input bucket in OCI

        Returns
        -------
        par: str
        """
        endpoint = (
            ENDPOINTS[self.environment.value]["HOST"]
            + ENDPOINTS[self.environment.value]["ORDS_PATH"]
            + "pars"
        )
        logging.debug(f"Fetching PAR; {endpoint=} {self.headers=}")
        r = requests.get(url=endpoint, headers=self.headers)
        if not r.ok:
            r.raise_for_status()
        result = json.loads(r.content)
        logging.debug(f"{result=}")
        if result.get("status") == "error":
            raise RuntimeError("Problem fetching PAR")
        logging.debug(f"{result}, {r.status_code}")
        self.par = result["par"]
        self.bucket = self.par.split("/")[-3]
        self.batch_url = self.par + self.batch_guid + "/"

    def _upload_samples(self):
        for s in tqdm.tqdm(
            self.samples,
            desc=f"Uploading",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            leave=False,
        ):
            s._upload_reads(self.batch_url, self.headers, self.json_messages)
            with logging_redirect_tqdm():
                logging.info(f"Uploaded {s.sample_name} ({s.guid})")

    def _finalise_submission(self):
        endpoint = (
            ENDPOINTS[self.environment.value]["HOST"]
            + ENDPOINTS[self.environment.value]["ORDS_PATH"]
            + "batches"
        )
        r = requests.get(url=endpoint, json=self.submission, headers=self.headers)
        r.raise_for_status()
        logging.debug(f"POSTing JSON {r.text=}")
        if r.json().get("status") != "success":
            raise misc.SubmissionError(r.json().get("errorMsg"))
        url = self.par + self.batch_guid + "/upload_done.txt"  # Finalisation mark
        r = requests.put(url=url, headers=self.headers)
        r.raise_for_status()
        logging.info(f"Finished uploading batch {self.batch_guid}")
        success_message = {
            "submission": {
                "status": "success",
                "batch": self.batch_guid,
                "samples": [s.sample_name for s in self.samples],
            }
        }
        if self.json_messages:
            print(json.dumps(success_message, indent=4))

    def _submit(self):
        try:
            self._fetch_par()
            self._upload_samples()
            self._prepare_submission()
            self._finalise_submission()
        except Exception as e:
            raise misc.SubmissionError(e) from None

    def _prepare_submission(self):
        """Prepare the JSON payload for the GPAS Upload app

        Returns
        -------
            dict : JSON payload to pass to GPAS Electron upload app via STDOUT
        """
        # self.sample_sheet = copy.deepcopy(self.df[['batch', 'run_number', 'sample_name', 'gpas_batch', 'gpas_run_number', 'gpas_sample_name']])
        # self.sample_sheet.rename(columns={'batch': 'local_batch', 'run_number': 'gpas_run_number', 'sample_name': 'local_sample_name'}, inplace=True)
        # self.df.set_index('gpas_sample_name', inplace=True)

        samples = []
        for s in self.samples:
            sample = {
                "name": s.guid,
                "run_number": s.gpas_run_number,
                "tags": s.tags,
                "control": s.control,
                "collection_date": str(s.collection_date.date()),
                "country": s.country,
                "region": s.region,
                "district": s.district,
                "specimen": s.specimen_organism,
                "host": s.host,
                "instrument": {"platform": s.instrument_platform},
                "primer_scheme": s.primer_scheme,
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
                "uploaded_on": str(self.uploaded_on),
                "uploaded_by": self.user,
                "organisation": self.organisation,
                # "run_numbers": list(self._get_sample_attrs('gpas_run_number').values()),
                "samples": samples,
            },
        }
        logging.debug(json.dumps(self.submission, indent=4))

    def upload(self, dry_run: bool = False):
        logging.info(f"Using {self.processes} processes")
        self._decontaminate()
        self._hash_fastqs()
        self._fetch_guids()
        self._build_mapping_csv()
        self._rename_fastqs()
        if not dry_run:
            self._submit()
        for s in self.samples:
            logging.debug(
                # f"{s.sample_name=}; {s.md5=}; {s.decontamination_stats=}"
                f" {s.clean_fastq}; {s.clean_fastq1}; {s.clean_fastq2}"
            )

    def _number_runs(self) -> None:
        """Enumerate unique values of run_number for submission"""
        samples_runs = self._get_sample_attrs("run_number")
        runs = set(samples_runs.values())
        if list(filter(None, runs)):  # More than just an empty string
            runs_numbers = {r: str(i) for i, r in enumerate(runs, start=1)}
        else:
            runs_numbers = {"": ""}
        # print(f"{samples_runs=}, {runs=}, {runs_numbers=} {bool(runs)}")
        for s in self.samples:
            s.gpas_run_number = runs_numbers[s.run_number]


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
    return {
        "in": count_in,
        "out": count_out,
        "fraction": round(delta / count_in, 4),
    }
