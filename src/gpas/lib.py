import json
import asyncio
import logging

from pathlib import Path

import httpx
import requests

import pandas as pd

from tqdm import tqdm

from gpas.misc import (
    ENVIRONMENTS,
    ENDPOINTS,
    GOOD_STATUSES,
    FILE_TYPES,
    DEFAULT_ENVIRONMENT,
)


def parse_token(token):
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


def fetch_status(
    guids: list,
    access_token: str,
    environment: ENVIRONMENTS = DEFAULT_ENVIRONMENT,
    raw: bool = False,
) -> list:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    endpoint = (
        ENDPOINTS[environment.value]["HOST"]
        + ENDPOINTS[environment.value]["API_PATH"]
        + "get_sample_detail/"
    )
    """
    Return a list of dictionaries given a list of guids
    """
    records = []
    for guid in tqdm(guids):
        r = requests.get(url=endpoint + guid, headers=headers)
        if r.ok:
            if raw:
                records.append(r.json())
            else:
                records.append(
                    dict(
                        sample=r.json()[0].get("name"), status=r.json()[0].get("status")
                    )
                )
        else:
            logging.warning(f"{guid} (error {r.status_code})")
    return records


async def async_fetch_status_single(client, guid, url, headers):
    # if '657a8b5a' in url:
    #     url += '-cat'
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
                f"Authorisation failed (HTTP {r.status_code}). Ensure token is valid"
            )
    return result


async def async_fetch_status(
    guids: list, access_token: str, environment: ENVIRONMENTS, raw: bool = False
) -> list:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    endpoint = (
        ENDPOINTS[environment.value]["HOST"]
        + ENDPOINTS[environment.value]["API_PATH"]
        + "get_sample_detail"
    )
    transport = httpx.AsyncHTTPTransport(retries=2)
    async with httpx.AsyncClient(transport=transport) as client:
        guids_urls = {guid: f"{endpoint}/{guid}" for guid in guids}
        tasks = [
            async_fetch_status_single(client, guid, url, headers)
            for guid, url in guids_urls.items()
        ]
        return [
            await f
            for f in tqdm(
                asyncio.as_completed(tasks),
                desc=f"Querying status for {len(guids)} samples",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
                total=len(tasks),
            )
        ]
        # results = []
        # for future in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        #         result = await future
        #         results.append(result)
        # return results


async def async_download(
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
    logging.info(f"Fetching file types {file_types}")
    transport = httpx.AsyncHTTPTransport(retries=5)
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    async with httpx.AsyncClient(transport=transport, limits=limits) as client:
        guids_types_urls = {}
        for guid in guids:
            for file_type in file_types:
                guids_types_urls[(guid, file_type)] = f"{endpoint}/{guid}/{file_type}"
        tasks = [
            async_download_single(
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


async def async_download_single(
    client, guid, file_type, url, headers, out_dir, name=None
):
    file_types_extensions = {
        "json": "json",
        "fasta": "fasta.gz",
        "bam": "bam",
        "vcf": "vcf",
    }
    # if '657a8b5a' in url:
    #     url += '-cat'
    prefix = name if name else guid
    r = await client.get(url=url, headers=headers)
    if r.status_code == httpx.codes.ok:
        Path(out_dir)
        with open(
            Path(out_dir) / Path(f"{prefix}.{file_types_extensions[file_type]}"), "wb"
        ) as fh:
            fh.write(r.content)
    else:
        result = dict(sample=guid, status="UNKNOWN")
        logging.warning(f"Skipping {guid}.{file_type} (HTTP {r.status_code})")
