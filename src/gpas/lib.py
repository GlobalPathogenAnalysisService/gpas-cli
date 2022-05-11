import json
import asyncio
import logging

from pathlib import Path

import httpx
import requests

import pandas as pd

from tqdm import tqdm

from gpas.misc import ENVIRONMENTS, ENDPOINTS


logging.basicConfig(level=logging.INFO)


def parse_token(token):
    return json.loads(token.read_text())


def parse_mapping(mapping_csv: Path = None):
    return pd.read_csv(mapping_csv)


def fetch_status(
    guids: list, access_token: str, environment: ENVIRONMENTS, raw: bool
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
        result = dict(sample=r_json.get("name"), status=r_json.get("status"))
    else:
        result = dict(sample=guid, status="UNKNOWN")
        logging.warning(f"HTTP {r.status_code} ({guid})")
    return result


async def async_fetch_status(
    guids: list, access_token: str, environment: ENVIRONMENTS, raw: bool
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
        return [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))]
        # results = []
        # for future in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        #         result = await future
        #         results.append(result)
        # return results


def download():
    pass
