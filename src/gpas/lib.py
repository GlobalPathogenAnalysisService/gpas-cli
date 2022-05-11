import json
import logging

from pathlib import Path

import tqdm
import requests

import pandas as pd

from gpas.misc import ENVIRONMENTS, ENDPOINTS


logging.basicConfig(level=logging.INFO)


def parse_token(token):
    return json.loads(token.read_text())


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
    for guid in tqdm.tqdm(guids):
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


def parse_mapping(mapping_csv: Path = None):
    df = pd.read_csv(mapping_csv)
    return df['gpas_sample_name'].tolist()


def download():
    pass
