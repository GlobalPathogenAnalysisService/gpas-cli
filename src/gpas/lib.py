import logging

from pathlib import Path

import requests

from gpas.misc import ENDPOINTS

from gpas.misc import ENVIRONMENTS


logging.basicConfig(level=logging.INFO)


def fetch_status(
    guids: list,
    access_token: str,
    environment: ENVIRONMENTS,
    raw: bool) -> list:
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    endpoint = ENDPOINTS[environment.value]['HOST'] + ENDPOINTS[environment.value]['API_PATH'] + 'get_sample_detail/'
    '''
    Return a list of dictionaries given a list of guids
    '''
    records = []
    for guid in guids:
        r = requests.get(url=endpoint+guid, headers=headers)    
        if r.ok:
            if raw:
                records.append(r.json())
            else:
                records.append(dict(sample=r.json()[0].get('name'), status=r.json()[0].get('status')))
        else:
            logging.warning(f'{guid}, status:{r.status_code}')
    return records


def download():
    pass