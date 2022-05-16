from _typeshed import Incomplete
from gpas import lib as lib
from gpas.misc import (
    DEFAULT_ENVIRONMENT as DEFAULT_ENVIRONMENT,
    DISPLAY_FORMATS as DISPLAY_FORMATS,
    ENVIRONMENTS as ENVIRONMENTS,
    FILE_TYPES as FILE_TYPES,
    run as run,
)
from pathlib import Path

logger: Incomplete

def upload(
    upload_csv: Path,
    token: Path,
    *,
    working_dir: Path = ...,
    environment: ENVIRONMENTS = ...,
    mapping_prefix: str = ...,
    threads: int = ...,
    dry_run: bool = ...,
    json: bool = ...
): ...
def validate(
    upload_csv: Path,
    *,
    token: Path = ...,
    environment: ENVIRONMENTS = ...,
    json: bool = ...
): ...
def download(
    token: Path,
    mapping_csv: Path = ...,
    guids: str = ...,
    environment: ENVIRONMENTS = ...,
    file_types: str = ...,
    out_dir: Path = ...,
    rename: bool = ...,
): ...
def status(
    token: Path,
    *,
    mapping_csv: Path = ...,
    guids: str = ...,
    environment: ENVIRONMENTS = ...,
    format: DISPLAY_FORMATS = ...,
    rename: bool = ...,
    raw: bool = ...
): ...
def main() -> None: ...
