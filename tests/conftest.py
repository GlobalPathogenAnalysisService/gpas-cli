import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--online", action="store_true", default=False, help="run online tests"
    )
    parser.addoption(
        "--upload", action="store_true", default=False, help="run upload tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "online: mark test as online to run")
    config.addinivalue_line("markers", "upload: mark test as upload to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--online"):
        return
    elif config.getoption("--upload"):
        return
    skip_online = pytest.mark.skip(reason="need --online option to run")
    skip_upload = pytest.mark.skip(reason="need --upload option to run")

    for item in items:
        if "online" in item.keywords:
            item.add_marker(skip_online)
        elif "upload" in item.keywords:
            item.add_marker(skip_upload)
