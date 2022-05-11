# import pytest


# def pytest_addoption(parser):
#     parser.addoption(
#         "--online", action="store_true", default=False, help="run online tests"
#     )


# def pytest_configure(config):
#     config.addinivalue_line("markers", "online: mark test as online to run")


# def pytest_collection_modifyitems(config, items):
#     if config.getoption("--online"):
#         # --online given in cli: do not skip online tests
#         return
#     skip_online = pytest.mark.skip(reason="need --online option to run")
#     for item in items:
#         if "online" in item.keywords:
#             item.add_marker(skip_online)
