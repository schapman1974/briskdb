"""Target-neutral fixture for the frozen TinyMongo v1 contract corpus."""

import os
import re

import pytest

from ..adapters import TargetUnavailable, open_target
from .support import ContractTarget


APIS = ("sync", "async")
TARGETS = ("tinymongo", "mongodb", "briskdb")


def pytest_addoption(parser):
    group = parser.getgroup("mongo-contract")
    group.addoption(
        "--mongo-contract-target",
        choices=TARGETS,
        default=os.environ.get("BRISKDB_MONGO_CONTRACT_TARGET", "tinymongo"),
        help="implementation adapter used by the frozen Mongo contract",
    )
    group.addoption(
        "--mongo-contract-api",
        choices=("sync", "async", "both"),
        default=os.environ.get("BRISKDB_MONGO_CONTRACT_API", "both"),
        help="API flavor to execute (default: both)",
    )
    group.addoption(
        "--mongo-contract-backend",
        default=os.environ.get("BRISKDB_MONGO_CONTRACT_BACKEND", "memory"),
        help="TinyMongo backend ID (default: memory)",
    )
    group.addoption(
        "--mongo-contract-mongodb-uri",
        default=os.environ.get("BRISKDB_MONGODB_URI"),
        help="URI for the optional real MongoDB adapter",
    )
    group.addoption(
        "--mongo-contract-briskdb-uri",
        default=os.environ.get("BRISKDB_MONGO_PARITY_BRISKDB_URI"),
        help="URI for the BriskDB Mongo-compatible candidate",
    )
    group.addoption(
        "--mongo-contract-require-target",
        action="store_true",
        default=False,
        help="turn an unavailable optional target into a test failure",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "contract: frozen Mongo compatibility contract")
    config.addinivalue_line(
        "markers",
        "client_options(**kwargs): constructor options required by one contract",
    )


def _backend_id(config):
    target = config.getoption("--mongo-contract-target")
    if target == "mongodb":
        return "mongodb"
    if target == "briskdb":
        return "briskdb"
    return config.getoption("--mongo-contract-backend")


def _target_uri(config, target):
    if target == "briskdb":
        return config.getoption("--mongo-contract-briskdb-uri")
    return config.getoption("--mongo-contract-mongodb-uri")


def pytest_generate_tests(metafunc):
    if "contract_target" not in metafunc.fixturenames:
        return
    requested = metafunc.config.getoption("--mongo-contract-api")
    apis = APIS if requested == "both" else (requested,)
    backend = _backend_id(metafunc.config)
    metafunc.parametrize(
        "contract_target",
        [(api, backend) for api in apis],
        indirect=True,
        ids=["{0}-{1}".format(api, backend) for api in apis],
    )


def _contract_suite(request):
    filename = request.node.path.name
    if filename == "test_talkpython_contract.py":
        return "talkpython"
    if filename == "test_bson_comparison_contract.py":
        return "bson-comparison"
    if filename == "test_client_read_fidelity_contract.py":
        return "client-read-fidelity"
    return "core"


def _upstream_case_id(request, api, backend):
    name = request.node.name
    match = re.search(r"\[([^][]*)\]$", name)
    if match is not None:
        parameters = match.group(1)
        prefix = "{0}-{1}".format(api, backend)
        if parameters != prefix and not parameters.startswith(prefix + "-"):
            raise pytest.UsageError(
                "contract parameter ID {0!r} does not begin with {1!r}".format(
                    parameters, prefix
                )
            )
        remaining = parameters[len(prefix) :].lstrip("-")
        base = name[: match.start()]
        name = "{0}[{1}]".format(base, remaining) if remaining else base
    module = request.node.path.stem
    return "tests.contracts.{0}::{1}".format(module, name)


@pytest.fixture
def contract_target(request, tmp_path):
    """Yield one sync-shaped target selected through a lazy adapter import."""

    api, backend = request.param
    target = request.config.getoption("--mongo-contract-target")
    suite = _contract_suite(request)
    request.node.user_properties.extend(
        [
            ("tinymongo.api", api),
            ("tinymongo.backend", backend),
            ("tinymongo.suite", suite),
            ("tinymongo.contract_id", _upstream_case_id(request, api, backend)),
        ]
    )
    marker = request.node.get_closest_marker("client_options")
    client_options = dict(marker.kwargs) if marker else {}
    try:
        context = open_target(
            target,
            api,
            tmp_path,
            client_options,
            backend=backend,
            uri=_target_uri(request.config, target),
        )
        with context as handles:
            yield ContractTarget(
                name=handles.name,
                transport=handles.transport,
                api=api,
                client=handles.client,
                database=handles.database,
                collection=handles.collection,
                unsupported_warning=handles.unsupported_warning,
            )
    except TargetUnavailable as error:
        required = request.config.getoption("--mongo-contract-require-target")
        if required or target != "mongodb":
            pytest.fail(str(error), pytrace=False)
        pytest.skip(str(error))
