import atexit
import logging
import os
import subprocess
import time
import uuid
import warnings
from subprocess import DEVNULL

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

logger = logging.getLogger(__name__)

_TIMESCALE_POSTGRES_USER = "tsusr"
_TIMESCALE_POSTGRES_PASSWORD = "default"
_TIMESCALE_DB_NAME = "tsdb"

pytest.TEARDOWN_LOCAL_POSTGRES = False
pytest.delete_environment_variables = []


def block_until_endpoint_connect(endpoint):
    engine = create_engine(endpoint)
    i = 1
    t = time.time()
    while True:
        try:
            with engine.connect():
                break
        except OperationalError:
            logger.info(
                f"Waiting for database container ({i} attempts, {time.time() - t:.3f}s)"
            )
            i += 1
            time.sleep(0.5)


def setup_timescale():
    logger.info("Setting up Timescale...")
    has_docker = (
        subprocess.run("which docker".split(), capture_output=True).returncode == 0
    )
    if has_docker:
        timescale_image = subprocess.run(
            [
                "bash",
                os.path.join(
                    os.path.dirname(__file__), "get_latest_timescale_image.sh"
                ),
            ],
            capture_output=True,
            check=True,
            text=True,
        ).stdout
        os.environ["TIMESCALE_LATEST"] = timescale_image
    else:
        pytest.TEARDOWN_LOCAL_POSTGRES = True
        subprocess.run(
            [
                "bash",
                os.path.join(os.path.dirname(__file__), "setup_timescale_ubuntu.sh"),
            ],
            check=True,
            capture_output=True,
        )
        logger.info("Timescale setup complete.")
        subprocess.run(
            ["service", "postgresql", "start"], check=True, capture_output=True
        )
        subprocess.run(
            [
                "su",
                "-",
                "postgres",
                "-c",
                f"psql -c \"ALTER USER postgres WITH PASSWORD '{_TIMESCALE_POSTGRES_PASSWORD}';\"",
            ],
            check=True,
            capture_output=True,
        )


def pytest_sessionstart():
    if "AWS_DEFAULT_REGION" not in os.environ:
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"
        pytest.delete_environment_variables.append("AWS_DEFAULT_REGION")
    # Ensure credentials are accessible from environment variables
    for cred in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]:
        if cred not in os.environ:
            response = subprocess.run(
                ["aws", "configure", "get", cred.lower()],
                check=True,
                capture_output=True,
            )
            os.environ[cred] = response.stdout.decode().strip()
            pytest.delete_environment_variables.append(cred)
    if "TEST_TIMESCALE_ENDPOINT" not in os.environ:
        setup_timescale()


def pytest_sessionfinish():
    for var in pytest.delete_environment_variables:
        try:
            del os.environ[var]
        except KeyError:
            pass
    if pytest.TEARDOWN_LOCAL_POSTGRES:
        subprocess.run(
            ["service", "postgresql", "stop"], stdout=DEVNULL, stderr=DEVNULL
        )


@pytest.fixture(scope="session")
def timescale_endpoint():
    if "TEST_TIMESCALE_ENDPOINT" in os.environ:
        yield os.environ["TEST_TIMESCALE_ENDPOINT"]
    elif "TIMESCALE_LATEST" in os.environ:
        PORT = 57887
        timescale_container = f"timescaledb-{uuid.uuid1()}"
        subprocess.run(
            (
                f"docker run -d --rm --name {timescale_container} "
                + f"-p {PORT}:5432 "
                + f"-e POSTGRES_USER={_TIMESCALE_POSTGRES_USER} "
                + f"-e POSTGRES_PASSWORD={_TIMESCALE_POSTGRES_PASSWORD} "
                + f"-e POSTGRES_DB={_TIMESCALE_DB_NAME} "
                + f"{os.environ['TIMESCALE_LATEST']}"
            ).split(),
            check=True,
            capture_output=True,
        )

        def ensure_container_stop():
            subprocess.run(
                f"docker stop {timescale_container}".split(),
                stdout=DEVNULL,
                stderr=DEVNULL,
            )

        atexit.register(ensure_container_stop)
        endpoint = "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(
            host="localhost",
            port=PORT,
            user=_TIMESCALE_POSTGRES_USER,
            password=_TIMESCALE_POSTGRES_PASSWORD,
            dbname=_TIMESCALE_DB_NAME,
        )
        block_until_endpoint_connect(endpoint)
        yield endpoint
        subprocess.run(
            f"docker stop {timescale_container}".split(), stdout=DEVNULL, stderr=DEVNULL
        )
    else:
        subprocess.run(
            ["su", "-", "postgres", "-c", f"dropdb --if-exists {_TIMESCALE_DB_NAME}"],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "su",
                "-",
                "postgres",
                "-c",
                f'psql -c "CREATE DATABASE {_TIMESCALE_DB_NAME};"',
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "su",
                "-",
                "postgres",
                "-c",
                f'psql -d {_TIMESCALE_DB_NAME} -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"',
            ],
            check=True,
            capture_output=True,
        )
        yield "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(
            host="localhost",
            port="5432",
            user=_TIMESCALE_POSTGRES_USER,
            password=_TIMESCALE_POSTGRES_PASSWORD,
            dbname=_TIMESCALE_DB_NAME,
        )
        subprocess.run(
            ["su", "-", "postgres", "-c", f"dropdb --if-exists {_TIMESCALE_DB_NAME}"],
            stdout=DEVNULL,
            stderr=DEVNULL,
        )


@pytest.fixture
def cleanup():
    fns = []
    yield fns
    for fn in fns:
        try:
            fn()
        except Exception as e:
            warnings.warn(f"Cleanup failed with exception: {str(e)}")
