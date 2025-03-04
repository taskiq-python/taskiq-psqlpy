import os
import random
import string
from typing import AsyncGenerator, TypeVar

import pytest

from taskiq_psqlpy.broker import PSQLPyBroker
from taskiq_psqlpy.result_backend import PSQLPyResultBackend

_ReturnType = TypeVar("_ReturnType")


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """
    Anyio backend.

    Backend for anyio pytest plugin.
    :return: backend name.
    """
    return "asyncio"


@pytest.fixture
def postgres_table() -> str:
    """
    Name of a postgresql table for current test.

    :return: random string.
    """
    return "".join(
        random.choice(
            string.ascii_lowercase,
        )
        for _ in range(10)
    )


@pytest.fixture
def postgresql_dsn() -> str:
    """
    DSN to PostgreSQL.

    :return: dsn to PostgreSQL.
    """
    return (
        os.environ.get("POSTGRESQL_URL")
        or "postgresql://postgres:postgres@localhost:5432/taskiqpsqlpy"
    )


@pytest.fixture()
async def psqlpy_result_backend(
    postgresql_dsn: str,
    postgres_table: str,
) -> AsyncGenerator[PSQLPyResultBackend[_ReturnType], None]:
    backend: PSQLPyResultBackend[_ReturnType] = PSQLPyResultBackend(
        dsn=postgresql_dsn,
        table_name=postgres_table,
    )
    await backend.startup()
    yield backend
    async with backend._database_pool.acquire() as conn:
        _ = await conn.execute(
            querystring=f"DROP TABLE {postgres_table}",
        )
    await backend.shutdown()


@pytest.fixture()
async def psqlpy_broker(
    postgresql_dsn: str,
    postgres_table: str,
) -> AsyncGenerator[PSQLPyBroker, None]:
    """
    Fixture to set up and tear down the broker.

    Initializes the broker with test parameters.
    """
    broker = PSQLPyBroker(
        dsn=postgresql_dsn,
        channel_name=f"{postgres_table}_channel",
        table_name=postgres_table,
    )
    await broker.startup()

    yield broker

    assert broker.write_pool
    async with broker.write_pool.acquire() as conn:
        _ = await conn.execute(f"DROP TABLE {postgres_table}")
    await broker.shutdown()
