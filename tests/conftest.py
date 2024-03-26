import os
import random
import string
from typing import AsyncGenerator, TypeVar

import pytest

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
            string.ascii_uppercase,
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
    await backend._database_pool.execute(
        querystring=f"DROP TABLE {postgres_table}",
    )
    await backend.shutdown()
