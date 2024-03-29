from typing import Any, Final, Optional

from psqlpy import PSQLPool
from taskiq import ScheduleSource

from taskiq_psqlpy.queries import (
    CREATE_RESULT_BACKEND_INDEX_QUERY,
    CREATE_SCHEDULE_SOURCE_TABLE_QUERY,
    DELETE_SCHEDULE_QUERY,
)


class PSQLPyScheduleSource(ScheduleSource):
    """
    Source of schedules for PostgreSQL based on PSQLPy.

    This class allows you to store schedules in PostgreSQL.
    Also it supports dynamic schedules.
    """

    def __init__(
        self,
        dsn: Optional[str] = "postgres://postgres:postgres@localhost:5432/postgres",
        keep_results: bool = True,
        table_name: str = "taskiq_schedules",
        **connect_kwargs: Any,
    ) -> None:
        """Construct new result backend.

        :param dsn: connection string to PostgreSQL.
        :param keep_results: flag to not remove results from Redis after reading.
        :param table_name: name of the table for taskiq schedules.
        :param connect_kwargs: additional arguments for nats `PSQLPool` class.
        """
        self.dsn: Final = dsn
        self.keep_results: Final = keep_results
        self.table_name: Final = table_name
        self.connect_kwargs: Final = connect_kwargs

        self._database_pool: PSQLPool

    async def startup(self) -> None:
        """Initialize the schedule source.

        Construct new connection pool
        and create new table for schedules if not exists.
        """
        self._database_pool = PSQLPool(
            dsn=self.dsn,
            **self.connect_kwargs,
        )
        await self._database_pool.execute(
            querystring=CREATE_SCHEDULE_SOURCE_TABLE_QUERY.format(
                self.table_name,
            ),
        )
        await self._database_pool.execute(
            querystring=CREATE_RESULT_BACKEND_INDEX_QUERY.format(
                self.table_name,
                self.table_name,
            ),
        )

    async def shutdown(self) -> None:
        """Close the connection pool."""
        await self._database_pool.close()

    async def delete_schedule(self, schedule_id: str) -> None:
        """Remove schedule by id."""
        await self._database_pool.execute(
            DELETE_SCHEDULE_QUERY.format(
                self.table_name,
            ),
            parameters=[schedule_id],
        )
