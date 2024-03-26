import pickle
from typing import Any, Final, Literal, Optional, TypeVar, cast

from psqlpy import PSQLPool
from psqlpy.exceptions import RustPSQLDriverPyBaseError
from taskiq import AsyncResultBackend, TaskiqResult

from taskiq_psqlpy.exceptions import ResultIsMissingError
from taskiq_psqlpy.queries import (
    CREATE_INDEX_QUERY,
    CREATE_TABLE_QUERY,
    DELETE_RESULT_QUERY,
    INSERT_RESULT_QUERY,
    IS_RESULT_EXISTS_QUERY,
    SELECT_RESULT_QUERY,
)

_ReturnType = TypeVar("_ReturnType")


class PSQLPyResultBackend(AsyncResultBackend[_ReturnType]):
    """Result backend for TaskIQ based on PSQLPy."""

    def __init__(
        self,
        dsn: Optional[str] = "postgres://postgres:postgres@localhost:5432/postgres",
        keep_results: bool = True,
        table_name: str = "taskiq_results",
        field_for_task_id: Literal["VarChar", "Text"] = "VarChar",
        **connect_kwargs: Any,
    ) -> None:
        """Construct new result backend.

        :param dsn: connection string to PostgreSQL.
        :param keep_results: flag to not remove results from Redis after reading.
        :param connect_kwargs: additional arguments for nats `PSQLPool` class.
        """
        self.dsn: Final = dsn
        self.keep_results: Final = keep_results
        self.table_name: Final = table_name
        self.field_for_task_id: Final = field_for_task_id
        self.connect_kwargs: Final = connect_kwargs

        self._database_pool: PSQLPool

    async def startup(self) -> None:
        """Initialize the result backend.

        Construct new connection pool
        and create new table for results if not exists.
        """
        self._database_pool = PSQLPool(
            dsn=self.dsn,
            **self.connect_kwargs,
        )
        await self._database_pool.execute(
            querystring=CREATE_TABLE_QUERY.format(
                self.table_name,
                self.field_for_task_id,
            ),
        )
        await self._database_pool.execute(
            querystring=CREATE_INDEX_QUERY.format(
                self.table_name,
                self.table_name,
            ),
        )

    async def shutdown(self) -> None:
        """Close the connection pool."""
        await self._database_pool.close()

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[_ReturnType],
    ) -> None:
        """Set result to the PostgreSQL table.

        :param task_id: ID of the task.
        :param result: result of the task.
        """
        await self._database_pool.execute(
            querystring=INSERT_RESULT_QUERY.format(
                self.table_name,
            ),
            parameters=[
                task_id,
                pickle.dumps(result),
            ],
        )

    async def is_result_ready(self, task_id: str) -> bool:
        """Returns whether the result is ready.

        :param task_id: ID of the task.

        :returns: True if the result is ready else False.
        """
        connection: Final = await self._database_pool.connection()
        return cast(
            bool,
            await connection.fetch_val(
                querystring=IS_RESULT_EXISTS_QUERY.format(
                    self.table_name,
                ),
                parameters=[task_id],
            ),
        )

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Retrieve result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :raises ResultIsMissingError: if there is no result when trying to get it.
        :return: TaskiqResult.
        """
        connection: Final = await self._database_pool.connection()
        try:
            result_in_bytes = cast(
                bytes,
                await connection.fetch_val(
                    querystring=SELECT_RESULT_QUERY.format(
                        self.table_name,
                    ),
                    parameters=[task_id],
                ),
            )
        except RustPSQLDriverPyBaseError as exc:
            raise ResultIsMissingError(
                f"Cannot find record with task_id = {task_id} in PostgreSQL",
            ) from exc

        if not self.keep_results:
            await self._database_pool.execute(
                querystring=DELETE_RESULT_QUERY.format(
                    self.table_name,
                ),
                parameters=[task_id],
            )

        taskiq_result: Final = pickle.loads(  # noqa: S301
            result_in_bytes,
        )

        if not with_logs:
            taskiq_result.log = None

        return taskiq_result
