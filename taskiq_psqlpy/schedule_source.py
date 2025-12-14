import uuid
from collections.abc import Callable
from logging import getLogger
from typing import Any, Final

from psqlpy import ConnectionPool
from psqlpy.extra_types import JSONB
from pydantic import ValidationError
from taskiq import AsyncBroker, ScheduledTask, ScheduleSource

from taskiq_psqlpy.queries import (
    CREATE_SCHEDULES_TABLE_QUERY,
    DELETE_ALL_SCHEDULES_QUERY,
    DELETE_SCHEDULE_QUERY,
    INSERT_SCHEDULE_QUERY,
    SELECT_SCHEDULES_QUERY,
)

logger = getLogger("taskiq_pg.psqlpy_schedule_source")


class PSQLPyScheduleSource(ScheduleSource):
    """Schedule source that uses psqlpy to store schedules in PostgreSQL."""

    _database_pool: ConnectionPool

    def __init__(
        self,
        broker: AsyncBroker,
        dsn: (
            str | Callable[[], str]
        ) = "postgresql://taskiq_psqlpy:look_in_vault@localhost:5432/taskiq_psqlpy",
        table_name: str = "taskiq_schedules",
        **connect_kwargs: Any,
    ) -> None:
        """
        Initialize the PostgreSQL scheduler source.

        Sets up a scheduler source that stores scheduled tasks in a PostgreSQL database.
        This scheduler source manages task schedules, allowing for persistent storage and retrieval of scheduled tasks
        across application restarts.

        Args:
            dsn: PostgreSQL connection string
            table_name: Name of the table to store scheduled tasks. Will be created automatically if it doesn't exist.
            broker: The TaskIQ broker instance to use for finding and managing tasks.
                Required if startup_schedule is provided.
            **connect_kwargs: Additional keyword arguments passed to the database connection pool.

        """
        self._broker: Final = broker
        self._dsn: Final = dsn
        self._table_name: Final = table_name
        self._connect_kwargs: Final = connect_kwargs

    @property
    def dsn(self) -> str | None:
        """
        Get the DSN string.

        Returns the DSN string or None if not set.
        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn

    def _extract_scheduled_tasks_from_broker(self) -> list[ScheduledTask]:
        """
        Extract schedules from tasks that were registered in broker.

        Returns:
            A list of ScheduledTask instances extracted from the task's labels.
        """
        scheduled_tasks_for_creation: list[ScheduledTask] = []
        for task_name, task in self._broker.get_all_tasks().items():
            if "schedule" not in task.labels:
                logger.debug("Task %s has no schedule, skipping", task_name)
                continue
            if not isinstance(task.labels["schedule"], list):
                logger.warning(
                    "Schedule for task %s is not a list, skipping",
                    task_name,
                )
                continue
            for schedule in task.labels["schedule"]:
                try:
                    new_schedule = ScheduledTask.model_validate(
                        {
                            "task_name": task_name,
                            "labels": schedule.get("labels", {}),
                            "args": schedule.get("args", []),
                            "kwargs": schedule.get("kwargs", {}),
                            "schedule_id": str(uuid.uuid4()),
                            "cron": schedule.get("cron", None),
                            "cron_offset": schedule.get("cron_offset", None),
                            "interval": schedule.get("interval", None),
                            "time": schedule.get("time", None),
                        },
                    )
                    scheduled_tasks_for_creation.append(new_schedule)
                except ValidationError:
                    logger.exception(
                        "Schedule for task %s is not valid, skipping",
                        task_name,
                    )
                    continue
        return scheduled_tasks_for_creation

    async def _update_schedules_on_startup(
        self,
        schedules: list[ScheduledTask],
    ) -> None:
        """Update schedules in the database on startup: truncate table and insert new ones."""
        async with (
            self._database_pool.acquire() as connection,
            connection.transaction(),
        ):
            await connection.execute(
                DELETE_ALL_SCHEDULES_QUERY.format(self._table_name),
            )
            data_to_insert = []
            for schedule in schedules:
                schedule_dict = schedule.model_dump(
                    mode="json",
                    exclude={"schedule_id", "task_name"},
                )
                data_to_insert.append(
                    [
                        uuid.UUID(schedule.schedule_id),
                        schedule.task_name,
                        JSONB(schedule_dict),
                    ],
                )

            await connection.execute_many(
                INSERT_SCHEDULE_QUERY.format(self._table_name),
                data_to_insert,
            )

    async def startup(self) -> None:
        """
        Initialize the schedule source.

        Construct new connection pool, create new table for schedules if not exists
        and fill table with schedules from task labels.
        """
        self._database_pool = ConnectionPool(
            dsn=self.dsn,
            **self._connect_kwargs,
        )
        async with self._database_pool.acquire() as connection:
            await connection.execute(
                CREATE_SCHEDULES_TABLE_QUERY.format(
                    self._table_name,
                ),
            )
        scheduled_tasks_for_creation = self._extract_scheduled_tasks_from_broker()
        await self._update_schedules_on_startup(scheduled_tasks_for_creation)

    async def shutdown(self) -> None:
        """Close the connection pool."""
        if getattr(self, "_database_pool", None) is not None:
            self._database_pool.close()

    async def get_schedules(self) -> list["ScheduledTask"]:
        """Fetch schedules from the database."""
        async with self._database_pool.acquire() as connection:
            rows_with_schedules = await connection.fetch(
                SELECT_SCHEDULES_QUERY.format(self._table_name),
            )
        schedules = []
        for row in rows_with_schedules.result():
            schedule = row["schedule"]
            schedules.append(
                ScheduledTask.model_validate(
                    {
                        "schedule_id": str(row["id"]),
                        "task_name": row["task_name"],
                        "labels": schedule["labels"],
                        "args": schedule["args"],
                        "kwargs": schedule["kwargs"],
                        "cron": schedule["cron"],
                        "cron_offset": schedule["cron_offset"],
                        "time": schedule["time"],
                        "interval": schedule["interval"],
                    },
                ),
            )
        return schedules

    async def add_schedule(self, schedule: "ScheduledTask") -> None:
        """
        Add a new schedule.

        Args:
            schedule: schedule to add.
        """
        async with self._database_pool.acquire() as connection:
            schedule_dict = schedule.model_dump(
                mode="json",
                exclude={"schedule_id", "task_name"},
            )
            await connection.execute(
                INSERT_SCHEDULE_QUERY.format(self._table_name),
                [
                    uuid.UUID(schedule.schedule_id),
                    schedule.task_name,
                    JSONB(schedule_dict),
                ],
            )

    async def delete_schedule(self, schedule_id: str) -> None:
        """
        Method to delete schedule by id.

        This is useful for schedule cancelation.

        Args:
            schedule_id: id of schedule to delete.
        """
        async with self._database_pool.acquire() as connection:
            await connection.execute(
                DELETE_SCHEDULE_QUERY.format(self._table_name),
                [uuid.UUID(schedule_id)],
            )

    async def post_send(self, task: ScheduledTask) -> None:
        """Delete a task after it's completed."""
        if task.time is not None:
            await self.delete_schedule(task.schedule_id)
