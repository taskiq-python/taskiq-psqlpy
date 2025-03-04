import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import (
    AsyncGenerator,
    Callable,
    Optional,
    TypedDict,
    TypeVar,
    cast,
    override,
)

import psqlpy
from psqlpy.extra_types import JSONB
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

from taskiq_psqlpy.broker_queries import (
    CREATE_TABLE_QUERY,
    DELETE_MESSAGE_QUERY,
    INSERT_MESSAGE_QUERY,
    SELECT_MESSAGE_QUERY,
)

_T = TypeVar("_T")
logger = logging.getLogger("taskiq.asyncpg_broker")


@dataclass
class MessageRow:
    """Message in db table."""

    id: int
    task_id: str
    task_name: str
    message: str
    labels: JSONB
    created_at: datetime


class PoolKwargs(TypedDict, total=False):
    """Psqlpy connection kwargs.

    Excludes dsn/user params because dsn used in broker.
    """

    target_session_attrs: psqlpy.TargetSessionAttrs | None
    options: str | None
    application_name: str | None
    connect_timeout_sec: int | None
    connect_timeout_nanosec: int | None
    tcp_user_timeout_sec: int | None
    tcp_user_timeout_nanosec: int | None
    keepalives: bool | None
    keepalives_idle_sec: int | None
    keepalives_idle_nanosec: int | None
    keepalives_interval_sec: int | None
    keepalives_interval_nanosec: int | None
    keepalives_retries: int | None
    load_balance_hosts: psqlpy.LoadBalanceHosts | None
    conn_recycling_method: psqlpy.ConnRecyclingMethod | None
    ssl_mode: psqlpy.SslMode | None
    ca_file: str | None


class PSQLPyBroker(AsyncBroker):
    """Broker that uses PostgreSQL and asyncpg with LISTEN/NOTIFY."""

    def __init__(
        self,
        dsn: str = "postgresql://postgres:postgres@localhost:5432/postgres",
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        channel_name: str = "taskiq",
        table_name: str = "taskiq_messages",
        max_retry_attempts: int = 5,
        read_pool_kwargs: Optional[PoolKwargs] = None,
        write_pool_kwargs: Optional[PoolKwargs] = None,
        max_write_pool_size: int = 2,
    ) -> None:
        """
        Construct a new broker.

        :param dsn: Connection string to PostgreSQL.
        :param result_backend: Custom result backend.
        :param task_id_generator: Custom task_id generator.
        :param channel_name: Name of the channel to listen on.
        :param table_name: Name of the table to store messages.
        :param max_retry_attempts: Maximum number of message processing attempts.
        :param connection_kwargs: Additional arguments for asyncpg connection.
        :param pool_kwargs: Additional arguments for asyncpg pool creation.
        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )

        self.dsn: str = dsn
        self.channel_name: str = channel_name
        self.table_name: str = table_name
        self.read_pool_kwargs: PoolKwargs = read_pool_kwargs if read_pool_kwargs else {}
        self.write_pool_kwargs: PoolKwargs = (
            write_pool_kwargs if write_pool_kwargs else {}
        )
        self.max_retry_attempts: int = max_retry_attempts
        self.read_conn: Optional[psqlpy.Connection] = None
        self.write_pool: Optional[psqlpy.ConnectionPool] = None
        self.max_write_pool_size: int = max_write_pool_size

        self._queue: Optional[asyncio.Queue[str]] = None

    @override
    async def startup(self) -> None:
        """Initialize the broker."""
        await super().startup()
        self.read_conn = await psqlpy.connect(
            dsn=self.dsn,
            **self.read_pool_kwargs,
        ).connection()
        self.write_pool = psqlpy.connect(
            dsn=self.dsn,
            max_db_pool_size=self.max_write_pool_size,
            **self.write_pool_kwargs,
        )

        # create messages table if doesn't exist
        async with self.write_pool.acquire() as conn:
            _ = await conn.execute(CREATE_TABLE_QUERY.format(self.table_name))

        # listen to notification channel
        listener = self.write_pool.listener()
        await listener.add_callback(self.channel_name, self._notification_handler)
        await listener.startup()
        listener.listen()

        self._queue = asyncio.Queue()

    @override
    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()
        if self.read_conn is not None:
            self.read_conn.back_to_pool()
        if self.write_pool is not None:
            self.write_pool.close()

    async def _notification_handler(
        self,
        connection: psqlpy.Connection,  # pyright: ignore[reportUnusedParameter]
        payload: str,
        channel: str,
        process_id: int,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        """Handle NOTIFY messages.

        From asyncpg.connection.add_listener docstring:
            A callable or a coroutine function receiving the following arguments:
            **connection**: a Connection the callback is registered with;
            **pid**: PID of the Postgres server that sent the notification;
            **channel**: name of the channel the notification was sent to;
            **payload**: the payload.
        """
        logger.debug(f"Received notification on channel {channel}: {payload}")
        if self._queue is not None:
            self._queue.put_nowait(payload)

    @override
    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message into the database and sends a NOTIFY.

        :param message: Message to send.
        """
        if self.write_pool is None:
            raise ValueError("Please run startup before kicking.")

        async with self.write_pool.acquire() as conn:
            # insert message into db table
            message_inserted_id = cast(
                int,
                await conn.fetch_val(
                    INSERT_MESSAGE_QUERY.format(self.table_name),
                    [
                        message.task_id,
                        message.task_name,
                        message.message.decode(),
                        JSONB(message.labels),
                    ],
                ),
            )

            delay_value = cast(Optional[str], message.labels.get("delay"))
            if delay_value is not None:
                delay_seconds = int(delay_value)
                _ = asyncio.create_task(  # noqa: RUF006
                    self._schedule_notification(message_inserted_id, delay_seconds),
                )
            else:
                # Send NOTIFY with message ID as payload
                _ = await conn.execute(
                    f"NOTIFY {self.channel_name}, '{message_inserted_id}'",
                )

    async def _schedule_notification(self, message_id: int, delay_seconds: int) -> None:
        """Schedule a notification to be sent after a delay."""
        await asyncio.sleep(delay_seconds)
        if self.write_pool is None:
            raise ValueError("Call startup before starting listening.")
        async with self.write_pool.acquire() as conn:
            # Send NOTIFY with message ID as payload
            _ = await conn.execute(f"NOTIFY {self.channel_name}, '{message_id}'")

    @override
    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to the channel.

        Yields messages as they are received.

        :yields: AckableMessage instances.
        """
        if self.read_conn is None:
            raise ValueError("Call startup before starting listening.")
        if self._queue is None:
            raise ValueError("Startup did not initialize the queue.")

        while True:
            try:
                payload = await self._queue.get()
                message_id = int(payload)  # payload is the message id
                message_row = await self.read_conn.fetch_row(
                    SELECT_MESSAGE_QUERY.format(self.table_name),
                    [message_id],
                )
                # ugly type hacks b/c SingleQueryResult.as_class return type is wrong
                message_row_result = cast(
                    MessageRow,
                    cast(object, message_row.as_class(MessageRow)),
                )
                message_data = message_row_result.message.encode()

                async def ack(*, _message_id: int = message_id) -> None:
                    if self.write_pool is None:
                        raise ValueError("Call startup before starting listening.")

                    async with self.write_pool.acquire() as conn:
                        _ = await conn.execute(
                            DELETE_MESSAGE_QUERY.format(self.table_name),
                            [_message_id],
                        )

                yield AckableMessage(data=message_data, ack=ack)
            except Exception as e:
                logger.exception(f"Error processing message: {e}")
                continue
