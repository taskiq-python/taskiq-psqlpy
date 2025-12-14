import asyncio
import logging
import typing as tp
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import datetime

import psqlpy
from psqlpy.exceptions import ConnectionExecuteError
from psqlpy.extra_types import JSONB
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

from taskiq_psqlpy.queries import (
    CLAIM_MESSAGE_QUERY,
    CREATE_MESSAGE_TABLE_QUERY,
    DELETE_MESSAGE_QUERY,
    INSERT_MESSAGE_QUERY,
)

logger = logging.getLogger("taskiq.psqlpy_broker")
_T = tp.TypeVar("_T")


@dataclass
class MessageRow:
    """Message in db table."""

    id: int
    task_id: str
    task_name: str
    message: str
    labels: JSONB
    status: str
    created_at: datetime


class PSQLPyBroker(AsyncBroker):
    """Broker that uses PostgreSQL and PSQLPy with LISTEN/NOTIFY."""

    _read_conn: psqlpy.Connection
    _write_pool: psqlpy.ConnectionPool
    _listener: psqlpy.Listener

    def __init__(
        self,
        dsn: (
            str | tp.Callable[[], str]
        ) = "postgresql://taskiq_psqlpy:look_in_vault@localhost:5432/taskiq_psqlpy",
        result_backend: AsyncResultBackend[_T] | None = None,
        task_id_generator: tp.Callable[[], str] | None = None,
        channel_name: str = "taskiq",
        table_name: str = "taskiq_messages",
        max_retry_attempts: int = 5,
        read_kwargs: dict[str, tp.Any] | None = None,
        write_kwargs: dict[str, tp.Any] | None = None,
    ) -> None:
        """
        Construct a new broker.

        Args:
            dsn: connection string to PostgreSQL, or callable returning one.
            result_backend: Custom result backend.
            task_id_generator: Custom task_id generator.
            channel_name: Name of the channel to listen on.
            table_name: Name of the table to store messages.
            max_retry_attempts: Maximum number of message processing attempts.
            read_kwargs: Additional arguments for read connection creation.
            write_kwargs: Additional arguments for write pool creation.

        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self._dsn: str | tp.Callable[[], str] = dsn
        self.channel_name: str = channel_name
        self.table_name: str = table_name
        self.read_kwargs: dict[str, tp.Any] = read_kwargs or {}
        self.write_kwargs: dict[str, tp.Any] = write_kwargs or {}
        self.max_retry_attempts: int = max_retry_attempts
        self._queue: asyncio.Queue[str] | None = None

    @property
    def dsn(self) -> str:
        """
        Get the DSN string.

        Returns:
            A string with dsn or None if dsn isn't set yet.

        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn

    async def startup(self) -> None:
        """Initialize the broker."""
        await super().startup()
        self._read_conn = await psqlpy.connect(
            dsn=self.dsn,
            **self.read_kwargs,
        )
        self._write_pool = psqlpy.ConnectionPool(
            dsn=self.dsn,
            **self.write_kwargs,
        )

        # create messages table if it doesn't exist
        async with self._write_pool.acquire() as conn:
            await conn.execute(CREATE_MESSAGE_TABLE_QUERY.format(self.table_name))

        # listen to notification channel
        self._listener = self._write_pool.listener()
        await self._listener.add_callback(self.channel_name, self._notification_handler)
        await self._listener.startup()
        self._listener.listen()

        self._queue = asyncio.Queue()

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()
        if self._read_conn is not None:
            self._read_conn.close()
        if self._write_pool is not None:
            self._write_pool.close()
        if self._listener is not None:
            self._listener.abort_listen()
            await self._listener.shutdown()

    async def _notification_handler(
        self,
        connection: psqlpy.Connection,
        payload: str,
        channel: str,
        process_id: int,
    ) -> None:
        """
        Handle NOTIFY messages.

        https://psqlpy-python.github.io/components/listener.html#usage
        """
        logger.debug("Received notification on channel %s: %s", channel, payload)
        if self._queue is not None:
            self._queue.put_nowait(payload)

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message into the database and sends a NOTIFY.

        :param message: Message to send.
        """
        async with self._write_pool.acquire() as conn:
            # insert message into db table
            message_inserted_id = tp.cast(
                "int",
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

            delay_value = tp.cast("str | None", message.labels.get("delay"))
            if delay_value is not None:
                delay_seconds = int(delay_value)
                asyncio.create_task(  # noqa: RUF006
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
        async with self._write_pool.acquire() as conn:
            # Send NOTIFY with message ID as payload
            _ = await conn.execute(f"NOTIFY {self.channel_name}, '{message_id}'")

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to the channel.

        Yields messages as they are received.

        :yields: AckableMessage instances.
        """
        while True:
            try:
                payload = await self._queue.get()  # type: ignore[union-attr]
                message_id = int(payload)  # payload is the message id
                try:
                    async with self._write_pool.acquire() as conn:
                        claimed_message = await conn.fetch_row(
                            CLAIM_MESSAGE_QUERY.format(self.table_name),
                            [message_id],
                        )
                except ConnectionExecuteError:  # message was claimed by another worker
                    continue
                message_row_result = tp.cast(
                    "MessageRow",
                    tp.cast("object", claimed_message.as_class(MessageRow)),
                )
                message_data = message_row_result.message.encode()

                async def ack(*, _message_id: int = message_id) -> None:
                    async with self._write_pool.acquire() as conn:
                        _ = await conn.execute(
                            DELETE_MESSAGE_QUERY.format(self.table_name),
                            [_message_id],
                        )

                yield AckableMessage(data=message_data, ack=ack)
            except Exception:
                logger.exception("Error processing message")
                continue
