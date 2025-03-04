import asyncio
import json
import uuid
from typing import cast

import psqlpy
from psqlpy.extra_types import JSONB
import pytest
from taskiq import AckableMessage, BrokerMessage
from taskiq.utils import maybe_awaitable

from taskiq_psqlpy import PSQLPyBroker
from taskiq_psqlpy.broker_queries import (
    INSERT_MESSAGE_QUERY,
)


async def get_first_task(psqlpy_broker: PSQLPyBroker) -> AckableMessage:
    """
    Get the first message from the broker's listen method.

    :param broker: Instance of AsyncpgBroker.
    :return: The first AckableMessage received.
    """
    async for message in psqlpy_broker.listen():
        return message
    msg = "Unreachable"
    raise RuntimeError(msg)


@pytest.mark.anyio
async def test_kick_success(psqlpy_broker: PSQLPyBroker) -> None:
    """
    Test that messages are published and read correctly.

    We kick the message, listen to the queue, and check that
    the received message matches what was sent.
    """
    # Create a unique task ID and name
    task_id = uuid.uuid4().hex
    task_name = uuid.uuid4().hex

    # Construct the message
    sent = BrokerMessage(
        task_id=task_id,
        task_name=task_name,
        message=b"my_msg",
        labels={
            "label1": "val1",
        },
    )

    # Send the message
    await psqlpy_broker.kick(sent)

    # Listen for the message
    message = await asyncio.wait_for(get_first_task(psqlpy_broker), timeout=1.0)

    # Check that the received message matches the sent message
    assert message.data == sent.message

    # Acknowledge the message
    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_startup(psqlpy_broker: PSQLPyBroker) -> None:
    """
    Test the startup process of the broker.

    We drop the messages table, restart the broker, and ensure
    that the table is recreated.
    """
    # Drop the messages table
    assert psqlpy_broker.write_pool is not None
    assert psqlpy_broker.read_conn is not None

    async with psqlpy_broker.write_pool.acquire() as conn:
        _ = await conn.execute(f"DROP TABLE IF EXISTS {psqlpy_broker.table_name}")

    # Shutdown and restart the broker
    await psqlpy_broker.shutdown()
    await psqlpy_broker.startup()

    # Verify that the table exists
    table_exists = cast(
        bool,
        await psqlpy_broker.read_conn.fetch_val(
            """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = $1
        )
        """,
            [
                psqlpy_broker.table_name,
            ],
        ),
    )
    assert table_exists, f"couldn't find {psqlpy_broker.table_name}"


@pytest.mark.anyio
async def test_listen(psqlpy_broker: PSQLPyBroker) -> None:
    """
    Test listen.

    Test that the broker can listen to messages inserted directly into the database
    and notified via the channel.
    """
    # Insert a message directly into the database
    pool = psqlpy.connect(psqlpy_broker.dsn)
    message_content = b"test_message"
    task_id = uuid.uuid4().hex
    task_name = "test_task"
    labels = {"label1": "label_val"}
    async with pool.acquire() as conn:
        message_id = cast(
            bool,
            await conn.fetch_val(
                INSERT_MESSAGE_QUERY.format(psqlpy_broker.table_name),
                [
                    task_id,
                    task_name,
                    message_content.decode(),
                    JSONB(labels),
                ],
            ),
        )
        # Send a NOTIFY with the message ID
        _ = await conn.execute(f"NOTIFY {psqlpy_broker.channel_name}, '{message_id}'")

    # Listen for the message
    message = await asyncio.wait_for(get_first_task(psqlpy_broker), timeout=1.0)
    assert message.data == message_content

    # Acknowledge the message
    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_wrong_format(psqlpy_broker: PSQLPyBroker) -> None:
    """Test that messages with incorrect formats are still received."""
    # Insert a message with missing task_id and task_name
    pool = psqlpy.connect(psqlpy_broker.dsn)
    async with pool.acquire() as conn:
        message_id = cast(
            bool,
            await conn.fetch_val(
                INSERT_MESSAGE_QUERY.format(psqlpy_broker.table_name),
                [
                    "",  # Missing task_id
                    "",  # Missing task_name
                    "wrong",  # Message content
                    JSONB({}),  # Empty labels
                ],
            ),
        )
        # Send a NOTIFY with the message ID
        _ = await conn.execute(f"NOTIFY {psqlpy_broker.channel_name}, '{message_id}'")

    # Listen for the message
    message = await asyncio.wait_for(get_first_task(psqlpy_broker), timeout=1.0)
    assert message.data == b"wrong"  # noqa: PLR2004

    # Acknowledge the message
    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_delayed_message(psqlpy_broker: PSQLPyBroker) -> None:
    """Test that delayed messages are delivered correctly after the specified delay."""
    # Send a message with a delay
    task_id = uuid.uuid4().hex
    task_name = "test_task"
    sent = BrokerMessage(
        task_id=task_id,
        task_name=task_name,
        message=b"delayed_message",
        labels={
            "delay": "2",  # Delay in seconds
        },
    )
    await psqlpy_broker.kick(sent)

    # Try to get the message immediately (should not be available yet)
    with pytest.raises(asyncio.TimeoutError):
        _ = await asyncio.wait_for(get_first_task(psqlpy_broker), timeout=1.0)

    # Wait for the delay to pass and receive the message
    message = await asyncio.wait_for(get_first_task(psqlpy_broker), timeout=3.0)
    assert message.data == sent.message

    # Acknowledge the message
    await maybe_awaitable(message.ack())
