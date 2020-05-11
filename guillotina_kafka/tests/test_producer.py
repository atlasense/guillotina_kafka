from aiokafka.structs import RecordMetadata
from guillotina import app_settings
from guillotina_kafka import get_kafka_producer

import asyncio
import pytest


pytestmark = pytest.mark.asyncio


async def test_producer_utility(kafka_container, event_loop, container_requester):
    TEST_TOPIC = "test-topic"
    BOOTSTRAP_SERVERS = app_settings["kafka"]["brokers"]
    producer = get_kafka_producer()
    assert not producer.is_ready

    await producer.setup(bootstrap_servers=BOOTSTRAP_SERVERS, loop=event_loop)
    assert producer.is_ready

    result = await producer.send(TEST_TOPIC, {"foo": "bar"})
    assert isinstance(result, asyncio.Future)
    record = await result
    assert isinstance(record, RecordMetadata)
    assert record.topic == TEST_TOPIC
    await producer.stop()
    assert not producer.is_ready
