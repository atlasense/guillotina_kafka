import json
import pytest
import asyncio
from guillotina import app_settings
from guillotina.component import get_adapter
from aiokafka.structs import RecordMetadata
from guillotina_kafka import get_kafka_producer
from guillotina_kafka.producer import GetKafkaProducer

pytestmark = pytest.mark.asyncio


async def test_producer_adapter(kafka_container, event_loop, container_requester):
    TEST_TOPIC = 'test-topic'
    producer = GetKafkaProducer('json', app_settings)
    result = await producer.send(TEST_TOPIC, {'foo': 'bar'})
    assert isinstance(result, tuple)
    assert result[0] is True
    assert isinstance(result[1], RecordMetadata)
    assert result[1].topic == TEST_TOPIC
    await producer.stop()


async def test_producer_utility(kafka_container, event_loop, container_requester):
    TEST_TOPIC = 'test-topic'
    BOOTSTRAP_SERVERS = f"{app_settings['kafka']['host']}:{app_settings['kafka']['port']}"
    producer = get_kafka_producer()
    assert not producer.is_ready

    await producer.setup(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        loop=event_loop
    )
    assert producer.is_ready

    result = await producer.send(TEST_TOPIC, {'foo': 'bar'})
    assert isinstance(result, asyncio.Future)
    record = await result
    assert isinstance(record, RecordMetadata)
    assert record.topic == TEST_TOPIC
    await producer.stop()
    assert not producer.is_ready
