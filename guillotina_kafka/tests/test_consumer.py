import json
import pytest
import asyncio
from guillotina import app_settings
from aiokafka.structs import RecordMetadata
from aiokafka.structs import ConsumerRecord
from guillotina.component import get_adapter
from guillotina_kafka.producer import GetKafkaProducer
from guillotina_kafka.interfaces import IConsumerUtility
from guillotina_kafka.consumer.stream import StreamConsumer
from guillotina_kafka.producer import GetKafkaProducer



async def test_stream_consumer(event_loop, kafka_container):

    TEST_TOPICS = ['test-topic-1', 'test-topic-2']
    TEST_GROUP = 'test-group'
    BOOTSTRAP_SERVERS=[f"{app_settings['kafka']['host']}:{app_settings['kafka']['port']}"]

    producer = GetKafkaProducer('json', app_settings)

    async def worker_func(*args, **kwargs):
        record = args[0]
        assert isinstance(record, ConsumerRecord)
        assert record.topic in RECORDS_SENT
        assert record.value in RECORDS_SENT[record.topic]

    RECORDS_SENT = {}

    for index, topic in enumerate(TEST_TOPICS):
        fut = await producer.send(topic, value=f'foobar{index}')
        assert isinstance(fut, asyncio.Future)
        record = await fut
        assert isinstance(record, RecordMetadata)
        assert record.topic == topic
        RECORDS_SENT.setdefault(record.topic, []).append(record.value)

    await producer.stop()

    consumer = StreamConsumer(
        TEST_TOPICS,
        worker=worker_func,
        group_id=TEST_GROUP,
        bootstrap_servers=BOOTSTRAP_SERVERS
    )

    # consumer = get_adapter(consumer, IConsumerUtility, name='stream')
    # await consumer.consume(None, None)


