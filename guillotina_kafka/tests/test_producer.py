from guillotina_kafka import get_kafka_producer
from guillotina_kafka.producers import WebApiSendMessage

from aiokafka.structs import RecordMetadata
import asyncio


async def test_utility(kafka_container, loop, container_requester):
    TEST_TOPIC = 'test-topic-1'

    producer_utility = get_kafka_producer(loop=loop)
    assert producer_utility.host
    assert producer_utility.port
    assert not producer_utility.is_ready
    await producer_utility.start()
    assert producer_utility.is_ready

    result = await producer_utility.send(TEST_TOPIC, 'foobar')
    # The send function returns a future that will set when message is
    # processed
    assert isinstance(result, asyncio.Future)
    record = await result
    assert isinstance(record, RecordMetadata)
    assert record.topic == TEST_TOPIC
    await producer_utility.stop()
    assert not producer_utility.is_ready


async def test_adapter(kafka_container, loop, container_requester):
    TEST_TOPIC = 'test-topic-2'

    producer = WebApiSendMessage(loop=loop)

    for i in range(2):
        result = await producer.send(TEST_TOPIC, {'foo': 'bar'})
        assert isinstance(result, asyncio.Future)
        record = await result
        assert isinstance(record, RecordMetadata)
        assert record.topic == TEST_TOPIC
        assert record.offset == i
