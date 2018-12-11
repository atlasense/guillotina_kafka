import asyncio
from guillotina_kafka.util import get_kafka_producer

TEST_TOPIC = 'test-topic'


async def test_simple(kafka_container, loop):
    kafka_producer = get_kafka_producer(topic=TEST_TOPIC, loop=loop)
    assert kafka_producer.host
    assert kafka_producer.port
    assert kafka_producer.is_ready
    assert kafka_producer.topic == TEST_TOPIC

    result = await kafka_producer.send({'foo': 'bar'})
    # The send function returns a future that will set when message is
    # processed
    assert isinstance(result, asyncio.Future)
    resp = await kafka_producer.stop()
