from guillotina_kafka import get_kafka_producer
from guillotina_kafka.producers import IWebApiSendMessage
from guillotina.component import get_adapter
import asyncio


TEST_TOPIC = 'test-topic'


async def test_utility(kafka_container, loop, container_requester):
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
    await producer_utility.stop()
    assert not producer_utility.is_ready


async def test_adapter(kafka_container, loop, container_requester):
    producer_utility = get_kafka_producer(loop=loop)
    producer = get_adapter(producer_utility, IWebApiSendMessage)
    import pdb; pdb.set_trace()
    pass
