from guillotina_kafka.utilities import get_kafka_producer
from guillotina_kafka.consumers import KafkaConsumer
from aiokafka.structs import ConsumerRecord


async def test_base_consumer(kafka_container, loop, container_requester):
    TEST_TOPICS = ['consumer-topic1']
    TEST_GROUP = 'test-group'

    # Get a producer to send some data to kafka
    producer = get_kafka_producer(loop=loop)
    future1 = await producer.send(TEST_TOPICS[0], 'foobar1')
    assert not future1.done()

    # Get a kafka consumer
    kafka_consumer = KafkaConsumer(
        'test-app',
        group=TEST_GROUP,
        topics=TEST_TOPICS,
        deserializer=lambda x: x.decode(),
        loop=loop,
    )
    assert kafka_consumer.application_name == 'test-app'
    assert kafka_consumer.group == TEST_GROUP
    assert kafka_consumer.topics == TEST_TOPICS

    record = await kafka_consumer.getone()
    assert isinstance(record, ConsumerRecord)
    assert record.topic in TEST_TOPICS
    assert record.offset == 0
    assert future1.done()
