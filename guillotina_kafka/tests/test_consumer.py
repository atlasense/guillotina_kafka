from guillotina import app_settings
from aiokafka.structs import RecordMetadata
from aiokafka.structs import ConsumerRecord
from guillotina.component import get_adapter
from guillotina_kafka.producer import GetKafkaProducer
from guillotina_kafka.interfaces import IConsumerUtility
from guillotina_kafka.consumer.stream import StreamConsumer




async def test_stream_consumer(kafka_container, event_loop, container_requester):

    TEST_TOPICS = ['test-topic-1', 'test-topic-2']
    TEST_GROUP = 'test-group'
    BOOTSTRAP_SERVERS=[f"{app_settings['kafka']['host']}:{app_settings['kafka']['port']}"]

    producer = GetKafkaProducer('bytes', app_settings)

    async def worker_func(record):
        assert isinstance(record, ConsumerRecord)
        assert record.topic in RECORDS_SENT
        assert record.value in RECORDS_SENT[record.topic]

    RECORDS_SENT = {}

    for index, topic in enumerate(TEST_TOPICS):
        result = await producer.send(topic, value=f'foobar{index}')
        assert isinstance(result, tuple)
        assert result[0] is True
        assert isinstance(result[1], RecordMetadata)
        assert result[1].topic == topic
        RECORDS_SENT.setdefault(result[1].topic, []).append(f'foobar{index}'.encode())

    await producer.stop()

    consumer = StreamConsumer(
        TEST_TOPICS,
        worker=worker_func,
        group_id=TEST_GROUP,
        bootstrap_servers=BOOTSTRAP_SERVERS
    )

    consumer = get_adapter(consumer, IConsumerUtility, name='stream')
    assert consumer.consumer.is_ready is False
    await consumer.consumer.init()
    assert consumer.consumer.is_ready is True
    messages = await consumer.consumer.get(max_records=None, within=0)
    assert isinstance(messages, dict)
    for message in messages:
        await consumer.consumer.worker(message)
