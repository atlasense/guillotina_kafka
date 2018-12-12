TEST_TOPIC = 'test-topic'

async def test_simple(kafka_container, loop, containter_requester):
    # Get a producer to send some data to kafka
    kafka_producer = get_kafka_producer(topic=TEST_TOPIC, loop=loop)

    # Get a consumer
    kafka_consumer =
