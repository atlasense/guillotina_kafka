import json
import logging
from guillotina import app_settings
from guillotina4_kafka.interfaces import Producer

logger = logging.getLogger('guillotina_kafka')
kafka_producer = None

async def get_kafka_producer():
    global kafka_producer
    if kafka_producer is None:
        kafka_producer = Producer(
            app_settings['kafka'].get('host'),
            app_settings['kafka'].get('port'),
            None,
            serializer=lambda data: json.dumps(data).encode()
        )
        await kafka_producer.start()
    return kafka_producer
