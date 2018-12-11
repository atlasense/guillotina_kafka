import json
import logging
from guillotina import app_settings
from guillotina_kafka.interfaces import IKafkaProducerUtility
from guillotina.component import get_utility

logger = logging.getLogger('guillotina_kafka')


def get_kafka_producer(name='basic', topic=None, loop=None):
    kafka_producer = get_utility(IKafkaProducerUtility, name)
    kafka_producer.configure(
        host=app_settings['kafka']['host'],
        port=app_settings['kafka']['port'],
        serializer=lambda data: json.dumps(data).encode(),
        loop=loop,
    )
    return kafka_producer
