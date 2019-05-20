import json
import asyncio
from functools import partial
from guillotina import configure
from guillotina import app_settings
from aiokafka import AIOKafkaProducer
from guillotina.component import get_utility
from guillotina_kafka.interfaces import IKafkaProducerUtility

@configure.utility(provides=IKafkaProducerUtility)
class KafkaProducerUtility:
    """This defines the singleton that will hold the connection to kafka
    and allows to send messages from it.
    """
    def __init__(self, loop=None):
        # Get kafka connection details from app settings
        self.loop = loop
        self.producer = None

    async def setup(self, **kwargs):
        """Gets or creates the connection to kafka"""
        self.config = {
            'bootstrap_servers': app_settings['kafka']['brokers'],
            'loop': self.loop or asyncio.get_event_loop(),
            **app_settings['kafka'].get('producer', {}),
            **kwargs
        }
        if self.producer is None:
            self.producer = AIOKafkaProducer(**self.config)
            await self.producer.start()
        return self.producer

    @property
    def is_ready(self):
        """Returns whether aiokafka producer connection is ready"""
        if self.producer is None:
            return False
        return True

    async def send(self, *args, **kwargs):
        if not self.is_ready:
            raise Exception('Producer utility is not ready')
        return await self.producer.send(*args, **kwargs)

    async def stop(self):
        if not self.is_ready:
            raise Exception('Producer utility is not ready')
        _ = await self.producer.stop()
        self.producer = None
        return _


def get_kafka_producer():
    return get_utility(IKafkaProducerUtility)


PRODUCER_REGISTRY = {}
producer_utility = None

def get_valide_topics(_for=None):
    topic_prefix = app_settings['kafka'].get('topic_prefix', '')
    topics = set(app_settings['kafka']['consumer'].get('topics', []))
    workers = app_settings['kafka']['consumer']['workers']
    if _for is not None:
        workers = filter(lambda w: w.get('name') == _for, workers)
    for worker in workers:
        topics.update(worker.get('topics', []))
    return [f'{topic_prefix}{topic}' for topic in topics]

def make_producers(_for=None):
    assert _for is not None, 'Invalide consumer worker name'
    topics = []
    result = (None, None)

    global producer_utility
    if producer_utility is None:
        producer_utility = get_kafka_producer()

    for worker in app_settings['kafka']['consumer']['workers']:
        topic = worker.get('provide_producer_for')
        if topic and worker.get('name') == _for:
            result = (topic, partial(producer_utility.send(topic)))
            break
    return result

def register_producer(name):
    global PRODUCER_REGISTRY
    topic, producer = make_producers(_for=name)
    PRODUCER_REGISTRY.setdefault(name, (topic, producer))

def get_producer(_for=None):
    assert _for is not None, 'Invalide consumer worker name'
    global PRODUCER_REGISTRY
    return PRODUCER_REGISTRY.get(_for, (None, None))
