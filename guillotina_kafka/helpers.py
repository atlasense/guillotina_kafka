from guillotina import app_settings
from guillotina.component import get_adapter
from guillotina.utils import resolve_dotted_name
from guillotina_kafka.utilities.producers import Producer
from guillotina_kafka.utilities.consumers import Consumer
from guillotina_kafka.utilities.consumers import ConsumerLookupError
from guillotina_kafka.utilities.consumers.batch import GenericBatchConsumerUtility
from guillotina_kafka.utilities.consumers.stream import StreamConsumerUtility 


CONSUMER_REGISTRY = dict()
PRODUCER_REGISTRY = dict()


async def get_consumer(consumer_type, name, group, topics):
    consumer = CONSUMER_REGISTRY.get(name)
    if consumer is not None:
        return consumer
    return await make_consumer(consumer_type, name, group, topics)


async def make_consumer(consumer_type, name, group, topics):
    if name in CONSUMER_REGISTRY:
        consumer = CONSUMER_REGISTRY.pop(name)
        _ = await consumer.stop()
    consumer = Consumer(
        name, app_settings['kafka']['host'],
        app_settings['kafka']['port'], group, topics,
        enable_auto_commit=False if consumer_type == 'batch' else True
    )
    try:
        consumer_interface = app_settings['kafka']['consumers'][name]
        consumer_interface = resolve_dotted_name(consumer_interface)
    except KeyError:
        raise ConsumerLookupError(f'Consumer {name} has not been define.')
    except Exception:
        raise ConsumerLookupError(
            'Could not resolve Interface path for this consumer '
            'please check consumers settings.'
        )
    return get_adapter(consumer, consumer_interface)



async def get_producer(name):
    producer = PRODUCER_REGISTRY.get(name)
    if producer is not None:
        return producer
    return await make_producer(name)


async def make_producer(name):
    if name in PRODUCER_REGISTRY:
        producer = PRODUCER_REGISTRY.pop(name)
        _ = await producer.stop()
    producer = Producer(
        name, app_settings['kafka']['host'],
        app_settings['kafka']['port'],
        value_serializer=lambda data: data
    )

