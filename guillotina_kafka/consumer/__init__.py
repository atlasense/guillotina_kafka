import time
import asyncio
from aiokafka import AIOKafkaConsumer

KAFKA_CONSUMER_STAT = {}

def counter():
    counters = {}

    def inc(name, i):
        counters[name] = counters.setdefault(name, 0) + i
        return counters[name]
    return inc


class consumer_stat(object):

    def __init__(self, **kwargs):
        global KAFKA_CONSUMER_STAT
        self.name = kwargs.get('name')
        self.group = kwargs.get('group')
        self.worker = kwargs.get('worker')
        self.counter = counter()
        KAFKA_CONSUMER_STAT[self.name] = {
            'topics': {},
            'name': self.name,
            'group': self.group,
            'worker': self.worker,
            'current_offset': None,
            'current_partition': None,
            'current_timestamp': None,
            'total_count': 0
        }

    def compute_stat(self, record):
        KAFKA_CONSUMER_STAT[self.name]['total_count'] += 1
        KAFKA_CONSUMER_STAT[self.name]['current_offset'] = record.offset
        KAFKA_CONSUMER_STAT[self.name]['current_timestamp'] = time.ctime(
            record.timestamp / 1000)
        KAFKA_CONSUMER_STAT[self.name]['current_partition'] = record.partition
        KAFKA_CONSUMER_STAT[self.name]['topics'][record.topic] = {
            'timestamp': time.ctime(record.timestamp / 1000),
            'partition': record.partition,
            'offset': record.offset,
            'count': self.counter(record.topic, 1)
        }

    def __call__(self, f):
        async def wrapped_f(*args, **kwargs):
            self.compute_stat(args[0])
            await f(*args, **kwargs)
        return wrapped_f


@consumer_stat(
    name='default',
    group='default',
    worker="guillotina_kafka.consumer.default_worker")
async def default_worker(*args, **kwargs):
    print('Default', args[0])
    return


async def es_worker(*args, **kwargs):
    print('ES', args[0])
    return


class ConsumerWorkerLookupError(Exception):
    pass


class InvalidConsumerType(Exception):
    pass


class Consumer(object):

    def __init__(
            self, topics, loop=None,
            worker=lambda data: print(data), **kwargs):

        self.topics = topics
        self.worker = worker
        self._consumer = None

        self.config = {
            'loop': loop or asyncio.get_event_loop(),
            'metadata_max_age_ms': 5000,
            **kwargs
        }

    async def init(self):
        if self._consumer is None:
            self._consumer = AIOKafkaConsumer(**self.config)
            if isinstance(self.topics, str):
                self._consumer.subscribe(pattern=self.topics)
            if isinstance(self.topics, (list, set, tuple)):
                self._consumer.subscribe(topics=self.topics)
            await self._consumer.start()
        return self._consumer

    @property
    def has_regex_topic(self):
        return isinstance(self.topics, str)

    @property
    def is_ready(self):
        return self._consumer is not None

    async def get(self, max_records=1, within=60 * 1000):
        return await self._consumer.getmany(
            timeout_ms=within, max_records=max_records)

    async def stop(self):
        return await self._consumer.stop()
