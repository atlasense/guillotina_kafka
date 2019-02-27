import asyncio
from aiokafka import AIOKafkaConsumer


async def default_worker(*args, **kwargs):
    print('Default', args[0], kwargs)
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

    async def get(self,max_records=1, within=60*1000):
        return await self._consumer.getmany(
            timeout_ms=within, max_records=max_records)

    async def stop(self):
        return await self._consumer.stop()
