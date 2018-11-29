import uuid
import asyncio
from guillotina import configure
from zope.interface import implementer
from guillotina_kafka.interfaces import IConsumer
from guillotina_kafka.interfaces import IConsumerUtility

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError, KafkaTimeoutError
from aiokafka import TopicPartition


@implementer(IConsumer)
class StreamConsumer(object):

    def __init__(
            self, topics, loop=None,
            worker=lambda data: print(data), **kwargs):

        self.topics = topics
        self.worker = worker
        self._consumer = None

        self.config = {
            'loop': loop or asyncio.get_event_loop(),
            **kwargs
        }

    async def init(self):
        if self._consumer is None:
            self._consumer = AIOKafkaConsumer(
                *self.topics, **self.config
            )
            await self._consumer.start()
        return self._consumer

    async def seek(self, step=-1):
        _ = await self.init()
        for topic in self.topics:
            pid = self._consumer.partitions_for_topic(topic).pop()
            tp = TopicPartition(topic, pid)
            position = await self._consumer.position(tp)
            if position > 0:
                self._consumer.seek(tp, position + step)

    @property
    def is_ready(self):
        return self._consumer is not None

    async def stop(self):
        return await self._consumer.stop()

    async def __aiter__(self):
        return await self.init()


@configure.adapter(
    for_=IConsumer, provides=IConsumerUtility, name='stream')
class StreamConsumerUtility:

    def __init__(self, consumer: StreamConsumer):
        self.consumer = consumer

    async def consume(self, arguments, settings):

        if not self.consumer.is_ready:
            await self.consumer.init()

        try:
            print('Starting StreamConsumerUtility ...')
            # import pdb; pdb.set_trace()
            await self.consumer.seek(step=-1) # Move to the previous offset
            async for message in self.consumer:
                _ = await self.consumer.worker(message)
        finally:
            await self.consumer.stop()
            print('Stoped StreamConsumerUtility.')
