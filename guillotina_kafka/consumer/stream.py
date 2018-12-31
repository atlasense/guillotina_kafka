import uuid
import asyncio
from guillotina import configure
from zope.interface import implementer
from guillotina_kafka.consumer import Consumer
from guillotina_kafka.interfaces import IConsumer
from guillotina_kafka.interfaces import IConsumerUtility

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError, KafkaTimeoutError
from aiokafka import TopicPartition


@implementer(IConsumer)
class StreamConsumer(Consumer):

    async def seek(self, step=-1):
        await self.init()
        for topic in self.topics:
            pid = self._consumer.partitions_for_topic(topic).pop()
            tp = TopicPartition(topic, pid)
            position = await self._consumer.position(tp)
            if position > 0:
                self._consumer.seek(tp, position + step)

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
                _ = await self.consumer.worker(message, arguments=arguments, settings=settings)
        finally:
            await self.consumer.stop()
            print('Stoped StreamConsumerUtility.')
