import uuid
import asyncio
from guillotina import configure
from zope.interface import implementer
from guillotina_kafka.consumer import Consumer
from guillotina_kafka.interfaces import IConsumer
from guillotina_kafka.interfaces import IConsumerUtility

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError, KafkaTimeoutError


@implementer(IConsumer)
class BatchConsumer(Consumer):

    def __init__(
            self, topics, loop=None,
            worker=lambda data: print(data), **kwargs):
        super().__init__(topics, loop=loop, worker=worker, **kwargs)

        self.config = {
            **kwargs,
            'enable_auto_commit': False,
            'group_id': str(uuid.uuid4()),
            'loop': loop or asyncio.get_event_loop()
        }

    async def take(self, max_records, within=60*1000):
        while True:
            result = await self._consumer.getmany(
                timeout_ms=within, max_records=max_records
            )
            for topic_partition, messages in result.items():
                yield topic_partition, messages

    async def commit_offset(self, offset:dict):
        await self._consumer.commit(offset)


@configure.adapter(
    for_=IConsumer, provides=IConsumerUtility, name='batch')
class BatchConsumerUtility:

    def __init__(self, consumer: BatchConsumer):
        self.consumer = consumer

    async def consume(self, arguments, settings):

        max_records = arguments.take or 1000
        within = arguments.within or 60*1000

        if not self.consumer.is_ready:
            await self.consumer.init()

        try:
            async for tp, messages in self.consumer.take(max_records, within=within):
                if messages:
                    _ = await self.consumer.worker(messages)
                    await self.consumer.commit_offset({tp: messages[-1].offset + 1})
        finally:
            await self.consumer.stop()
            print('Stoped BatchConsumerUtility.')
