import logging
from guillotina import configure
from zope.interface import Interface
from guillotina_kafka.interfaces import IConsumer
from aiokafka import AIOKafkaConsumer
from zope.interface import implementer
import asyncio

logger = logging.getLogger('TemplateConsumer')


@implementer(IConsumer)
class Consumer(object):

    def __init__(
            self, application_name, host, port,
            group, topics, deserializer=None):

        self.application_name = application_name
        self.host = host
        self.port = port
        self.group = group
        self.topics = topics
        self.deserializer = deserializer
        self._consumer = None

    async def init_consumer(self):
        if self._consumer is None:
            self._consumer = AIOKafkaConsumer(
                *self.topics, group_id=self.group,
                loop=asyncio.get_event_loop(),
                bootstrap_servers=f'{self.host}:{self.port}',
                value_deserializer=self.deserializer,
                auto_offset_reset='earliest'
            )
            await self._consumer.start()
        return self._consumer

    async def __aiter__(self):
        return await self.init_consumer()

    async def stop(self):
        return (await self._consumer.stop())


class ITemplateConsumer(Interface):
    async def consume():
        pass


@configure.adapter(for_=IConsumer, provides=ITemplateConsumer)
class TemplateConsumer:

    def __init__(self, consumer: Consumer):
        self.consumer = consumer

    async def consume(self, **kwargs):
        print('Started TemplateConsumer.')

        try:
            async for message in self.consumer:
                print(message)
        finally:
            await self.consumer.stop()
            print('Stoped TemplateConsumer.')
