import logging
from guillotina import configure
from guillotina import app_settings
from zope.interface import Interface
from guillotina_kafka.interfaces import IKafkaConsumer
from aiokafka import AIOKafkaConsumer
from zope.interface import implementer
import asyncio

logger = logging.getLogger('TemplateConsumer')


@implementer(IKafkaConsumer)
class KafkaConsumer:
    def __init__(self, application_name, group=None, topics=None,
                 deserializer=None, loop=None):
        self.host = app_settings['kafka']['host']
        self.port = app_settings['kafka']['port']
        self.application_name = application_name
        self.group = group
        self.topics = topics
        self.deserializer = deserializer
        if not deserializer:
            # Default deserializer
            self.deserializer = lambda x: x.decode()
        self._consumer = None
        self.loop = loop

    async def init_consumer(self):
        if self._consumer is None:
            self._consumer = AIOKafkaConsumer(
                *self.topics, group_id=self.group,
                loop=self.loop or asyncio.get_event_loop(),
                bootstrap_servers=f'{self.host}:{self.port}',
                value_deserializer=self.deserializer,
                auto_offset_reset='earliest'
            )
            await self._consumer.start()
        return self._consumer

    async def getone(self):
        """Reads only one message
        """
        if not self._consumer:
            await self.init_consumer()
        return await self._consumer.getone()

    async def __aiter__(self):
        """Yield messages from consumed from Kafka
        """
        return await self.init_consumer()

    async def stop(self):
        return await self._consumer.stop()


class ITemplateConsumer(Interface):
    async def consume():
        pass


@configure.adapter(for_=IKafkaConsumer, provides=ITemplateConsumer)
class TemplateConsumer:

    def __init__(self, consumer: KafkaConsumer):
        self.consumer = consumer

    async def consume(self, **kwargs):
        print('Started TemplateConsumer.')
        try:
            async for message in self.consumer:
                print(message)
        finally:
            await self.consumer.stop()
            print('Stoped TemplateConsumer.')
