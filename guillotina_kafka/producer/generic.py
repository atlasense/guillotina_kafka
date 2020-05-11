from aiokafka import AIOKafkaProducer
from guillotina import configure
from guillotina_kafka.interfaces import IProducer
from guillotina_kafka.interfaces import IProducerUtility
from zope.interface import implementer

import asyncio


@implementer(IProducer)
class GenericProducer(object):
    def __init__(self, loop=None, **kwargs):

        self._producer = None
        self.config = {**kwargs, "loop": loop or asyncio.get_event_loop()}

    @property
    def is_ready(self):
        if self._producer is None:
            return False
        return True

    async def init(self):
        if self._producer is None:
            self._producer = AIOKafkaProducer(**self.config)
            await self._producer.start()
        return self._producer

    async def send(self, *args, **kwargs):
        if not self.is_ready:
            await self.init()
        return await self._producer.send(*args, **kwargs)

    async def stop(self):
        return await self._producer.stop()


@configure.adapter(for_=IProducer, provides=IProducerUtility, name="generic")
class GenericProducerUtility:
    def __init__(self, producer: GenericProducer):
        self.producer = producer

    async def send(self, *args, **kwargs):
        return await self.producer.send(*args, **kwargs)

    async def stop(self):
        await self.producer.stop()
