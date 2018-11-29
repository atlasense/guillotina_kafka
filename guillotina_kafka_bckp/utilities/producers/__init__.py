import asyncio
from aiokafka import AIOKafkaProducer
from zope.interface import implementer
from guillotina_kafka.interfaces.producer import IProducer


@implementer(IProducer)
class Producer(object):

    def __init__(
            self, app_name, loop=None, max_request_size=None, **kwargs):

        self.app_name = app_name
        self._producer = None

        self.config = {
            'loop': loop or asyncio.get_event_loop(),
            'max_request_size': max_request_size or 104857600,
            **kwargs
        }

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

    @property
    def send(self):
        return self._producer.send

    @property
    def send_and_wait(self):
        return self._producer.send_and_wait

    async def stop(self):
        return (await self._producer.stop())
