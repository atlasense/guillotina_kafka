from aiokafka import AIOKafkaProducer
from guillotina import app_settings
from guillotina.component import get_utility
from guillotina_kafka.interfaces import IKafkaProducerUtility
from guillotina_kafka.producer import SERIALIZER

import asyncio
import json


class KafkaProducerUtility:
    """This defines the singleton that will hold the connection to kafka
    and allows to send messages from it.
    """
    def __init__(self, settings, loop=None):
        # Get kafka connection details from app settings
        self.loop = loop
        self.producer = None
        self.settings = settings or {}

    async def setup(self, **kwargs):
        """Gets or creates the connection to kafka"""
        serializer = SERIALIZER.get(
            self.settings.get('serializer') or 'json',
            lambda data: json.dumps(data).encode('utf-8')
        )
        config = {**{
            'bootstrap_servers': app_settings['kafka']['brokers'],
            'value_serializer': serializer
        }, **self.settings, **kwargs}
        config.setdefault(
            'loop', self.loop or asyncio.get_event_loop())
        if self.producer is None:
            self.producer = AIOKafkaProducer(**config)
            await self.producer.start()
        return self.producer

    @property
    def is_ready(self):
        """Returns whether aiokafka producer connection is ready"""
        if self.producer is None:
            return False
        return True

    async def send(self, *args, **kwargs):
        if not self.is_ready:
            raise Exception('Producer utility is not ready')
        return await self.producer.send(*args, **kwargs)

    async def stop(self):
        if not self.is_ready:
            raise Exception('Producer utility is not ready')
        _ = await self.producer.stop()
        self.producer = None
        return _


def get_kafka_producer():
    return get_utility(IKafkaProducerUtility)
