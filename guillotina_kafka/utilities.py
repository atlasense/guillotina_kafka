from aiokafka import AIOKafkaProducer
from guillotina import app_settings
from guillotina import configure
from guillotina.component import get_utilities_for
from guillotina.component import get_utility
from guillotina_kafka.interfaces import IActiveConsumer
from guillotina_kafka.interfaces import IKafkaProducerUtility

import asyncio
import json
import logging

logger = logging.getLogger(__name__)


@configure.utility(provides=IKafkaProducerUtility)
class KafkaProducerUtility:
    """This defines the singleton that will hold the connection to kafka
    and allows to send messages from it.
    """
    def __init__(self, loop=None):
        # Get kafka connection details from app settings
        self.loop = loop
        self.producer = None
        self._lock = None

    async def stop_active_consumers(self):
        for name, worker in get_utilities_for(IActiveConsumer):
            if hasattr(worker, '__consumer__') and not getattr(worker, '__stopped__', False):
                try:
                    logging.warning(f'Stopping {name} consumer')
                    await worker.__consumer__.stop()
                    worker.__stopped__ = True
                except Exception:
                    logger.warning(f"Error stopping consumer: {name}", exc_info=True)

    @property
    def lock(self):
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def setup(self, **kwargs):
        """Gets or creates the connection to kafka"""
        async with self.lock:
            # make configuration is locked so multiple tasks can't attempt
            if self.is_ready:
                return
            self.config = {**{
                'bootstrap_servers': app_settings['kafka']['brokers'],
                'value_serializer': lambda data: json.dumps(data).encode('utf-8')
            }, **kwargs}
            self.config.setdefault(
                'loop', self.loop or asyncio.get_event_loop())
            if self.producer is None:
                producer = AIOKafkaProducer(**self.config)
                await producer.start()
                # delay setting the value until after the producer object
                # is setup; otherwise, other async tasks will attempt
                # to use this object before it is ready and get errors
                self.producer = producer
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
