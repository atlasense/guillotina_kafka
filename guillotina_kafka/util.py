import json
import asyncio
import logging
from guillotina import app_settings
from guillotina import configure
from .interfaces import IKafkaUtility
from zope.interface import Interface
from aiokafka import AIOKafkaProducer

logger = logging.getLogger('guillotina_kafka')


def get_kafa_host():
    # print(app_settings['applications'])
    host = app_settings['kafka'].get('host')
    port = app_settings['kafka'].get('port')
    return f'{host}:{port}'


class KafkaProducer:

    def __init__(self, loop, bootstrap_servers):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.bootstrap_servers = bootstrap_servers
        self.conn = None

    def serializer(self, data):
        return json.dumps(data).encode()

    async def connect(self):
        self.conn = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.bootstrap_servers)
        await self.conn.start()

    async def close(self):
        await self.conn.stop()

    async def send(self, topic, data):
        self.conn.send_and_wait(topic, self.serializer(data))


@configure.utility(provides=IKafkaUtility)
class KafkaUtility:

    def __init__(self):
        self.bootstrap_servers = get_kafa_host()
        self._instance = None

    async def get(self):
        return self._instance

    async def initialize(self, app=None):
        self._instance = KafkaProducer(None,  self.bootstrap_servers)
        await self._instance.connect()

    async def finalize(self, app=None):
        await self._instance.close()
