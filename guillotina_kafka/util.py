import json
import aiohttp
import asyncio
import logging
from guillotina import app_settings
from guillotina import configure
from zope.interface import Interface
from aiokafka import AIOKafkaProducer
import kafka.common as kafkaError

logger = logging.getLogger('guillotina_kafka')
kafka_producer = None


def get_kafa_host():
    host = app_settings['kafka'].get('host')
    port = app_settings['kafka'].get('port')
    return f'{host}:{port}'


def get_producer_api_url():
    return app_settings['kafka'].get('producer_api_url')


async def get_kafka_producer():
    global kafka_producer
    if kafka_producer is None:
        kafka_producer = KafkaProducer(None,  get_kafa_host())
        await kafka_producer.connect()
    return kafka_producer


async def send_to_kafka(topic, payload):
    url = f'{get_producer_api_url()}/{topic}'
    auth = aiohttp.BasicAuth(login='root', password='root')
    async with aiohttp.ClientSession(json_serialize=json.dumps) as session:
        respons = await session.post(url, json=payload, auth=auth)
    return (respons.status, await respons.json())


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
        try:
            result = await self.conn.send(topic, self.serializer(data))
            return True, await result
        except kafkaError.RequestTimedOutError as e:
            return False, e
