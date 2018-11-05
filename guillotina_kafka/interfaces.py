import json
import asyncio
from guillotina import app_settings
from zope.interface import Attribute
from zope.interface import Interface
from zope.interface import implementer
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
import kafka.common as kafkaError


class IKafak(Interface):
    application_name = Attribute('Name of the apllication')
    host = Attribute('Kafka brocker host')
    port = Attribute('Kafka brocker port')


class IProducer(IKafak):
    topic = Attribute('Kafka topic to produce to.')


class IConsumer(IKafak):
    topics = Attribute('Kafka topics to consume from.')
    group = Attribute(
        'Consumer group, used by consumers to '
        'coordinate message consumption from topics')


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

    async def __aiter__(self):

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

    async def stop(self):
        return (await self._consumer.stop())


@implementer(IProducer)
class Producer(object):

    def __init__(
            self, host, port, topic, max_request_size=104857600,
            serializer=lambda msg: msg.encode()):

        self.host = host
        self.port = port
        self.topic = topic
        self.serializer = serializer
        self.max_request_size = max_request_size
        self._producer = None

    @property
    def is_ready(self):
        if self._producer is None:
            return False
        return True

    async def start(self):
        if self._producer is None:
            # enable_idempotence=True Add his option for exactly once semantics
            self._producer = AIOKafkaProducer(
                loop=asyncio.get_event_loop(),
                max_request_size=self.max_request_size,
                bootstrap_servers=f'{self.host}:{self.port}'
            )
            await self._producer.start()

    async def _send(self, **kwargs):
        topic = kwargs.get('topic')
        data = kwargs.get('data')
        try:
            result = await self._producer.send(topic, self.serializer(data))
            return True, await result
        except kafkaError.RequestTimedOutError as e:
            return False, e

    async def send(self, data):
        return (await self._send(topic=self.topic, data=data))

    async def stop(self):
        return (await self._producer.stop())
