from guillotina import configure
from zope.interface import Interface
from guillotina_kafka.interfaces import IKafkaProducerUtility
from aiokafka import AIOKafkaProducer
import kafka.common as kafkaError
import asyncio
from zope.interface import implementer


@configure.utility(provides=IKafkaProducerUtility, name='basic')
class KafkaProducerUtility:
    def __init__(self, topic=None, max_request_size=104857600,
                 serializer=lambda msg: msg.encode(), loop=None):
        self.host = app_settings['kafka']['host']
        self.port = app_settings['kafka']['port']
        self.topic = topic
        self.serializer = serializer
        self.max_request_size = max_request_size
        self.loop = loop or asyncio.get_event_loop()
        self._producer = None

    @property
    def producer(self):
        """Gets or creates the connection to kafka"""
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                loop=self.loop,
                max_request_size=self.max_request_size,
                bootstrap_servers=f'{self.host}:{self.port}'
            )
        return self._producer

    @property
    def is_ready(self):
        """Returns whether aiokafka producer connection is ready"""
        if self._producer is None:
            return False
        import pdb; pdb.set_trace()
        return self._producer.client.started

    async def start(self):
        """Starts the producer connection if not it's not ready already
        """
        if self.is_ready:
            # Already started
            return
        await self._producer.start()

    async def send(self, data, topic=None):
        """
        If topic not specified, will use class attribute
        """
        if not self.is_ready:
            await self.start()

        return await self._producer.send(topic or self.topic, self.serializer(data))

    async def stop(self):
        return (await self._producer.stop())


class ICliSendMessage(Interface):
    async def send_one():
        pass

    async def send():
        pass


@configure.adapter(for_=IKafkaProducerUtility, provides=ICliSendMessage)
class CliSendMessage:

    def __init__(self, producer: Producer):
        self.producer = producer

    async def send_one(self, message):
        _ = await self.producer.start()
        response = await self.producer.send(message)
        _ = await self.producer.stop()
        return response

    async def send(self):
        await self.producer.start()
        while True:
            message = input("> ")
            if not message:
                break
            print(await self.producer.send(message))
        await self.producer.stop()


class IWebApiSendMessage(Interface):
    async def send():
        pass


@configure.adapter(for_=IKafkaProducerUtility, provides=IWebApiSendMessage)
class WebApiSendMessage:

    def __init__(self, producer: Producer):
        self.producer = producer

    async def send(self, topic, message):
        if not self.producer.is_ready:
            await self.producer.start()

        self.producer.topic = topic
        return (await self.producer._send(topic=topic, data=message))
