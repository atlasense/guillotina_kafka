from guillotina import configure
from zope.interface import Interface
from zope.interface import implementer
from guillotina_kafka.interfaces import IKafkaProducerUtility
from guillotina_kafka.utility import KafkaProducerUtility


from aiokafka import AIOKafkaProducer
import asyncio


class ICliSendMessage(Interface):
    async def send_one(self, topic, message):
        pass

    async def send(self):
        pass


@configure.adapter(for_=IKafkaProducerUtility, provides=ICliSendMessage)
class CliSendMessage:
    """Adapter of kafka producer utility to send cli messages to kafka
    """
    def __init__(self, util: KafkaProducerUtility):
        self.util = util

    async def send_one(self, topic, message):
        if not self.util.is_ready:
            await self.util.start()
        response = await self.util.send(topic, message)
        await self.util.stop()
        return response

    async def send(self):
        await self.util.start()
        while True:
            message = input("> ")
            if not message:
                break
            topic = message.split(' ')[0]
            data = message.split(' ')[1:]
            await self.util.send(topic, data)

        await self.util.stop()


class IWebApiSendMessage(Interface):
    async def send(self, topic, message):
        pass


@configure.adapter(for_=IKafkaProducerUtility, provides=IWebApiSendMessage)
class WebApiSendMessage:
    """Adapter that simplifies the producer interface to just allow
    sending messages to a arbitrary kafka topics
    """

    def __init__(self, util: KafkaProducerUtility):
        self.util = util

    async def send(self, topic, message):
        if not self.util.is_ready:
            await self.util.start()

        return await self.util.send(topic=topic, data=message)
