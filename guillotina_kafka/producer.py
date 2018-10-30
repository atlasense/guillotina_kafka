import logging
from guillotina import configure
from zope.interface import Interface
from guillotina_kafka.interfaces import Producer
from guillotina_kafka.interfaces import IProducer


logger = logging.getLogger('SendMessage')


class ICliSendMessage(Interface):
    async def send_one():
        pass

    async def send():
        pass


@configure.adapter(for_=IProducer, provides=ICliSendMessage)
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


@configure.adapter(for_=IProducer, provides=IWebApiSendMessage)
class WebApiSendMessage:

    def __init__(self, producer: Producer):
        self.producer = producer

    async def send(self, topic, message):
        if not self.producer.is_ready:
            await self.producer.start()
        return (await self.producer._send(topic=topic, data=message))
