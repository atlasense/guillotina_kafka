from guillotina import configure
from guillotina_kafka.interfaces import IKafkaProducerUtility
from guillotina_kafka.interfaces import ICliSendMessage
from guillotina_kafka.interfaces import IWebApiSendMessage
from guillotina_kafka.utilities import KafkaProducerUtility

import json


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

    async def send(self, topic):
        await self.util.start()
        while True:
            message = input("> ")
            if not message:
                break
            await self.util.send(topic, message)

        await self.util.stop()


@configure.adapter(for_=IKafkaProducerUtility, provides=IWebApiSendMessage)
class WebApiSendMessage:
    """Adapter that simplifies the producer interface to just allow
    sending messages to a arbitrary kafka topics
    """
    def __init__(self, util: KafkaProducerUtility):
        self.util = util
        self.serializer = lambda x: json.dumps(x).encode()

    async def send(self, topic, message):
        if not self.util.is_ready:
            await self.util.start()

        return await self.util.send(topic=topic, data=message,
                                    serializer=self.serializer)
