from guillotina_kafka.utilities import get_kafka_producer

import json


class CliSendMessage:
    """Example of kafka producer utility to send cli messages to kafka

    """
    def __init__(self, loop=None):
        self.util = get_kafka_producer(loop)

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


class WebApiSendMessage:
    """Example of producer that just allows sending messages to a
    arbitrary kafka topics
    """
    def __init__(self, loop=None):
        self.util = get_kafka_producer(loop)
        self.serializer = lambda x: json.dumps(x).encode()

    async def send(self, topic, message):
        if not self.util.is_ready:
            await self.util.start()

        return await self.util.send(topic=topic, data=message,
                                    serializer=self.serializer)
