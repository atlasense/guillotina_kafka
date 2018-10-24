import logging
from guillotina import configure
from zope.interface import Interface
from guillotina_kafka.interfaces import Consumer
from guillotina_kafka.interfaces import IConsumer


logger = logging.getLogger('WsConsumer')


class IWsConsumer(Interface):
    async def consume():
        pass


@configure.adapter(for_=IConsumer, provides=IWsConsumer)
class WsConsumer:

    def __init__(self, consumer: Consumer):
        self.consumer = consumer

    async def consume(self, ws):
        print('Started WsConsumer.')

        try:
            async for message in self.consumer:
                await ws.send_str(message.value)
        finally:
            await self.consumer.stop()
            await ws.send_str('CLOSE')
            await ws.close()
            print('Stoped WsConsumer.')
