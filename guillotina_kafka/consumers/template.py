import logging
from guillotina import configure
from zope.interface import Interface
from guillotina_kafka.interfaces import Consumer
from guillotina_kafka.interfaces import IConsumer


logger = logging.getLogger('TemplateConsumer')


class ITemplateConsumer(Interface):
    async def consume():
        pass


@configure.adapter(for_=IConsumer, provides=ITemplateConsumer)
class TemplateConsumer:

    def __init__(self, consumer: Consumer):
        self.consumer = consumer

    async def consume(self, **kwargs):
        print('Started TemplateConsumer.')

        try:
            async for message in self.consumer:
                print(message)
        finally:
            await self.consumer.stop()
            print('Stoped TemplateConsumer.')
