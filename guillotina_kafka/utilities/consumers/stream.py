from guillotina import configure
from guillotina_kafka.utilities.consumers import Consumer
from guillotina_kafka.interfaces.consumers import IConsumer
from guillotina_kafka.interfaces.consumers import IStreamConsumer


@configure.adapter(for_=IConsumer, provides=IStreamConsumer)
class StreamConsumerUtility:

    def __init__(self, consumer: Consumer):
        self.consumer = consumer

    async def consume(self, **kwargs):
        print('Started TemplateConsumer.')

        consumer_worker = kwargs.get('consumer_worker')
        try:
            async for message in self.consumer:
                _ = await consumer_worker(message)
        finally:
            await self.consumer.stop()
            print('Stoped TemplateConsumer.')