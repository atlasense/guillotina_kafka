from guillotina import configure
from guillotina_kafka.utilities.consumers import Consumer
from guillotina_kafka.interfaces.consumers import IConsumer
from guillotina_kafka.interfaces.consumers import IGenericBatchConsumer


@configure.adapter(for_=IConsumer, provides=IGenericBatchConsumer)
class GenericBatchConsumerUtility:

    def __init__(self, consumer: Consumer):
        self.consumer = consumer

    async def consume(self, **kwargs):
        print(f'Started {self.consumer.app_name}.')

        timeout = kwargs.get('timeout_ms', 60*1000)
        max_records = kwargs.get('max_records')
        consumer_worker = kwargs.get('consumer_worker')

        _ = await self.consumer.init()
        try:
            async for tp, messages in self.consumer.take(
                timeout_ms=timeout, max_records=max_records):
                if messages:
                    _ = await consumer_worker(messages)
                    _ = await self.consumer.commit_offset({tp: messages[-1].offset + 1})
        finally:
            await self.consumer.stop()
            print('Stoped TemplateConsumer.')