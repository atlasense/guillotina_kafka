from aiokafka import TopicPartition
from aiokafka.errors import IllegalStateError
from guillotina import configure
from guillotina_kafka.consumer import Consumer
from guillotina_kafka.interfaces import IConsumer
from guillotina_kafka.interfaces import IConsumerUtility
from zope.interface import implementer


@implementer(IConsumer)
class StreamConsumer(Consumer):

    async def _seek(self, topic, step):
        partition = self._consumer.partitions_for_topic(topic)
        if not partition:
            return
        pid = partition.pop()
        tp = TopicPartition(topic, pid)

        try:
            position = await self._consumer.position(tp)
        except IllegalStateError:
            position = 0

        if position > 0:
            self._consumer.seek(tp, position + step)

    async def seek(self, step=-1):
        await self.init()
        if self.has_regex_topic:
            return
        for topic in self.topics:
            await self._seek(topic, step)

    async def __aiter__(self):
        return await self.init()


@configure.adapter(
    for_=IConsumer, provides=IConsumerUtility, name='stream')
class StreamConsumerUtility:

    def __init__(self, consumer: StreamConsumer):
        self.consumer = consumer

    async def consume(self, arguments, settings):

        if not self.consumer.is_ready:
            await self.consumer.init()

        try:
            print('Starting StreamConsumerUtility ...')
            await self.consumer.seek(step=-1)  # Move to the previous offset
            async for message in self.consumer:
                _ = await self.consumer.worker(
                    message, arguments=arguments, settings=settings)
        finally:
            await self.consumer.stop()
            print('Stoped StreamConsumerUtility.')
