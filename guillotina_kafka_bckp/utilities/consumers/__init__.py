import asyncio
from aiokafka import AIOKafkaConsumer
from zope.interface import implementer
from guillotina_kafka.interfaces.consumers import IConsumer


class ConsumerLookupError(Exception):
    pass


@implementer(IConsumer)
class Consumer(object):

    def __init__(
            self, app_name, host, port, group,
            topics, auto_offset_reset=None, enable_auto_commit=True, 
            loop=None, key_deserializer=None, value_deserializer=None):

        self.app_name = app_name
        self._consumer = None

        self.config  = {
            'topics': topics,
            'group_id': group,
            'key_deserializer': key_deserializer,
            'bootstrap_servers': f'{host}:{port}',
            'value_deserializer': value_deserializer,
            'loop': loop or asyncio.get_event_loop(),
            'enable_auto_commit': enable_auto_commit,
            'auto_offset_reset': auto_offset_reset or 'earliest'
        }

    async def init(self):
        if self._consumer is None:
            self._consumer = AIOKafkaConsumer(
                *self.config['topics'], **self.config
            )
            await self._consumer.start()
        return self._consumer

    async def __aiter__(self):
        return await self.init()

    async def take(self, timeout_ms:int, max_records:int):
        while True:
            result = await self._consumer.getmany(
                timeout_ms=timeout_ms, max_records=max_records
            )
            for topic_partition, messages in result.items():
                yield topic_partition, messages

    @property
    def commit_offset(self):
        return self._consumer.commit

    async def stop(self):
        return (await self._consumer.stop())