import json
import asyncio
from guillotina import configure
from guillotina.api.service import Service
from guillotina_kafka.interfaces import IKafkaUtility
from guillotina.component import get_utility

# producer = get_utility(IKafkaUtility)


@configure.service(
    method='POST',
    name='@kafka-producer/{topic}',
    permission='guillotina.AccessContent')
class Run(Service):

    async def __call__(self):
        topic = self.request.matchdict.get('topic')
        data = await self.request.json()
        # await producer.send(topic, data)
        return {
            'topic': topic,
            'data': data
        }
