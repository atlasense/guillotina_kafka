import json
import asyncio
from guillotina import configure
from guillotina.api.service import Service
from guillotina_kafka.util import get_kafka_producer


@configure.service(
    method='POST',
    name='@kafka-producer/{topic}',
    permission='guillotina.AccessContent')
class Run(Service):

    async def __call__(self):

        producer = await get_kafka_producer()
        topic = self.request.matchdict.get('topic')
        data = await self.request.json()
        sent, result = await producer.send(topic, data)

        if not sent:
            result = dict(error=str(result))
        else:
            result = {
                'topic': result.topic,
                'partition': result.partition,
                'offset': result.offset,
                'timestamp': result.timestamp,
            }

        return {'sent': sent, 'result': result}
