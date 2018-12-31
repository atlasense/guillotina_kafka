import json
import asyncio
from aiohttp import web
from guillotina import configure
from guillotina import app_settings
from guillotina.api.service import Service
from guillotina_kafka.producer import GetKafkaProducer


producer = None

@configure.service(
    method='POST',
    name='@kafka-producer/{topic}',
    permission='guillotina.AccessContent')
class producer_service(Service):

    async def __call__(self):
        global producer
        if producer is None:
            producer = GetKafkaProducer('json', app_settings)
        topic = self.request.matchdict.get('topic')
        data = await self.request.json()
        sent, result = await producer.send(topic, value=data)

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
