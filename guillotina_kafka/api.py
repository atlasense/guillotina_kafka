import json
import asyncio
from aiohttp import web
from guillotina import configure
from guillotina import app_settings
from guillotina.api.service import Service
from guillotina.component import get_adapter
from guillotina_kafka.util import get_kafka_producer
from guillotina_kafka.producer import IWebApiSendMessage
from guillotina_kafka.interfaces import Consumer
from guillotina_kafka.consumers.ws_consumer import IWsConsumer


@configure.service(
    method='POST',
    name='@kafka-producer/{topic}',
    permission='guillotina.AccessContent')
class producer_service(Service):

    async def __call__(self):

        producer = await get_kafka_producer()
        producer = get_adapter(producer, IWebApiSendMessage)
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


@configure.service(
    method='GET',
    name='@kafka-consumer/{topic}/{consumer_group}',
    permission='guillotina.AccessContent')
class consumer_service(Service):

    async def __call__(self):

        topic = self.request.matchdict.get('topic')
        consumer_group = self.request.matchdict.get('consumer_group')

        ws = web.WebSocketResponse()
        await ws.prepare(self.request)

        consumer = Consumer(
            'ws-consumer-on-{topic}',
            app_settings['kafka'].get('host', '127.0.0.1'),
            app_settings['kafka'].get('port', 9092),
            consumer_group,
            [topic]
        )

        consumer = get_adapter(consumer, IWsConsumer)
        await consumer.consume(ws)
