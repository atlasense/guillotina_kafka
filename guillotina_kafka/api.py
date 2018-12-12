from guillotina import configure
from guillotina.api.service import Service
from guillotina.component import get_adapter
from guillotina_kafka.utility import get_kafka_producer
from guillotina_kafka.producers import IWebApiSendMessage


@configure.service(
    method='POST',
    name='@kafka-producer/{topic}',
    permission='guillotina.AccessContent')
class producer_service(Service):

    async def __call__(self):
        producer = get_kafka_producer()
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
