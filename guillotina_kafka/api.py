from guillotina import configure
from guillotina.api.service import Service
from guillotina_kafka.utilities import get_kafka_producer
from guillotina_kafka.producers import WebApiSendMessage


@configure.service(
    method='POST',
    name='@kafka-producer/{topic}',
    permission='guillotina.AccessContent')
class producer_service(Service):

    async def __call__(self):
        producer_utility = get_kafka_producer()
        producer = WebApiSendMessage(producer_utility)
        topic = self.request.matchdict.get('topic')
        data = await self.request.json()

        future = await producer.send(topic, data)
        # Get kafka record metadata
        record = await future
        return {
            'topic': record.topic,
            'partition': record.partition,
            'offset': record.offset,
            'timestamp': record.timestamp,
        }
