from guillotina import configure
from guillotina.api.service import Service
from guillotina_kafka.util import get_kafka_producer
import json


@configure.service(
    method='POST',
    name='@kafka-producer/{topic}',
    permission='guillotina.AccessContent')
class Run(Service):
    async def __call__(self):
        producer = await get_kafka_producer()
        topic = self.request.matchdict.get('topic')
        import pdb; pdb.set_trace()

        data = await self.request.json()
        module = self.request.GET.get('module').encode()
        result = await producer.conn.send_and_wait(topic,
                                                   value=json.dumps(data).encode(),
                                                   key=module)
        if not result:
            result = dict(error='failed')
        else:
            result = {
                'topic': result.topic,
                'partition': result.partition,
                'offset': result.offset,
                'timestamp': result.timestamp,
            }

        return {
            'result': result
        }
