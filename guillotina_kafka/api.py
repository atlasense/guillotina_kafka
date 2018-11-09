from guillotina import configure
from guillotina.api.service import Service
from guillotina_kafka.util import get_kafka_producer
import json
from aiokafka.structs import OffsetAndMetadata, TopicPartition


@configure.service(
    method="POST", name="@kafka-producer/{topic}", permission="guillotina.AccessContent"
)
class Run(Service):
    async def __call__(self):
        producer = await get_kafka_producer()
        topic = self.request.matchdict.get("topic")
        data = await self.request.json()
        module = self.request.GET.get("module")
        key = json.dumps({"module": module}).encode()
        result = await producer.conn.send_and_wait(
            topic, value=json.dumps(data).encode(), partition=1, key=key
        )
        if not result:
            result = dict(error="failed")
        else:
            result = {
                "topic": result.topic,
                "partition": result.partition,
                "offset": result.offset,
                "timestamp": result.timestamp,
            }

        return {"result": result}
