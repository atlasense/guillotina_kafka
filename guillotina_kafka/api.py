import asyncio
import json

from guillotina import app_settings, configure
from guillotina.api.service import Service

from guillotina_kafka.consumer import KAFKA_CONSUMER_STAT
from guillotina_kafka.producer import GetKafkaProducer

producer = None


@configure.service(
    method="POST", name="@kafka-producer/{topic}", permission="guillotina.ModifyContent"
)
class producer_service(Service):
    async def __call__(self):
        global producer
        if producer is None:
            producer = GetKafkaProducer("json", app_settings)
        topic = self.request.matchdict.get("topic")
        data = await self.request.json()
        sent, result = await producer.send(topic, value=data)

        if not sent:
            result = dict(error=str(result))
        else:
            result = {
                "topic": result.topic,
                "partition": result.partition,
                "offset": result.offset,
                "timestamp": result.timestamp,
            }

        return {"sent": sent, "result": result}


@configure.service(
    method="GET", name="@consumer-stat", permission="guillotina.AccessContent"
)
class consumer_stat(Service):
    async def __call__(self):
        return KAFKA_CONSUMER_STAT
