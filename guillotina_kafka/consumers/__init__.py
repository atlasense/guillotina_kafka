import asyncio
import json
from json import JSONDecodeError
from aiokafka import AIOKafkaConsumer


def deserializer(msg):
    try:
        msg = msg.decode('utf-8')
        result = json.loads(msg)
    except JSONDecodeError as e:
        result = {}
    return result


async def make_consumer(kafka_hosts, topics, group_id=None):
    consumer = AIOKafkaConsumer(
        *topics, group_id=group_id,
        loop=asyncio.get_event_loop(),
        bootstrap_servers=kafka_hosts,
        value_deserializer=deserializer)
    return consumer
