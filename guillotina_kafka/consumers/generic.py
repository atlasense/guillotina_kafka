from guillotina_kafka.consumers import make_consumer
from guillotina_kafka.interfaces import IConsumer
import logging
import json
from guillotina import configure

logger = logging.getLogger('guillotina_kafka')


@configure.utility(provides=IConsumer, name='generic')
class GenericConsumer:
    def consumer(self):
        return generic_consumer


async def generic_consumer(kafka_hosts, topics, group_id='g1'):
    print(f'Starting es_consumer:{group_id} <= {topics!r}')
    consumer = await make_consumer(kafka_hosts, topics, group_id)
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                data = msg.value
                key = json.loads(msg.key.decode())
                mod = __import__(key.get('module'))
                fn = getattr(mod, data['fn'])
                if 'kwargs' in data:
                    res = fn(*data['args'], **data.get('kwargs'))
                else:
                    res = fn(*data['args'])
                print(res)
            except Exception:
                pass
    finally:
        print('Stoping consumer ...')
        consumer.stop()
