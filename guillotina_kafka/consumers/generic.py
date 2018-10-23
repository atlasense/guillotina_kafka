from guillotina_kafka.consumers import make_consumer
import logging
import json


logger = logging.getLogger('guillotina_kafka')


async def generic_consumer(kafka_hosts, topics, group_id='g1'):
    print(f'Starting es_consumer:{group_id} <= {topics!r}')
    consumer = await make_consumer(kafka_hosts, topics, group_id)
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                data = msg.value
                mod = __import__(msg.key.decode())
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
