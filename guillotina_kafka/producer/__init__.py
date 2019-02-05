import json
import pickle
from functools import wraps
from guillotina.component import get_adapter
from aiokafka.errors import KafkaError, KafkaTimeoutError
from guillotina_kafka.producer.generic import GenericProducer
from guillotina_kafka.producer.generic import IProducerUtility

SERIALIZER = {
    'json': lambda data: json.dumps(data).encode('utf-8'),
    'bytes': lambda data: data.encode('utf-8'),
    'pickle': pickle.dumps,
}


__instances = {}
def singleton(cls):
    @wraps(cls)
    def getInstance(*args, **kwargs):
        instance = __instances.get(cls, None)
        if not instance:
            instance = cls(*args, **kwargs)
            __instances[cls] = instance
        return instance
    return getInstance


@singleton
class GetKafkaProducer:

    def __init__(self, serializer, settings, **kwargs):
        serializer = SERIALIZER.get(
            serializer, lambda data: data.encode('utf-8')
        )
        self.producer = GenericProducer(
            bootstrap_servers=settings['kafka']['brokers'],
            value_serializer=serializer, **kwargs
        )
        self.producer = get_adapter(self.producer, IProducerUtility, name='generic')

    async def send(self, *args, **kwargs):
        try:
            result = await self.producer.send(*args, **kwargs)
            return True, await result
        except KafkaTimeoutError:
            return False, 'produce timeout... maybe we want to resend data again?'
        except KafkaError as err:
            return False, str(err)

    async def stop(self):
        return await self.producer.stop()
