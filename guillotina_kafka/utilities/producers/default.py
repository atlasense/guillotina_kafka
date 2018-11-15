from guillotina import configure
from aiokafka.errors import KafkaError, KafkaTimeoutError
from guillotina_kafka.utilities.producers import Producer
from guillotina_kafka.interfaces.producer import IProducer
from guillotina_kafka.interfaces.producer import IDefaultProducer


@configure.adapter(for_=IProducer, provides=IDefaultProducer)
class DefaultProducerUtility:

    def __init__(self, producer: Producer):
        self.producer = producer

    async def send(self, topic, data=None, key=None):
        if not self.producer.is_ready:
            await self.producer.init()

        try:
            result = await self.producer.send(topic, value=data, key=key)
            return True, await result
        except KafkaTimeoutError:
            return False, 'produce timeout... maybe we want to resend data again?'
        except KafkaError as err:
            return False, str(err)

    async def send_one(self, topic, data=None, key=None):
        result = await self.send(topic, data=data, key=key)
        await self.producer.stop()
        return result

    async def interactive_send(self, topic):
        while True:
            message = input("> ")
            if not message:
                break
            print(await self.send(topic, data=message))
        await self.producer.stop()
