from guillotina import configure
from zope.interface import Interface
from aiokafka.errors import KafkaError, KafkaTimeoutError
from guillotina_kafka.utilities.producers import Producer
from guillotina_kafka.interfaces.producer import IProducer
from guillotina_kafka.interfaces.producer import IDefaultProducer



class IDefaultProducer(Interface):
    async def send():
        pass

    async def send_one():
        pass

    async def interactive_send():
        pass


@configure.adapter(for_=IProducer, provides=IDefaultProducer)
class DefaultProducerUtility:

    def __init__(self, producer: Producer):
        self.producer = producer

    def __repr__(self):
        return f'name={self.producer.app_name}'

    async def send(self, topic, data=None, key=None, batch=False):
        if not self.producer.is_ready:
            await self.producer.init()

        sender = self.producer.send
        if batch:
            sender = self.producer.send_and_wait

        try:
            result = await sender(topic, value=data, key=key)
            return True, await result
        except KafkaTimeoutError:
            return False, 'produce timeout... maybe we want to resend data again?'
        except KafkaError as err:
            return False, str(err)

    async def send_one(self, topic, data=None, key=None):
        try:
            result = await self.producer.send(topic, data=data, key=key)
        finally:
            await self.producer.stop()
        return result

    async def interactive_send(self, topic):
        try:
            while True:
                message = input("> ")
                if not message:
                    break
                print(await self.send(topic, data=message))
        finally:
            await self.producer.stop()
