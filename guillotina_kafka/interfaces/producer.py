from zope.interface import Attribute
from zope.interface import Interface
from guillotina_kafka.interfaces.kafka import IKafak


class IProducer(IKafak):
    topic = Attribute('Kafka topic to produce to.')


class IDefaultProducer(Interface):
    async def send():
        pass

    async def send_one():
        pass

    async def interactive_send():
        pass
