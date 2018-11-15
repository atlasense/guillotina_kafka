from zope.interface import Attribute
from zope.interface import Interface
from guillotina_kafka.interfaces.kafka import IKafak


class IConsumer(IKafak):
    topics = Attribute('Kafka topics to consume from.')
    group = Attribute(
        'Consumer group, used by consumers to '
        'coordinate message consumption from topics')


class IStreamConsumer(Interface):
    async def consume():
        pass


class IGenericBatchConsumer(Interface):
    async def consume():
        pass
