from zope.interface import Attribute
from zope.interface import Interface


class IKafkaProducerUtility(Interface):
    pass


class IWebApiSendMessage(Interface):
    async def send(self, topic, message):
        pass


class ICliSendMessage(Interface):
    async def send_one(self, topic, message):
        pass

    async def send(self):
        pass


class IKafka(Interface):
    application_name = Attribute('Name of the application')
    host = Attribute('Kafka brocker host')
    port = Attribute('Kafka brocker port')


class IConsumer(IKafka):
    topics = Attribute('Kafka topics to consume from.')
    group = Attribute(
        'Consumer group, used by consumers to '
        'coordinate message consumption from topics')
