from zope.interface import Attribute
from zope.interface import Interface


class IWebApiSendMessage(Interface):
    async def send(self, topic, message):
        pass


class ICliSendMessage(Interface):
    async def send_one(self, topic, message):
        pass

    async def send(self):
        pass


class IKafka(Interface):
    host = Attribute('Kafka brocker host')
    port = Attribute('Kafka brocker port')


class IKafkaProducerUtility(IKafka):
    pass


class IKafkaConsumer(IKafka):
    application_name = Attribute('Name of the application')
    topics = Attribute('Kafka topics to consume from.')
    group = Attribute(
        'Consumer group, used by consumers to '
        'coordinate message consumption from topics')
