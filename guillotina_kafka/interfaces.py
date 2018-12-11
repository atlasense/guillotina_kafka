import json
import asyncio
from zope.interface import Attribute
from zope.interface import Interface


class IKafka(Interface):
    application_name = Attribute('Name of the application')
    host = Attribute('Kafka brocker host')
    port = Attribute('Kafka brocker port')


class IKafkaProducerUtility(IKafka):
    topic = Attribute('Kafka topic to produce to.')


class IConsumer(IKafka):
    topics = Attribute('Kafka topics to consume from.')
    group = Attribute(
        'Consumer group, used by consumers to '
        'coordinate message consumption from topics')
