from guillotina import configure
# from guillotina_kafka.utilities.producers.default import DefaultProducerUtility
# from guillotina_kafka.consumers.template import ITemplateConsumer


app_settings = {
    "commands": {
        "start-producer": "guillotina_kafka.commands.kafka_producer.SendMessageCommand",
        "start-consumer": "guillotina_kafka.commands.kafka_consumer.StartConsumerCommand"
    }
}


def includeme(root):
    """
    custom application initialization here
    """
    configure.scan('guillotina_kafka.api')
    configure.scan('guillotina_kafka.install')
