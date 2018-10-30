from guillotina import configure
from guillotina_kafka.consumers.template import ITemplateConsumer


app_settings = {
    "commands": {
        "kafka-producer": "guillotina_kafka.commands.kafka_producer.SendMessageCommand",
        "kafka-consumer": "guillotina_kafka.commands.start_consumer.StartConsumerCommand"
    },
    "kafka": {
        "host": "localhost",
        "port": 9092,
        "consumers": {
            "template": "guillotina_kafka.consumers.template.ITemplateConsumer"
        }
    }
}


def includeme(root):
    """
    custom application initialization here
    """
    configure.scan('guillotina_kafka.api')
    configure.scan('guillotina_kafka.install')
