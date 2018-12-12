from guillotina import configure
from .producers import *  # noqa
from .interfaces import *  # noqa
from .utility import *  # noqa


app_settings = {
    "commands": {
        "kafka-producer": "guillotina_kafka.commands.kafka_producer.SendMessageCommand",  # noqa
        "kafka-consumer": "guillotina_kafka.commands.start_consumer.StartConsumerCommand"  # noqa
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
    configure.scan('guillotina_kafka.producers')
    configure.scan('guillotina_kafka.utility')
    configure.scan('guillotina_kafka.api')
