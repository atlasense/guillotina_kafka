from guillotina import configure
# from .util import *  # noqa
# from .interfaces import *  # noqa


app_settings = {
    "commands": {
        "kafka-producer": "guillotina_kafka.commands.kafka_producer.SendMessageCommand",
        "start-consumer": "guillotina_kafka.commands.start_consumer.StartConsumerCommand"
    },
    "kafka": {
        "host": "localhost",
        "port": 9092
    }
}


def includeme(root):
    """
    custom application initialization here
    """
    configure.scan('guillotina_kafka.api')
    configure.scan('guillotina_kafka.install')
