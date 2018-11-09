from guillotina import configure
# from .util import *  # noqa
# from .interfaces import *  # noqa


app_settings = {
    "commands": {
        "kafka-consumer": "guillotina_kafka.commands.consumer.ConsumerCommand"
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
    configure.scan('guillotina_kafka.interfaces')
    configure.scan('guillotina_kafka.consumers.elasticsearch')
    configure.scan('guillotina_kafka.consumers.generic')
    configure.scan('guillotina_kafka.install')
