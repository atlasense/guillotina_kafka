from guillotina import configure
from .util import *  # noqa
from .interfaces import *  # noqa


app_settings = {
    'commands': {
        "kafka-consumer": "guillotina_kafka.commands.consumer.ConsumerCommand"
    }
}


def includeme(root):
    """
    custom application initialization here
    """
    configure.scan('guillotina_kafka.api')
    configure.scan('guillotina_kafka.install')
