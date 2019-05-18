from guillotina import configure
from .interfaces import *  # noqa
from .utilities import *  # noqa


app_settings = {
    "commands": {
        "consumer-stat": "guillotina_kafka.commands.kafka_tools.ConsumerStatCommand",  # noqa
        "start-producer": "guillotina_kafka.commands.kafka_producer.SendMessageCommand",  # noqa
        "start-consumer": "guillotina_kafka.commands.kafka_consumer.StartConsumerCommand",  # noqa
        "start-consumers": "guillotina_kafka.commands.kafka_multi_consumer.StartConsumersCommand"  # noqa
    },
    "kafka": {
        "topic_prefix": "dev-",
        "brokers": [
            "localhost:9092"
        ],
        "producer": {
            "request_timeout_ms": 5000,
            "key_serializer": lambda data: data.encode('utf-8'),
            "value_serializer": lambda data: data.encode('utf-8'),
        },
        "consumer": {
            "workers": [
                {
                    "name": "multi-default",
                    "group": "default",
                    "topics": ["default-topic"],
                    "provide_producer_for": "default-topic",
                    "path": "guillotina_kafka.consumer.multi_default_worker"
                },
                {
                    "name": "multi-es",
                    "group": "es-group",
                    "topics": ["es-topic"],
                    "provide_producer_for": "es-topic",
                    "path": "guillotina_kafka.consumer.multi_es_worker"
                },
                {
                    "name": "default",
                    "group": "default",
                    "topics": ["default-topic"],
                    "path": "guillotina_kafka.consumer.default_worker"
                },
                {
                    "name": "es",
                    "group": "es-group",
                    "topics": ["es-topic"],
                    "path": "guillotina_kafka.consumer.es_worker"
                }
            ],
            "topics": ["test-topic"]
        }
    }
}


def includeme(root):
    """
    custom application initialization here
    """
    configure.scan('guillotina_kafka.api')
    configure.scan('guillotina_kafka.utilities')
    configure.scan('guillotina_kafka.install')
